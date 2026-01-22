"""Schema persistence and catalog management for schema evolution."""
import json
import os
import hashlib
import logging
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
import singer
from singer.catalog import Catalog

LOGGER = logging.getLogger(__name__)


class SchemaStateManager:
    """Manages persistent schema state across tap runs."""
    
    def __init__(self, config: Dict[str, Any], workspace_path: str = "."):
        self.config = config
        self.workspace_path = workspace_path
        self.state_file = os.path.join(workspace_path, ".tap_schema_state.json")
        self.catalog_cache_file = os.path.join(workspace_path, ".tap_catalog_cache.json")
        self._state = self._load_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """Load schema state from disk."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                LOGGER.warning(f"Could not load schema state from {self.state_file}: {e}")
        
        return {
            'version': '1.0',
            'schemas': {},
            'last_discovery': None,
            'evolution_history': {}
        }
    
    def _save_state(self):
        """Save schema state to disk."""
        try:
            self._state['last_updated'] = datetime.now().isoformat()
            with open(self.state_file, 'w') as f:
                json.dump(self._state, f, indent=2)
            LOGGER.debug(f"Saved schema state to {self.state_file}")
        except IOError as e:
            LOGGER.error(f"Could not save schema state to {self.state_file}: {e}")
    
    def get_schema_hash(self, schema: Dict[str, Any]) -> str:
        """Generate a hash for a schema to detect changes."""
        # Create a normalized representation for hashing
        normalized = json.dumps(schema, sort_keys=True, separators=(',', ':'))
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def should_regenerate_catalog(self, table_specs: list) -> bool:
        """
        Determine if catalog should be regenerated based on:
        1. Config changes
        2. Schema evolution settings
        3. Time since last discovery
        4. Missing cached catalog
        5. Global cache settings
        """
        # Check global cache disable flag
        if self.config.get('disable_catalog_cache', False):
            LOGGER.info("Catalog cache is disabled, will regenerate")
            return True
        
        # Check force regeneration flag
        if self.config.get('force_catalog_regeneration', False):
            LOGGER.info("Catalog regeneration forced by configuration")
            return True
        
        # If no cached catalog exists, regenerate
        if not os.path.exists(self.catalog_cache_file):
            LOGGER.info("No cached catalog found, will regenerate")
            return True
        
        # Check cache TTL
        cache_ttl_hours = self.config.get('schema_cache_ttl_hours', 24)  # Default 24 hours
        try:
            cache_mtime = os.path.getmtime(self.catalog_cache_file)
            cache_age_hours = (datetime.now().timestamp() - cache_mtime) / 3600
            if cache_age_hours > cache_ttl_hours:
                LOGGER.info(f"Catalog cache expired ({cache_age_hours:.1f}h > {cache_ttl_hours}h), will regenerate")
                return True
        except OSError:
            return True
        
        # Check if config has changed
        current_config_hash = self._get_config_hash(table_specs)
        last_config_hash = self._state.get('last_config_hash')
        
        if current_config_hash != last_config_hash:
            LOGGER.info("Configuration changed, will regenerate catalog")
            return True
        
        # Check if any table has schema evolution enabled
        for table_spec in table_specs:
            if table_spec.get('evolve_schema_during_sync', False):
                # For tables with evolution enabled, check more frequently
                evolution_mode = table_spec.get('schema_evolution_mode', 'merge')
                if evolution_mode in ['merge', 'replace']:
                    # Check if we have evolved schemas that differ from catalog
                    stream_name = table_spec['name']
                    if self._has_evolved_schema(stream_name):
                        LOGGER.info(f"Stream {stream_name} has evolved schema, will regenerate catalog")
                        return True
        
        return False
    
    def _get_config_hash(self, table_specs: list) -> str:
        """Generate hash of relevant config portions."""
        config_subset = {
            'tables': [
                {
                    'name': spec['name'],
                    'path': spec['path'],
                    'pattern': spec['pattern'],
                    'format': spec['format'],
                    'schema_evolution_mode': spec.get('schema_evolution_mode', 'disabled'),
                    'schema_overrides': spec.get('schema_overrides', {}),
                    'prefer_number_vs_integer': spec.get('prefer_number_vs_integer', False),
                    'prefer_schema_as_string': spec.get('prefer_schema_as_string', False),
                }
                for spec in table_specs
            ]
        }
        normalized = json.dumps(config_subset, sort_keys=True, separators=(',', ':'))
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def _has_evolved_schema(self, stream_name: str) -> bool:
        """Check if a stream has an evolved schema that differs from cached catalog."""
        if stream_name not in self._state['schemas']:
            return False
        
        evolved_schema = self._state['schemas'][stream_name].get('current_schema')
        if not evolved_schema:
            return False
        
        # Compare with cached catalog
        try:
            with open(self.catalog_cache_file, 'r') as f:
                cached_catalog = json.load(f)
            
            for stream in cached_catalog.get('streams', []):
                if stream['tap_stream_id'] == stream_name:
                    cached_schema = stream['schema']
                    evolved_hash = self.get_schema_hash(evolved_schema)
                    cached_hash = self.get_schema_hash(cached_schema)
                    return evolved_hash != cached_hash
        except (IOError, json.JSONDecodeError, KeyError):
            return True  # If we can't read cache, assume we need to regenerate
        
        return False
    
    def cache_catalog(self, catalog: Catalog, table_specs: list):
        """Cache the catalog and update config hash."""
        try:
            catalog_dict = catalog.to_dict()
            with open(self.catalog_cache_file, 'w') as f:
                json.dump(catalog_dict, f, indent=2)
            
            # Update state with current config hash
            self._state['last_config_hash'] = self._get_config_hash(table_specs)
            self._state['last_discovery'] = datetime.now().isoformat()
            self._save_state()
            
            LOGGER.info(f"Cached catalog with {len(catalog_dict.get('streams', []))} streams")
        except IOError as e:
            LOGGER.error(f"Could not cache catalog: {e}")
    
    def load_cached_catalog(self) -> Optional[Catalog]:
        """Load catalog from cache if available."""
        if not os.path.exists(self.catalog_cache_file):
            return None
        
        try:
            with open(self.catalog_cache_file, 'r') as f:
                catalog_dict = json.load(f)
            
            # Update schemas with any evolved versions
            self._update_catalog_with_evolved_schemas(catalog_dict)
            
            return Catalog.from_dict(catalog_dict)
        except (IOError, json.JSONDecodeError) as e:
            LOGGER.warning(f"Could not load cached catalog: {e}")
            return None
    
    def _update_catalog_with_evolved_schemas(self, catalog_dict: Dict[str, Any]):
        """Update catalog dict with evolved schemas from state."""
        streams = catalog_dict.get('streams', [])
        
        for stream in streams:
            stream_name = stream['tap_stream_id']
            if stream_name in self._state['schemas']:
                evolved_schema = self._state['schemas'][stream_name].get('current_schema')
                if evolved_schema:
                    stream['schema'] = evolved_schema
                    LOGGER.debug(f"Updated catalog with evolved schema for {stream_name}")
    
    def record_schema_evolution(self, stream_name: str, old_schema: Dict[str, Any], 
                              new_schema: Dict[str, Any], evolution_type: str):
        """Record a schema evolution event."""
        if stream_name not in self._state['schemas']:
            self._state['schemas'][stream_name] = {
                'evolution_history': [],
                'current_schema': None,
                'original_schema': old_schema
            }
        
        # Record evolution event
        evolution_event = {
            'timestamp': datetime.now().isoformat(),
            'evolution_type': evolution_type,
            'old_schema_hash': self.get_schema_hash(old_schema),
            'new_schema_hash': self.get_schema_hash(new_schema),
            'changes_summary': self._summarize_schema_changes(old_schema, new_schema)
        }
        
        self._state['schemas'][stream_name]['evolution_history'].append(evolution_event)
        self._state['schemas'][stream_name]['current_schema'] = new_schema
        self._state['schemas'][stream_name]['last_evolved'] = datetime.now().isoformat()
        
        self._save_state()
        
        LOGGER.info(f"Recorded schema evolution for {stream_name}: {evolution_event['changes_summary']}")
    
    def _summarize_schema_changes(self, old_schema: Dict[str, Any], 
                                new_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Create a summary of changes between schemas."""
        old_props = old_schema.get('properties', {})
        new_props = new_schema.get('properties', {})
        
        added_fields = list(set(new_props.keys()) - set(old_props.keys()))
        removed_fields = list(set(old_props.keys()) - set(new_props.keys()))
        modified_fields = []
        
        for field in set(old_props.keys()) & set(new_props.keys()):
            if old_props[field] != new_props[field]:
                modified_fields.append({
                    'field': field,
                    'old_type': old_props[field].get('type'),
                    'new_type': new_props[field].get('type')
                })
        
        return {
            'added_fields': added_fields,
            'removed_fields': removed_fields,
            'modified_fields': modified_fields,
            'total_fields_before': len(old_props),
            'total_fields_after': len(new_props)
        }
    
    def get_schema_evolution_history(self, stream_name: str) -> list:
        """Get evolution history for a stream."""
        if stream_name in self._state['schemas']:
            return self._state['schemas'][stream_name].get('evolution_history', [])
        return []
    
    def cleanup_old_state(self, keep_days: int = 30):
        """Clean up old evolution history entries."""
        cutoff_time = datetime.now().timestamp() - (keep_days * 24 * 3600)
        
        for stream_name in self._state['schemas']:
            history = self._state['schemas'][stream_name]['evolution_history']
            filtered_history = []
            
            for event in history:
                try:
                    event_time = datetime.fromisoformat(event['timestamp']).timestamp()
                    if event_time > cutoff_time:
                        filtered_history.append(event)
                except (ValueError, KeyError):
                    # Keep entries with invalid timestamps
                    filtered_history.append(event)
            
            self._state['schemas'][stream_name]['evolution_history'] = filtered_history
        
        self._save_state()
        LOGGER.debug(f"Cleaned up schema evolution history older than {keep_days} days")
    
    def get_current_schema_version(self, stream_name: str) -> Optional[str]:
        """Get current schema version hash for a stream."""
        if stream_name in self._state['schemas']:
            current_schema = self._state['schemas'][stream_name].get('current_schema')
            if current_schema:
                return self.get_schema_hash(current_schema)
        return None
    
    def force_catalog_regeneration(self):
        """Force regeneration of catalog on next discovery."""
        if os.path.exists(self.catalog_cache_file):
            os.remove(self.catalog_cache_file)
        
        # Clear config hash to force regeneration
        self._state['last_config_hash'] = None
        self._save_state()
        
        LOGGER.info("Forced catalog regeneration")


def create_schema_aware_catalog_manager(config: Dict[str, Any], workspace_path: str = ".") -> SchemaStateManager:
    """Factory function to create a schema state manager."""
    return SchemaStateManager(config, workspace_path)