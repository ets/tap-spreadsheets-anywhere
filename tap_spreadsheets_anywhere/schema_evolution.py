"""Schema evolution utilities for tap-spreadsheets-anywhere."""
import logging
import copy
from typing import Dict, List, Any, Tuple, Optional
from singer.schema import Schema

LOGGER = logging.getLogger(__name__)


class SchemaEvolutionMode:
    """Defines how schema evolution should be handled."""
    STRICT = "strict"  # Fail on schema changes
    MERGE = "merge"    # Merge new fields, widen types
    REPLACE = "replace"  # Replace with new schema
    DISABLED = "disabled"  # No evolution


class SchemaComparator:
    """Utility class for comparing and evolving schemas."""
    
    TYPE_HIERARCHY = {
        'null': 0,
        'boolean': 1,
        'integer': 2,
        'number': 3,
        'string': 4,
        'array': 5,
        'object': 6
    }
    
    @classmethod
    def can_widen_type(cls, from_type: str, to_type: str) -> bool:
        """Check if we can safely widen from_type to to_type."""
        if from_type == to_type:
            return True
        
        # Allow integer -> number
        if from_type == 'integer' and to_type == 'number':
            return True
            
        # Allow any type -> string (safest fallback)
        if to_type == 'string':
            return True
            
        return False
    
    @classmethod
    def merge_field_types(cls, existing_types: List[str], new_types: List[str]) -> List[str]:
        """Merge two sets of field types, preferring wider types."""
        # Start with existing types
        merged = set(existing_types)
        
        for new_type in new_types:
            if new_type == 'null':
                merged.add('null')
                continue
                
            # Check if we can add this type or need to widen
            should_add = True
            types_to_remove = set()
            
            for existing_type in list(merged):
                if existing_type == 'null':
                    continue
                    
                if cls.can_widen_type(existing_type, new_type):
                    # New type is wider, remove existing and add new
                    types_to_remove.add(existing_type)
                elif cls.can_widen_type(new_type, existing_type):
                    # Existing type is wider, don't add new
                    should_add = False
                    break
            
            # Remove narrower types
            merged -= types_to_remove
            
            if should_add:
                merged.add(new_type)
        
        # Ensure 'null' is always included for flexibility
        merged.add('null')
        return sorted(list(merged))
    
    @classmethod
    def compare_schemas(cls, existing_schema: Dict[str, Any], new_schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare two schemas and return evolution information.
        
        Returns:
            Dict with keys: 'added_fields', 'modified_fields', 'removed_fields', 'compatible'
        """
        existing_props = existing_schema.get('properties', {})
        new_props = new_schema.get('properties', {})
        
        added_fields = set(new_props.keys()) - set(existing_props.keys())
        removed_fields = set(existing_props.keys()) - set(new_props.keys())
        common_fields = set(existing_props.keys()) & set(new_props.keys())
        
        modified_fields = {}
        compatible = True
        
        for field in common_fields:
            existing_field = existing_props[field]
            new_field = new_props[field]
            
            existing_types = existing_field.get('type', [])
            new_types = new_field.get('type', [])
            
            # Normalize to lists
            if isinstance(existing_types, str):
                existing_types = [existing_types]
            if isinstance(new_types, str):
                new_types = [new_types]
            
            if existing_types != new_types:
                # Check if types can be merged
                merged_types = cls.merge_field_types(existing_types, new_types)
                modified_fields[field] = {
                    'existing_types': existing_types,
                    'new_types': new_types,
                    'merged_types': merged_types,
                    'compatible': set(merged_types) >= set(existing_types)
                }
                
                if not modified_fields[field]['compatible']:
                    compatible = False
        
        return {
            'added_fields': list(added_fields),
            'modified_fields': modified_fields,
            'removed_fields': list(removed_fields),
            'compatible': compatible and len(removed_fields) == 0
        }
    
    @classmethod
    def evolve_schema(cls, existing_schema: Dict[str, Any], new_schema: Dict[str, Any], 
                     mode: str = SchemaEvolutionMode.MERGE) -> Tuple[Dict[str, Any], bool]:
        """
        Evolve existing schema based on new schema.
        
        Returns:
            Tuple of (evolved_schema, changed)
        """
        if mode == SchemaEvolutionMode.DISABLED:
            return existing_schema, False
        
        if mode == SchemaEvolutionMode.REPLACE:
            return new_schema, existing_schema != new_schema
        
        comparison = cls.compare_schemas(existing_schema, new_schema)
        
        if mode == SchemaEvolutionMode.STRICT:
            if not comparison['compatible'] or comparison['added_fields'] or comparison['removed_fields']:
                raise ValueError(f"Schema evolution not allowed in strict mode. Changes: {comparison}")
            return existing_schema, False
        
        # MERGE mode
        evolved_schema = copy.deepcopy(existing_schema)
        evolved_props = evolved_schema.setdefault('properties', {})
        new_props = new_schema.get('properties', {})
        
        changed = False
        
        # Add new fields
        for field in comparison['added_fields']:
            evolved_props[field] = new_props[field]
            changed = True
            LOGGER.info(f"Added new field to schema: {field}")
        
        # Merge modified fields
        for field, modification in comparison['modified_fields'].items():
            if modification['compatible']:
                evolved_props[field]['type'] = modification['merged_types']
                changed = True
                LOGGER.info(f"Evolved field type: {field} from {modification['existing_types']} to {modification['merged_types']}")
            else:
                LOGGER.warning(f"Cannot safely evolve field {field} from {modification['existing_types']} to {modification['new_types']}")
        
        return evolved_schema, changed


class SchemaEvolutionManager:
    """Manages schema evolution during tap execution."""
    
    def __init__(self, config: Dict[str, Any], state_manager=None):
        self.config = config
        self.schema_cache = {}
        self.state_manager = state_manager
    
    def get_evolution_mode(self, table_spec: Dict[str, Any]) -> str:
        """Get the schema evolution mode for a table."""
        return table_spec.get('schema_evolution_mode', 
                             self.config.get('default_schema_evolution_mode', SchemaEvolutionMode.MERGE))
    
    def should_evolve_during_sync(self, table_spec: Dict[str, Any]) -> bool:
        """Check if schema should be evolved during sync."""
        return table_spec.get('evolve_schema_during_sync', 
                             self.config.get('default_evolve_schema_during_sync', False))
    
    def get_schema_sample_size(self, table_spec: Dict[str, Any]) -> int:
        """Get number of records to sample for schema evolution."""
        return table_spec.get('evolution_sample_size', 
                             self.config.get('default_evolution_sample_size', 100))
    
    def detect_schema_from_records(self, records: List[Dict[str, Any]], table_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Generate schema from a sample of records."""
        from . import conversion
        
        prefer_number_vs_integer = table_spec.get('prefer_number_vs_integer', False)
        prefer_schema_as_string = table_spec.get('prefer_schema_as_string', False)
        
        data_schema = conversion.generate_schema(records, 
                                               prefer_number_vs_integer=prefer_number_vs_integer,
                                               prefer_schema_as_string=prefer_schema_as_string)
        
        return {
            'type': 'object',
            'properties': data_schema
        }
    
    def evolve_schema_if_needed(self, stream_name: str, current_schema: Dict[str, Any], 
                               sample_records: List[Dict[str, Any]], table_spec: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
        """
        Check if schema needs evolution and return evolved schema if needed.
        
        Returns:
            Tuple of (schema, evolved)
        """
        evolution_mode = self.get_evolution_mode(table_spec)
        
        if evolution_mode == SchemaEvolutionMode.DISABLED:
            return current_schema, False
        
        # Detect schema from sample
        detected_schema = self.detect_schema_from_records(sample_records, table_spec)
        
        # Compare and evolve
        try:
            evolved_schema, changed = SchemaComparator.evolve_schema(
                current_schema, detected_schema, evolution_mode
            )
            
            if changed:
                LOGGER.info(f"Schema evolved for stream {stream_name}")
                
                # Record evolution in state manager if available
                if self.state_manager:
                    self.state_manager.record_schema_evolution(
                        stream_name, current_schema, evolved_schema, evolution_mode
                    )
                
                # Cache the evolved schema
                self.schema_cache[stream_name] = evolved_schema
                return evolved_schema, True
            
            return current_schema, False
            
        except ValueError as e:
            LOGGER.error(f"Schema evolution failed for stream {stream_name}: {e}")
            raise e


def validate_schema_evolution_config(table_spec: Dict[str, Any]) -> None:
    """Validate schema evolution configuration for a table."""
    evolution_mode = table_spec.get('schema_evolution_mode', SchemaEvolutionMode.MERGE)
    
    valid_modes = [SchemaEvolutionMode.STRICT, SchemaEvolutionMode.MERGE, 
                   SchemaEvolutionMode.REPLACE, SchemaEvolutionMode.DISABLED]
    
    if evolution_mode not in valid_modes:
        raise ValueError(f"Invalid schema_evolution_mode: {evolution_mode}. Must be one of {valid_modes}")
    
    if table_spec.get('evolve_schema_during_sync', False):
        sample_size = table_spec.get('evolution_sample_size', 100)
        if not isinstance(sample_size, int) or sample_size <= 0:
            raise ValueError(f"evolution_sample_size must be a positive integer, got {sample_size}")