"""Tests for schema persistence and catalog caching functionality."""
import unittest
import tempfile
import os
import json
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from tap_spreadsheets_anywhere.schema_persistence import SchemaStateManager
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema


class TestSchemaStateManager(unittest.TestCase):
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config = {
            'tables': [
                {
                    'name': 'test_stream',
                    'path': 's3://bucket/path',
                    'pattern': '*.csv',
                    'format': 'csv',
                    'schema_evolution_mode': 'merge',
                    'evolve_schema_during_sync': True
                }
            ]
        }
        self.manager = SchemaStateManager(self.config, self.temp_dir)
    
    def tearDown(self):
        # Clean up temp files
        for file in [self.manager.state_file, self.manager.catalog_cache_file]:
            if os.path.exists(file):
                os.remove(file)
        os.rmdir(self.temp_dir)
    
    def test_initial_state_creation(self):
        """Test that initial state is created correctly."""
        state = self.manager._state
        self.assertEqual(state['version'], '1.0')
        self.assertEqual(state['schemas'], {})
        self.assertIsNone(state['last_discovery'])
    
    def test_schema_hash_generation(self):
        """Test schema hash generation for change detection."""
        schema1 = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']}
            }
        }
        
        schema2 = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']}
            }
        }
        
        schema3 = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']},
                'email': {'type': ['null', 'string']}
            }
        }
        
        hash1 = self.manager.get_schema_hash(schema1)
        hash2 = self.manager.get_schema_hash(schema2)
        hash3 = self.manager.get_schema_hash(schema3)
        
        self.assertEqual(hash1, hash2)  # Same schemas should have same hash
        self.assertNotEqual(hash1, hash3)  # Different schemas should have different hashes
    
    def test_should_regenerate_catalog_no_cache(self):
        """Test catalog regeneration when no cache exists."""
        should_regenerate = self.manager.should_regenerate_catalog(self.config['tables'])
        self.assertTrue(should_regenerate)
    
    def test_should_regenerate_catalog_cache_disabled(self):
        """Test catalog regeneration when cache is disabled."""
        self.config['disable_catalog_cache'] = True
        manager = SchemaStateManager(self.config, self.temp_dir)
        
        should_regenerate = manager.should_regenerate_catalog(self.config['tables'])
        self.assertTrue(should_regenerate)
    
    def test_should_regenerate_catalog_force_flag(self):
        """Test catalog regeneration when force flag is set."""
        self.config['force_catalog_regeneration'] = True
        manager = SchemaStateManager(self.config, self.temp_dir)
        
        should_regenerate = manager.should_regenerate_catalog(self.config['tables'])
        self.assertTrue(should_regenerate)
    
    @patch('os.path.getmtime')
    def test_should_regenerate_catalog_ttl_expired(self, mock_getmtime):
        """Test catalog regeneration when TTL has expired."""
        # Create a fake catalog cache file
        with open(self.manager.catalog_cache_file, 'w') as f:
            json.dump({'streams': []}, f)
        
        # Set cache to be older than TTL
        old_time = (datetime.now() - timedelta(hours=25)).timestamp()
        mock_getmtime.return_value = old_time
        
        self.config['schema_cache_ttl_hours'] = 24
        manager = SchemaStateManager(self.config, self.temp_dir)
        
        should_regenerate = manager.should_regenerate_catalog(self.config['tables'])
        self.assertTrue(should_regenerate)
    
    def test_config_hash_changes(self):
        """Test that config changes trigger catalog regeneration."""
        # Create initial state
        initial_hash = self.manager._get_config_hash(self.config['tables'])
        self.manager._state['last_config_hash'] = initial_hash
        
        # Create a fake catalog cache
        with open(self.manager.catalog_cache_file, 'w') as f:
            json.dump({'streams': []}, f)
        
        # First check - should not regenerate
        should_regenerate = self.manager.should_regenerate_catalog(self.config['tables'])
        self.assertFalse(should_regenerate)
        
        # Change config
        self.config['tables'][0]['schema_evolution_mode'] = 'strict'
        
        # Second check - should regenerate due to config change
        should_regenerate = self.manager.should_regenerate_catalog(self.config['tables'])
        self.assertTrue(should_regenerate)
    
    def test_record_schema_evolution(self):
        """Test recording schema evolution events."""
        old_schema = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']}
            }
        }
        
        new_schema = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']},
                'email': {'type': ['null', 'string']}
            }
        }
        
        self.manager.record_schema_evolution('test_stream', old_schema, new_schema, 'merge')
        
        # Check that evolution was recorded
        self.assertIn('test_stream', self.manager._state['schemas'])
        stream_state = self.manager._state['schemas']['test_stream']
        
        self.assertEqual(stream_state['current_schema'], new_schema)
        self.assertEqual(stream_state['original_schema'], old_schema)
        self.assertEqual(len(stream_state['evolution_history']), 1)
        
        evolution_event = stream_state['evolution_history'][0]
        self.assertEqual(evolution_event['evolution_type'], 'merge')
        self.assertEqual(evolution_event['changes_summary']['added_fields'], ['email'])
    
    def test_catalog_caching_and_loading(self):
        """Test catalog caching and loading functionality."""
        # Create a test catalog
        schema_dict = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']}
            }
        }
        
        catalog_entry = CatalogEntry(
            tap_stream_id='test_stream',
            stream='test_stream',
            schema=Schema.from_dict(schema_dict),
            key_properties=['id'],
            metadata=[],
            replication_key=None,
            is_view=None,
            database=None,
            table=None,
            row_count=None,
            stream_alias=None,
            replication_method=None
        )
        
        catalog = Catalog([catalog_entry])
        
        # Cache the catalog
        self.manager.cache_catalog(catalog, self.config['tables'])
        
        # Verify cache file was created
        self.assertTrue(os.path.exists(self.manager.catalog_cache_file))
        
        # Load the cached catalog
        loaded_catalog = self.manager.load_cached_catalog()
        
        self.assertIsNotNone(loaded_catalog)
        self.assertEqual(len(loaded_catalog.streams), 1)
        self.assertEqual(loaded_catalog.streams[0].tap_stream_id, 'test_stream')
    
    def test_catalog_loading_with_evolved_schemas(self):
        """Test that cached catalog is updated with evolved schemas."""
        # Create initial catalog
        original_schema = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']}
            }
        }
        
        evolved_schema = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']},
                'email': {'type': ['null', 'string']}
            }
        }
        
        # Cache catalog with original schema
        catalog_dict = {
            'streams': [{
                'tap_stream_id': 'test_stream',
                'stream': 'test_stream',
                'schema': original_schema,
                'key_properties': ['id'],
                'metadata': []
            }]
        }
        
        with open(self.manager.catalog_cache_file, 'w') as f:
            json.dump(catalog_dict, f)
        
        # Record schema evolution
        self.manager.record_schema_evolution('test_stream', original_schema, evolved_schema, 'merge')
        
        # Load catalog - should have evolved schema
        loaded_catalog = self.manager.load_cached_catalog()
        
        loaded_schema = loaded_catalog.streams[0].schema.to_dict()
        self.assertEqual(loaded_schema, evolved_schema)
        self.assertIn('email', loaded_schema['properties'])
    
    def test_schema_evolution_history_cleanup(self):
        """Test cleanup of old schema evolution history."""
        # Record multiple evolution events
        base_schema = {
            'type': 'object',
            'properties': {'id': {'type': ['null', 'integer']}}
        }
        
        for i in range(5):
            new_schema = {
                'type': 'object',
                'properties': {
                    'id': {'type': ['null', 'integer']},
                    f'field_{i}': {'type': ['null', 'string']}
                }
            }
            self.manager.record_schema_evolution('test_stream', base_schema, new_schema, 'merge')
        
        # Manually set old timestamps
        history = self.manager._state['schemas']['test_stream']['evolution_history']
        old_time = (datetime.now() - timedelta(days=35)).isoformat()
        history[0]['timestamp'] = old_time
        history[1]['timestamp'] = old_time
        
        # Run cleanup
        self.manager.cleanup_old_state(keep_days=30)
        
        # Check that old entries were removed
        updated_history = self.manager._state['schemas']['test_stream']['evolution_history']
        self.assertEqual(len(updated_history), 3)  # Should keep 3 recent entries
    
    def test_force_catalog_regeneration(self):
        """Test forcing catalog regeneration."""
        # Create cache files
        with open(self.manager.catalog_cache_file, 'w') as f:
            json.dump({'streams': []}, f)
        
        self.assertTrue(os.path.exists(self.manager.catalog_cache_file))
        
        # Force regeneration
        self.manager.force_catalog_regeneration()
        
        # Cache file should be removed and config hash cleared
        self.assertFalse(os.path.exists(self.manager.catalog_cache_file))
        self.assertIsNone(self.manager._state.get('last_config_hash'))
    
    def test_get_current_schema_version(self):
        """Test getting current schema version."""
        # Initially no version
        version = self.manager.get_current_schema_version('test_stream')
        self.assertIsNone(version)
        
        # Record evolution
        schema = {
            'type': 'object',
            'properties': {'id': {'type': ['null', 'integer']}}
        }
        
        new_schema = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']}
            }
        }
        
        self.manager.record_schema_evolution('test_stream', schema, new_schema, 'merge')
        
        # Should now have version
        version = self.manager.get_current_schema_version('test_stream')
        self.assertIsNotNone(version)
        self.assertEqual(version, self.manager.get_schema_hash(new_schema))


if __name__ == '__main__':
    unittest.main()