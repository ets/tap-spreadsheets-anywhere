"""Tests for schema evolution functionality."""
import unittest
from tap_spreadsheets_anywhere.schema_evolution import (
    SchemaComparator, SchemaEvolutionManager, SchemaEvolutionMode,
    validate_schema_evolution_config
)


class TestSchemaComparator(unittest.TestCase):
    def test_can_widen_type(self):
        self.assertTrue(SchemaComparator.can_widen_type('integer', 'integer'))
        self.assertTrue(SchemaComparator.can_widen_type('integer', 'number'))
        self.assertTrue(SchemaComparator.can_widen_type('integer', 'string'))
        self.assertTrue(SchemaComparator.can_widen_type('number', 'string'))
        self.assertFalse(SchemaComparator.can_widen_type('number', 'integer'))
        self.assertFalse(SchemaComparator.can_widen_type('string', 'integer'))

    def test_merge_field_types(self):
        # Same types
        result = SchemaComparator.merge_field_types(['null', 'integer'], ['null', 'integer'])
        self.assertEqual(set(result), {'null', 'integer'})

        # Widening integer to number
        result = SchemaComparator.merge_field_types(['null', 'integer'], ['null', 'number'])
        self.assertEqual(set(result), {'null', 'number'})
        
        # Adding string (fallback)
        result = SchemaComparator.merge_field_types(['null', 'integer'], ['null', 'string'])
        self.assertEqual(set(result), {'null', 'string'})
        
        # Multiple new types
        result = SchemaComparator.merge_field_types(['null', 'integer'], ['null', 'number', 'string'])
        self.assertEqual(set(result), {'null', 'string'})
    
    def test_compare_schemas_added_fields(self):
        existing = {
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']}
            }
        }
        
        new = {
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']},
                'email': {'type': ['null', 'string']}
            }
        }
        
        result = SchemaComparator.compare_schemas(existing, new)
        self.assertEqual(result['added_fields'], ['email'])
        self.assertEqual(result['removed_fields'], [])
        self.assertTrue(result['compatible'])
    
    def test_compare_schemas_modified_fields(self):
        existing = {
            'properties': {
                'id': {'type': ['null', 'integer']},
                'score': {'type': ['null', 'integer']}
            }
        }
        
        new = {
            'properties': {
                'id': {'type': ['null', 'integer']},
                'score': {'type': ['null', 'number']}
            }
        }
        
        result = SchemaComparator.compare_schemas(existing, new)
        self.assertEqual(result['added_fields'], [])
        self.assertEqual(result['removed_fields'], [])
        self.assertIn('score', result['modified_fields'])
        self.assertTrue(result['compatible'])
    
    def test_compare_schemas_incompatible_change(self):
        existing = {
            'properties': {
                'score': {'type': ['null', 'string']}
            }
        }
        
        new = {
            'properties': {
                'score': {'type': ['null', 'integer']}
            }
        }
        
        result = SchemaComparator.compare_schemas(existing, new)
        self.assertIn('score', result['modified_fields'])
        # String to integer is not a safe widening
        self.assertFalse(result['modified_fields']['score']['compatible'])
    
    def test_evolve_schema_merge_mode(self):
        existing = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']}
            }
        }
        
        new = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']},
                'email': {'type': ['null', 'string']},
                'score': {'type': ['null', 'number']}
            }
        }
        
        evolved, changed = SchemaComparator.evolve_schema(existing, new, SchemaEvolutionMode.MERGE)
        
        self.assertTrue(changed)
        self.assertIn('email', evolved['properties'])
        self.assertIn('score', evolved['properties'])
    
    def test_evolve_schema_strict_mode_fails(self):
        existing = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']}
            }
        }
        
        new = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']}
            }
        }
        
        with self.assertRaises(ValueError):
            SchemaComparator.evolve_schema(existing, new, SchemaEvolutionMode.STRICT)
    
    def test_evolve_schema_replace_mode(self):
        existing = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']}
            }
        }
        
        new = {
            'type': 'object',
            'properties': {
                'name': {'type': ['null', 'string']}
            }
        }
        
        evolved, changed = SchemaComparator.evolve_schema(existing, new, SchemaEvolutionMode.REPLACE)
        
        self.assertTrue(changed)
        self.assertEqual(evolved, new)
    
    def test_evolve_schema_disabled_mode(self):
        existing = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']}
            }
        }
        
        new = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']}
            }
        }
        
        evolved, changed = SchemaComparator.evolve_schema(existing, new, SchemaEvolutionMode.DISABLED)
        
        self.assertFalse(changed)
        self.assertEqual(evolved, existing)


class TestSchemaEvolutionManager(unittest.TestCase):
    
    def setUp(self):
        self.config = {
            'default_schema_evolution_mode': 'merge',
            'default_evolve_schema_during_sync': True,
            'default_evolution_sample_size': 50
        }
        self.manager = SchemaEvolutionManager(self.config)
    
    def test_get_evolution_mode_default(self):
        table_spec = {}
        mode = self.manager.get_evolution_mode(table_spec)
        self.assertEqual(mode, 'merge')
    
    def test_get_evolution_mode_override(self):
        table_spec = {'schema_evolution_mode': 'strict'}
        mode = self.manager.get_evolution_mode(table_spec)
        self.assertEqual(mode, 'strict')
    
    def test_should_evolve_during_sync_default(self):
        table_spec = {}
        should_evolve = self.manager.should_evolve_during_sync(table_spec)
        self.assertTrue(should_evolve)
    
    def test_should_evolve_during_sync_override(self):
        table_spec = {'evolve_schema_during_sync': False}
        should_evolve = self.manager.should_evolve_during_sync(table_spec)
        self.assertFalse(should_evolve)
    
    def test_get_schema_sample_size_default(self):
        table_spec = {}
        size = self.manager.get_schema_sample_size(table_spec)
        self.assertEqual(size, 50)
    
    def test_get_schema_sample_size_override(self):
        table_spec = {'evolution_sample_size': 200}
        size = self.manager.get_schema_sample_size(table_spec)
        self.assertEqual(size, 200)
    
    def test_detect_schema_from_records(self):
        records = [
            {'id': '1', 'name': 'Alice', 'score': '95.5'},
            {'id': '2', 'name': 'Bob', 'score': '87.0'},
        ]
        table_spec = {}
        
        schema = self.manager.detect_schema_from_records(records, table_spec)
        
        self.assertIn('properties', schema)
        self.assertIn('id', schema['properties'])
        self.assertIn('name', schema['properties'])
        self.assertIn('score', schema['properties'])


class TestValidateSchemaEvolutionConfig(unittest.TestCase):
    
    def test_valid_config(self):
        table_spec = {
            'schema_evolution_mode': 'merge',
            'evolve_schema_during_sync': True,
            'evolution_sample_size': 100
        }
        # Should not raise
        validate_schema_evolution_config(table_spec)
    
    def test_invalid_evolution_mode(self):
        table_spec = {
            'schema_evolution_mode': 'invalid_mode'
        }
        with self.assertRaises(ValueError):
            validate_schema_evolution_config(table_spec)
    
    def test_invalid_sample_size(self):
        table_spec = {
            'evolve_schema_during_sync': True,
            'evolution_sample_size': -10
        }
        with self.assertRaises(ValueError):
            validate_schema_evolution_config(table_spec)
    
    def test_non_integer_sample_size(self):
        table_spec = {
            'evolve_schema_during_sync': True,
            'evolution_sample_size': '100'
        }
        with self.assertRaises(ValueError):
            validate_schema_evolution_config(table_spec)


if __name__ == '__main__':
    unittest.main()