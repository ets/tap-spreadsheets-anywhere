'''Provides an object model for a our config file'''
import json
import logging

from voluptuous import Schema, Required, Any, Optional
LOGGER = logging.getLogger(__name__)

CONFIG_CONTRACT = Schema({
    Required('tables'): [{
        Required('path'): str,
        Required('name'): str,
        Required('pattern'): str,
        Required('start_date'): str,
        Required('key_properties'): [str],
        Required('format'): Any('csv', 'excel', 'json', 'jsonl', 'parquet', 'detect'),
        Optional('encoding'): str,
        Optional('invalid_format_action'): Any('ignore','fail'),
        Optional('universal_newlines'): bool,
        Optional('skip_initial'): int,
        Optional('selected'): bool,
        Optional('field_names'): [str],
        Optional('search_prefix'): str,
        Optional('worksheet_name'): str,
        Optional('delimiter'): str,
        Optional('quotechar'): str,
        Optional('json_path'): str,
        Optional('sample_rate'): int,
        Optional('max_sampling_read'): int,
        Optional('max_records_per_run'): int,
        Optional('max_sampled_files'): int,
        Optional('prefer_number_vs_integer'): bool,
        Optional('prefer_schema_as_string'): bool,
        Optional('schema_overrides'): {
            str: {
                Required('type'): Any(Any('null','string','integer','number','date-time','object'),
                                      [Any('null','string','integer','number','date-time','object')])
            }
        },
        Optional('schema_evolution_mode'): Any('strict', 'merge', 'replace', 'disabled'),
        Optional('evolve_schema_during_sync'): bool,
        Optional('evolution_sample_size'): int,
        Optional('schema_version'): str
    }],
    # Global schema evolution and caching settings
    Optional('default_schema_evolution_mode'): Any('strict', 'merge', 'replace', 'disabled'),
    Optional('default_evolve_schema_during_sync'): bool,
    Optional('default_evolution_sample_size'): int,
    Optional('disable_catalog_cache'): bool,
    Optional('force_catalog_regeneration'): bool,
    Optional('schema_cache_ttl_hours'): int
})

class Config():

    @classmethod
    def dump(cls, config_json, ostream):
        json.dump(config_json, ostream, indent=2)

    @classmethod
    def validate(cls, config_json):
        CONFIG_CONTRACT(config_json)
        return config_json

    @classmethod
    def load(cls, filename):
        with open(filename) as fp:  # pylint: disable=invalid-name
            return Config.validate(json.load(fp))
