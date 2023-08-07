'''Provides an object model for a our config file'''
import json
import logging

from voluptuous import Schema, Required, Any, Optional
LOGGER = logging.getLogger(__name__)

CONFIG_CONTRACT = Schema({
    Required('path'): str,
    Required('tables'): [{
        Optional('pattern'): str,
        Optional('delimiter'): str,
        Required('name'): str,
        Required('key_properties'): [str],
        Optional('encoding'): str,
        Optional('invalid_format_action'): Any('ignore','fail'),
        Optional('universal_newlines'): bool,
        Optional('skip_initial'): int,
        Optional('worksheet_name'): str,
        Optional('quotechar'): str,
        Optional('json_path'): str,
        Optional('sample_rate'): int,
        Optional('max_sampling_read'): int,
        Optional('max_records_per_run'): int,
        Optional('max_sampled_files'): int,
        Optional('prefer_number_vs_integer'): bool,
        Optional('full_table_replace'): bool,
        Optional('prefer_schema_as_string'): bool,
        Optional('schema_overrides'): {
            str: {
                Required('type'): Any(Any('null','string','integer','number','date-time'),
                                      [Any('null','string','integer','number','date-time')])
            }
        }
    }]
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
