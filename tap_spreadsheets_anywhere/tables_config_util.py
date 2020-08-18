import json
from json.decoder import JSONDecodeError

import singer

from voluptuous import Schema, Required, Any, Optional
logger = singer.get_logger()

CONFIG_CONTRACT = Schema({
    Required('tables'): [{
        Required('path'): str,
        Required('name'): str,
        Required('pattern'): str,
        Required('start_date'): str,
        Required('key_properties'): [str],
        Required('format'): Any('csv', 'excel'),
        Optional('universal_newlines'): bool,
        Optional('selected'): bool,
        Optional('field_names'): [str],
        Optional('search_prefix'): str,
        Optional('worksheet_name'): str,
        Optional('delimiter'): str,
        Optional('quotechar'): str,
        Optional('sample_rate'): int,
        Optional('max_sampling_read'): int,
        Optional('max_sampled_files'): int,
        Optional('prefer_number_vs_integer'): bool,
        Optional('schema_overrides'): {
            str: {
                Required('type'): Any(Any('null','string','integer','number','date-time'),
                                      [Any('null','string','integer','number','date-time')])
            }
        }
    }]
})


def validate(config_json):
    CONFIG_CONTRACT(config_json)
    return config_json
