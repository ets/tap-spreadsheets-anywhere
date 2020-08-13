import json
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
        Optional('schema_overrides'): {
            str: {
                Required('type'): Any(str, [str]),
                Required('_conversion_type'): Any('string',
                                                  'integer',
                                                  'number',
                                                  'date-time')
            }
        }
    }]
})


def load(filename):
    try:
        with open(filename) as handle:
            config = json.load(handle)
    except:
        logger.fatal("Failed to decode config file. Is it valid json?")
        raise RuntimeError

    CONFIG_CONTRACT(config)
    return config
