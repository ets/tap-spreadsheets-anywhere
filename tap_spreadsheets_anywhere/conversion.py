import dateutil
import pytz
import singer

LOGGER = singer.get_logger()


def convert_row(row, schema):
    to_return = {}
    for key, value in row.items():
        if key in schema['properties']:
            field_schema = schema['properties'][key]
            declared_types = field_schema.get('type', 'string')
        else:
            declared_types = ['string','null']

        LOGGER.debug('Converting {} value {} to {}'.format(key, value, declared_types))
        coerced = coerce(value, declared_types)
        to_return[key] = coerced

    return to_return

def coerce(datum,declared_types):
    if datum is None or datum == '':
        return None

    desired_type = declared_types
    if isinstance(declared_types, list):
        if "null" in declared_types:
            declared_types.remove("null")
        desired_type = declared_types[0]

    coerced, _ = convert(datum, desired_type)
    return coerced


def convert(datum, desired_type=None):
    """
    Returns tuple of (converted_data_point, json_schema_type,).
    """
    if datum is None or datum == '':
        return None, None,

    if desired_type in (None, 'integer'):
        try:
            datum_int = int(datum)  # Confirm it can be coerced to int
            if not datum.lstrip("-+").isdigit():
                raise TypeError
            return datum_int, 'integer',
        except (ValueError, TypeError):
            pass

    if desired_type in (None, 'number'):
        try:
            datum_float = float(datum)
            return datum_float, 'number',
        except (ValueError, TypeError):
            pass

    if desired_type == 'date-time':
        try:
            to_return = dateutil.parser.parse(datum)

            if (to_return.tzinfo is None or
                    to_return.tzinfo.utcoffset(to_return) is None):
                to_return = to_return.replace(tzinfo=pytz.utc)

            return to_return.isoformat(), 'date-time',
        except (ValueError, TypeError):
            pass

    return str(datum), 'string',


def count_sample(sample, start=None):
    if start is None:
        start = {}

    for key, value in sample.items():
        if key not in start:
            start[key] = {}

        (_, datatype) = convert(value)
        if datatype is not None:
            start[key][datatype] = start[key].get(datatype, 0) + 1

    return start


def count_samples(samples):
    to_return = {}

    for sample in samples:
        to_return = count_sample(sample, to_return)

    return to_return


def pick_datatype(counts,prefer_number_vs_integer=False):
    """
    If the underlying records are ONLY of type `integer`, `number`,
    or `date-time`, then return that datatype.

    If the underlying records are of type `integer` and `number` only,
    return `number`.

    Otherwise return `string`.
    """
    to_return = 'string'

    if len(counts) == 1:
        if counts.get('integer', 0) > 0:
            to_return = 'number' if prefer_number_vs_integer else 'integer'
        elif counts.get('number', 0) > 0:
            to_return = 'number'

    elif (len(counts) == 2 and
          counts.get('integer', 0) > 0 and
          counts.get('number', 0) > 0):
        to_return = 'number'

    return to_return


def generate_schema(samples,prefer_number_vs_integer=False):
    to_return = {}
    counts = count_samples(samples)

    for key, value in counts.items():
        datatype = pick_datatype(value,prefer_number_vs_integer)
        # if "survey_responses_count" == key:
        #     LOGGER.error(f"Key '{key}' held {value} and was typed as {datatype} with prefer_number_vs_integer={prefer_number_vs_integer}")

        if datatype == 'date-time':
            to_return[key] = {
                'type': ['null', 'string'],
                'format': 'date-time',
            }
        else:
            to_return[key] = {
                'type': ['null', datatype],
            }

    return to_return
