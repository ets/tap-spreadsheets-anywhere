import json
import logging
import re
from json import JSONDecodeError

LOGGER = logging.getLogger(__name__)


def generator_wrapper(root_iterator, table_spec):
    for obj in root_iterator:
        json_obj = json.loads(obj)
        if table_spec.get("skip_empty_rows", False) and all(value == None or value == "" for value in obj.values()):
            continue
        to_return = {}
        for key, value in json_obj.items():
            if key is None:
                key = "_smart_extra"

            formatted_key = key
            # remove non-word, non-whitespace characters
            formatted_key = re.sub(r"[^\w\s]", "", formatted_key)
            # replace whitespace with underscores
            formatted_key = re.sub(r"\s+", "_", formatted_key)

            # preserve mixed casing
            if formatted_key.isupper():
                formatted_key = formatted_key.lower()

            to_return[formatted_key] = value
        yield to_return


def get_row_iterator(table_spec, reader):
    try:
        return generator_wrapper(iter(reader), table_spec)
    except JSONDecodeError as jde:
        if jde.msg.startswith("Extra data"):
            reader.seek(0)
            json_objects = []
            for jobj in reader:
                json_objects.append(json.loads(jobj))
            return generator_wrapper(json_objects, table_spec)
        else:
            raise
