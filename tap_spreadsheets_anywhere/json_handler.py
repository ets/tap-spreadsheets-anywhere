import json
import re
from json import JSONDecodeError
import logging

LOGGER = logging.getLogger(__name__)

def generator_wrapper(root_iterator):
    for obj in root_iterator:
        to_return = {}
        for key, value in obj.items():
            if key is None:
                key = '_smart_extra'

            formatted_key = key
            # remove non-word, non-whitespace characters
            formatted_key = re.sub(r"[^\w\s]", '', formatted_key)
            # replace whitespace with underscores
            formatted_key = re.sub(r"\s+", '_', formatted_key)
            to_return[formatted_key.lower()] = value
        yield to_return


def get_row_iterator(table_spec, reader):
    try:
        json_array = json.load(reader)
        json_path = table_spec.get('json_path', None)
        if json_path is not None:
            json_array = json_array[json_path]

        # throw a TypeError if the root json object can not be iterated
        return generator_wrapper(iter(json_array))
    except JSONDecodeError as jde:
        if jde.msg.startswith("Extra data"):
            reader.seek(0)
            json_objects = []
            for jobj in reader:
                json_objects.append(json.loads(jobj))
            return generator_wrapper(json_objects)
        else:
            raise jde




