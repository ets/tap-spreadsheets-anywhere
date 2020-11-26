import csv
import json
import re
from json import JSONDecodeError

import singer

LOGGER = singer.get_logger()

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
        # throw a TypeError if the root json object can not be iterated
        return generator_wrapper(iter(json_array))
    except JSONDecodeError as jde:
        if jde.msg.startswith("Extra data"):
            reader.seek(0)
            json_objects = []
            for jobj in reader:
                json_objects.append(json.loads(jobj))
            print(json_objects)
            return generator_wrapper(json_objects)
        else:
            raise jde




