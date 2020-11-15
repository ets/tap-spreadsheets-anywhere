import csv
import json
import re
import singer

LOGGER = singer.get_logger()

def generator_wrapper(root_array):
    for row in root_array:
        to_return = {}
        for key, value in row.items():
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
    root_array = json.load(reader)
    return generator_wrapper(root_array)
