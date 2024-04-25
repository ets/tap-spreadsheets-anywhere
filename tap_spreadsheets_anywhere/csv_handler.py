import csv
import re
import logging
import sys

LOGGER = logging.getLogger(__name__)

def format_key (key):
    formatted_key = key
    # remove non-word, non-whitespace characters
    formatted_key = re.sub(r"[^\w\s]", '', formatted_key)
    # replace whitespace with underscores
    formatted_key = re.sub(r"\s+", '_', formatted_key)
    formatted_key = formatted_key.lower()
    return formatted_key

def generator_wrapper(reader, table_spec):
    split_edifact_column = table_spec.get('split_edifact_column')
    
    for row in reader:
        to_return = {}
                           
        for key, value in row.items():
            if key is None:
                key = '_smart_extra'
            to_return[format_key(key)] = value
            
        if split_edifact_column in row and len(row[split_edifact_column]) >= table_spec.get('edifact_max_size', 16*1024*1024):
            formatted_key = format_key(key)
            value = row[split_edifact_column]
            
            LOGGER.warning(f"Edifact Value for key {formatted_key} is too large, splitting...")
                
            header = re.search('^(.*?)UNH', value).group(1)
            LOGGER.warning("header: " + header)
                
            trailer = re.search("'(UNZ.*)$", value).group(1)
            LOGGER.warning("trailer: " + trailer)
                
            message_batch_size = table_spec.get('edifact_message_batch_size', 1000)
                
            messages = re.findall("(UNH.*?UNT.*?')", value)
                
            for i in range(0, len(messages), message_batch_size):
                batch = messages[i:i+message_batch_size]
                to_return[formatted_key] = header + ''.join(batch) + trailer
                yield to_return
            LOGGER.warning("Edifact Value split complete, handled " + str(len(messages)) + " edifact messages in " + str(len(range(0, len(messages), message_batch_size))) + " batches of " + str(message_batch_size) + ".")
        else:        
            yield to_return


def get_row_iterator(table_spec, reader):
    field_names = None
    if 'field_names' in table_spec:
        field_names = table_spec['field_names']

    dialect = 'excel'
    if 'delimiter' not in table_spec or table_spec['delimiter'] == 'detect':
        try:
            dialect = csv.Sniffer().sniff(reader.readline(), delimiters=[',', '\t', ';', ' ', ':', '|', ' '])
            if reader.seekable():
                reader.seek(0)
        except Exception as err:
            raise ValueError("Unable to sniff a delimiter")
    else:
        custom_delimiter = table_spec.get('delimiter', ',')
        custom_quotechar = table_spec.get('quotechar', '"')
        if custom_delimiter != ',' or custom_quotechar != '"':
            class custom_dialect(csv.excel):
                delimiter = custom_delimiter
                quotechar = custom_quotechar
            dialect = 'custom_dialect'
            csv.register_dialect(dialect, custom_dialect)

    reader = csv.DictReader(reader, fieldnames=field_names, dialect=dialect)
    csv.field_size_limit(sys.maxsize)
    return generator_wrapper(reader, table_spec)
