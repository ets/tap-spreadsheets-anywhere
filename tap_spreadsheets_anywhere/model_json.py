import os
import io
import json

from functools import lru_cache

from azure.storage.blob import BlobServiceClient

optionset_names = {
    'GlobalOptionsetMetadata',
    'OptionsetMetadata',
    'StatusMetadata',
    'StateMetadata',
}

def parse_path(path):
    path_parts = path.split('://', 1)
    return ('local', path_parts[0]) if len(path_parts) <= 1 else (path_parts[0], path_parts[1])

@lru_cache()
def _get_json_from_azure(container_name):
    sas_key = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    blob_service_client = BlobServiceClient.from_connection_string(sas_key)
    container_client = blob_service_client.get_container_client(container_name)
    blob = list(container_client.list_blobs(name_starts_with='model.json'))[0]
    data = io.BytesIO(container_client.download_blob(blob).readall())
    data = json.loads(data.read().decode('utf-8'))
    return data

def get_attribute_json(container_name) -> dict:
    data = _get_json_from_azure(container_name)
    return {e['name']: e['attributes'] for e in data['entities']}

def get_annotations_json(container_name) -> dict:
    data = _get_json_from_azure(container_name)
    return {e['name']: e['annotations'] for e in data['entities']}


def get_file_pattern(table_spec: dict):
    table_name = table_spec['name']
    _, bucket = parse_path(table_spec['path'])
    if table_name in optionset_names:
        regex = f"^OptionsetMetadata/{table_name}.csv$"
    else:
        annotations = get_annotations_json(bucket)[table_name]
        [partitioning_type] = [v['value'] for v in annotations if v['name'] == 'Athena:PartitionGranularity']
        if partitioning_type == 'Month':
            regex =  f'^{table_name}/[0-9]' + '{1}.*.csv$'  # e.g. "^team/[0-9]{1}.*.csv$"
        elif partitioning_type == 'Year':
            regex =  f'^{table_name}/[0-9]' + '{1}.*.csv$'  # e.g. "^team/[0-9]{1}.*.csv$"
    return regex



def get_table_schema(table_spec):
    _, bucket = parse_path(table_spec['path'])
    model_json = get_attribute_json(bucket)
    entity_schema = model_json[table_spec['name']]

    mapping = {
        'guid': {'type': ['null', 'string']},
        'dateTime': {"type": ['null', "string"],"format": "date-time"},
        'int64': {'type': ['null', 'integer']},
        'string': {'type': ['null', 'string']},
        'dateTimeOffset': {"type": ['null', "string"],"format": "date-time"},
        'double': {'type': ['null', 'number']},
        'boolean': {'type': ['null', 'boolean']},
        'decimal': {'type': ['null', 'number']},
    }

        #   need to apply this in the case that the field is a primary key, this is not permitted to be null.
    id_type = {'type': ['string']}

    entity_schema = {
        e['name'].lower(): id_type if e['name'].lower() == 'id' else mapping[e['dataType']]
        for e in entity_schema
    }

    return entity_schema

def get_table_headers(table_spec):
    _, bucket = parse_path(table_spec['path'])
    model_json = get_attribute_json(bucket)
    entity_schema = model_json[table_spec['name']]
    return [e['name'].lower() for e in entity_schema]