from enum import Enum, auto
import os
import io
import json

from functools import lru_cache
from typing import Dict, List

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

def get_model_json(container_name) -> dict:
    data = _get_json_from_azure(container_name)
    return {e['name']: e['attributes'] for e in data['entities']}


def get_table_schema(table_spec):
    _, bucket = parse_path(table_spec['path'])
    model_json = get_model_json(bucket)
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
    model_json = get_model_json(bucket)
    entity_schema = model_json[table_spec['name']]
    return [e['name'].lower() for e in entity_schema]

def get_annotations_json(container_name) -> dict:
    data = _get_json_from_azure(container_name)
    return {e['name']: e['annotations'] for e in data['entities']}

def get_file_pattern(table_spec: dict) -> str:
    """
    Either return regex for the snapshot csv files, or the appropriate optionset path to csv files"""
    table_name = table_spec['name']
    if table_name in optionset_names:
        regex = f"^OptionsetMetadata/{table_name}.csv$"
    else:
        regex = f'^{table_name}\/Snapshot\/.*.csv$'
    return regex


def get_mapping_specs() -> List[Dict]:
    return [
        {
                "name": "StateMetadata",
                "key_properties": [
                    "entityname",
                    "option"
                ],
                "schema_overrides": {
                    "entityname": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "option": {
                        "type": [
                            "integer"
                        ]
                    },
                    "isuserlocalizedlabel": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "localizedlabellanguagecode": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "localizedlabel": {
                        "type": [
                            "null",
                            "string"
                        ]
                    }
                }
            },
            {
                "name": "StatusMetadata",
                "key_properties": [
                    "entityname",
                    "customoption"
                ],
                "schema_overrides": {
                    "entityname": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "option": {
                        "type": [
                            "integer"
                        ]
                    },
                    "customoption": {
                        "type": [
                            "integer"
                        ]
                    },
                    "isuserlocalizedlabel": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "localizedlabellanguagecode": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "localizedlabel": {
                        "type": [
                            "null",
                            "string"
                        ]
                    }
                }
            },
            {
                "name": "OptionsetMetadata",
                "key_properties": [
                    "entityname",
                    "optionsetname",
                    "option"
                ],
                "schema_overrides": {
                    "entityname": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "optionsetname": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "option": {
                        "type": [
                            "integer"
                        ]
                    },
                    "isuserlocalizedlabel": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "localizedlabellanguagecode": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "localizedlabel": {
                        "type": [
                            "null",
                            "string"
                        ]
                    }
                }
            },
            {
                "name": "GlobalOptionsetMetadata",
                "key_properties": [
                    "globaloptionsettable",
                    "globaloptionsetname",
                    "option"
                ],
                "schema_overrides": {
                    "attributename": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "option": {
                        "type": [
                            "integer"
                        ]
                    },
                    "isuserlocalizedlabel": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "localizedlabellanguagecode": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "localizedlabel": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "globaloptionsetname": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "globaloptionsettable": {
                        "type": [
                            "null",
                            "string"
                        ]
                    }
                }
            },
    ]


class SyncType(Enum):
    append = auto()
    insert = auto()


def generate_table_spec(table_name: str, sync_type: SyncType) -> Dict:
    if sync_type is SyncType.insert:
        keys = ['id']
    if sync_type is SyncType.append:
        keys = ['id', 'versionnumber']

    return  {
        "name": table_name,
        "key_properties": keys,
    }


def generate_tables_config(tables: dict) -> List[Dict]:
    config = []
    if inserts := tables.get('insert'):
        insert_specs = [generate_table_spec(t, SyncType.insert)for t in inserts]
        config.extend(insert_specs)
    if appends := tables.get('append'):
        append_specs = [generate_table_spec(t, SyncType.append)for t in appends]
        config.extend(append_specs)
    config.extend(get_mapping_specs())
    return config