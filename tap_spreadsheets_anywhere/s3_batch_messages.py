
import logging
import json
import sys
from typing import List, Dict

import singer
from singer.schema import Schema
import tap_spreadsheets_anywhere.file_utils as file_utils

LOGGER = logging.getLogger(__name__)

def format_message(message: Dict) -> str:
    return json.dumps(message)


def write_message(message: Dict):
    sys.stdout.write(format_message(message) + '\n')
    sys.stdout.flush()


def write_batch_message(list_of_files: List[str], stream: Schema) -> None:
    # output custom BATCH type message from Meltano SDK - https://sdk.meltano.com/en/latest/batch.html

    batch_msg = {
        'type': 'BATCH',
        'stream': stream.tap_stream_id,
        'encoding': {
            'format': 'csv',
            'compression': 'none',
        },
        'manifest': list_of_files,
    }
    write_message(batch_msg)


def process_batch(stream: Schema, target_files: List[Dict], table_spec: Dict, config: Dict, state: Dict):
    protocol, source_bucket = file_utils.parse_path(table_spec['path'])
    
    is_s3_protocol = protocol == 's3'
    if not is_s3_protocol:
        LOGGER.error(f'Skipping BATCH processing for stream [{stream.tap_stream_id}] because protocol {protocol} is not supported. Only S3 is supported.')
        return
    
    if not len(target_files):
        LOGGER.info('No files were found')
        return
    
    # TODO: if many files are present we should chunk and output multiple BATCH messages
    batch_msg_files = [f's3://{source_bucket}/{t_file["key"]}' for t_file in target_files]
    write_batch_message(batch_msg_files, stream)
    latest_modified = max(target_files, key=lambda x: x['last_modified'])
    state[stream.tap_stream_id] = {'modified_since': latest_modified['last_modified'].isoformat()}
    singer.write_state(state)
