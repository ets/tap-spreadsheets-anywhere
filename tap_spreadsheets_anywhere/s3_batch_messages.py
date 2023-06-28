
import logging
import json
import sys
from typing import List, Dict

import boto3
from botocore.exceptions import BotoCoreError

import singer
from singer.schema import Schema
import tap_spreadsheets_anywhere.file_utils as file_utils

LOGGER = logging.getLogger(__name__)

def format_message(message: Dict) -> str:
    return json.dumps(message)


def write_message(message: Dict):
    sys.stdout.write(format_message(message) + '\n')
    sys.stdout.flush()


def _copy_file(s3, src_bucket: str, dest_bucket: str, key: str, new_key: str, multipart_threshold=5*1024**3):
    """
    AWS S3 threshold for a single object copy is 5GB therefore the default value for multipart_threshold is also 5GB.
    """
    copy_source = {
        'Bucket': src_bucket,
        'Key': key
    }
    
    try:
        metadata = s3.head_object(Bucket=src_bucket, Key=key)

        # If the source object size is greater than or equal to multipart_threshold,
        # use multipart copy
        if metadata['ContentLength'] >= multipart_threshold:
            multipart_upload = s3.create_multipart_upload(Bucket=dest_bucket, Key=new_key)

            position = 0
            part_number = 1
            parts = []

            while position < metadata['ContentLength']:
                copy_range = f'bytes={position}-{min(position + multipart_threshold - 1, metadata["ContentLength"] - 1)}'

                copy_part = s3.upload_part_copy(
                    Bucket=dest_bucket,
                    Key=new_key,
                    PartNumber=part_number,
                    UploadId=multipart_upload['UploadId'],
                    CopySource=copy_source,
                    CopySourceRange=copy_range,
                )

                parts.append({
                    'PartNumber': part_number,
                    'ETag': copy_part['CopyPartResult']['ETag']
                })

                position += multipart_threshold
                part_number += 1

            s3.complete_multipart_upload(
                Bucket=dest_bucket,
                Key=new_key,
                UploadId=multipart_upload['UploadId'],
                MultipartUpload={'Parts': parts}
            )
        else:
            s3.copy(copy_source, dest_bucket, new_key)
    except BotoCoreError as e:
        LOGGER.error(f'Error during S3 file copying: {e}')


def copy_files_to_stage(config: Dict, target_files: List[Dict], source_bucket: str, destination_bucket: str) -> None:
    s3_arn_role = config.get('s3_arn_role')
    
    client = boto3.client('sts')
    credentials = client.assume_role(RoleArn=s3_arn_role,
                                     RoleSessionName='SourceToStageELT')['Credentials']

    s3_client = boto3.resource(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    ).meta.client
    
    for tfile in target_files:
        key = tfile['key']
        _copy_file(s3_client, source_bucket, destination_bucket, key, key)
    

def write_batch_message(list_of_files: List[str], stream: Schema) -> None:
    # output custom BATCH type message as specified in Meltano SDK - https://sdk.meltano.com/en/latest/batch.html

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

    s3_stage_bucket = config.get('s3_stage_bucket')
    if s3_stage_bucket is None:
        LOGGER.error('No S3 stage bucket path found.')
        return
    
    if not len(target_files):
        LOGGER.info('No files were found')
        return

    if s3_stage_bucket != source_bucket:
        LOGGER.info(f'Copying files from {source_bucket} to {s3_stage_bucket} stage bucket.')
        copy_files_to_stage(config, target_files, source_bucket, s3_stage_bucket)
        
    # TODO: if many files are present we should chunk and output multiple BATCH messages
    batch_msg_files = [f's3://{s3_stage_bucket}/{t_file["key"]}' for t_file in target_files]
    write_batch_message(batch_msg_files, stream)

    latest_modified = max(target_files, key=lambda x: x['last_modified'])
    state[stream.tap_stream_id] = {'modified_since': latest_modified['last_modified'].isoformat()}
    singer.write_state(state)
