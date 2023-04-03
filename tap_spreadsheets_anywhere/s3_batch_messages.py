
import logging
import tempfile
import time
import json
import sys
import csv
from typing import List, Dict

import boto3
import singer
from singer.schema import Schema
import tap_spreadsheets_anywhere.file_utils as file_utils

LOGGER = logging.getLogger(__name__)

def format_message(message: Dict) -> str:
    return json.dumps(message)


def write_message(message: Dict):
    sys.stdout.write(format_message(message) + '\n')
    sys.stdout.flush()


# def copy_files_to_stage():
#     s3_arn_role = config.get('s3_arn_role')
#     if s3_arn_role is None:
#         LOGGER.warn('No S3 arn role found.')
#         return
    
#     s3_manifest_job = f'{stream.tap_stream_id}_manifesto.csv'
#     s3 = boto3.client('s3')

#     # create S3 batch job manifesto
#     with tempfile.NamedTemporaryFile(mode='w', newline='') as temp_file:
#         writer = csv.writer(temp_file)
#         for t_file in target_files:
#             writer.writerow([source_bucket, t_file['key']])
#         temp_file.flush()

#         s3.upload_file(temp_file.name, s3_stage_bucket, s3_manifest_job)

#         manifest_metadata = s3.head_object(Bucket=s3_stage_bucket, Key=s3_manifest_job)
#         manifest_etag = manifest_metadata['ETag']

#     sts = boto3.client('sts')
#     response = sts.get_caller_identity()
#     account_id = response['Account']

#     s3_control = boto3.client('s3control', region_name='us-east-1')

#     response = s3_control.create_job(
#         AccountId=account_id,
#         ConfirmationRequired=False,
#         Operation={
#             'S3PutObjectCopy': {
#                 'TargetResource': f'arn:aws:s3:::{s3_stage_bucket}',
#                 'TargetKeyPrefix': stream.tap_stream_id,
#             },
#         },
#         Report={
#             #'Bucket': f'arn:aws:s3:::{s3_stage_bucket}',
#             #'Format': 'Report_CSV_20180820',
#             'Enabled': False,
#             #'Prefix': 'reports',
#             #'ReportScope': 'AllTasks'
#         },
#         Manifest={
#             'Spec': {
#                 'Format': 'S3BatchOperations_CSV_20180820',
#                 'Fields': ['Bucket', 'Key'],
#             },
#             'Location': {
#                 'ObjectArn': f'arn:aws:s3:::{s3_stage_bucket}/{s3_manifest_job}',
#                 'ETag': manifest_etag
#             }
#         },
#         Priority=10,
#         RoleArn=s3_arn_role,
#     )
#     job_id = response['JobId']
#     timeout = 1800  # 30 minutes
#     check_interval = 10

#     start_time = time.time()
#     timeout_time = start_time + timeout
#     while True:
#         response = s3_control.describe_job(AccountId=account_id, JobId=job_id)
#         status = response['Job']['Status']
#         if status == 'Complete':
#             LOGGER.info('S3 Batch job is completed.')
#             break

#         # Check if the job is still running
#         if status == 'Failed' or status == 'Cancelled':
#             LOGGER.error('S3 Batch job failed or was cancelled.')
#             return

#         # Check if the timeout has been reached
#         current_time = time.time()
#         if current_time > timeout_time:
#             LOGGER.error('S3 Batch Job time out reached.')
#             return
#         time.sleep(check_interval)


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

    s3_stage_bucket = config.get('s3_stage_bucket')
    if s3_stage_bucket is None:
        LOGGER.error('No S3 stage bucket path found.')
        return
    
    if s3_stage_bucket != source_bucket:
        LOGGER.error('Currently only S3 files located in staged bucket are supported.')
        return
    
    if not len(target_files):
        LOGGER.info('No files were found')
        return
    
    # TODO: if many files are present we should chunk and output multiple BATCH messages
    batch_msg_files = [f's3://{s3_stage_bucket}/{t_file["key"]}' for t_file in target_files]
    write_batch_message(batch_msg_files, stream)
    latest_modified = max(target_files, key=lambda x: x['last_modified'])
    state[stream.tap_stream_id] = {'modified_since': latest_modified['last_modified'].isoformat()}
    singer.write_state(state)
