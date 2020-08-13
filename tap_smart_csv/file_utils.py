import re
import singer
import boto3
import tap_smart_csv.format_handler
import tap_smart_csv.conversion as conversion

LOGGER = singer.get_logger()


def write_file(target_filename, table_spec, schema):
    LOGGER.info('Syncing file "{}".'.format(target_filename))
    target_uri = table_spec['path'] + '/' + target_filename
    iterator = tap_smart_csv.format_handler.get_row_iterator(table_spec, target_uri)
    records_synced = 0

    for row in iterator:
        metadata = {
            '_smart_source_bucket': table_spec['path'],
            '_smart_source_file': target_filename,
            # index zero, +1 for header row
            '_s3_source_lineno': records_synced + 2
        }

        try:
            to_write = [{**conversion.convert_row(row, schema), **metadata}]
            singer.write_records(table_spec['name'], to_write)
        except BrokenPipeError as bpe:
            LOGGER.error(
                f'Pipe to loader broke after {records_synced} records were written from {target_filename}: troubled '
                f'line was {row}')
            raise bpe

        records_synced += 1

    return records_synced


def sample_file(table_spec, target_filename, sample_rate, max_records):
    LOGGER.info('Sampling {} ({} records, every {}th record).'
                .format(target_filename, max_records, sample_rate))

    target_uri = table_spec['path'] + '/' + target_filename
    iterator = tap_smart_csv.format_handler.get_row_iterator(table_spec, target_uri)
    samples = []
    current_row = 0

    for row in iterator:
        if (current_row % sample_rate) == 0:
            samples.append(row)

        current_row += 1

        if len(samples) >= max_records:
            break

    LOGGER.info('Sampled {} records.'.format(len(samples)))

    return samples


def sample_files(table_spec, target_files,
                 sample_rate=10, max_records=1000, max_files=5):
    to_return = []

    files_so_far = 0

    for target_file in target_files:
        to_return += sample_file(table_spec, target_file['key'], sample_rate, max_records)
        files_so_far += 1

        if files_so_far >= max_files:
            break

    return to_return


def parse_path(path):
    path_parts = path.split('://', 1)
    return ('local', path_parts[0]) if len(path_parts) <= 1 else (path_parts[0], path_parts[1])


def get_input_files_for_table(table_spec, modified_since=None):
    protocol, bucket = parse_path(table_spec['path'])

    if protocol == 's3':
        target_objects = list_files_in_s3_bucket(bucket, table_spec.get('search_prefix'))
    else:
        raise ValueError("Protocol {} not yet supported. Pull Requests are welcome!")

    pattern = table_spec['pattern']
    matcher = re.compile(pattern)
    LOGGER.debug(f'Checking bucket "{bucket}" for keys matching "{pattern}"')

    to_return = []
    for obj in target_objects:
        key = obj['Key']
        last_modified = obj['LastModified']

        if (matcher.search(key) and
                (modified_since is None or modified_since < last_modified)):
            LOGGER.debug('Will download key "{}"'.format(key))
            LOGGER.debug('Last modified: {}'.format(last_modified) + ' comparing to {} '.format(modified_since))
            to_return.append({'key': key, 'last_modified': last_modified})
        else:
            LOGGER.debug('Will not download key "{}"'.format(key))

    return sorted(to_return, key=lambda item: item['last_modified'])


def list_files_in_s3_bucket(bucket, search_prefix=None):
    s3_client = boto3.client('s3')
    s3_objects = []

    max_results = 1000
    args = {
        'Bucket': bucket,
        'MaxKeys': max_results,
    }
    if search_prefix is not None:
        args['Prefix'] = search_prefix

    result = s3_client.list_objects_v2(**args)

    s3_objects += result['Contents']
    next_continuation_token = result.get('NextContinuationToken')

    while next_continuation_token is not None:
        LOGGER.debug('Continuing pagination with token "{}".'.format(next_continuation_token))

        continuation_args = args.copy()
        continuation_args['ContinuationToken'] = next_continuation_token

        result = s3_client.list_objects_v2(**continuation_args)

        s3_objects += result['Contents']
        next_continuation_token = result.get('NextContinuationToken')

    LOGGER.info("Found {} files.".format(len(s3_objects)))

    return s3_objects
