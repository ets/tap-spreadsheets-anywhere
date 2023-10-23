from collections import defaultdict
import re

import pytz
from datetime import datetime, timezone

import dateutil
import requests
import singer
import boto3
# from google.cloud import storage
import os, logging
from os import walk
import tap_spreadsheets_anywhere.format_handler
import tap_spreadsheets_anywhere.conversion as conversion
import smart_open.ssh as ssh_transport
from azure.storage.blob import BlobServiceClient
import smart_open.ftp as ftp_transport
from tap_spreadsheets_anywhere.model_json import optionset_names, get_file_pattern

LOGGER = logging.getLogger(__name__)

logger=logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
logger.setLevel(logging.WARNING)


def resolve_target_uri(table_spec, target_filename):
    protocol, bucket = parse_path(table_spec['path'])
    # TODO: logic below is disabled because we can't currently support reading filenames from Content-Disposition (Excel limitations)
    if False and protocol in ["http", "https"] and table_spec['pattern'] != target_filename:
        # Handle case where URL returns a filename in the response so we do NOT append the pattern to get the URI
        return table_spec['path']
    else:
        return table_spec['path'] + "/" + target_filename

def write_record(stream_name, record, time_extracted=None, version=None):
    try:
        if version:
            singer.messages.write_message(
                singer.messages.RecordMessage(
                    stream=stream_name,
                    record=record,
                    version=version,
                    time_extracted=time_extracted))
        else:
            singer.messages.write_record(
                stream_name=stream_name,
                record=record,
                time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: {}'.format(stream_name))
        LOGGER.info('record: {}'.format(record))
        raise err

def _hide_credentials(path):
    import re
    if path.startswith('sftp'):
        return re.sub('sftp://.*?@', "********", path, flags=re.DOTALL)
    elif path.startswith('ftp'):
        return re.sub('ftp://.*?@', "********", path, flags=re.DOTALL)
    return path


def write_file(target_filename, table_spec, schema, max_records=-1, version=None):
    LOGGER.info('Syncing file "{}".'.format(target_filename))
    target_uri = resolve_target_uri(table_spec, target_filename)
    records_synced = 0
    try:
        iterator = tap_spreadsheets_anywhere.format_handler.get_row_iterator(table_spec, target_uri)
        for row in iterator:
            metadata = {
                '_smart_source_bucket': _hide_credentials(table_spec['path']),
                '_smart_source_file': target_filename,
                # index zero, +1 for header row
                '_smart_source_lineno': records_synced + 2
            }

            try:
                record_with_meta = {**conversion.convert_row(row, schema), **metadata}
                write_record(table_spec['name'], record_with_meta, version=version)
            except BrokenPipeError as bpe:
                LOGGER.error(
                    f'Pipe to loader broke after {records_synced} records were written from {target_filename}: troubled '
                    f'line was {record_with_meta}')
                raise bpe

            records_synced += 1
            if 0 < max_records <= records_synced:
                break

    except tap_spreadsheets_anywhere.format_handler.InvalidFormatError as ife:
        if table_spec.get('invalid_format_action','fail').lower() == "ignore":
            LOGGER.exception(f"Ignoring unparseable file: {target_filename}",ife)
        else:
            raise ife

    return records_synced


def sample_file(table_spec, target_filename, sample_rate, max_records):
    LOGGER.info('Sampling {} ({} records, every {}th record).'
                .format(target_filename, max_records, sample_rate))

    target_uri = resolve_target_uri(table_spec,target_filename)
    samples = []
    current_row = 0
    try:
        iterator = tap_spreadsheets_anywhere.format_handler.get_row_iterator(table_spec, target_uri)

        for row in iterator:
            if (current_row % sample_rate) == 0:
                samples.append(row)

            current_row += 1
            if len(samples) >= max_records:
                break
    except tap_spreadsheets_anywhere.format_handler.InvalidFormatError as ife:
        if table_spec.get('invalid_format_action','fail').lower() != "ignore":
            raise ife
        else:
            LOGGER.exception(f"Unable to parse {target_filename}",ife)

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


def get_matching_objects(table_spec, modified_since=None):
    protocol, bucket = parse_path(table_spec['path'])

    # TODO Breakout the transport schemes here similar to the registry/loading pattern used by smart_open
    if protocol == 's3':
        target_objects = list_files_in_s3_bucket(bucket, table_spec.get('search_prefix'))
    elif protocol == 'file':
        target_objects = list_files_in_local_bucket(bucket, table_spec.get('search_prefix'))
    elif protocol in ["sftp"]:
        target_objects = list_files_in_SSH_bucket(table_spec['path'],table_spec.get('search_prefix'))
    elif protocol in ["ftp"]:
        target_objects = list_files_in_ftp_server(table_spec['path'],table_spec.get('search_prefix'))
    elif protocol in ["gs"]:
        target_objects = list_files_in_gs_bucket(bucket,table_spec.get('search_prefix'))
    elif protocol in ["http", "https"]:
        target_objects = convert_URL_to_file_list(table_spec)
    elif protocol in ["azure"]:
        name = table_spec['name']
        search_prefix = 'OptionsetMetadata' if name in optionset_names else name
        target_objects = list_files_in_azure_bucket(bucket, search_prefix)
    else:
        raise ValueError("Protocol {} not yet supported. Pull Requests are welcome!")

    # pattern = table_spec['pattern']
    # the optionsets provide regexes, but otherwise can be inferred from entity name and partitioning scheme
    pattern = get_file_pattern(table_spec)
    matcher = re.compile(pattern)
    if modified_since:
        LOGGER.info(f'Checking {len(target_objects)} resolved objects for any that match regular expression "{pattern}" and were modified since {modified_since}')
    else:
        LOGGER.info(f'Checking {len(target_objects)} resolved objects for any that match regular expression "{pattern}"')

    to_return = []
    for obj in target_objects:
        key = obj['Key']
        last_modified = obj['LastModified']

        # noinspection PyTypeChecker
        if matcher.search(key) and (modified_since is None or modified_since < last_modified):
            LOGGER.debug('Including key "{}"'.format(key))
            LOGGER.debug('Last modified: {}'.format(last_modified) + ' comparing to {} '.format(modified_since))
            to_return.append({'key': key, 'last_modified': last_modified})
        else:
            LOGGER.debug('Not including key "{}"'.format(key))

    if not LOGGER.isEnabledFor(logging.DEBUG):
        LOGGER.info(f'Processing {len(to_return)} resolved objects that met our criteria. Enable debug verbosity logging for more details.')
    return sorted(to_return, key=lambda item: item['last_modified'])


def list_files_in_SSH_bucket(uri, search_prefix=None):
    try:
        import paramiko
    except ImportError:
        LOGGER.warn(
            'paramiko missing, opening SSH/SCP/SFTP paths will be disabled. '
            '`pip install paramiko` to suppress'
        )
        raise

    parsed_uri = ssh_transport.parse_uri(uri)
    uri_path = parsed_uri.pop('uri_path')
    transport_params={'connect_kwargs':{'allow_agent':False,'look_for_keys':False}}
    ssh = ssh_transport._connect(parsed_uri['host'], parsed_uri['user'], parsed_uri['port'], parsed_uri['password'], transport_params=transport_params)
    sftp_client = ssh.get_transport().open_sftp_client()
    entries = []
    max_results = 10000
    from stat import S_ISREG
    import fnmatch
    for entry in sftp_client.listdir_attr(uri_path):
        if search_prefix is None or fnmatch.fnmatch(entry.filename,search_prefix):
            mode = entry.st_mode
            if S_ISREG(mode):
                entries.append({'Key':entry.filename,'LastModified':datetime.fromtimestamp(entry.st_mtime, timezone.utc)})
            if len(entries) > max_results:
                raise ValueError(f"Read more than {max_results} records from the path {uri_path}. Use a more specific "
                                 f"search_prefix")

    LOGGER.info("Found {} files.".format(entries))
    return entries

def convert_URL_to_file_list(table_spec):
    url = table_spec["path"] + "/" + table_spec["pattern"]
    LOGGER.info(f"Assembled {url} as the URL to a source file.")
    r = requests.get(url, allow_redirects=True)
    if r:
        if 'last-modified' in r.headers:
            last_modified = pytz.UTC.localize(datetime.strptime(r.headers['last-modified'], '%a, %d %b %Y %H:%M:%S %Z'))
        else:
            LOGGER.warning("URL did not return a last-modified header so using current date and time.")
            last_modified = datetime.now(tz=timezone.utc)

        filename = table_spec["pattern"]
        # TODO: logic below is disabled because we can't currently support reading filenames from Content-Disposition (Excel limitations)
        # if 'content-disposition' in r.headers:
        #     cd = r.headers['content-disposition']
        #     filename = unquote(re.findall("filename.?=(.+)", cd)[0])
        #     LOGGER.info("URL returned '" + filename + "' as the targeted filename.")
        # else:
        #     LOGGER.warning("URL did not return a content-disposition header so using pattern '"+table_spec["pattern"]+"' as the targeted filename.")

        return [{'Key': filename, 'LastModified':last_modified}]
    else:
        raise ValueError(f"Configured URL {url} could not be read.")


def list_files_in_ftp_server(uri, search_prefix=None):
    parsed_uri = ftp_transport.parse_uri(uri)
    uri_path = parsed_uri.pop('uri_path')
    secure_conn = True if parsed_uri["scheme"] == "ftps" else False
    ftp = ftp_transport._connect(parsed_uri['host'], parsed_uri['user'], parsed_uri['port'], parsed_uri['password'], secure_conn, transport_params={})
    entries = []
    max_results = 10000
    from stat import S_ISREG
    import fnmatch
    for row in ftp.mlsd(uri_path):
        if search_prefix is None or fnmatch.fnmatch(entry[0],search_prefix):
            if row[1]['type'] == 'file':
                entries.append({'Key':row[0],'LastModified':datetime.strptime(row[1]['modify'], '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc)})
            if len(entries) > max_results:
                raise print(f"Read more than {max_results} records from the path {uri_path}. Use a more specific "
                             f"search_prefix")

    LOGGER.info("Found {} files.".format(entries))
    return entries


def list_files_in_local_bucket(bucket, search_prefix=None):
    local_filenames = []
    path = bucket
    if search_prefix is not None:
        path = os.path.join(bucket, search_prefix)

    LOGGER.info(f"Walking {path}.")
    max_results = 10000
    for (dirpath, dirnames, filenames) in walk(path):
        for filename in filenames:
            abspath = os.path.join(dirpath,filename)
            relpath = os.path.relpath(abspath, path)
            local_filenames.append(relpath)
        if len(local_filenames) > max_results:
            raise ValueError(f"Read more than {max_results} records from the path {path}. Use a more specific "
                             f"search_prefix")

    LOGGER.info("Found {} files.".format(len(local_filenames)))
    # for filename in local_filenames:
    #     LOGGER.info(f"Found {filename} and {os.path.join(path, filename)} exists {os.path.exists(os.path.join(path, filename))}")

    return [{'Key': filename, 'LastModified': datetime.fromtimestamp(os.path.getmtime(os.path.join(path, filename)), timezone.utc)} for
            filename in local_filenames if os.path.exists(os.path.join(path, filename))]

def list_files_in_gs_bucket(bucket, search_prefix=None):
    gs_client = storage.Client()
        
    blobs = gs_client.list_blobs(bucket, prefix=search_prefix)

    target_objects = [{'Key': blob.name, 'LastModified': blob.updated} for blob in blobs]
    
    LOGGER.info("Found {} files.".format(len(target_objects)))

    return target_objects

def list_files_in_azure_bucket(container_name, search_prefix=None):
    sas_key = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    blob_service_client = BlobServiceClient.from_connection_string(sas_key)
    container_client = blob_service_client.get_container_client(container_name)
    blob_iterator = container_client.list_blobs(name_starts_with=search_prefix + '/')
    
    # keep all blobs including 0-size
    blobs = [
        {'Key': blob.name, 'LastModified': blob.last_modified, 'Size': blob.size} 
        for blob in blob_iterator
    ]
    if search_prefix in optionset_names:
        return blobs
    else:
        # get the latest snapshot for each partition, don't care about the earlier ones.
        snapshot_blobs = [
            k for k in blobs
            if 'Snapshot' in k['Key']
        ]
        # use a dict to only keep the last relevant file for partition e.g. '2023-08' prefix, it will be the latest.
        blob_dict = {
            k['Key'].split('/')[-1].split('_')[0]: k
            for k in snapshot_blobs
        }
        # now drop any 0-size blobs. If the latest file in that date-partition is 0-size
        # that means all rows in that partition have been deleted, nothing to emit.
        blobs = [
            blob for blob in blob_dict.values()
            if blob['Size'] > 0
        ]
        return blobs


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
    if result['KeyCount'] > 0:
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


def config_by_crawl(crawl_config, bucket: str):
    config = {'tables': []}
    for source in crawl_config:
        entries = {}
        modified_since = dateutil.parser.parse(source['start_date'] if 'start_date' in source else
                                               "1970-01-01T00:00:00Z")
        target_files = get_matching_objects(source, modified_since=modified_since)
        for file in target_files:
            if not file['key'].endswith('/'):
                dirs = file['key'].split('/')
                if len(dirs) > 1:
                    table = re.sub(r'\W+', '', "_".join(dirs[0:-1]))
                else:
                    table = re.sub(r'\W+', '', dirs[0])
                directory = "/".join(dirs[0:-1])
                parts = file['key'].split('.')
                # group all files in the same directory and with the same extension
                if len(parts) > 1:
                    rel_pattern = ".*" + parts[-1]
                else:
                    rel_pattern = parts[0]
                abs_pattern = directory + '/' + rel_pattern + '$'
                if table not in entries:
                    entries[table] = {
                        "path": source['path'],
                        "name": table,
                        "search_prefix": directory,
                        "pattern": abs_pattern,
                        "key_properties": [],
                        "format": "detect",
                        "encoding": source.get('encoding', 'utf-8'),
                        "invalid_format_action": "ignore",
                        "delimiter": "detect",
                        "max_records_per_run": source.get('max_records_per_run',-1),
                        "max_sampled_files": source.get('max_sampled_files', 5),
                        "max_sampling_read": source.get('max_sampling_read', 1000),
                        "universal_newlines": source.get('universal_newlines', True),
                        "prefer_number_vs_integer": source.get('prefer_number_vs_integer', False),
                        "full_table_replace": source.get('full_table_replace', False),
                        "prefer_schema_as_string": source.get('prefer_schema_as_string', False),
                        "start_date": modified_since.isoformat()
                    }
                elif abs_pattern != entries[table]["pattern"]:
                    # We've identified an additional pattern under the same table so give it a unique table name
                    table_with_pattern = re.sub(r'\W+', '', table + '_' + rel_pattern)
                    if table_with_pattern not in entries:
                        entries[table_with_pattern] = {
                            "path": source['path'],
                            "name": table_with_pattern,
                            "search_prefix": directory,
                            "pattern": abs_pattern,
                            "key_properties": [],
                            "format": "detect",
                            "encoding": source.get('encoding', 'utf-8'),
                            "invalid_format_action": "ignore",
                            "delimiter": "detect",
                            "max_records_per_run": source.get('max_records_per_run', -1),
                            "max_sampled_files": source.get('max_sampled_files', 5),
                            "max_sampling_read": source.get('max_sampling_read', 1000),
                            "universal_newlines": source.get('universal_newlines', True),
                            "prefer_number_vs_integer": source.get('prefer_number_vs_integer', False),
                            "prefer_schema_as_string": source.get('prefer_schema_as_string', False),
                            "start_date": modified_since.isoformat()
                        }

            else:
                LOGGER.debug(f"Skipping config for {file['key']} because it looks like a folder not a file")
        config['tables'] += entries.values()
        return config
