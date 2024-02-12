import re

import pytz
from datetime import datetime, timezone

import dateutil
import requests
import singer
import boto3
import os, logging
from os import walk
import tap_spreadsheets_anywhere.format_handler
import tap_spreadsheets_anywhere.conversion as conversion
import smart_open.ssh as ssh_transport
import smart_open.ftp as ftp_transport

LOGGER = logging.getLogger(__name__)


def resolve_target_uri(table_spec, target_filename):
    protocol, bucket = parse_path(table_spec['path'])
    # TODO: logic below is disabled because we can't currently support reading filenames from Content-Disposition (Excel limitations)
    if False and protocol in ["http", "https"] and table_spec['pattern'] != target_filename:
        # Handle case where URL returns a filename in the response so we do NOT append the pattern to get the URI
        return table_spec['path']
    else:
        return table_spec['path'] + "/" + target_filename


def _hide_credentials(path):
    import re
    if path.startswith('sftp'):
        return re.sub('sftp://.*?@', "********", path, flags=re.DOTALL)
    elif path.startswith('ftp'):
        return re.sub('ftp://.*?@', "********", path, flags=re.DOTALL)
    return path


def write_file(target_filename, table_spec, schema, max_records=-1):
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
                singer.write_record(table_spec['name'], record_with_meta)
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
    elif protocol in ["http", "https"]:
        target_objects = convert_URL_to_file_list(table_spec)
    else:
        raise ValueError("Protocol {} not yet supported. Pull Requests are welcome!")

    pattern = table_spec['pattern']
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

def raise_error(error):
    raise error

def list_files_in_local_bucket(bucket, search_prefix=None):
    local_filenames = []
    path = bucket
    if search_prefix is not None:
        path = os.path.join(bucket, search_prefix)

    LOGGER.info(f"Walking {path}.")
    max_results = 10000
    for (dirpath, dirnames, filenames) in walk(path, onerror=raise_error):
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


def setup_aws_client(tables_config):
    """
    Initialize a default AWS session
    :param config: connection config
    """
    LOGGER.info("Attempting to create AWS session")
    tables_config = tables_config
    # Get the required parameters from config file and/or environment variables
    aws_access_key_id = tables_config.get('aws_access_key_id') or os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = tables_config.get('aws_secret_access_key') or os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_session_token = tables_config.get('aws_session_token') or os.environ.get('AWS_SESSION_TOKEN')
    aws_role_arn = tables_config.get('aws_role_arn') or os.environ.get('AWS_ROLE_ARN')
    aws_external_id = tables_config.get('aws_external_id') or os.environ.get('AWS_EXTERNAL_ID')
    # AWS credentials based authentication
    # Login as an IAM User
    if aws_access_key_id and aws_secret_access_key:
        LOGGER.info("Setting up default session")
        LOGGER.info(aws_access_key_id)
        boto3.setup_default_session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )

    # Assume Role if desired
    if aws_role_arn and aws_external_id:
        assume_role(aws_role_arn, aws_external_id)
    elif aws_role_arn and not aws_external_id:
        raise ValueError("If aws_role_arn is defined in configuration, aws_external_id must also be defined.")
    
    


def assume_role(aws_role_arn, aws_external_id):
    """
    Assume and IAM role if a IAM Role ARN is available
    The IAM User that the tap will use must have permission to assume the Role. 
    :param aws_role_arn: The ARN of the AWS IAM Role that you wish to assume

    """
    LOGGER.info("Attempting to Assume Role")
    sts_client = boto3.client('sts')
    assumed_role_dict = sts_client.assume_role(
            RoleArn=aws_role_arn,
            RoleSessionName = "TestSession",
            DurationSeconds= 3600,
            ExternalId = aws_external_id
    )
    
    #Save credientals received from assuming role
    credentials = assumed_role_dict['Credentials']

    temp_access_key_id = credentials['AccessKeyId']
    temp_secret_access_key = credentials['SecretAccessKey']
    temp_session_token = credentials['SessionToken']

    #Setup another Default Session using the credentials received from 
    boto3.setup_default_session(
            aws_access_key_id = temp_access_key_id,
            aws_secret_access_key = temp_secret_access_key,
            aws_session_token = temp_session_token
            
    )

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


def config_by_crawl(crawl_config):
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
