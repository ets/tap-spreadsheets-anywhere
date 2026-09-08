from __future__ import annotations

import fnmatch
import importlib.util
import logging
import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from os import walk
from stat import S_ISREG
from urllib.parse import urlparse, urlunparse

import boto3
import dateutil
import pytz
import requests
import smart_open.ftp as ftp_transport
import smart_open.ssh as ssh_transport
from azure.storage.blob import BlobServiceClient

import tap_spreadsheets_anywhere.format_handler
from tap_spreadsheets_anywhere import conversion
from tap_spreadsheets_anywhere.record_sink import SingerRecordSink

LOGGER = logging.getLogger(__name__)


def resolve_target_uri(table_spec, target_filename):
    path: str = table_spec["path"]
    parsed = urlparse(path)

    if parsed.scheme == "imap":
        ## target filename is fully resolved here
        return urlunparse(parsed._replace(path="/")) + target_filename

    # TODO: logic below is disabled because we can't currently support reading filenames from Content-Disposition (Excel limitations)
    if False:
        # Handle case where URL returns a filename in the response so we do NOT append the pattern to get the URI
        return path
    else:
        return path + "/" + target_filename


def _hide_credentials(path):
    import re

    if path.startswith("sftp"):
        return re.sub("sftp://.*?@", "********", path, flags=re.DOTALL)
    elif path.startswith("ftp"):
        return re.sub("ftp://.*?@", "********", path, flags=re.DOTALL)
    return path


def write_file(target_filename, last_modified_iso, table_spec, schema, max_records=-1, sink=None):
    LOGGER.info('Syncing file "%s".', target_filename)
    target_uri = resolve_target_uri(table_spec, target_filename)
    sink = sink or SingerRecordSink(table_spec["name"])
    records_synced = 0
    try:
        iterator = tap_spreadsheets_anywhere.format_handler.get_row_iterator(table_spec, target_uri)
        for row in iterator:
            metadata = {
                "_smart_source_bucket": _hide_credentials(table_spec["path"]),
                "_smart_source_file": target_filename,
                # index zero, +1 for header row
                "_smart_source_lineno": records_synced + 2,
                "_smart_source_last_modified": last_modified_iso,
            }

            try:
                record_with_meta = {**conversion.convert_row(row, schema), **metadata}
                sink.write(record_with_meta)
            except BrokenPipeError:
                LOGGER.error(
                    "Pipe to loader broke after %d records were written from %s: troubled line was %s",
                    records_synced,
                    target_filename,
                    record_with_meta,
                )
                raise

            records_synced += 1
            if 0 < max_records <= records_synced:
                break

    except tap_spreadsheets_anywhere.format_handler.InvalidFormatError:
        if table_spec.get("invalid_format_action", "fail").lower() == "ignore":
            LOGGER.exception("Ignoring unparseable file: %s", target_filename)
        else:
            raise

    return records_synced


def sample_file(table_spec, target_filename, ignore_undefined_field_names, sample_rate, max_records):
    LOGGER.info("Sampling %s (%s records, every %sth record).", target_filename, max_records, sample_rate)

    target_uri = resolve_target_uri(table_spec, target_filename)
    samples = []
    current_row = 0
    try:
        iterator = tap_spreadsheets_anywhere.format_handler.get_row_iterator(table_spec, target_uri)

        for row in iterator:
            if (current_row % sample_rate) == 0:
                if table_spec.get("skip_empty_rows", False) and all(value == None for value in row.values()):
                    continue
                else:
                    samples.append(row)

            current_row += 1
            if len(samples) >= max_records:
                break
    except tap_spreadsheets_anywhere.format_handler.InvalidFormatError:
        if table_spec.get("invalid_format_action", "fail").lower() != "ignore":
            raise
        else:
            LOGGER.exception("Unable to parse %s", target_filename)

    if ignore_undefined_field_names:
        for row in samples:
            row.pop("", None)

    LOGGER.info("Sampled %d records.", len(samples))
    return samples


def sample_files(
    table_spec,
    target_files,
    ignore_undefined_field_names,
    sample_rate=10,
    max_records=1000,
    max_files=5,
):
    to_return = []

    for files_so_far, target_file in enumerate(target_files):
        to_return += sample_file(
            table_spec,
            target_file["key"],
            ignore_undefined_field_names,
            sample_rate,
            max_records,
        )

        if files_so_far + 1 >= max_files:
            break

    return to_return


def parse_path(path):
    path_parts = path.split("://", 1)
    return ("local", path_parts[0]) if len(path_parts) <= 1 else (path_parts[0], path_parts[1])


def get_matching_objects(table_spec, modified_since=None):
    protocol, bucket = parse_path(table_spec["path"])

    # TODO Breakout the transport schemes here similar to the registry/loading pattern used by smart_open
    if protocol == "s3":
        target_objects = list_files_in_s3_bucket(bucket, table_spec.get("search_prefix"))
    elif protocol == "file":
        target_objects = list_files_in_local_bucket(bucket, table_spec.get("search_prefix"))
    elif protocol in ["sftp"]:
        target_objects = list_files_in_SSH_bucket(table_spec["path"], table_spec.get("search_prefix"))
    elif protocol in ["ftp"]:
        target_objects = list_files_in_ftp_server(table_spec["path"], table_spec.get("search_prefix"))
    elif protocol in ["gs"]:
        target_objects = list_files_in_gs_bucket(bucket, table_spec.get("search_prefix"))
    elif protocol in ["http", "https"]:
        target_objects = convert_URL_to_file_list(table_spec)
    elif protocol in ["azure"]:
        target_objects = list_files_in_azure_bucket(
            bucket,
            table_spec.get("search_prefix"),
            modified_since,
        )
    elif protocol in ["imap"]:
        target_objects = list_files_in_imap_mailbox(
            table_spec["path"],
            table_spec.get("search_prefix"),
            modified_since,
        )
    elif protocol in ["sharepoint"]:
        target_objects = list_files_in_sharepoint(table_spec["path"], table_spec.get("search_prefix"))
    else:
        raise ValueError("Protocol {} not yet supported. Pull Requests are welcome!")

    pattern = table_spec["pattern"]
    matcher = re.compile(pattern)
    if modified_since:
        LOGGER.info(
            'Checking %d resolved objects for any that match regular expression "%s" and were modified since %s',
            len(target_objects),
            pattern,
            modified_since,
        )
    else:
        LOGGER.info(
            'Checking %d resolved objects for any that match regular expression "%s"',
            len(target_objects),
            pattern,
        )

    to_return = []
    for obj in target_objects:
        key = obj["Key"]
        last_modified = obj["LastModified"]

        # noinspection PyTypeChecker
        if matcher.search(key) and (modified_since is None or modified_since < last_modified):
            LOGGER.debug('Including key "%s"', key)
            LOGGER.debug("Last modified: %s comparing to %s ", last_modified, modified_since)
            to_return.append({"key": key, "last_modified": last_modified})
        else:
            LOGGER.debug('Not including key "%s"', key)

    if not LOGGER.isEnabledFor(logging.DEBUG):
        LOGGER.info(
            "Processing %d resolved objects that met our criteria. Enable debug verbosity logging for more details.",
            len(to_return),
        )
    return sorted(to_return, key=lambda item: item["last_modified"])


def list_files_in_SSH_bucket(uri, search_prefix=None):
    if importlib.util.find_spec("paramiko") is None:
        LOGGER.warning(
            "paramiko missing, opening SSH/SCP/SFTP paths will be disabled. `pip install paramiko` to suppress"
        )
        raise ImportError("paramiko is required to open SSH/SCP/SFTP paths")

    parsed_uri = ssh_transport.parse_uri(uri)
    uri_path = parsed_uri.pop("uri_path")
    transport_params = tap_spreadsheets_anywhere.format_handler.get_transport_params(parsed_uri.pop("scheme"))
    ssh = ssh_transport._connect_ssh(
        parsed_uri["host"],
        parsed_uri["user"],
        parsed_uri["port"] or 22,
        parsed_uri["password"],
        transport_params.pop("connect_kwargs", None),
    )
    sftp_client = ssh.get_transport().open_sftp_client()
    entries = []
    max_results = 10000

    for entry in sftp_client.listdir_attr(uri_path):
        if search_prefix is None or fnmatch.fnmatch(entry.filename, search_prefix):
            mode = entry.st_mode
            if S_ISREG(mode):
                entries.append(
                    {
                        "Key": entry.filename,
                        "LastModified": datetime.fromtimestamp(entry.st_mtime, timezone.utc),
                    }
                )
            if len(entries) > max_results:
                raise ValueError(
                    f"Read more than {max_results} records from the path {uri_path}. Use a more specific search_prefix"
                )

    LOGGER.info("Found %d files.", len(entries))
    return entries


def convert_URL_to_file_list(table_spec):
    url = table_spec["path"] + "/" + table_spec["pattern"]
    LOGGER.info("Assembled %s as the URL to a source file.", url)
    r = requests.get(url, allow_redirects=True)
    if r:
        if "last-modified" in r.headers:
            # The header is immediately localized to UTC below, so the naive strptime() result here is transient.
            last_modified = pytz.UTC.localize(
                datetime.strptime(r.headers["last-modified"], "%a, %d %b %Y %H:%M:%S %Z")  # noqa: DTZ007
            )
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

        return [{"Key": filename, "LastModified": last_modified}]
    else:
        raise ValueError(f"Configured URL {url} could not be read.")


def list_files_in_ftp_server(uri, search_prefix=None):
    parsed_uri = ftp_transport.parse_uri(uri)
    uri_path = parsed_uri.pop("uri_path")
    secure_conn = parsed_uri["scheme"] == "ftps"
    ftp = ftp_transport._connect(
        parsed_uri["host"],
        parsed_uri["user"],
        parsed_uri["port"],
        parsed_uri["password"],
        secure_conn,
        transport_params={},
    )
    entries = []
    max_results = 10000

    for row in ftp.mlsd(uri_path):
        if search_prefix is None or fnmatch.fnmatch(row[0], search_prefix):
            if row[1]["type"] == "file":
                entries.append(
                    {
                        "Key": row[0],
                        "LastModified": datetime.strptime(row[1]["modify"], "%Y%m%d%H%M%S").replace(
                            tzinfo=timezone.utc
                        ),
                    }
                )
            if len(entries) > max_results:
                raise ValueError(
                    f"Read more than {max_results} records from the path {uri_path}. Use a more specific search_prefix"
                )

    LOGGER.info("Found %d files.", len(entries))
    return entries


def raise_error(error):
    raise error


def list_files_in_local_bucket(bucket, search_prefix=None):
    local_filenames = []
    path = bucket
    if search_prefix is not None:
        path = os.path.join(bucket, search_prefix)

    LOGGER.info("Walking %s.", path)
    max_results = 10000
    for dirpath, dirnames, filenames in walk(path, onerror=raise_error):
        for filename in filenames:
            abspath = os.path.join(dirpath, filename)
            relpath = os.path.relpath(abspath, path)
            local_filenames.append(relpath)
        if len(local_filenames) > max_results:
            raise ValueError(
                f"Read more than {max_results} records from the path {path}. Use a more specific search_prefix"
            )

    LOGGER.info("Found %d files.", len(local_filenames))
    # for filename in local_filenames:
    #     LOGGER.info(f"Found {filename} and {os.path.join(path, filename)} exists {os.path.exists(os.path.join(path, filename))}")

    return [
        {
            "Key": filename,
            "LastModified": datetime.fromtimestamp(os.path.getmtime(os.path.join(path, filename)), timezone.utc),
        }
        for filename in local_filenames
        if os.path.exists(os.path.join(path, filename))
    ]


def list_files_in_gs_bucket(bucket, search_prefix=None):
    gs_client = tap_spreadsheets_anywhere.format_handler.get_gcs_client()
    blobs = gs_client.list_blobs(bucket, prefix=search_prefix)

    target_objects = [{"Key": blob.name, "LastModified": blob.updated} for blob in blobs]

    LOGGER.info("Found %d files.", len(target_objects))

    return target_objects


def list_files_in_azure_bucket(
    container_name: str,
    search_prefix: str | None = None,
    modified_since: datetime | None = None,
):
    sas_key = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(sas_key)
    container_client = blob_service_client.get_container_client(container_name)
    blob_iterator = container_client.list_blobs(name_starts_with=search_prefix)
    return [
        {"Key": blob.name, "LastModified": blob.last_modified}
        for blob in blob_iterator
        if blob.size > 0 and blob.last_modified >= modified_since
    ]


def list_files_in_s3_bucket(bucket, search_prefix=None):
    s3_client = boto3.client("s3")
    s3_objects = []

    max_results = 1000
    args = {
        "Bucket": bucket,
        "MaxKeys": max_results,
    }
    if search_prefix is not None:
        args["Prefix"] = search_prefix

    result = s3_client.list_objects_v2(**args)
    if result["KeyCount"] > 0:
        s3_objects += result["Contents"]
        next_continuation_token = result.get("NextContinuationToken")

        while next_continuation_token is not None:
            LOGGER.debug('Continuing pagination with token "%s".', next_continuation_token)

            continuation_args = args.copy()
            continuation_args["ContinuationToken"] = next_continuation_token

            result = s3_client.list_objects_v2(**continuation_args)

            s3_objects += result["Contents"]
            next_continuation_token = result.get("NextContinuationToken")

    LOGGER.info("Found %d files.", len(s3_objects))

    return s3_objects


@lru_cache
def list_files_in_imap_mailbox(
    uri: str,
    search_prefix: str | None = None,
    modified_since: datetime | None = None,
):
    parsed = urlparse(uri)
    fs = tap_spreadsheets_anywhere.format_handler.get_imap_fs(parsed.netloc)
    target_objects = []

    for f in fs.ls(parsed.path, since=modified_since.date()):
        if search_prefix is None or fnmatch.fnmatch(f["name"], search_prefix):
            last_modified = f.get("last_modified") or datetime.now(tz=timezone.utc)
            target_objects.append(
                {
                    "Key": f["name"],
                    "LastModified": last_modified,
                }
            )

    LOGGER.info("Found %d files.", len(target_objects))

    return target_objects


@lru_cache
def list_files_in_sharepoint(
    uri: str,
    search_prefix: str | None = None,
    path: str = "",
):
    fs = tap_spreadsheets_anywhere.format_handler.get_sharepoint_fs(uri)
    target_objects = []

    for f in fs.ls(path):
        if f["type"] == "directory":
            target_objects.extend(list_files_in_sharepoint(uri, search_prefix=search_prefix, path=f["name"]))
            continue

        if search_prefix is None or fnmatch.fnmatch(f["name"], search_prefix):
            target_objects.append({"Key": f["name"], "LastModified": f["mtime"]})

    return target_objects


def config_by_crawl(crawl_config):
    config = {"tables": []}
    for source in crawl_config:
        entries = {}
        modified_since = dateutil.parser.parse(source.get("start_date", "1970-01-01T00:00:00Z"))
        target_files = get_matching_objects(source, modified_since=modified_since)
        for file in target_files:
            if not file["key"].endswith("/"):
                dirs = file["key"].split("/")
                if len(dirs) > 1:
                    table = re.sub(r"\W+", "", "_".join(dirs[0:-1]))
                else:
                    table = re.sub(r"\W+", "", dirs[0])
                directory = "/".join(dirs[0:-1])
                parts = file["key"].split(".")
                # group all files in the same directory and with the same extension
                if len(parts) > 1:
                    rel_pattern = ".*" + parts[-1]
                else:
                    rel_pattern = parts[0]
                abs_pattern = directory + "/" + rel_pattern + "$"
                if table not in entries:
                    entries[table] = {
                        "path": source["path"],
                        "name": table,
                        "search_prefix": directory,
                        "pattern": abs_pattern,
                        "key_properties": [],
                        "format": "detect",
                        "encoding": source.get("encoding", "utf-8"),
                        "invalid_format_action": "ignore",
                        "delimiter": "detect",
                        "max_records_per_run": source.get("max_records_per_run", -1),
                        "max_sampled_files": source.get("max_sampled_files", 5),
                        "max_sampling_read": source.get("max_sampling_read", 1000),
                        "universal_newlines": source.get("universal_newlines", True),
                        "prefer_number_vs_integer": source.get("prefer_number_vs_integer", False),
                        "prefer_schema_as_string": source.get("prefer_schema_as_string", False),
                        "start_date": modified_since.isoformat(),
                    }
                elif abs_pattern != entries[table]["pattern"]:
                    # We've identified an additional pattern under the same table so give it a unique table name
                    table_with_pattern = re.sub(r"\W+", "", table + "_" + rel_pattern)
                    if table_with_pattern not in entries:
                        entries[table_with_pattern] = {
                            "path": source["path"],
                            "name": table_with_pattern,
                            "search_prefix": directory,
                            "pattern": abs_pattern,
                            "key_properties": [],
                            "format": "detect",
                            "encoding": source.get("encoding", "utf-8"),
                            "invalid_format_action": "ignore",
                            "delimiter": "detect",
                            "max_records_per_run": source.get("max_records_per_run", -1),
                            "max_sampled_files": source.get("max_sampled_files", 5),
                            "max_sampling_read": source.get("max_sampling_read", 1000),
                            "universal_newlines": source.get("universal_newlines", True),
                            "prefer_number_vs_integer": source.get("prefer_number_vs_integer", False),
                            "prefer_schema_as_string": source.get("prefer_schema_as_string", False),
                            "start_date": modified_since.isoformat(),
                        }

            else:
                LOGGER.debug("Skipping config for %s because it looks like a folder not a file", file["key"])
        config["tables"] += entries.values()
        return config
