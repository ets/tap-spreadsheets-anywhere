#!/usr/bin/env python3
import os
import logging

import dateutil
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from tap_spreadsheets_anywhere.configuration import Config
from tap_spreadsheets_anywhere.s3_batch_messages import process_batch
import tap_spreadsheets_anywhere.conversion as conversion
import tap_spreadsheets_anywhere.file_utils as file_utils

LOGGER = logging.getLogger(__name__)

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def merge_dicts(first, second):
    to_return = first.copy()

    for key in second:
        if key in first:
            if isinstance(first[key], dict) and isinstance(second[key], dict):
                to_return[key] = merge_dicts(first[key], second[key])
            else:
                to_return[key] = second[key]
        else:
            to_return[key] = second[key]

    return to_return


def override_schema_with_config(inferred_schema, table_spec):
    override_schema = {'properties': table_spec.get('schema_overrides', {}),
                       'selected': table_spec.get('selected', True)}
    # Note that we directly support setting selected through config so that this tap is useful outside Meltano
    return merge_dicts(inferred_schema, override_schema)


def generate_schema(table_spec, samples):
    if table_spec.get('batch'):
        metadata_schema = {}
    else:
        metadata_schema = {
            '_smart_source_bucket': {'type': 'string'},
            '_smart_source_file': {'type': 'string'},
            '_smart_source_lineno': {'type': 'integer'},
        }
    prefer_number_vs_integer = table_spec.get('prefer_number_vs_integer', False)
    prefer_schema_as_string = table_spec.get('prefer_schema_as_string', False)
    data_schema = conversion.generate_schema(samples, prefer_number_vs_integer=prefer_number_vs_integer, prefer_schema_as_string=prefer_schema_as_string)
    inferred_schema = {
        'type': 'object',
        'properties': merge_dicts(data_schema, metadata_schema)
    }

    merged_schema = override_schema_with_config(inferred_schema, table_spec)
    return Schema.from_dict(merged_schema)

def discover(config):
    streams = []
    for table_spec in config['tables']:
        try:
            modified_since = dateutil.parser.parse(table_spec['start_date'])
            target_files = file_utils.get_matching_objects(table_spec, modified_since)
            sample_rate = table_spec.get('sample_rate',5)
            max_sampling_read = table_spec.get('max_sampling_read', 1000)
            max_sampled_files = table_spec.get('max_sampled_files', 50)
            samples = file_utils.sample_files(table_spec, target_files,sample_rate=sample_rate,
                                              max_records=max_sampling_read, max_files=max_sampled_files)
            schema = generate_schema(table_spec, samples)
            stream_metadata = []
            key_properties = table_spec.get('key_properties', [])
            streams.append(
                CatalogEntry(
                    tap_stream_id=table_spec['name'],
                    stream=table_spec['name'],
                    schema=schema,
                    key_properties=key_properties,
                    metadata=stream_metadata,
                    replication_key=None,
                    is_view=None,
                    database=None,
                    table=None,
                    row_count=None,
                    stream_alias=None,
                    replication_method=None,
                )
            )
        except Exception as err:
            LOGGER.error(f"Unable to write Catalog entry for '{table_spec['name']}' - it will be skipped due to error {err}")

    return Catalog(streams)


def sync(config, state, catalog):
    # Loop over selected streams in catalog
    LOGGER.info(f"Processing {len(list(catalog.get_selected_streams(state)))} selected streams from Catalog")
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        catalog_schema = stream.schema.to_dict()
        table_spec = next((x for x in config['tables'] if x['name'] == stream.tap_stream_id), None)
        if table_spec is not None:
            # Allow updates to our tables specification to override any previously extracted schema in the catalog
            merged_schema = override_schema_with_config(catalog_schema, table_spec)
            singer.write_schema(
                stream_name=stream.tap_stream_id,
                schema=merged_schema,
                key_properties=stream.key_properties,
            )
            modified_since = dateutil.parser.parse(
                state.get(stream.tap_stream_id, {}).get('modified_since') or table_spec['start_date'])
            target_files = file_utils.get_matching_objects(table_spec, modified_since)
            max_records_per_run = table_spec.get('max_records_per_run', -1)
            records_streamed = 0

            should_output_batch = table_spec.get('batch', False)
            if should_output_batch:
                process_batch(stream, target_files, table_spec, config, state)
            else:
                for t_file in target_files:
                    records_streamed += file_utils.write_file(t_file['key'], table_spec, merged_schema, max_records=max_records_per_run-records_streamed)
                    if 0 < max_records_per_run <= records_streamed:
                        LOGGER.info(f'Processed the per-run limit of {records_streamed} records for stream "{stream.tap_stream_id}". Stopping sync for this stream.')
                        break
                    state[stream.tap_stream_id] = {'modified_since': t_file['last_modified'].isoformat()}
                    singer.write_state(state)

                LOGGER.info(f'Wrote {records_streamed} records for stream "{stream.tap_stream_id}".')
        else:
            LOGGER.warn(f'Skipping processing for stream [{stream.tap_stream_id}] without a config block.')
    return

REQUIRED_CONFIG_KEYS = 'tables'

@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args([REQUIRED_CONFIG_KEYS])
    crawl_paths = [x for x in args.config['tables'] if "crawl_config" in x and x["crawl_config"]]
    if len(crawl_paths) > 0: # Our config includes at least one crawl block
        LOGGER.info("Executing experimental 'crawl' mode to auto-generate a table config per bucket.")
        tables_config = file_utils.config_by_crawl(crawl_paths)
        # Add back in the non-crawl blocks
        tables_config['tables'] += [x for x in args.config['tables'] if "crawl_config" not in x or not x["crawl_config"]]
        crawl_results_file = "crawled-config.json"
        LOGGER.info(f"Writing expanded crawl blocks to {crawl_results_file}.")
        Config.dump(tables_config, open(crawl_results_file, "w"))
    else:
        tables_config = args.config

    tables_config = Config.validate(tables_config)
    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover(tables_config)
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
            LOGGER.info(f"Using supplied catalog {args.catalog_path}.")
        else:
            LOGGER.info(f"Generating catalog through sampling.")
            catalog = discover(tables_config)
        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug(f"Catalog has streams: {catalog.to_dict()}")
        sync(tables_config, args.state, catalog)

if __name__ == "__main__":
    main()
