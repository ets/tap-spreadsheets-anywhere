#!/usr/bin/env python3
import os

import dateutil
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from tap_spreadsheets_anywhere.configuration import Config
import tap_spreadsheets_anywhere.conversion as conversion
import tap_spreadsheets_anywhere.file_utils as file_utils

LOGGER = singer.get_logger()


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


def discover(config):
    streams = []
    for table_spec in config['tables']:
        modified_since = dateutil.parser.parse(table_spec['start_date'])
        target_files = file_utils.get_matching_objects(table_spec, modified_since)
        sample_rate = table_spec.get('sample_rate',10)
        max_sampling_read = table_spec.get('max_sampling_read', 1000)
        max_sampled_files = table_spec.get('max_sampled_files', 5)
        prefer_number_vs_integer = table_spec.get('prefer_number_vs_integer', False)
        samples = file_utils.sample_files(table_spec, target_files,sample_rate=sample_rate,
                                          max_records=max_sampling_read, max_files=max_sampled_files)

        metadata_schema = {
            '_smart_source_bucket': {'type': 'string'},
            '_smart_source_file': {'type': 'string'},
            '_smart_source_lineno': {'type': 'integer'},
        }
        data_schema = conversion.generate_schema(samples,prefer_number_vs_integer=prefer_number_vs_integer)
        inferred_schema = {
            'type': 'object',
            'properties': merge_dicts(data_schema, metadata_schema)
        }

        merged_schema = override_schema_with_config(inferred_schema, table_spec)
        schema = Schema.from_dict(merged_schema)

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
    return Catalog(streams)


def sync(config, state, catalog):
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        catalog_schema = stream.schema.to_dict()
        table_spec = next((x for x in config['tables'] if x['name'] == stream.tap_stream_id), None)
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
        records_streamed = 0
        for t_file in target_files:
            records_streamed += file_utils.write_file(t_file['key'], table_spec, merged_schema)
            state[stream.tap_stream_id] = {'modified_since': t_file['last_modified'].isoformat()}
            singer.write_state(state)

        LOGGER.info(f'Wrote {records_streamed} records for table "{stream.tap_stream_id}".')
    return

REQUIRED_CONFIG_KEYS = 'tables'

@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args([REQUIRED_CONFIG_KEYS])
    if len(args.config['crawl']) > 0:
        LOGGER.info("Executing experimental 'crawl' mode to auto-generate a table per bucket.")
        config = {'tables': []}
        for source in args.config['crawl']:
            entries = {}
            target_files = file_utils.get_matching_objects(source, modified_since=source['modified_since'] if 'modified_since' in source else None )
            for file in target_files:
                if not file['key'].endswith('/'):
                    target_uri = source['path'] + '/' + file['key']
                    dirs = file['key'].split('/')
                    table = "_".join(dirs[0:-1])
                    directory = "/".join(dirs[0:-1])
                    parts = file['key'].split('.')
                    # group all files in the same directory and with the same extension
                    if len(parts) > 1:
                        pattern = '.*\.'+parts[-1]
                    else:
                        pattern = parts[0]
                    if table not in entries:
                        entries[table] = {
                            "path": source['path'],
                            "name": table,
                            "pattern": directory + '/' + pattern,
                            "key_properties": [],
                            "format": "detect",
                            "delimiter": "detect"
                        }
                    else:
                        if (directory + '/' + pattern) != entries[table]["pattern"]:
                            unique_table = table+'_'+pattern
                            entries[unique_table] = {
                                "path": source['path'],
                                "name": unique_table,
                                "pattern": directory + '/' + pattern,
                                "key_properties": [],
                                "format": "detect",
                                "delimiter": "detect"
                            }
                else:
                    LOGGER.debug(f"Skipping config for {file['key']} because it looks like a folder not a file")
            config['tables'] += entries.values()
        Config.dump(config)
    else:
        tables_config = Config.validate(args.config)
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
            sync(tables_config, args.state, catalog)

if __name__ == "__main__":
    main()
