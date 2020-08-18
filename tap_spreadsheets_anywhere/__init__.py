#!/usr/bin/env python3
import os

import dateutil
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

import tap_spreadsheets_anywhere.tables_config_util as tables_config_util
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
        target_files = file_utils.get_input_files_for_table(table_spec, modified_since)
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
        target_files = file_utils.get_input_files_for_table(table_spec, modified_since)
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
    tables_config = tables_config_util.validate(args.config)

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
