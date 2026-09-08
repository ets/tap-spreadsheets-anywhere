import logging
import os

import dateutil
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from tap_spreadsheets_anywhere import arrow_batch, conversion, file_utils, format_handler
from tap_spreadsheets_anywhere.configuration import Config
from tap_spreadsheets_anywhere.record_sink import SingerRecordSink

LOGGER = logging.getLogger(__name__)

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)


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
    override_schema = {
        "properties": table_spec.get("schema_overrides", {}),
        "selected": table_spec.get("selected", True),
    }
    # Note that we directly support setting selected through config so that this tap is useful outside Meltano
    return merge_dicts(inferred_schema, override_schema)


def generate_schema(table_spec, samples):
    metadata_schema = {
        "_smart_source_bucket": {"type": "string"},
        "_smart_source_file": {"type": "string"},
        "_smart_source_lineno": {"type": "integer"},
        "_smart_source_last_modified": {"type": "string", "format": "date-time"},
    }
    prefer_number_vs_integer = table_spec.get("prefer_number_vs_integer", False)
    prefer_schema_as_string = table_spec.get("prefer_schema_as_string", False)
    data_schema = conversion.generate_schema(
        samples,
        prefer_number_vs_integer=prefer_number_vs_integer,
        prefer_schema_as_string=prefer_schema_as_string,
    )
    inferred_schema = {
        "type": "object",
        "properties": merge_dicts(data_schema, metadata_schema),
    }

    merged_schema = override_schema_with_config(inferred_schema, table_spec)
    return Schema.from_dict(merged_schema)


def discover(config, state=None):
    streams = []
    for table_spec in config["tables"]:
        try:
            state_modified_since = (
                (state or {}).get(table_spec["name"], {}).get("modified_since")
                if table_spec.get("state_based_discovery")
                else None
            )
            sample_rate = table_spec.get("sample_rate", 5)
            max_sampling_read = table_spec.get("max_sampling_read", 1000)
            max_sampled_files = table_spec.get("max_sampled_files", 50)
            modified_since = dateutil.parser.parse(state_modified_since or table_spec["start_date"])
            target_files = file_utils.get_matching_objects(table_spec, modified_since)
            samples = file_utils.sample_files(
                table_spec,
                target_files,
                table_spec.get("ignore_undefined_field_names", False),
                sample_rate=sample_rate,
                max_records=max_sampling_read,
                max_files=max_sampled_files,
            )
            schema = generate_schema(table_spec, samples)
            stream_metadata = []
            key_properties = table_spec.get("key_properties", [])
            streams.append(
                CatalogEntry(
                    tap_stream_id=table_spec["name"],
                    stream=table_spec["name"],
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
            LOGGER.error(
                "Unable to write Catalog entry for '%s' - it will be skipped due to error %s",
                table_spec["name"],
                err,
            )
            raise

    return Catalog(streams)


def sync(config, state, catalog):
    # Built once for the whole run: presence of `batch_config` opts every stream
    # into Singer BATCH (Arrow) output instead of per-row RECORD messages.
    batch_config = arrow_batch.BatchConfig.from_config(config)

    # Loop over selected streams in catalog
    LOGGER.info("Processing %d selected streams from Catalog", len(list(catalog.get_selected_streams(state))))
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream: %s", stream.tap_stream_id)
        catalog_schema = stream.schema.to_dict()
        state_modified_since = state.get(stream.tap_stream_id, {}).get("modified_since")
        table_specs = [t for t in config["tables"] if t["name"] == stream.tap_stream_id]

        if not table_specs:
            LOGGER.warning("Skipping processing for stream [%s] without a config block.", stream.tap_stream_id)
            continue

        for table_spec in table_specs:
            # Allow updates to our tables specification to override any previously extracted schema in the catalog
            merged_schema = override_schema_with_config(catalog_schema, table_spec)
            singer.write_schema(
                stream_name=stream.tap_stream_id,
                schema=merged_schema,
                key_properties=stream.key_properties,
            )
            modified_since = dateutil.parser.parse(
                table_spec["start_date"]
                if table_spec.get("ignore_state", False)
                else (state_modified_since or table_spec["start_date"])
            )

            sink = (
                arrow_batch.ArrowBatchWriter(stream.tap_stream_id, merged_schema, batch_config)
                if batch_config is not None
                else SingerRecordSink(stream.tap_stream_id)
            )

            target_files = file_utils.get_matching_objects(table_spec, modified_since)
            max_records_per_run = table_spec.get("max_records_per_run", -1)
            records_streamed = 0
            for t_file in target_files:
                last_modified_iso = t_file["last_modified"].isoformat()
                records_streamed += file_utils.write_file(
                    t_file["key"],
                    last_modified_iso,
                    table_spec,
                    merged_schema,
                    max_records=max_records_per_run - records_streamed,
                    sink=sink,
                )
                if 0 < max_records_per_run <= records_streamed:
                    LOGGER.info(
                        'Processed the per-run limit of %d records for stream "%s". Stopping sync for this stream.',
                        records_streamed,
                        stream.tap_stream_id,
                    )
                    break
                state[stream.tap_stream_id] = {"modified_since": last_modified_iso}
                # TODO: when processing multiple table configs for the same stream, it
                # is not safe to write state like this as target files for each config
                # are implicitly ordered, and by processing multiple configs, this order
                # is invalidated at the start of every next one
                # we should resolve all target files by config first, sort by file last
                # modified value and then write records/state, since we would guarantee
                # total ordering, therefore allowing us to safely emit state after
                # processing each file
                singer.write_state(state)

            sink.flush()
            LOGGER.info('Wrote %d records for stream "%s".', records_streamed, stream.tap_stream_id)


REQUIRED_CONFIG_KEYS = "tables"


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args([REQUIRED_CONFIG_KEYS])
    crawl_paths = [x for x in args.config["tables"] if x.get("crawl_config")]
    if len(crawl_paths) > 0:  # Our config includes at least one crawl block
        LOGGER.info("Executing experimental 'crawl' mode to auto-generate a table config per bucket.")
        tables_config = file_utils.config_by_crawl(crawl_paths)
        # Add back in the non-crawl blocks
        tables_config["tables"] += [
            x for x in args.config["tables"] if "crawl_config" not in x or not x["crawl_config"]
        ]
        crawl_results_file = "crawled-config.json"
        LOGGER.info("Writing expanded crawl blocks to %s.", crawl_results_file)
        with open(crawl_results_file, "w") as f:
            Config.dump(tables_config, f)
    else:
        tables_config = args.config

    tables_config = Config.validate(tables_config)
    format_handler.set_config(tables_config)
    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover(tables_config, args.state)
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
            LOGGER.info("Using supplied catalog %s.", args.catalog_path)
        else:
            LOGGER.info("Generating catalog through sampling.")
            catalog = discover(tables_config, args.state)
        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug("Catalog has streams: %s", catalog.to_dict())
        sync(tables_config, args.state, catalog)


if __name__ == "__main__":
    main()
