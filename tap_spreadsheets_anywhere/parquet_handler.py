import re
import logging
import pyarrow.parquet as pq

LOGGER = logging.getLogger(__name__)


def generator_wrapper(table, _={}) -> dict:
    # change column name
    def format_name(name=""):
        formatted_key = re.sub(r"[^\w\s]", "", name)
        # replace whitespace with underscores
        formatted_key = re.sub(r"\s+", "_", formatted_key)
        return formatted_key.lower()

    table = table.rename_columns([format_name(name) for name in table.column_names])

    for row in table.to_pylist():
        yield row


def get_row_iterator(table_spec, file_handle):
    try:
        parquet_file = pq.ParquetFile(file_handle)
    except Exception as e:
        LOGGER.error("Unable to read the Parquet file: %s", e)
        raise e

    # Use batch to read the Parquet file
    for batch in parquet_file.iter_batches():
        yield from generator_wrapper(batch, table_spec)
