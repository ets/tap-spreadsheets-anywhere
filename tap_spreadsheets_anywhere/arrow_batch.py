"""Singer BATCH message support, encoding rows as Arrow IPC files.

Follows the same `batch_config` shape used by the Meltano Singer SDK
(https://sdk.meltano.com) and already adopted by tap-mysql for its own
hand-rolled BATCH support, since this tap (like tap-mysql) predates
singer-sdk and has no `BaseBatcher`/`get_batches` to build on:

    {"batch_config": {"encoding": {"format": "arrow"},
                      "storage": {"root": "/path/to/dir"},
                      "batch_size": 100000}}

Only `format: arrow` is supported; other singer-sdk batch encodings
(`jsonl`, `parquet`) are intentionally out of scope for now.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os
import sys
import tempfile
import uuid
from typing import Any

import pyarrow as pa
from pyarrow import ipc

LOGGER = logging.getLogger(__name__)

BATCH_FORMATS = frozenset({"arrow"})
DEFAULT_BATCH_SIZE = 100_000


@dataclasses.dataclass(frozen=True)
class BatchConfig:
    """Validated configuration for Singer BATCH message mode."""

    batch_size: int
    batch_root_dir: str = "."
    format: str = "arrow"

    def __post_init__(self):
        if self.batch_size <= 0:
            raise ValueError(f"batch_config.batch_size must be a positive integer, got {self.batch_size}")
        if not os.path.isdir(self.batch_root_dir):
            raise ValueError(f"batch_config.storage.root does not exist or is not a directory: {self.batch_root_dir!r}")
        if self.format not in BATCH_FORMATS:
            raise ValueError(
                f"batch_config.encoding.format must be one of {sorted(BATCH_FORMATS)}, got {self.format!r}"
            )

    @classmethod
    def from_config(cls, config: dict) -> BatchConfig | None:
        """Return a BatchConfig if the tap config sets `batch_config`, else None (RECORD mode)."""
        raw = config.get("batch_config")
        if raw is None:
            return None

        encoding = raw.get("encoding") or {}
        storage = raw.get("storage") or {}
        batch_config = cls(
            batch_size=int(raw.get("batch_size", DEFAULT_BATCH_SIZE)),
            batch_root_dir=storage.get("root", tempfile.gettempdir()),
            format=encoding.get("format", "arrow"),
        )
        LOGGER.info(
            "Using batch_config: format=%s, batch_size=%d, root=%s",
            batch_config.format,
            batch_config.batch_size,
            batch_config.batch_root_dir,
        )
        return batch_config


def _pick_arrow_type(property_schema: dict) -> pa.DataType:
    """Map a single Singer JSON-schema property definition to an Arrow type."""
    if property_schema.get("format") == "date-time":
        # Values are already ISO-8601 strings by the time they reach us (see
        # conversion.convert_row), so represent them as strings here too -
        # keeps BATCH output consistent with RECORD output for the same data.
        return pa.string()

    declared_types = property_schema.get("type", ["null", "string"])
    if isinstance(declared_types, str):
        declared_types = [declared_types]
    non_null_types = [t for t in declared_types if t != "null"] or ["string"]
    json_type = non_null_types[0]

    return {
        "integer": pa.int64(),
        "number": pa.float64(),
        "boolean": pa.bool_(),
    }.get(json_type, pa.string())


def schema_to_arrow_schema(schema: dict) -> pa.Schema:
    """Map a Singer JSON schema (as produced by tap_spreadsheets_anywhere.generate_schema) to an Arrow schema."""
    properties = schema.get("properties", {})
    fields = [pa.field(name, _pick_arrow_type(prop), nullable=True) for name, prop in properties.items()]
    return pa.schema(fields)


def rows_to_arrow_table(rows: list[dict], arrow_schema: pa.Schema) -> pa.Table:
    """Build a pyarrow Table from a list of row dicts, matching `arrow_schema`.

    Values holding nested structures (dict/list, from a Singer 'object' typed
    property) are JSON-encoded, since they're mapped to Arrow strings above.
    """
    arrays = []
    for field in arrow_schema:
        values = []
        for row in rows:
            value = row.get(field.name)
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            values.append(value)
        arrays.append(pa.array(values, type=field.type))

    return pa.Table.from_arrays(arrays, schema=arrow_schema)


def write_arrow_ipc_file(table: pa.Table, path: str) -> int:
    """Write `table` to `path` in Arrow IPC file format. Returns the file size in bytes."""
    with pa.OSFile(path, "wb") as sink, ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return os.path.getsize(path)


def build_batch_message(stream_name: str, manifest_paths: list[str]) -> dict:
    """Build the raw Singer BATCH message dict for `manifest_paths`."""
    return {
        "type": "BATCH",
        "stream": stream_name,
        "encoding": {"format": "arrow"},
        "manifest": manifest_paths,
    }


def write_message(message: dict, output: Any = None) -> None:
    """Write a raw Singer message dict as a JSON line to `output` (defaults to stdout).

    BATCH isn't a message type singer-python (the legacy library this tap is
    built on) knows how to serialize, so - mirroring tap-mysql's approach -
    we bypass its typed Message classes and write the JSON line ourselves.
    """
    stream = output if output is not None else sys.stdout
    stream.write(json.dumps(message) + "\n")
    stream.flush()


class ArrowBatchWriter:
    """Buffers rows and emits Singer BATCH messages backed by Arrow IPC files.

    Mirrors the `.write(record)` / `.flush()` contract expected of a "record
    sink" (see `record_sink.SingerRecordSink`), so callers can swap between
    BATCH and RECORD mode without branching on which one is active.
    """

    def __init__(
        self,
        stream_name: str,
        schema: dict,
        batch_config: BatchConfig,
        output: Any = None,
    ):
        self.stream_name = stream_name
        self._arrow_schema = schema_to_arrow_schema(schema)
        self._batch_config = batch_config
        self._output = output
        self._rows: list[dict] = []

    def write(self, record: dict) -> None:
        self._rows.append(record)
        if len(self._rows) >= self._batch_config.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._rows:
            return

        table = rows_to_arrow_table(self._rows, self._arrow_schema)
        filename = f"tap-spreadsheets-anywhere-{uuid.uuid4().hex}.arrow"
        path = os.path.join(self._batch_config.batch_root_dir, filename)
        size_bytes = write_arrow_ipc_file(table, path)
        LOGGER.info(
            "Wrote Arrow batch file: %s (%d rows, %d bytes)",
            path,
            len(self._rows),
            size_bytes,
        )

        write_message(build_batch_message(self.stream_name, [f"file://{path}"]), self._output)
        self._rows = []
