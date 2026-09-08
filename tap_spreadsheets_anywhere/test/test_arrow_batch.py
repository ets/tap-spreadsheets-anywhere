import json
import os
import tempfile
import unittest

import pyarrow as pa
from pyarrow import ipc

from tap_spreadsheets_anywhere.arrow_batch import (
    ArrowBatchWriter,
    BatchConfig,
    build_batch_message,
    rows_to_arrow_table,
    schema_to_arrow_schema,
    write_arrow_ipc_file,
    write_message,
)


class TestBatchConfig(unittest.TestCase):
    def test_from_config_returns_none_without_batch_config(self):
        self.assertIsNone(BatchConfig.from_config({}))

    def test_from_config_applies_defaults(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            batch_config = BatchConfig.from_config(
                {
                    "batch_config": {"storage": {"root": tmp_dir}},
                }
            )
            self.assertEqual(batch_config.format, "arrow")
            self.assertEqual(batch_config.batch_root_dir, tmp_dir)
            self.assertGreater(batch_config.batch_size, 0)

    def test_from_config_honors_overrides(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            batch_config = BatchConfig.from_config(
                {
                    "batch_config": {
                        "encoding": {"format": "arrow"},
                        "storage": {"root": tmp_dir},
                        "batch_size": 10,
                    },
                }
            )
            self.assertEqual(batch_config.batch_size, 10)
            self.assertEqual(batch_config.batch_root_dir, tmp_dir)

    def test_rejects_unsupported_format(self):
        with tempfile.TemporaryDirectory() as tmp_dir, self.assertRaises(ValueError):
            BatchConfig(batch_size=10, batch_root_dir=tmp_dir, format="jsonl")

    def test_rejects_nonpositive_batch_size(self):
        with tempfile.TemporaryDirectory() as tmp_dir, self.assertRaises(ValueError):
            BatchConfig(batch_size=0, batch_root_dir=tmp_dir)

    def test_rejects_missing_batch_root_dir(self):
        with self.assertRaises(ValueError):
            BatchConfig(batch_size=10, batch_root_dir="/no/such/directory/exists")


class TestSchemaToArrowSchema(unittest.TestCase):
    def test_maps_primitive_types(self):
        schema = {
            "properties": {
                "an_int": {"type": ["null", "integer"]},
                "a_number": {"type": ["null", "number"]},
                "a_bool": {"type": ["null", "boolean"]},
                "a_string": {"type": ["null", "string"]},
                "a_date": {"type": ["null", "string"], "format": "date-time"},
                "untyped": {},
            },
        }
        arrow_schema = schema_to_arrow_schema(schema)
        by_name = {field.name: field.type for field in arrow_schema}

        self.assertEqual(by_name["an_int"], pa.int64())
        self.assertEqual(by_name["a_number"], pa.float64())
        self.assertEqual(by_name["a_bool"], pa.bool_())
        self.assertEqual(by_name["a_string"], pa.string())
        self.assertEqual(by_name["a_date"], pa.string())
        self.assertEqual(by_name["untyped"], pa.string())

    def test_handles_bare_string_type(self):
        schema = {"properties": {"field": {"type": "integer"}}}
        arrow_schema = schema_to_arrow_schema(schema)
        self.assertEqual(arrow_schema.field("field").type, pa.int64())


class TestRowsToArrowTable(unittest.TestCase):
    def test_builds_table_matching_schema(self):
        arrow_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": None}]

        table = rows_to_arrow_table(rows, arrow_schema)

        self.assertEqual(table.schema, arrow_schema)
        self.assertEqual(table.column("id").to_pylist(), [1, 2])
        self.assertEqual(table.column("name").to_pylist(), ["a", None])

    def test_json_encodes_nested_values(self):
        arrow_schema = pa.schema([pa.field("payload", pa.string())])
        rows = [{"payload": {"k": "v"}}]

        table = rows_to_arrow_table(rows, arrow_schema)

        self.assertEqual(json.loads(table.column("payload")[0].as_py()), {"k": "v"})


class TestWriteArrowIpcFile(unittest.TestCase):
    def test_round_trips_table(self):
        table = pa.table({"id": [1, 2, 3]})
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "out.arrow")
            size_bytes = write_arrow_ipc_file(table, path)

            self.assertGreater(size_bytes, 0)
            with ipc.open_file(path) as reader:
                self.assertEqual(reader.read_all().column("id").to_pylist(), [1, 2, 3])


class TestBuildBatchMessage(unittest.TestCase):
    def test_shape(self):
        message = build_batch_message("my_stream", ["file:///a.arrow"])
        self.assertEqual(
            message,
            {
                "type": "BATCH",
                "stream": "my_stream",
                "encoding": {"format": "arrow"},
                "manifest": ["file:///a.arrow"],
            },
        )


class FakeOutput:
    def __init__(self):
        self.lines = []
        self.flushed = False

    def write(self, data):
        self.lines.append(data)

    def flush(self):
        self.flushed = True


class TestWriteMessage(unittest.TestCase):
    def test_writes_json_line_and_flushes(self):
        output = FakeOutput()
        write_message({"type": "BATCH"}, output)
        self.assertEqual(json.loads(output.lines[0]), {"type": "BATCH"})
        self.assertTrue(output.flushed)


class TestArrowBatchWriter(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp_dir.cleanup)
        self.schema = {"properties": {"id": {"type": ["null", "integer"]}}}

    def _writer(self, batch_size, output):
        batch_config = BatchConfig(batch_size=batch_size, batch_root_dir=self.tmp_dir.name)
        return ArrowBatchWriter("my_stream", self.schema, batch_config, output=output)

    def test_flush_with_no_rows_is_a_noop(self):
        output = FakeOutput()
        writer = self._writer(batch_size=10, output=output)
        writer.flush()
        self.assertEqual(output.lines, [])

    def test_auto_flushes_at_batch_size(self):
        output = FakeOutput()
        writer = self._writer(batch_size=2, output=output)

        writer.write({"id": 1})
        self.assertEqual(output.lines, [], "should not flush before batch_size rows are buffered")
        writer.write({"id": 2})

        self.assertEqual(len(output.lines), 1)
        message = json.loads(output.lines[0])
        self.assertEqual(message["type"], "BATCH")
        self.assertEqual(message["stream"], "my_stream")
        manifest_path = message["manifest"][0].removeprefix("file://")
        with ipc.open_file(manifest_path) as reader:
            self.assertEqual(reader.read_all().column("id").to_pylist(), [1, 2])

    def test_explicit_flush_emits_partial_batch(self):
        output = FakeOutput()
        writer = self._writer(batch_size=10, output=output)

        writer.write({"id": 1})
        writer.flush()

        self.assertEqual(len(output.lines), 1)
        writer.flush()
        self.assertEqual(
            len(output.lines),
            1,
            "a second flush with no new rows should not emit another message",
        )

    def test_each_flush_writes_a_new_file(self):
        output = FakeOutput()
        writer = self._writer(batch_size=1, output=output)

        writer.write({"id": 1})
        writer.write({"id": 2})

        paths = [json.loads(line)["manifest"][0] for line in output.lines]
        self.assertEqual(len(set(paths)), 2)


if __name__ == "__main__":
    unittest.main()
