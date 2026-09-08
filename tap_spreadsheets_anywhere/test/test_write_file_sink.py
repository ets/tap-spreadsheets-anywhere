import unittest
from unittest.mock import patch

from tap_spreadsheets_anywhere import file_utils
from tap_spreadsheets_anywhere.record_sink import SingerRecordSink

TABLE_SPEC = {
    "path": "file://./does-not-matter",
    "name": "my_stream",
    "format": "csv",
}
SCHEMA = {"properties": {"id": {"type": ["null", "string"]}}}


class FakeSink:
    def __init__(self):
        self.written = []
        self.flushed = False

    def write(self, record):
        self.written.append(record)

    def flush(self):
        self.flushed = True


class TestWriteFileSink(unittest.TestCase):
    def _rows(self, *rows):
        return patch.object(
            file_utils.tap_spreadsheets_anywhere.format_handler,
            "get_row_iterator",
            return_value=iter(rows),
        )

    def test_writes_each_row_to_the_provided_sink(self):
        sink = FakeSink()
        with self._rows({"id": "1"}, {"id": "2"}):
            records_synced = file_utils.write_file("f.csv", "2024-01-01T00:00:00Z", TABLE_SPEC, SCHEMA, sink=sink)

        self.assertEqual(records_synced, 2)
        self.assertEqual([r["id"] for r in sink.written], ["1", "2"])
        self.assertEqual(sink.written[0]["_smart_source_file"], "f.csv")

    def test_respects_max_records(self):
        sink = FakeSink()
        with self._rows({"id": "1"}, {"id": "2"}, {"id": "3"}):
            records_synced = file_utils.write_file(
                "f.csv",
                "2024-01-01T00:00:00Z",
                TABLE_SPEC,
                SCHEMA,
                max_records=2,
                sink=sink,
            )

        self.assertEqual(records_synced, 2)
        self.assertEqual(len(sink.written), 2)

    def test_defaults_to_a_singer_record_sink_when_none_provided(self):
        calls = []
        with (
            self._rows({"id": "1"}),
            patch(
                "tap_spreadsheets_anywhere.record_sink.singer.write_record",
                side_effect=lambda stream, record: calls.append((stream, record)),
            ),
        ):
            records_synced = file_utils.write_file("f.csv", "2024-01-01T00:00:00Z", TABLE_SPEC, SCHEMA)

        self.assertEqual(records_synced, 1)
        self.assertEqual(calls[0][0], "my_stream")
        self.assertEqual(calls[0][1]["id"], "1")

    def test_write_file_never_constructs_its_own_singer_record_sink_by_name(self):
        # Guards against the default sink silently swallowing errors: a broken
        # pipe from the sink should still propagate out of write_file.
        class BrokenPipeSink(SingerRecordSink):
            def write(self, record):
                raise BrokenPipeError()

        with self._rows({"id": "1"}), self.assertRaises(BrokenPipeError):
            file_utils.write_file(
                "f.csv",
                "2024-01-01T00:00:00Z",
                TABLE_SPEC,
                SCHEMA,
                sink=BrokenPipeSink("my_stream"),
            )


if __name__ == "__main__":
    unittest.main()
