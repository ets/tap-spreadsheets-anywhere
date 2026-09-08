import unittest

from tap_spreadsheets_anywhere.record_sink import SingerRecordSink


class TestSingerRecordSink(unittest.TestCase):
    def test_write_delegates_to_injected_write_record(self):
        calls = []
        sink = SingerRecordSink(
            "my_stream",
            write_record=lambda stream, record: calls.append((stream, record)),
        )

        sink.write({"id": 1})

        self.assertEqual(calls, [("my_stream", {"id": 1})])

    def test_flush_is_a_noop(self):
        sink = SingerRecordSink("my_stream", write_record=lambda stream, record: None)
        sink.flush()  # should not raise


if __name__ == "__main__":
    unittest.main()
