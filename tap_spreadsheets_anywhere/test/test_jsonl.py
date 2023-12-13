import logging
import unittest

from tap_spreadsheets_anywhere import format_handler

LOGGER = logging.getLogger(__name__)

TEST_TABLE_SPEC = {
    "tables": [
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "json_sample_multiple_records",
            "pattern": "sample\\.json",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "json"
        },
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "json_sample_one_record",
            "pattern": "one-row-sample\\.json",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "json"
        },
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "jsonl_sample_multiple_records",
            "pattern": "sample-jsonl\\.json",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "jsonl",
            "universal_newlines": True
        },
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "jsonl_sample_one_record",
            "pattern": "one-row-sample-jsonl\\.json",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "jsonl"
        },
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "jsonl_sample_multiple_records_detect",
            "pattern": "sample\\.jsonl",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "detect"
        },
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "jsonl_sample_one_record_detect",
            "pattern": "one-row-sample\\.jsonl",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "detect"
        },
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "jsonl_sample_with_array",
            "pattern": "sample-array\\.jsonl",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "detect"
        },
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "jsonl_sample_with_object",
            "pattern": "sample-object\\.jsonl",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "detect"
        }
    ]
}


class TestJsonFormatHandler(unittest.TestCase):

    def test_json_file(self):
        test_filename_uri = './tap_spreadsheets_anywhere/test/sample.json'
        iterator = format_handler.get_row_iterator(TEST_TABLE_SPEC['tables'][0], test_filename_uri)
        expected_row_count = 6
        row_count = 0
        for row in iterator:
            row_count += 1
            self.assertIsNotNone(row['id'], f"ID field is None for row {row}")
        self.assertEqual(expected_row_count, row_count, f"Expected row_count to be {expected_row_count} but was {row_count}")

    def test_one_row_json_file(self):
        test_filename_uri = './tap_spreadsheets_anywhere/test/one-row-sample.json'
        iterator = format_handler.get_row_iterator(TEST_TABLE_SPEC['tables'][1], test_filename_uri)
        expected_row_count = 1
        row_count = 0
        for row in iterator:
            row_count += 1
            self.assertEqual(3884, row['id'], f"ID field is {row['id']} - expected it to be 3884.")
        self.assertEqual(expected_row_count, row_count, f"Expected row_count to be {expected_row_count} but was {row_count}")

    def test_jsonl_file(self):
        test_filename_uri = './tap_spreadsheets_anywhere/test/sample-jsonl.json'
        iterator = format_handler.get_row_iterator(TEST_TABLE_SPEC['tables'][2], test_filename_uri)
        expected_row_count = 6
        row_count = 0
        for row in iterator:
            row_count += 1
            self.assertIsNotNone(row['id'], f"ID field is None for row {row}")
        self.assertEqual(expected_row_count, row_count, f"Expected row_count to be {expected_row_count} but was {row_count}")

    def test_one_row_jsonl_file(self):
        test_filename_uri = './tap_spreadsheets_anywhere/test/one-row-sample-jsonl.json'
        iterator = format_handler.get_row_iterator(TEST_TABLE_SPEC['tables'][3], test_filename_uri)
        expected_row_count = 1
        row_count = 0
        for row in iterator:
            row_count += 1
            self.assertEqual(3884, row['id'], f"ID field is {row['id']} - expected it to be 3884.")
        self.assertEqual(expected_row_count, row_count, f"Expected row_count to be {expected_row_count} but was {row_count}")

    def test_jsonl_file_detect(self):
        test_filename_uri = './tap_spreadsheets_anywhere/test/sample.jsonl'
        iterator = format_handler.get_row_iterator(TEST_TABLE_SPEC['tables'][4], test_filename_uri)
        expected_row_count = 6
        row_count = 0
        for row in iterator:
            row_count += 1
            self.assertIsNotNone(row['id'], f"ID field is None for row {row}")
        self.assertEqual(expected_row_count, row_count, f"Expected row_count to be {expected_row_count} but was {row_count}")

    def test_one_row_jsonl_file_detect(self):
        test_filename_uri = './tap_spreadsheets_anywhere/test/one-row-sample.jsonl'
        iterator = format_handler.get_row_iterator(TEST_TABLE_SPEC['tables'][5], test_filename_uri)
        expected_row_count = 1
        row_count = 0
        for row in iterator:
            row_count += 1
            self.assertEqual(3884, row['id'], f"ID field is {row['id']} - expected it to be 3884.")
        self.assertEqual(expected_row_count, row_count, f"Expected row_count to be {expected_row_count} but was {row_count}")

    def test_jsonl_with_array(self):
        """
        Verify arrays are propagated without serializing them to strings.
        """
        test_filename_uri = './tap_spreadsheets_anywhere/test/type-array.jsonl'
        iterator = format_handler.get_row_iterator(TEST_TABLE_SPEC['tables'][6], test_filename_uri)
        records = list(iterator)
        expected_row_count = 3
        row_count = len(records)
        self.assertEqual({"id": 1, "value": [1.1, 2.1, 1.1, 1.3]}, records[0])
        self.assertEqual(expected_row_count, row_count, f"Expected row_count to be {expected_row_count} but was {row_count}")

    def test_jsonl_with_object(self):
        """
        Verify objects are propagated without serializing them to strings.
        """
        test_filename_uri = './tap_spreadsheets_anywhere/test/type-object.jsonl'
        iterator = format_handler.get_row_iterator(TEST_TABLE_SPEC['tables'][7], test_filename_uri)
        records = list(iterator)
        expected_row_count = 6
        row_count = len(records)
        self.assertEqual([
            {"id": 1, "value": {"string": "foo"}},
            {"id": 2, "value": {"integer": 42}},
            {"id": 3, "value": {"float": 42.42}},
            {"id": 4, "value": {"boolean": True}},
            {"id": 5, "value": {"nested-array": [1, 2, 3]}},
            {"id": 6, "value": {"nested-object": {"foo": "bar"}}},
        ], records)
        self.assertEqual(expected_row_count, row_count, f"Expected row_count to be {expected_row_count} but was {row_count}")
