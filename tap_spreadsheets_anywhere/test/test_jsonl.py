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
