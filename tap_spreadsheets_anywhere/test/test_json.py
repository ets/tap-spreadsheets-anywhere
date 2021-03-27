import unittest

import dateutil
from io import StringIO
from tap_spreadsheets_anywhere import configuration, file_utils, csv_handler, json_handler

TEST_TABLE_SPEC = {
    "tables": [
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "badnewlines",
            "pattern": ".*\\.xlsx",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "excel",
            "worksheet_name": "sample_with_bad_newlines"
        },
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "badnewlines",
            "pattern": ".*\\.json",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "detect"
        },
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "nestedlist",
            "pattern": ".*\\.json",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "json_path": "someKey",
            "format": "detect"
        }
    ]
}


class TestFormatHandler(unittest.TestCase):

    def test_json_flat_array(self):
        reader = StringIO('[{"k":"v"},{"k":"v"},{"k":"v"}]')
        json_handler.get_row_iterator(TEST_TABLE_SPEC['tables'][0], reader)

    def test_json_object_lists(self):
        reader = StringIO('{"k":"v"}\n{"k":"v"}\n{"k":"v"}')
        json_handler.get_row_iterator(TEST_TABLE_SPEC['tables'][0], reader)

    def test_json_nested_array(self):
        reader = StringIO('{"someKey": [{"k":"v"},{"k":"v"},{"k":"v"}]}')
        iterator = json_handler.get_row_iterator(TEST_TABLE_SPEC['tables'][2], reader)
        for row in iterator:
            self.assertEqual(row['k'], 'v')
