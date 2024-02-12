import codecs
import json
import logging
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import dateutil
import pytest
import smart_open
from six import StringIO

from tap_spreadsheets_anywhere import configuration, file_utils, csv_handler, json_handler, generate_schema
from tap_spreadsheets_anywhere.format_handler import monkey_patch_streamreader, get_row_iterator
from tap_spreadsheets_anywhere.test.test_excel_handler import get_worksheet


LOGGER = logging.getLogger(__name__)

TEST_TABLE_SPEC = {
    "tables": [
        {
            "path": "s3://any_bucket_willdo",
            "name": "products",
            "pattern": "g2/.*roduct.*",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": ["id"],
            "format": "csv",
            "prefer_number_vs_integer": True,
            "prefer_schema_as_string": True,
            "universal_newlines": False,
            "sample_rate": 5,
            "max_sampling_read": 2000,
            "max_sampled_files": 3,
            "schema_overrides": {
                "id": {
                    "type": "integer"
                }
            }
        },
        {
            "path": "file://./artifacts",
            "name": "badnewlines",
            "pattern": '.*\\.csv',
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "csv",
            "universal_newlines": False,
            "sample_rate": 5,
            "max_sampling_read": 2000,
            "max_sampled_files": 3
        },
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "badnewlines",
            "pattern": ".*bad_newlines\\.xlsx",
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
            "path": "https://www.treasury.gov/ofac/downloads",
            "name": "sdn",
            "pattern": "sdn.csv",
            "start_date": "1970-05-01T00:00:00Z",
            "key_properties": [],
            "format": "csv",
            "field_names": ["id","name","a" ,"country","b" ,"c" ,"d" ,"e" ,"f" ,"g" ,"h" ,"i"]
        },
        {
            "path": "https://dataverse.harvard.edu/api/access/datafile",
            "name": "dataverse",
            "pattern": "4202836",
            "start_date": "1970-05-01T00:00:00Z",
            "key_properties": [],
            "format": "csv"
        },
        {
            "path": "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/27763",
            "name": "us__military_deaths",
            "pattern": 'ADYC1Q&name=10-F-1140.xls',
            "start_date": '2014-11-04T18:38:22Z',
            "key_properties": [],
            "format": "excel",
            "worksheet_name": " Worldwide",
        },
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "error-free",
            "pattern": ".*no_errors\\.xlsx",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "excel"
        }
    ]
}


class TestFormatHandler(unittest.TestCase):

    def test_custom_config(self):
        configuration.CONFIG_CONTRACT(TEST_TABLE_SPEC)

    def test_handle_newlines_local_excel(self):
        test_filename_uri = './tap_spreadsheets_anywhere/test/excel_with_bad_newlines.xlsx'
        iterator = get_row_iterator(TEST_TABLE_SPEC['tables'][2], test_filename_uri)

        for row in iterator:
            self.assertTrue(isinstance(row['id'], float) or isinstance(row['id'], int),
                            "Parsed ID is not a number for: {}".format(row['id']))

    def test_handle_newlines_local_json(self):
        test_filename_uri = './tap_spreadsheets_anywhere/test/sample.json'
        iterator = get_row_iterator(TEST_TABLE_SPEC['tables'][3], test_filename_uri)

        for row in iterator:
            self.assertTrue(isinstance(row['id'], float) or isinstance(row['id'], int),
                            "Parsed ID is not a number for: {}".format(row['id']))

    def test_strip_newlines_local_custom_mini(self):
        test_filename_uri = './tap_spreadsheets_anywhere/test/sample_with_bad_newlines.csv'
        iterator = get_row_iterator(TEST_TABLE_SPEC['tables'][0], test_filename_uri)

        for row in iterator:
            self.assertTrue(row['id'].isnumeric(), "Parsed ID is not a number for: {}".format(row['id']))

    def test_strip_newlines_monkey_patch_locally(self):
        """Load the file in binary mode to force the use of StreamHandler and the monkey patch"""
        test_filename = './tap_spreadsheets_anywhere/test/sample_with_bad_newlines.csv'

        file_handle = smart_open.open(test_filename, 'rb', errors='surrogateescape')
        reader = codecs.getreader('utf-8')(file_handle)
        reader = monkey_patch_streamreader(reader)
        iterator = csv_handler.get_row_iterator(TEST_TABLE_SPEC['tables'][0], reader)

        for row in iterator:
            self.assertTrue(row['id'].isnumeric(), "Parsed ID is not a number for: {}".format(row['id']))

    def test_smart_columns(self):
        with patch('sys.stdout', new_callable=StringIO) as fake_out:
            records_streamed = 0
            table_spec = TEST_TABLE_SPEC['tables'][7]
            modified_since = dateutil.parser.parse(table_spec['start_date'])
            target_files = file_utils.get_matching_objects(table_spec, modified_since)
            samples = file_utils.sample_files(table_spec, target_files, sample_rate=1)
            schema = generate_schema(table_spec, samples)
            for t_file in target_files:
                records_streamed += file_utils.write_file(t_file['key'], table_spec, schema.to_dict())

            raw_records = fake_out.getvalue().split('\n')
            records = [json.loads(raw) for raw in raw_records if raw]
            self.assertEqual(records_streamed, len(records),"Number records written to the pipe differed from records read from the pipe.")
            self.assertTrue(records[0]['type'] == "RECORD")
            self.assertTrue(len(records[0]) == 3)
            self.assertTrue(len(records[0]['record']) == 7)
            self.assertTrue( "_smart_source_bucket" in records[0]['record'] )
            self.assertTrue("_smart_source_lineno" in records[0]['record'])


    def test_local_bucket(self):
        table_spec = TEST_TABLE_SPEC['tables'][1]
        modified_since = dateutil.parser.parse(table_spec['start_date'])
        target_files = file_utils.get_matching_objects(table_spec, modified_since)
        assert len(target_files) == 1

    def test_https_bucket(self):
        table_spec = TEST_TABLE_SPEC['tables'][4]
        modified_since = dateutil.parser.parse(table_spec['start_date'])
        target_files = file_utils.get_matching_objects(table_spec, modified_since)
        assert len(target_files) == 1

        target_uri = table_spec['path'] + '/' + table_spec['pattern']
        iterator = get_row_iterator(TEST_TABLE_SPEC['tables'][4], target_uri)

        row = next(iterator)
        self.assertTrue(int(row['id']) > 0,row['id']+" was not positive")

    def test_indirect_https_bucket(self):
        table_spec = TEST_TABLE_SPEC['tables'][5]
        modified_since = dateutil.parser.parse(table_spec['start_date'])
        target_files = file_utils.get_matching_objects(table_spec, modified_since)
        assert len(target_files) == 1

        target_uri = table_spec['path'] + '/' + table_spec['pattern']
        iterator = get_row_iterator(TEST_TABLE_SPEC['tables'][5], target_uri)

        row = next(iterator)
        self.assertTrue( row['year'] == '1976', "Row did not contain expected data")

    def test_renamed_https_object(self):
        table_spec = TEST_TABLE_SPEC['tables'][6]
        modified_since = dateutil.parser.parse(table_spec['start_date'])
        target_files = file_utils.get_matching_objects(table_spec, modified_since)
        assert len(target_files) == 1

        target_uri = table_spec['path'] + '/' + table_spec['pattern']
        iterator = get_row_iterator(TEST_TABLE_SPEC['tables'][6], target_uri)

        row = next(iterator)
        self.assertTrue(len(row)>1,"Not able to read a row.")


class TestFormatHandlerExcelXlsxSkipInitial:
    """pytests to validate Skip Initial for Excel `.xlsx` files works as expected."""
    bad_file = "./tap_spreadsheets_anywhere/test/sample_with_bad_blank_line_above_headings.xlsx"
    uri = f"file://{bad_file}"

    def test_validate_iterator(self, tmpdir):
        xlsx = tmpdir / "fake_test.xlsx"
        uri = f"file://{xlsx}"
        _, workbook, _, exp = get_worksheet()
        workbook.save(xlsx)

        iterator = get_row_iterator({"format": "excel"}, uri)
        assert next(iterator) == exp[0]
        assert next(iterator) == exp[1]
        assert next(iterator) == exp[2]
        with pytest.raises(StopIteration):
            next(iterator)

    def test_bad_blank_line_above_headings_raises(self):
        """Test to verify a sample file that raises #52.
        Iteratting through this bad sample file will currently fail
        when parsing the blank line.
        """
        table_spec = {"format": "excel"}
        iterator = get_row_iterator(table_spec, self.uri)
        with pytest.raises(IndexError):
            for _ in iterator:
                continue

    def test_bad_blank_line_above_headings_skip_initial_over_bad_row(self):
        """Test to verify a sample file that raises #52, does not fail when
        using: `skip_interval`, to avoid the bad row.
        """
        # NOTE: that `get_row_iterator` will compress the header row and each
        # subsequent data row together, so count one less row than in the file
        # + expect a dict.
        exp = {
           'account': 'Sales - Commission Fees',
           'account_code': 9999.0,
           'account_type': 'Revenue',
           'contact': 'Company A Limited',
           'credit_gbp': 123.45,
           'date': datetime(2023, 1, 31, 0, 0),
           'debit_gbp': 0.0,
           'description': 'Description for Company A',
           'gross_gbp': 123.45,
           'invoice_number': 'INV-1234',
           'net_gbp': 1234.45,
           'reference': 'REF-1234',
           'revenue_type': 'Commission Fees',
           'vat_gbp': 0.0,
        }
        table_spec = {"format": "excel", "skip_initial": 4}
        # NOTE: `get_row_iterator` should no longer fail with Issue #52, now
        # that: `excel_handler.generator_wrapper` is not parsing skipped rows.
        iterator = get_row_iterator(table_spec, self.uri)
        # Assert that the expected row, after skipping, is next.
        assert next(iterator) == exp
