import codecs
import unittest

import smart_open

from tap_smart_csv import tables_config_util
from tap_smart_csv.format_handler import get_filetype_handler, monkey_patch_streamreader

TEST_TABLE_SPEC = {
    "tables":[
        {
            "path": "s3://any_bucket_willdo",
            "name": "products",
            "pattern": "g2/.*roduct.*",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": ["id"],
            "format": "csv",
            "prefer_number_vs_integer": True,
            "universal_newlines": False,
            "sample_rate": 5,
            "max_sampling_read": 2000,
            "max_sampled_files": 3,
            "schema_overrides": {
                "id": {
                    "type": "integer"
                }
            }
        }
    ]
}


class TestFormatHandler(unittest.TestCase):

    def test_custom_config(self):
        tables_config_util.CONFIG_CONTRACT(TEST_TABLE_SPEC)

    def test_strip_newlines_local_custom(self):
        test_filename = './tap_smart_csv/test/sample_with_bad_newlines.csv'

        file_handle = smart_open.open(test_filename, 'rb', errors='surrogateescape')
        reader = codecs.getreader('utf-8')(file_handle)
        reader = monkey_patch_streamreader(reader)
        iterator = get_filetype_handler(TEST_TABLE_SPEC['tables'][0], reader)

        for row in iterator:
            self.assertTrue(row['id'].isnumeric(), "Parsed ID is not a number for: {}".format(row['id']))
