import logging
import unittest

from tap_spreadsheets_anywhere import format_handler

LOGGER = logging.getLogger(__name__)

TEST_TABLE_SPEC = {
    "tables": [
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "parquet-iris",
            "pattern": "iris\\-sample\\.parquet",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "parquet",
        },
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "parquet-mt",
            "pattern": "mt\\-sample\\.parquet",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "parquet",
        },
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "parquet-iris-detect",
            "pattern": "iris\\.parquet",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "detect",
        },
        {
            "path": "file://./tap_spreadsheets_anywhere/test",
            "name": "parquet-mt-detect",
            "pattern": "mt\\.parquet",
            "start_date": "2017-05-01T00:00:00Z",
            "key_properties": [],
            "format": "detect",
        },
    ]
}


class TestParquet(unittest.TestCase):
    def test_iris(self):
        table_spec = TEST_TABLE_SPEC["tables"][0]
        uri = "./tap_spreadsheets_anywhere/test/iris-sample.parquet"
        iterator = format_handler.get_row_iterator(table_spec, uri)

        rows = list(iterator)
        self.assertEqual(len(rows), 150)
        self.assertEqual(
            rows[0],
            {
                "sepallength": 5.1,
                "sepalwidth": 3.5,
                "petallength": 1.4,
                "petalwidth": 0.2,
                "variety": "Setosa",
            },
        )

    def test_mt(self):
        table_spec = TEST_TABLE_SPEC["tables"][1]
        uri = "./tap_spreadsheets_anywhere/test/mt-sample.parquet"
        iterator = format_handler.get_row_iterator(table_spec, uri)

        rows = list(iterator)
        self.assertEqual(len(rows), 32)
        self.assertEqual(
            rows[0],
            {
                "model": "Mazda RX4",
                "mpg": 21.0,
                "cyl": 6,
                "disp": 160.0,
                "hp": 110,
                "drat": 3.9,
                "wt": 2.62,
                "qsec": 16.46,
                "vs": 0,
                "am": 1,
                "gear": 4,
                "carb": 4,
            },
        )

    def test_iris_detect(self):
        table_spec = TEST_TABLE_SPEC["tables"][2]
        uri = "./tap_spreadsheets_anywhere/test/iris-sample.parquet"
        iterator = format_handler.get_row_iterator(table_spec, uri)

        rows = list(iterator)
        self.assertEqual(len(rows), 150)
        self.assertEqual(
            rows[0],
            {
                "sepallength": 5.1,
                "sepalwidth": 3.5,
                "petallength": 1.4,
                "petalwidth": 0.2,
                "variety": "Setosa",
            },
        )

    def test_mt_detect(self):
        table_spec = TEST_TABLE_SPEC["tables"][3]
        uri = "./tap_spreadsheets_anywhere/test/mt-sample.parquet"
        iterator = format_handler.get_row_iterator(table_spec, uri)

        rows = list(iterator)
        self.assertEqual(len(rows), 32)
        self.assertEqual(
            rows[0],
            {
                "model": "Mazda RX4",
                "mpg": 21.0,
                "cyl": 6,
                "disp": 160.0,
                "hp": 110,
                "drat": 3.9,
                "wt": 2.62,
                "qsec": 16.46,
                "vs": 0,
                "am": 1,
                "gear": 4,
                "carb": 4,
            },
        )
