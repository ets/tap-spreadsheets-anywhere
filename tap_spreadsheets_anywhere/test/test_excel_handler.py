import logging

import pytest
from openpyxl import Workbook

from tap_spreadsheets_anywhere.excel_handler import generator_wrapper

LOGGER = logging.getLogger(__name__)


def get_worksheet():
    """Create a basic workbook that can be manipulated for tests.
    See: https://openpyxl.readthedocs.io/en/stable/usage.html.
    """
    wb = Workbook()
    ws = wb.active
    tree_data = [
        ["Type", "Leaf Color", "Height"],
        ["Maple", "Red", 549],
        ["Oak", "Green", 783],
        ["Pine", "Green", 1204],
    ]
    # generator_wrapper preserves mixed-case header names as-is (only fully-uppercase
    # headers get lowercased), so "Type"/"Leaf Color"/"Height" survive with casing intact.
    exp_tree_data = [
        {"Type": "Maple", "Leaf_Color": "Red", "Height": 549},
        {"Type": "Oak", "Leaf_Color": "Green", "Height": 783},
        {"Type": "Pine", "Leaf_Color": "Green", "Height": 1204},
    ]
    [ws.append(row) for row in tree_data]
    return ws, wb, tree_data, exp_tree_data


class TestExcelHandlerGeneratorWrapper:
    """Validate the expected state of the `excel_handler.generator_wrapper`."""

    def test_parse_data(self):
        worksheet, _, _, exp = get_worksheet()
        _generator = generator_wrapper(worksheet)
        assert next(_generator) == exp[0]
        assert next(_generator) == exp[1]
        assert next(_generator) == exp[2]
        with pytest.raises(StopIteration):
            next(_generator)
