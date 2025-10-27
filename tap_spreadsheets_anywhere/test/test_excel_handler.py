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
        ["Pine", "Green", 1204]
    ]
    exp_tree_data = [
        {'type': 'Maple', 'leaf_color': 'Red', 'height': 549},
        {'type': 'Oak', 'leaf_color': 'Green', 'height': 783},
        {'type': 'Pine', 'leaf_color': 'Green', 'height': 1204},
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
