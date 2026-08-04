"""
Tests for the fix to LOGGER.exception calls in file_utils.py.

Previously, LOGGER.exception was called with the exception as a second
positional argument:

    LOGGER.exception(f"...: {filename}", ife)

Python's logging treats extra positional args as %-style format values.
When a handler formats the record, it calls ``record.getMessage()`` which
does ``msg % self.args``.  If the exception's ``str()`` contains ``%``
characters (e.g. a filename like "100%_complete.json") and the f-string
message contains no matching ``%s`` placeholders, this raises:

    ValueError: unsupported format character …

By default, logging handlers catch and suppress that error via
``handleError()``.  In production the crash surfaces when the logging
handler is configured to raise on errors (common in strict environments
and in the actual Singer runtime that called this code).

The fix embeds the exception directly in the f-string so no extra
positional args are passed:

    LOGGER.exception(f"...: {filename}: {ife}")
"""

import io
import logging
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from tap_spreadsheets_anywhere import file_utils
from tap_spreadsheets_anywhere.format_handler import InvalidFormatError


def _make_table_spec(invalid_format_action="ignore"):
    return {
        "path": "file://./tap_spreadsheets_anywhere/test",
        "name": "test_table",
        "pattern": ".*",
        "start_date": "2017-05-01T00:00:00Z",
        "key_properties": [],
        "format": "csv",
        "invalid_format_action": invalid_format_action,
    }


def _make_invalid_format_error_with_percent():
    """Return an InvalidFormatError whose str() contains '%' characters.

    This is the class of input that triggered the ValueError in the old code,
    because logging tried to use the exception as a %-format argument when
    calling ``msg % self.args`` inside ``LogRecord.getMessage()``.
    """
    return InvalidFormatError(
        "100%_complete.json",
        message="unexpected character: % in field name",
    )


@contextmanager
def _strict_logging_handler(logger_name):
    """Attach a StreamHandler to *logger_name* that raises instead of silently
    swallowing logging format errors.

    Python's default ``logging.Handler.handleError()`` prints to stderr and
    continues.  This context manager replaces that behaviour so that any
    ``ValueError`` / ``TypeError`` raised while formatting a log record (e.g.
    from ``msg % args`` inside ``LogRecord.getMessage()``) propagates as a
    test failure — exactly the scenario that occurred in production.

    A ``StreamHandler`` (not ``NullHandler``) is required because only handlers
    that actually call ``emit()`` trigger ``getMessage()`` and therefore expose
    the formatting bug.
    """
    logger = logging.getLogger(logger_name)
    handler = logging.StreamHandler(io.StringIO())
    old_level = logger.level
    logger.setLevel(logging.DEBUG)

    def _raise_on_error(record):
        import traceback
        raise AssertionError(
            f"Logging failed to format record: {traceback.format_exc()}"
        )

    handler.handleError = _raise_on_error
    logger.addHandler(handler)
    try:
        yield
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)


class TestInvalidFormatErrorLogging(unittest.TestCase):
    """Verify that write_file and sample_file do not raise when an
    InvalidFormatError whose message contains '%' characters is logged while
    invalid_format_action='ignore' is set.

    The strict handler installed by each test ensures that any logging format
    error (which the old code triggered) is raised as a test failure rather
    than silently swallowed."""

    def test_write_file_does_not_raise_on_percent_in_exception(self):
        """write_file must not crash when the InvalidFormatError message
        contains '%' characters."""
        table_spec = _make_table_spec("ignore")
        ife = _make_invalid_format_error_with_percent()

        with _strict_logging_handler("tap_spreadsheets_anywhere.file_utils"):
            with patch(
                "tap_spreadsheets_anywhere.format_handler.get_row_iterator",
                side_effect=ife,
            ):
                result = file_utils.write_file("100%_complete.json", table_spec, {})

        self.assertEqual(result, 0)

    def test_sample_file_does_not_raise_on_percent_in_exception(self):
        """sample_file must not crash when the InvalidFormatError message
        contains '%' characters."""
        table_spec = _make_table_spec("ignore")
        ife = _make_invalid_format_error_with_percent()

        with _strict_logging_handler("tap_spreadsheets_anywhere.file_utils"):
            with patch(
                "tap_spreadsheets_anywhere.format_handler.get_row_iterator",
                side_effect=ife,
            ):
                result = file_utils.sample_file(table_spec, "100%_complete.json", 1, 100)

        self.assertEqual(result, [])

    def test_write_file_reraises_when_action_is_fail(self):
        """write_file must still propagate InvalidFormatError when
        invalid_format_action is not 'ignore'."""
        table_spec = _make_table_spec("fail")
        ife = _make_invalid_format_error_with_percent()

        with patch(
            "tap_spreadsheets_anywhere.format_handler.get_row_iterator",
            side_effect=ife,
        ):
            with self.assertRaises(InvalidFormatError):
                file_utils.write_file("bad.json", table_spec, {})

    def test_sample_file_reraises_when_action_is_fail(self):
        """sample_file must still propagate InvalidFormatError when
        invalid_format_action is not 'ignore'."""
        table_spec = _make_table_spec("fail")
        ife = _make_invalid_format_error_with_percent()

        with patch(
            "tap_spreadsheets_anywhere.format_handler.get_row_iterator",
            side_effect=ife,
        ):
            with self.assertRaises(InvalidFormatError):
                file_utils.sample_file(table_spec, "bad.json", 1, 100)
