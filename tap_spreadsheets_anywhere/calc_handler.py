from __future__ import annotations

import logging
import re
from collections.abc import Generator, Iterable, Mapping
from typing import Any, BinaryIO

logger = logging.getLogger(__name__)


def sanitize_header(value: str | None) -> str:
    if not value:
        return ''
    value = value.lower()
    value = re.sub(r"[^\w\s]", '', value)

    # replace whitespace with underscores
    return re.sub(r"\s+", '_', value)


def generator_wrapper(reader, table_spec: Mapping[str, Any]) -> Generator[Mapping[str, Any]]:
    skip_initial = table_spec.get("skip_initial", 0)

    it = iter(reader)
    while skip_initial:
        skip_initial -= 1
        next(it)

    header_row = [sanitize_header(cell) for cell in next(it)]

    for row in it:
        yield {header_row[index]: cell for index, cell in enumerate(row)}


def get_row_iterator(table_spec: Mapping, file_handle: BinaryIO) -> Iterable[Mapping]:
    import pyexcel
    workbook = pyexcel.get_book(file_content=file_handle, file_type='ods')
    sheet = workbook[table_spec["worksheet_name"]]

    return generator_wrapper(sheet, table_spec)
