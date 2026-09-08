"""RECORD-mode counterpart to `arrow_batch.ArrowBatchWriter`.

Both expose the same `.write(record)` / `.flush()` contract so callers (see
`file_utils.write_file`) don't need to branch on whether BATCH mode is active.
"""

import singer


class SingerRecordSink:
    """Writes each record immediately as a Singer RECORD message."""

    def __init__(self, stream_name: str, write_record=None):
        self.stream_name = stream_name
        self._write_record = write_record or singer.write_record

    def write(self, record: dict) -> None:
        self._write_record(self.stream_name, record)

    def flush(self) -> None:
        pass
