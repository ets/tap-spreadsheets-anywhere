import smart_open

from codecs import StreamReader
import tap_spreadsheets_anywhere.csv_handler
import tap_spreadsheets_anywhere.excel_handler
import tap_spreadsheets_anywhere.json_handler
import tap_spreadsheets_anywhere.jsonl_handler
import tap_spreadsheets_anywhere.parquet_handler

from azure.storage.blob import BlobServiceClient
import os


class InvalidFormatError(Exception):
    def __init__(self, fname, message="The file was not in the expected format"):
        self.name = fname
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.name} could not be parsed: {self.message}'


def get_streamreader(uri, universal_newlines=True, newline='', open_mode='r', encoding='utf-8'):
    kwarg_dispatch = {
        "azure": lambda: {
            "transport_params": {
                "client": BlobServiceClient.from_connection_string(
                    os.environ['AZURE_STORAGE_CONNECTION_STRING'],
                )
            }
        },
    }

    SCHEME_SEP = "://"
    kwargs = kwarg_dispatch.get(uri.split(SCHEME_SEP, 1)[0], lambda: {})()

    # When reading in binary mode, undefine `encoding`.
    # Otherwise, `smart_open` will return a `TextIOWrapper` in `"r"` mode.
    # However, reading binary streams needs a `BufferedReader`.
    if "b" in open_mode:
        encoding = None
    streamreader = smart_open.open(uri, open_mode, newline=newline, errors='surrogateescape', encoding=encoding, **kwargs)

    if not universal_newlines and isinstance(streamreader, StreamReader):
        return monkey_patch_streamreader(streamreader)
    return streamreader


def monkey_patch_streamreader(streamreader):
    streamreader.mp_newline = '\n'
    streamreader.readline = mp_readline.__get__(streamreader, StreamReader)
    return streamreader


def mp_readline(self, size=None, keepends=False):
    """
        Modified version of readline for StreamReader that avoids the use of splitlines
        in favor of a call to split(self.mp_newline)
        This supports poorly formatted CSVs that the author has sadly seen in the wild
        from commercial vendors.
    """
    # If we have lines cached from an earlier read, return
    # them unconditionally
    if self.linebuffer:
        line = self.linebuffer[0]
        del self.linebuffer[0]
        if len(self.linebuffer) == 1:
            # revert to charbuffer mode; we might need more data
            # next time
            self.charbuffer = self.linebuffer[0]
            self.linebuffer = None
        if not keepends:
            line = line.split(self.mp_newline)[0]
        return line

    readsize = size or 72
    line = self._empty_charbuffer
    # If size is given, we call read() only once
    while True:
        data = self.read(readsize, firstline=True)
        if data:
            # If we're at a "\r" read one extra character (which might
            # be a "\n") to get a proper line ending. If the stream is
            # temporarily exhausted we return the wrong line ending.
            if (isinstance(data, str) and data.endswith("\r")) or \
                    (isinstance(data, bytes) and data.endswith(b"\r")):
                data += self.read(size=1, chars=1)

        line += data
        lines = line.split(self.mp_newline)
        if lines:
            if len(lines) > 1:
                # More than one line result; the first line is a full line
                # to return
                line = lines[0]
                del lines[0]
                if len(lines) > 1:
                    # cache the remaining lines
                    lines[-1] += self.charbuffer
                    self.linebuffer = lines
                    self.charbuffer = None
                else:
                    # only one remaining line, put it back into charbuffer
                    self.charbuffer = lines[0] + self.charbuffer
                if not keepends:
                    line = line.split(self.mp_newline)[0]
                break
            line0withend = lines[0]
            line0withoutend = lines[0].split(self.mp_newline)[0]
            if line0withend != line0withoutend:  # We really have a line end
                # Put the rest back together and keep it until the next call
                self.charbuffer = self._empty_charbuffer.join(lines[1:]) + \
                                  self.charbuffer
                if keepends:
                    line = line0withend
                else:
                    line = line0withoutend
                break
        # we didn't get anything or this was our only try
        if not data or size is not None:
            if line and not keepends:
                line = line.split(self.mp_newline)[0]
            break
        if readsize < 8000:
            readsize *= 2
    return line


def get_row_iterator(table_spec, uri):
    universal_newlines = table_spec['universal_newlines'] if 'universal_newlines' in table_spec else True
    encoding = table_spec['encoding'] if 'encoding' in table_spec else 'utf-8'
    skip_initial = table_spec.get("skip_initial", 0)

    if 'format' not in table_spec or table_spec['format'] == 'detect':
        lowered_uri = uri.lower()
        if lowered_uri.endswith(".xlsx") or lowered_uri.endswith(".xls"):
            format = 'excel'
        elif lowered_uri.endswith(".json") or lowered_uri.endswith(".js"):
            format = 'json'
        elif lowered_uri.endswith(".jsonl"):
            format = 'jsonl'
        elif lowered_uri.endswith(".csv"):
            format = 'csv'
        elif lowered_uri.endswith(".parquet"):
            format = 'parquet'
        else:
            # TODO: some protocols provide the ability to pull format (content-type) info & we could make use of that here
            reader = get_streamreader(uri, universal_newlines=universal_newlines, open_mode='r', encoding=encoding)
            buf = reader.read(10)
            reader.seek(0)
            if len(buf) > 0:
                if buf[0].lstrip() == "[":
                    format = 'json'
                elif buf[0].isprintable():
                    format = 'csv'
                else:
                    raise ValueError(f"Unable to detect the format for {uri}")
            else:
                raise ValueError(f"Unable to read {uri} for type detection")

    else:
        format = table_spec['format']

    try:
        if format == 'csv':
            reader = get_streamreader(uri, universal_newlines=universal_newlines, open_mode='r', encoding=encoding)
            iterator = tap_spreadsheets_anywhere.csv_handler.get_row_iterator(table_spec, reader)
        elif format == 'excel':
            if uri.lower().endswith(".xls"):
                reader = get_streamreader(uri, universal_newlines=universal_newlines,newline=None, open_mode='rb')
                iterator = tap_spreadsheets_anywhere.excel_handler.get_legacy_row_iterator(table_spec, reader)
            else:
                # If encoding is set, smart_open will override binary mode ('b' in open_mode) and it will result in a BadZipFile error
                reader = get_streamreader(uri, universal_newlines=universal_newlines,newline=None, open_mode='rb', encoding=None)
                iterator = tap_spreadsheets_anywhere.excel_handler.get_row_iterator(table_spec, reader)
        elif format == 'parquet':
            reader = get_streamreader(uri, universal_newlines=universal_newlines, newline=None, open_mode='rb')
            iterator = tap_spreadsheets_anywhere.parquet_handler.get_row_iterator(table_spec, reader)
        elif format == 'json':
            reader = get_streamreader(uri, universal_newlines=universal_newlines, open_mode='r', encoding=encoding)
            iterator = tap_spreadsheets_anywhere.json_handler.get_row_iterator(table_spec, reader)
        elif format == 'jsonl':
            reader = get_streamreader(uri, universal_newlines=universal_newlines, open_mode='r', encoding=encoding)
            iterator = tap_spreadsheets_anywhere.jsonl_handler.get_row_iterator(table_spec, reader)
    except (ValueError,TypeError) as err:
        raise InvalidFormatError(uri,message=err)

    if format != 'excel':
        # Reduce the scope of changes to fix Issue #52.
        for _ in range(skip_initial):
            next(iterator)

    return iterator
