import fsspec
from cs336_data.extraction import extract_text_from_html_bytes
from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import *

WARC_RECORD_RESPONSE_ENUM_FLAG = 4

with fsspec.open("example_wet.gz", 'w', compression="infer") as f_out:
    for record in ArchiveIterator(GZipStream(FileStream('/data/CC/example.warc.gz', 'rb')), record_types=WARC_RECORD_RESPONSE_ENUM_FLAG):
        record_body = record.reader.read()
        f_out.write(extract_text_from_html_bytes(record_body) + "\n\n" + "=" * 50)
