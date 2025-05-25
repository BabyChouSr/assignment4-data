import fsspec
from cs336_data.extraction import extract_text_from_html_bytes
from cs336_data.gopher import check_gopher_filters
from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import *

WARC_RECORD_RESPONSE_ENUM_FLAG = 4


num_positive_documents = 0
iterator = ArchiveIterator(GZipStream(FileStream('/data/CC/example.warc.gz', 'rb')), record_types=WARC_RECORD_RESPONSE_ENUM_FLAG)
with fsspec.open('example_gopher.gz', 'w', compression="infer") as f:
    for i, record in enumerate(iterator):
        record_body = record.reader.read()
        record_text = extract_text_from_html_bytes(record_body)
        
        if check_gopher_filters(record_text):
            num_positive_documents += 1
            f.write(f"=" * 50 + "POSITIVE!\n")
        else:
            f.write(f"=" * 50 + "NEGATIVE!\n")
            

        f.write(record_text + "\n")

        if num_positive_documents == 20:
            break
    
