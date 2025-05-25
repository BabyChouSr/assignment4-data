import fsspec
from cs336_data.extraction import extract_text_from_html_bytes
from cs336_data.language_identification import identify_language
from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import *

WARC_RECORD_RESPONSE_ENUM_FLAG = 4


num_english_documents = 0
total_documents = 0
iterator = ArchiveIterator(GZipStream(FileStream('/data/CC/example.warc.gz', 'rb')), record_types=WARC_RECORD_RESPONSE_ENUM_FLAG)
for i, record in enumerate(iterator):
    record_body = record.reader.read()
    record_text = extract_text_from_html_bytes(record_body)
    language = identify_language(record_text)
    
    if language[0] == 'en':
        num_english_documents += 1
    
    total_documents += 1

    if i == 20:
        break

print(f"Proportion of english documents: {num_english_documents / total_documents}")

