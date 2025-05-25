import fsspec
from cs336_data.extraction import extract_text_from_html_bytes
from cs336_data.nsfw_detection import classify
from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import *

WARC_RECORD_RESPONSE_ENUM_FLAG = 4


num_harm_documents = 0
total_documents = 0
iterator = ArchiveIterator(GZipStream(FileStream('/data/CC/example.warc.gz', 'rb')), record_types=WARC_RECORD_RESPONSE_ENUM_FLAG)
for i, record in enumerate(iterator):
    record_body = record.reader.read()
    record_text = extract_text_from_html_bytes(record_body)
    
    is_harmful = False
    for harm_type in ['hate', 'nsfw']:
        output = classify(record_text, harm_type)
        print(f"{i}: {output}")

        if output[0] == 'toxic' or output[0] == 'nsfw':
            is_harmful = True

    if is_harmful:
        num_harm_documents += 1

    total_documents += 1

    if i == 20:
        break

print(f"Proportion of harmful documents: {num_harm_documents / total_documents}")

