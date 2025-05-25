import fsspec
from cs336_data.extraction import extract_text_from_html_bytes
from cs336_data.mask_pii import mask_pii
from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import *

WARC_RECORD_RESPONSE_ENUM_FLAG = 4


num_positive_documents = 0
num_negative_documents = 0
iterator = ArchiveIterator(GZipStream(FileStream('/data/CC/example.warc.gz', 'rb')), record_types=WARC_RECORD_RESPONSE_ENUM_FLAG)
with fsspec.open('example_mask_pii.gz', 'w', compression="infer") as f:
    for i, record in enumerate(iterator):
        record_body = record.reader.read()
        record_text = extract_text_from_html_bytes(record_body)
        
        total_num_changes = 0
        for pii_type in ['email', 'phone_number', 'ip_address']:
            record_text, num_changes = mask_pii(record_text, pii_type)
            total_num_changes += num_changes

        f.write(f"=" * 50 + f"TOTAL NUM CHANGES: {total_num_changes}" + '=' * 50 + '\n')
        f.write(record_text + "\n")
    

        if total_num_changes > 0:
            num_positive_documents += 1
        else:
            num_negative_documents += 1
        
        if num_positive_documents >= 20 and num_negative_documents >= 20:
            break
