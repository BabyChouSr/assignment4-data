from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import *
from cs336_data.quality_classify import classify_quality
import glob

# Find a real WET file
wet_files = glob.glob("/data/CC/*.warc.wet.gz")
if not wet_files:
    print("No WET files found")
    exit(1)

wet_file = wet_files[0]
print(f"Testing with file: {wet_file}")

# Read a few records from a WET file
file_iterator = ArchiveIterator(GZipStream(FileStream(wet_file, 'rb')), record_types=WarcRecordType.conversion)

count = 0
for record in file_iterator:
    if count >= 3:  # Just check first 3 records
        break
    
    record_body = record.reader.read()
    text = record_body.decode('utf-8', errors='replace')
    
    # Skip empty records
    if len(text.strip()) == 0:
        continue
    
    # Test quality classification
    result = classify_quality(text)
    print(f'Record {count + 1}:')
    print(f'Text length: {len(text)}')
    print(f'First 200 chars: {repr(text[:200])}')
    print(f'Quality result: {result}')
    print('---')
    count += 1 