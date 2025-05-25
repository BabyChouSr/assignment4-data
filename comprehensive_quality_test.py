from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import *
from cs336_data.quality_classify import classify_quality
from cs336_data.language_identification import identify_language
from cs336_data.nsfw_detection import classify
from cs336_data.gopher import check_gopher_filters
import glob

# Find a real WET file
wet_files = glob.glob("/data/CC/*.warc.wet.gz")
if not wet_files:
    print("No WET files found")
    exit(1)

wet_file = wet_files[0]
print(f"Testing with file: {wet_file}")

# Read records from a WET file and apply the full filtering pipeline
file_iterator = ArchiveIterator(GZipStream(FileStream(wet_file, 'rb')), record_types=WarcRecordType.conversion)

english_filtered_count = 0
gopher_filtered_count = 0
toxic_filtered_count = 0
nsfw_filtered_count = 0
quality_filtered_count = 0
number_kept = 0
number_total = 0

quality_results = {"hq": 0, "lq": 0}

count = 0
for record in file_iterator:
    if count >= 100:  # Test first 100 records
        break
    
    number_total += 1
    record_body = record.reader.read()
    text = record_body.decode('utf-8', errors='replace')
    
    # Apply filters in the same order as the main code
    language, _ = identify_language(text)
    if language != "en":
        english_filtered_count += 1
        count += 1
        continue
    
    gopher_flag_to_keep = check_gopher_filters(text)
    if not gopher_flag_to_keep:
        gopher_filtered_count += 1
        count += 1
        continue
    
    toxic_flag, toxic_score = classify(text, "hate")
    nsfw_flag, nsfw_score = classify(text, "nsfw")
    if toxic_flag == "toxic" or nsfw_flag == "nsfw":
        if toxic_flag == "toxic":
            toxic_filtered_count += 1
        if nsfw_flag == "nsfw":
            nsfw_filtered_count += 1
        count += 1
        continue
    
    quality_flag, quality_score = classify_quality(text)
    quality_results[quality_flag] += 1
    
    if quality_flag == "lq":
        print(f"LOW QUALITY FOUND - Record {count + 1}:")
        print(f"  Text length: {len(text)}")
        print(f"  Quality score: {quality_score}")
        print(f"  First 200 chars: {repr(text[:200])}")
        print("---")
        quality_filtered_count += 1
        count += 1
        continue
    
    number_kept += 1
    count += 1

print(f"\n=== SUMMARY ===")
print(f"Total processed: {number_total}")
print(f"English filtered: {english_filtered_count}")
print(f"Gopher filtered: {gopher_filtered_count}")
print(f"Toxic filtered: {toxic_filtered_count}")
print(f"NSFW filtered: {nsfw_filtered_count}")
print(f"Quality filtered: {quality_filtered_count}")
print(f"Number kept: {number_kept}")
print(f"\nQuality distribution among documents that reached quality check:")
print(f"High quality (hq): {quality_results['hq']}")
print(f"Low quality (lq): {quality_results['lq']}")
print(f"Total quality checked: {quality_results['hq'] + quality_results['lq']}")

if quality_results['hq'] + quality_results['lq'] > 0:
    lq_percentage = (quality_results['lq'] / (quality_results['hq'] + quality_results['lq'])) * 100
    print(f"Percentage of low quality: {lq_percentage:.2f}%") 