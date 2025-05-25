from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import *
from cs336_data.quality_classify import classify_quality
from cs336_data.language_identification import identify_language
from cs336_data.nsfw_detection import classify
from cs336_data.gopher import check_gopher_filters
from cs336_data.process_wet_files import process_single_wet_file
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

process_single_wet_file(wet_file, "/data/c-cychou/debug/troll.warc.wet.gz")