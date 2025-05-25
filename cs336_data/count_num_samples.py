from dataclasses import dataclass
import draccus
from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import *

WARC_RECORD_RESPONSE_ENUM_FLAG = 4

@dataclass
class FileConfig:
    input_path: str

@draccus.wrap()
def main(config: FileConfig):
    iterator = ArchiveIterator(GZipStream(FileStream(config.input_path, 'rb')), record_types=WARC_RECORD_RESPONSE_ENUM_FLAG)
    count = 0
    for record in iterator:
        count += 1
    print(f"Number of samples: {count}")


if __name__ == "__main__":
    main()
