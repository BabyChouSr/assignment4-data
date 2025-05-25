import fsspec
import random
from cs336_data.extraction import extract_text_from_html_bytes
from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import *
import draccus
from dataclasses import dataclass
from typing import Optional, Literal

WARC_RECORD_RESPONSE_ENUM_FLAG = 4

@dataclass
class WarcToTextConfig:
    input_path: str
    output_path: str
    # Random sampling parameters
    label: Literal["hq", "lq"]
    sampling_method: Optional[Literal["probability", "count"]] = None
    # For probability-based sampling (0.0-1.0)
    sampling_rate: float = 1.0
    # For count-based sampling (fixed number of samples)
    sample_count: int = -1
    # Random seed for reproducible sampling
    random_seed: Optional[int] = 42

@draccus.wrap()
def main(config: WarcToTextConfig):
    # Set random seed if specified
    if config.random_seed is not None:
        random.seed(config.random_seed)
        
    iterator = ArchiveIterator(GZipStream(FileStream(config.input_path, 'rb')), record_types=WARC_RECORD_RESPONSE_ENUM_FLAG)
    
    # If no sampling method is specified, process all records
    if config.sampling_method is None:
        process_all_records(iterator, config.output_path, config.label, config.sample_count)
    # Probability-based sampling
    elif config.sampling_method == "probability":
        probability_sampling(iterator, config.output_path, config.sampling_rate, config.label)
    # Count-based sampling (reservoir sampling)
    elif config.sampling_method == "count":
        count_sampling(iterator, config.output_path, config.sample_count, config.label)
    else:
        raise ValueError(f"Unknown sampling method: {config.sampling_method}")

def get_fasttext_format(record_body, label):
    text = extract_text_from_html_bytes(record_body)
    text = text.replace("\n", " ")
    return f"__label__{label} {text}"

def process_all_records(iterator, output_path, label, sample_count):
    with fsspec.open(output_path, 'w', compression="infer") as f_out:
        for i, record in enumerate(iterator):
            if sample_count != -1 and i >= sample_count:
                break
            record_body = record.reader.read()
            f_out.write(get_fasttext_format(record_body, label) + "\n")

def probability_sampling(iterator, output_path, sampling_rate, label):
    """Sample records with a fixed probability."""
    with fsspec.open(output_path, 'w', compression="infer") as f_out:
        for record in iterator:
            # Skip with probability (1 - sampling_rate)
            if random.random() > sampling_rate:
                continue
                
            record_body = record.reader.read()
            f_out.write(get_fasttext_format(record_body, label) + "\n")

def count_sampling(iterator, output_path, sample_count, label):
    """Use reservoir sampling to select a fixed number of samples."""
    if sample_count <= 0:
        raise ValueError("Sample count must be positive")
        
    # First pass to count total records (optional if total count is known)
    # This is needed because we can't sample exactly N records without knowing the total
    # OR we can use reservoir sampling which doesn't require knowing the total
    
    # Reservoir sampling implementation
    reservoir = []
    count = 0
    
    for record in iterator:
        count += 1
        record_body = record.reader.read()
        text = extract_text_from_html_bytes(record_body)
        
        if len(reservoir) < sample_count:
            # Fill the reservoir until we have sample_count items
            reservoir.append(text)
        else:
            # Randomly replace items with decreasing probability
            j = random.randint(0, count - 1)
            if j < sample_count:
                reservoir[j] = text
    
    # Write the sampled records to the output file
    with fsspec.open(output_path, 'w', compression="infer") as f_out:
        for text in reservoir:
            f_out.write(get_fasttext_format(text, label) + "\n")

if __name__ == "__main__":
    main()