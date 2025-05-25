import os
import hashlib
import json
import gzip
from collections import Counter
from dataclasses import dataclass
import draccus
import glob
import tqdm

@dataclass
class Config:
    input_dir: str = "/data/c-cychou/documents-3"
    output_dir: str = "/data/c-cychou/documents-3-deduped"

def exact_line_deduplication_warc(input_files: list[str], output_path: str):
    os.makedirs(output_path, exist_ok=True)

    # First pass: count occurrences of each text line
    counts = Counter()
    for input_file in tqdm.tqdm(input_files):
        print(f"Counting lines in {input_file}")
        with gzip.open(input_file, "rt", encoding="utf-8") as f_in:
            for line in f_in:
                try:
                    data = json.loads(line.strip())
                    text = data.get("text", "")
                    if text:
                        line_hash = hashlib.sha256(text.encode()).hexdigest()
                        counts[line_hash] += 1
                except json.JSONDecodeError:
                    continue

    num_duplicated = 0
    # Second pass: write deduplicated files
    for input_file in tqdm.tqdm(input_files):
        input_file_basename = os.path.basename(input_file)
        output_file_path = os.path.join(output_path, input_file_basename)
        print(f"Deduplicating {input_file} -> {output_file_path}")

        if os.path.exists(f"{output_file_path}.SUCCESS"):
            print(f"Skipping {input_file} because it has already been deduplicated")
            continue
        
        with gzip.open(output_file_path, "wt", encoding="utf-8") as f_out:
            with gzip.open(input_file, "rt", encoding="utf-8") as f_in:
                for line in f_in:
                    try:
                        data = json.loads(line.strip())
                        text = data.get("text", "")
                        if text:
                            line_hash = hashlib.sha256(text.encode()).hexdigest()
                            if counts[line_hash] > 1:
                                num_duplicated += 1
                                continue
                        f_out.write(line)
                    except json.JSONDecodeError:
                        # Keep malformed JSON lines as-is
                        f_out.write(line)
        
        with gzip.open(f"{output_file_path}.SUCCESS", "wt", encoding="utf-8") as f_out_deduped:
            f_out_deduped.write(f"Deduplicated {input_file} -> {output_file_path}\n")

    print(f"Number of duplicated documents removed: {num_duplicated}")
    print(f"Number of documents: {len(counts)}")

@draccus.wrap()
def main(config: Config):
    # Find all .jsonl.gz files in the input directory
    input_files = glob.glob(os.path.join(config.input_dir, "*.jsonl.gz"))
    
    if not input_files:
        print(f"No .jsonl.gz files found in {config.input_dir}")
        return
    
    print(f"Found {len(input_files)} files to process")
    exact_line_deduplication_warc(input_files, config.output_dir)
    print("Deduplication complete!")

if __name__ == "__main__":
    main()
