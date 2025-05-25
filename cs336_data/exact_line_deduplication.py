import os
import hashlib
from collections import Counter

def exact_line_deduplication(input_files: list[str], output_path: str):
    os.makedirs(output_path, exist_ok=True)


    counts = Counter()
    for input_file in input_files:
        input_file_basename = os.path.basename(input_file)
        with open(input_file, "r") as f_in:
            for line in f_in:
                line_hash = hashlib.sha256(line.encode()).hexdigest()
                counts[line_hash] += 1

    
    for input_file in input_files:
        input_file_basename = os.path.basename(input_file)
        output_file_path = os.path.join(output_path, input_file_basename)
        with open(output_file_path, "w") as f_out:
            with open(input_file, "r") as f_in:
                for line in f_in:
                    line_hash = hashlib.sha256(line.encode()).hexdigest()
                    if counts[line_hash] > 1:
                        continue
                    else:
                        f_out.write(line)
                    