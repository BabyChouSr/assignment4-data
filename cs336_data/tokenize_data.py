import os
import pathlib
import glob
import random
import submitit
import tqdm
import draccus
import json
import numpy as np
from transformers import AutoTokenizer
import fsspec
from dataclasses import dataclass

@dataclass
class TokenizeConfig:
    input_path: str = "/data/c-cychou/documents-3-deduped"
    output_path: str = "/data/c-cychou/tokenized_documents.npy"
    temp_dir: str = "/data/c-cychou/temp_tokenized"
    max_files: int | None = None

def tokenize_line_and_add_eos(line, tokenizer):
    return tokenizer.encode(line) + [tokenizer.eos_token_id]

def process_single_file(input_filepath: str, temp_dir: str):
    """
    Processes a single JSONL file:
    - Reads JSONL records with text
    - Tokenizes each line and adds EOS token
    - Saves as numpy array in temp directory
    """
    os.makedirs(temp_dir, exist_ok=True)
    
    # Create output filename based on input filename
    input_basename = os.path.basename(input_filepath)
    output_filename = input_basename.replace('.jsonl.gz', '.npy')
    output_path = os.path.join(temp_dir, output_filename)
    
    # Skip if already processed
    if os.path.exists(output_path):
        print(f"Output file {output_path} already exists, skipping processing")
        return output_path

    tokenizer = AutoTokenizer.from_pretrained("gpt2")
    
    all_ids = []
    total_line_count = 0
    
    with fsspec.open(input_filepath, "r", compression="infer") as f:
        for line in f:
            data = json.loads(line.strip())
            text = data["text"]
            token_ids = tokenize_line_and_add_eos(text, tokenizer)
            all_ids.extend(token_ids)
            total_line_count += 1
    
    print(f"Tokenized {input_filepath} into {len(all_ids)} tokens from {total_line_count} lines")
    ids_array = np.array(all_ids, dtype=np.uint16)
    np.save(output_path, ids_array)
    
    return output_path

def concatenate_tokenized_files(temp_dir: str, output_path: str):
    """
    Concatenates all tokenized numpy arrays from temp directory into single array
    """
    # Find all .npy files in temp directory
    npy_files = glob.glob(os.path.join(temp_dir, "*.npy"))
    npy_files.sort()  # Ensure consistent ordering
    
    print(f"Found {len(npy_files)} tokenized files to concatenate")
    
    all_arrays = []
    total_tokens = 0
    
    for npy_file in tqdm.tqdm(npy_files, desc="Loading tokenized files"):
        array = np.load(npy_file)
        all_arrays.append(array)
        total_tokens += len(array)
        print(f"Loaded {npy_file}: {len(array)} tokens")
    
    print(f"Concatenating {len(all_arrays)} arrays with total {total_tokens} tokens")
    concatenated = np.concatenate(all_arrays)
    
    print(f"Final array shape: {concatenated.shape}")
    np.save(output_path, concatenated)
    
    return output_path

def get_files(input_directory: str):
    filenames = glob.glob(os.path.join(input_directory, "*.jsonl.gz"))
    return [filename for filename in filenames if "example" not in filename]

@draccus.wrap()
def main(config: TokenizeConfig):
    executor = submitit.AutoExecutor(folder="slurm_outputs")
    executor.update_parameters(
        timeout_min=30,
        mem_gb=4,
        cpus_per_task=1,
        slurm_partition="a4-cpu",
        slurm_qos="a4-cpu-qos",
        stderr_to_stdout=True,
    )
    # Get list of files to process
    num_files_future = executor.submit(
        get_files,
        config.input_path
    )    
    input_filepaths = num_files_future.result()

    if config.max_files is not None:
        input_filepaths = random.sample(input_filepaths, config.max_files)

    print(f"Found {len(input_filepaths)} files to process")
    
    # Configure executor for individual file processing jobs
    executor = submitit.AutoExecutor(folder="slurm_outputs")
    executor.update_parameters(
        slurm_array_parallelism=12,  # Process up to 50 files simultaneously
        timeout_min=30,
        mem_gb=4,
        cpus_per_task=1,
        slurm_partition="a4-cpu",
        slurm_qos="a4-cpu-qos",
        stderr_to_stdout=True,
    )
    
    # Submit batch array of jobs to process each file
    print("Submitting batch jobs for individual file tokenization...")
    with executor.batch():
        futures = []
        for input_filepath in input_filepaths:
            future = executor.submit(
                process_single_file,
                input_filepath,
                config.temp_dir
            )
            futures.append(future)
    
    print(f"Submitted {len(futures)} tokenization jobs")
    
    # Wait for all tokenization jobs to complete
    print("Waiting for all tokenization jobs to complete...")
    tokenized_files = []
    for future in tqdm.tqdm(futures, desc="Waiting for jobs"):
        tokenized_file = future.result()
        tokenized_files.append(tokenized_file)
    
    print(f"All tokenization jobs completed. {len(tokenized_files)} files tokenized.")
    
    # Configure executor for concatenation job
    executor.update_parameters(
        timeout_min=30,
        mem_gb=100,
        cpus_per_task=4,
        slurm_partition="a4-cpu",
        slurm_qos="a4-cpu-qos",
        stderr_to_stdout=True,
    )
    
    # Submit concatenation job
    print("Submitting concatenation job...")
    concat_future = executor.submit(
        concatenate_tokenized_files,
        config.temp_dir,
        config.output_path
    )
    
    print(f"Concatenation job submitted with job id: {concat_future.job_id}")
    output_file = concat_future.result()
    print(f"Final output file written: {output_file}")

if __name__ == "__main__":
    main()