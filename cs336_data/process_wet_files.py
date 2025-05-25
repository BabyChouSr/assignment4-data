import os
import json
import submitit
from tqdm import tqdm
import pathlib
import glob
import fsspec
from fastwarc.warc import ArchiveIterator, WarcRecordType
from cs336_data.gopher import check_gopher_filters
from cs336_data.language_identification import identify_language
from cs336_data.nsfw_detection import classify
from cs336_data.quality_classify import classify_quality
from fastwarc.stream_io import *
import random
import draccus
from dataclasses import dataclass


@dataclass
class ProcessWetFilesConfig:
    output_path: str = "/data/c-cychou/documents"
    max_files: int | None = None

def replace_file_extensions(filename: str) -> str:
    basename_without_extensions = filename.split(".warc.wet.gz")[0]
    return basename_without_extensions, f"{basename_without_extensions}.jsonl.gz"

def process_single_wet_file(input_path: str, output_path: str):
    """
    Processes a single WET file:
    - Reads WET records with extracted text
    - Applies language, NSFW, hate, and Gopher filters
    - Writes filtered documents to output_path
    """

    basename_without_extensions, output_path = replace_file_extensions(output_path)

    if os.path.exists(f"{basename_without_extensions}.SUCCESS"):
        print(f"Skipping {input_path} because it has already been processed at {basename_without_extensions}.SUCCESS")
        return output_path

    filtered_lines = []
    file_iterator = ArchiveIterator(GZipStream(FileStream(input_path, 'rb')), record_types=WarcRecordType.conversion)
    
    english_filtered_count = 0
    gopher_filtered_count = 0
    toxic_filtered_count = 0
    nsfw_filtered_count = 0
    quality_filtered_count = 0
    number_kept = 0
    number_total = 0

    for record in file_iterator:
        number_total += 1
        record_body = record.reader.read()
        # WET files already contain extracted text
        text = record_body.decode('utf-8', errors='replace')
        
        # Apply filters
        language, _ = identify_language(text)
        if language != "en":
            # print("Skipping non-English document")
            english_filtered_count += 1
            continue
        gopher_flag_to_keep = check_gopher_filters(text)
        if not gopher_flag_to_keep:
            # print("Skipping Gopher document")
            gopher_filtered_count += 1
            continue
        toxic_flag, _ = classify(text, "hate")
        nsfw_flag, _ = classify(text, "nsfw")
        if toxic_flag == "toxic" or nsfw_flag == "nsfw":
            # print("Skipping toxic or NSFW document")
            if toxic_flag == "toxic":
                toxic_filtered_count += 1
            if nsfw_flag == "nsfw":
                nsfw_filtered_count += 1
            continue
        quality_flag, quality_score = classify_quality(text)
        # print(quality_flag, quality_score)
        if quality_flag == "lq":
            print("Skipping low quality document")
            quality_filtered_count += 1
            continue

        filtered_lines.append(text)
        number_kept += 1

    with fsspec.open(output_path, "w", compression="infer") as f_out:
        for line in filtered_lines:
            f_out.write(json.dumps({"text": line}) + "\n")

    resp = {
        "english_filtered_count": english_filtered_count,
        "gopher_filtered_count": gopher_filtered_count,
        "toxic_filtered_count": toxic_filtered_count,
        "nsfw_filtered_count": nsfw_filtered_count,
        "quality_filtered_count": quality_filtered_count,
        "number_kept": number_kept,
        "number_total": number_total,
    }
    with open(f"{basename_without_extensions}.stats", "w") as f_stats:
        f_stats.write(json.dumps(resp))

    with fsspec.open(f"{basename_without_extensions}.SUCCESS", "w") as f_success:
        f_success.write(f"Successfully processed {input_path}")

    return output_path

def get_files(wet_directory: str):
    filenames = glob.glob(os.path.join(wet_directory, "*.warc.wet.gz"))
    return [filename for filename in filenames if "example" not in filename]
    
def submit_job(wet_filepaths: list[str], output_directory: str):
    # Set up the submitit executor
    executor = submitit.AutoExecutor(folder="slurm_outputs")
    max_simultaneous_jobs = 12

    # Configure parameters of each job launched by submitit
    executor.update_parameters(
        slurm_array_parallelism=max_simultaneous_jobs,
        timeout_min=30,
        mem_gb=4,
        cpus_per_task=1,
        slurm_partition="a4-cpu",
        slurm_qos="a4-cpu-qos",
        stderr_to_stdout=True,
    )

    futures = []
    # Use executor.batch() context manager to group all of the jobs in a Slurm array
    with executor.batch():
        for wet_filepath in wet_filepaths:
            wet_filename = str(pathlib.Path(wet_filepath).name)
            future = executor.submit(
                process_single_wet_file,
                wet_filepath,
                os.path.join(output_directory, wet_filename)
            )
            futures.append(future)

    # Use tqdm to display progress
    for future in tqdm(
        submitit.helpers.as_completed(futures),
        total=len(wet_filepaths),
    ):
        output_file = future.result()
        print(f"Output file written: {output_file}")

@draccus.wrap()
def main(config: ProcessWetFilesConfig):
    executor = submitit.AutoExecutor(folder="slurm_outputs")
    executor.update_parameters(
        timeout_min=30,
        mem_gb=1,
        cpus_per_task=1,
        slurm_partition="a4-cpu",
        slurm_qos="a4-cpu-qos",
        stderr_to_stdout=True,
    )
    future = executor.submit(
        get_files,
        wet_directory="/data/CC",
    )
    print(f"Job submitted with job id: {future.job_id}")
    wet_filepaths = future.result()

    if config.max_files is not None:
        wet_filepaths = random.sample(wet_filepaths, config.max_files)

    print(f"Job completed with {len(wet_filepaths)} files")
    submit_job(wet_filepaths, config.output_path)

if __name__ == "__main__":
    main()

