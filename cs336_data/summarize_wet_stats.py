import draccus
from dataclasses import dataclass
import glob
import os
import json
import submitit

@dataclass
class SummarizeWetStatsConfig:
    input_path: str = "/data/c-cychou/documents-2"

def summarize_wet_stats(input_path):
    stats_files = glob.glob(os.path.join(input_path, "*.stats"))
    stats = []
    for stats_file in stats_files:
        with open(stats_file, "r") as f:
            stats.append(json.load(f))

    total_english_filtered_count = sum(stat["english_filtered_count"] for stat in stats)
    total_gopher_filtered_count = sum(stat["gopher_filtered_count"] for stat in stats)
    total_toxic_filtered_count = sum(stat["toxic_filtered_count"] for stat in stats)
    total_nsfw_filtered_count = sum(stat["nsfw_filtered_count"] for stat in stats)
    total_quality_filtered_count = sum(stat["quality_filtered_count"] for stat in stats)
    total_number_kept = sum(stat["number_kept"] for stat in stats)
    total_number_total = sum(stat["number_total"] for stat in stats)

    return {
        "total_english_filtered_count": total_english_filtered_count,
        "total_gopher_filtered_count": total_gopher_filtered_count,
        "total_toxic_filtered_count": total_toxic_filtered_count,
        "total_nsfw_filtered_count": total_nsfw_filtered_count,
        "total_quality_filtered_count": total_quality_filtered_count,
        "total_number_kept": total_number_kept,
        "total_number_total": total_number_total,
        "percent_english_filtered": total_english_filtered_count / total_number_total,
        "percent_gopher_filtered": total_gopher_filtered_count / total_number_total,
        "percent_toxic_filtered": total_toxic_filtered_count / total_number_total,
        "percent_nsfw_filtered": total_nsfw_filtered_count / total_number_total,
        "percent_quality_filtered": total_quality_filtered_count / total_number_total,
        "percent_kept": total_number_kept / total_number_total,
    }

@draccus.wrap()
def main(config: SummarizeWetStatsConfig):
    executor = submitit.AutoExecutor(folder="slurm_outputs")
    executor.update_parameters(
        timeout_min=30,
        mem_gb=1,
        cpus_per_task=1,
        slurm_partition="a4-cpu",
        slurm_qos="a4-cpu-qos",
        stderr_to_stdout=True,
    )

    future = executor.submit(summarize_wet_stats, config.input_path)
    print(f"Job submitted with job id: {future.job_id}")
    result = future.result()
    print(result)


if __name__ == "__main__":
    main()    