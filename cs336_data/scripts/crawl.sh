#!/bin/bash

# Parameters
#SBATCH --cpus-per-task=8
#SBATCH --job-name=crawl_100k
#SBATCH --mem=100GB
#SBATCH --open-mode=append
#SBATCH --output=/home/c-cychou/assignment4-data/slurm_outputs/%j_0_log.out
#SBATCH --partition=a1-batch-cpu
#SBATCH --qos=a1-batch-cpu-qos
#SBATCH --time=12:00:00

uv run python cs336_data/parallel_warc_scraper.py --url_file subsampled_positive_urls_100k.txt --output_warc scraped_urls_100k.warc