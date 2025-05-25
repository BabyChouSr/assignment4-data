#!/bin/bash

# Parameters
#SBATCH --cpus-per-task=4
#SBATCH --job-name=benchmarking_script
#SBATCH --mem=100GB
#SBATCH --nodes=1
#SBATCH --open-mode=append
#SBATCH --output=/home/c-cychou/assignment2-systems/slurm_outputs/%j_0_log.out
#SBATCH --partition=a4-cpu
#SBATCH --qos=a4-cpu-qos
#SBATCH --signal=USR2@90
#SBATCH --time=30

uv run python cs336_data/train_fasttext.py --input_path train.txt --output_path fasttext_model.bin