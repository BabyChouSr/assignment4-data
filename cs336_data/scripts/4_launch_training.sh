#!/bin/bash

# Parameters
#SBATCH --cpus-per-task=4
#SBATCH --gpus=2
#SBATCH --job-name=train
#SBATCH --mem=100GB
#SBATCH --open-mode=append
#SBATCH --output=/home/c-cychou/assignment4-data/slurm_outputs/%j_0_log.out
#SBATCH --partition=a4-batch
#SBATCH --qos=a4-batch-qos
#SBATCH --time=12:00:00

cd cs336-basics && uv run torchrun --standalone --nproc_per_node=2 scripts/train.py --config-name=experiment/your_data