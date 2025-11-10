#!/bin/bash
#SBATCH --job-name=extract_papers
#SBATCH --partition=gpu-h100,gpu-a100
#SBATCH --nodes=1
#SBATCH --gres=gpu:1              # Request 3 GPUs
#SBATCH --cpus-per-task=5         # Reasonable CPU allocation
#SBATCH --mem=64G                  # Total system RAM
#SBATCH --time=00:45:00            # Adjust time as needed
#SBATCH --output=//home/youssef.abdelwaha1/biopapersnlp/general_logs/logs/test_olcr_extract_%j.out
#SBATCH --error=/home/youssef.abdelwaha1/biopapersnlp/general_logs/logs/test_olcr_extract_%j.err


set -euo pipefail
source /home/youssef.abdelwaha1/biopapersnlp/env_vars.sh

export INPUT_DIR="$test_papers_dir_2"
export OUT_DIR="$test_papers_output"
 
echo "Loading env vars"
echo "Loading modules"



module --force purge
module load cuda/12.6
module load gcc/13.3

: "${env_vars:?set in env_vars.sh}"

echo "Activating venv"
source /home/youssef.abdelwaha1/software/miniforge3/etc/profile.d/conda.sh
conda activate olmocr || { echo "Conda activate failed"; exit 1; }
echo "Activated: $(which python)"



echo "[DBG] Host: $(hostname)"
echo "[DBG] SLURM_JOB_ID=${SLURM_JOB_ID:-unset} \
SLURM_GPUS=${SLURM_GPUS:-unset} \
SLURM_JOB_GPUS=${SLURM_JOB_GPUS:-unset} \
SLURM_STEP_GPUS=${SLURM_STEP_GPUS:-unset} \
CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-unset}"


srun -u --ntasks=1 --gpus=1 python - <<'PY'
import torch; print("cuda?", torch.cuda.is_available(), "count", torch.cuda.device_count())
if torch.cuda.is_available(): print(torch.cuda.get_device_name(0))
PY
  

echo "Crucial Libararies Available, GPU Node is Visible"

echo "[INFO] Starting Paper Extraction Process"


srun -u --ntasks=1 --gres=gpu:1 \
  python "/home/youssef.abdelwaha1/biopapersnlp/Document_Extraction/s3_preprint_extraction.py" \
  --model-name allenai/olmOCR-7B-0725-FP8 \
  --port 5000 \
  --max-model-len 24576 \
  --gpu-memory-utilization 0.95 \
  --out-dir "${OUT_DIR}" \
  --markdown \
  --input-dir "${INPUT_DIR}" \
  --recursive
