#!/bin/bash
#SBATCH --job-name=tokenizer
#SBATCH --output=run_tokenizer.out
#SBATCH --error=run_tokenizer.err
#SBATCH --time=24:00:00
#SBATCH --partition=broadwl
#SBATCH --cpus-per-task=1
#SBATCH --ntasks=4
#SBATCH --account=pi-jevans

module load python/3.6.1+intel-16.0
module load java/1.8
module load libffi

source /project2/jevans/virt_sbatch/bin/activate

echo "Starting"
echo `which python`

python tokenizing.py
