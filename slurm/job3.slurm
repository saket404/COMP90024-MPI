#!/bin/bash
#SBATCH --account=COMP90024
#SBATCH --partition=physical
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=0-00:05:00
#SBATCH --job-name=Twitter_job3
#SBATCH -o slurm_job3.out
# Use this email address:
#SBATCH --mail-user=saketk@student.unimelb.edu.au
#SBATCH --mail-type=FAIL,END

echo "----------- 2-node-8-cores -----------"
echo ""

# Load required modules
module load Python/3.4.3-goolf-2015a

# Launch multiple process python code
time mpiexec python processor.py -i bigTwitter.json