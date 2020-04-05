#!/bin/bash
#SBATCH --account=COMP90024
#SBATCH --partition=physical
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=0-00:12:00
#SBATCH --job-name=Twitter_job1
#SBATCH -o slurm_job1.out
# Use this email address:
#SBATCH --mail-user=saketk@student.unimelb.edu.au
#SBATCH --mail-type=FAIL,END

echo "----------- 1-node-1-core -----------"
echo ""

# Load required modules
module load Python/3.4.3-goolf-2015a

# Launch multiple process python code
time mpiexec python processor.py -i bigTwitter.json