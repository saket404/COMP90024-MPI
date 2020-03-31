#!/bin/bash
#SBATCH --account=COMP90024
#SBATCH --partition=physical
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=0-00:12:00
#SBATCH --job-name=Twitter_job1
#SBATCH -e log_job1.log
#SBATCH -o slurm_job1.out
# Use this email address:
#SBATCH --mail-user=saketk@student.unimelb.edu.au
#SBATCH --mail-type=FAIL,END

echo "----------- 1-node-1-core -----------"
echo ""

# Load required modules
module load Python/3.7.3-spartan_gcc-8.1.0

# Launch multiple process python code
mpiexec python parser.py -i bigTwitter.json