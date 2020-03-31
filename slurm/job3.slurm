#!/bin/bash
#SBATCH --account=COMP90024
#SBATCH --partition=physical
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=0-00:05:00
#SBATCH --job-name=Twitter_job3
#SBATCH -e log_job3.log
#SBATCH -o slurm_job3.out
# Use this email address:
#SBATCH --mail-user=saketk@student.unimelb.edu.au
#SBATCH --mail-type=FAIL,END

echo "----------- 2-node-8-cores -----------"
echo ""

# Load required modules
module load Python/3.7.3-spartan_gcc-8.1.0

# Launch multiple process python code
mpiexec python parser.py -i bigTwitter.json