#!/bin/bash
#SBATCH --account=COMP90024
#SBATCH --partition=physical
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=0-00:20:00
#SBATCH --job-name=Twitter_job2
#SBATCH -e log_job2.log
#SBATCH -o slurm_job2.out
# Use this email address:
#SBATCH --mail-user=saketk@student.unimelb.edu.au
#SBATCH --mail-type=FAIL
#SBATCH --mail-type=END

echo "----------- 1-node-8-cores -----------"
echo ""

# Load required modules
module load Python/3.7.3-spartan_gcc-8.1.0

# Launch multiple process python code
mpiexec python parser.py -i bigTwitter.json