#!/bin/bash

# testing some scripts

#SBATCH --job-name=NLDAS_Hourly_Processing
#SBATCH -n 10
#SBATCH -N 10
#SBATCH -p normal
#SBATCH --account=norock
#SBATCH --time=6-23:00:00
#SBATCH --mail-type=ALL
#SBATCH --mail-user=tcline@usgs.gov
#SBATCH -o NLDAS_Hourly_Download.out

module load gnu8/8.3.0 R/4.1.1 

prun Rscript ~/Annual_VIC/VIC_Code/VIC_NLDASdownload_multiNode.R
