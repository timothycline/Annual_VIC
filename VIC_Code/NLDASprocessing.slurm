#!/bin/bash

# testing some scripts

#SBATCH --job-name=NLDAS_Hourly_Processing
#SBATCH -N 20
#SBATCH -p normal
#SBATCH --account=norock
#SBATCH --time=6-23:00:00
#SBATCH --mail-type=ALL
#SBATCH --mail-user=tcline@usgs.gov
#SBATCH -o NLDAS_Hourly_Processing.out

cd ~/Annual_VIC
module load R/4.1.1 gdal/3.1.0 geos/3.8.1 proj/7.0.1

prun Rscript ~/Annual_VIC/VIC_Code/ProcessHourlyNLDAS_CreateDailyRunoff.R
