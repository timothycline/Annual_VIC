#!/bin/bash

# testing some scripts

#SBATCH --job-name=CalculateAnnualFlowStats_PN17
#SBATCH -n 5
#SBATCH -N 5
#SBATCH -p normal
#SBATCH --account=norock
#SBATCH --time=6-23:00:00
#SBATCH --mail-type=ALL
#SBATCH --mail-user=tcline@usgs.gov
#SBATCH -o CalculateAnnualFlowStats.out

cd ~/Annual_VIC
git pull

module load R/4.1.1 gdal/3.1.0 geos/3.8.1 proj/7.0.1

prun Rscript VIC_Code/CalculateStreamFlowMetrics.R
