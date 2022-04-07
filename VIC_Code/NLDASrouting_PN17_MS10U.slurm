#!/bin/bash

# testing some scripts

#SBATCH --job-name=NLDAS_Routing_MultiNode_PN17_MS10U
#SBATCH -n 10
#SBATCH -N 10
#SBATCH -p normal
#SBATCH --account=norock
#SBATCH --time=6-23:00:00
#SBATCH --mail-type=ALL
#SBATCH --mail-user=tcline@usgs.gov
#SBATCH -o NLDAS_Routing_MultiNode_PN17_MS10U.out

cd ~/Annual_VIC
git pull

module load R/4.1.1 gdal/3.1.0 geos/3.8.1 proj/7.0.1

prun Rscript VIC_Code/NLDASrouting_PN17_MS10U_multiNode.R
