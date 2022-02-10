#!/bin/bash

# testing some scripts

#SBATCH --job-name=NLDAS_Routing_MultiNode_PN17
#SBATCH -n 10
#SBATCH -N 10
#SBATCH -p normal
#SBATCH --account=norock
#SBATCH --time=6-23:00:00
#SBATCH --mail-type=ALL
#SBATCH --mail-user=tcline@usgs.gov
#SBATCH -o NLDAS_Routing_MultiNode_PN17.out

prun Rscript ~/Annual_VIC/VIC_Code/NLDASrouting_PN17_multiNode.R
