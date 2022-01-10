ssh tcline@yeti.cr.usgs.gov
TClinP@$$W0rd20211#!

salloc -p normal -A norock -n 1 -N 1 -t 3-01:00:00

cd ~/Annual_VIC
git pull
module load R/4.1.1

R

library(here)
library(dplyr)
library(doParallel)
library(foreach)
library(raster)

fulldir <- list.files(here('NLDASdata','GRB_H'))
all_grb <- fulldir[grep(".grb",fulldir, invert = FALSE)]
all_dates <- substr(all_grb,18,25)

#Get all days to process
uni_dates <- unique(all_dates)

#Split days across Nodes
NumNodes <- as.numeric(Sys.getenv('SLURM_JOB_NUM_NODES')) #Get the number of nodes assigned to the job
taskID <- as.numeric(Sys.getenv('SLURM_PROCID')) #Get the Node number
dirsplit <- split(uni_dates,1:NumNodes) #split total number of directories into groups by

#vector of days to process on this node
task_dirlist <- dirsplit[[taskID + 1]]

#srun Rscript VIC_Code/detectCores.R
srun Rscript VIC_Code/Download_VIC_2020_04_16_multiNode.R
#srun Rscript MPVA_ReddsOnly_Yeti.R

exit