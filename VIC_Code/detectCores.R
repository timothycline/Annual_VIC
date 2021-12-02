library(doParallel)
#library(Rmpi)

taskID <- as.numeric(Sys.getenv('SLURM_PROCID'))
NumNodes <- as.numeric(Sys.getenv('SLURM_JOB_NUM_NODES'))
detectCores()
print(taskID)

