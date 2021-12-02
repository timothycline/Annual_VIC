library(doParallel)
#library(Rmpi)

taskID <- as.numeric(Sys.getenv('SLURM_PROCID'))
detectCores()
#print(taskID)

