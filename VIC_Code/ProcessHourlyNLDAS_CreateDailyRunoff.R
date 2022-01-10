#Process hourly NLDAS data into daily data

library(here)
library(dplyr)
library(doParallel)
library(foreach)
library(raster)

#NLDAS_VIC0125_H.A19790102.0000.002.grb
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

#Register cluster on this node
cl <- makeCluster(detectCores())
registerDoParallel(cl)

#Loop over days to process on this node in parallel
foreach(dd=1:length(task_dirlist),.packages=c('dplyr','raster','here')) %dopar% {
  this.date <- which(all_dates==task_dirlist[dd]) #match all files that belong to this date
  #Load all gribs that correspond to this date
  GRIBS<-lapply(this.date,FUN=function(x){
    brick(here('NLDASdata','GRB_H',all_grb[x])) %>% as.array()
  })
  #Extract BGRUN
  BGRUNS <- lapply(GRIBS,FUN=function(x){
    return(x[,,13])
  })
  #Extract SSRUN
  SSRUNS <- lapply(GRIBS,FUN=function(x){
    return(x[,,12])
  })
  
  #Combine sum across all hourly data
  BGRUN_sum <- Reduce('+',BGRUNS)
  SSRUN_sum <- Reduce('+',SSRUNS)
  
  #Rebuild array
  AllRUN <- array(data=NA,dim=c(nrow(BGRUN_sum),ncol(BGRUN_sum),2))
  AllRUN[,,1] <- BGRUN_sum
  AllRUN[,,2] <- SSRUN_sum
  
  saveRDS(AllRUN,file=here('NLDASdata','DailyRunoffs',paste0('Runoff_',uni_dates[dd],'RDS')))
}

stopCluster(cl)





