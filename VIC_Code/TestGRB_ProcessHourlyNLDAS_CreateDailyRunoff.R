#Process hourly NLDAS data into daily data

library(here,quietly=T)
library(dplyr,quietly=T)
library(doParallel,quietly=T)
library(foreach,quietly=T)
library(raster,quietly=T)

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
#foreach(dd=1:1,.packages=c('dplyr','raster','here')) %dopar% {
alloutput <- foreach(dd=1:length(task_dirlist),.packages=c('dplyr','raster','here')) %dopar% {
  this.date <- which(all_dates==task_dirlist[dd]) #match all files that belong to this date
  #Load all gribs that correspond to this date
  GRIBS<-lapply(this.date,FUN=function(x){
    GRB1<-tryCatch(brick(here('NLDASdata','GRB_H',all_grb[x])) %>% as.array(),error=function(e){print(paste0('Error: brick failure on ',all_grb[x]))})
    return(GRB1)
  })
  #Extract BGRUN
  # BGRUNS <- lapply(GRIBS,FUN=function(x){
  #   BG1<-x[,,13]
  #   return(BG1)
  # })
  # #Extract SSRUN
  # SSRUNS <- lapply(GRIBS,FUN=function(x){
  #   SS1<-x[,,12]
  #   return(SS1)
  # })
  
  #Combine sum across all hourly data
  # BGRUN_sum <- Reduce('+',BGRUNS)
  # SSRUN_sum <- Reduce('+',SSRUNS)
  
  #Rebuild list
  # AllRUN <- array(data=NA,dim=c(nrow(BGRUN_sum),ncol(BGRUN_sum),2))
  # AllRUN[,,1] <- BGRUN_sum
  # AllRUN[,,2] <- SSRUN_sum
  
  # AllRUN <- list(BGRUN = BGRUN_sum, SSRUN = SSRUN_sum)
  
  # saveRDS(AllRUN,file=here('NLDASdata','DailyRunoffs',paste0('DailyRunoff_',uni_dates[dd],'.RDS')))
  return(paste0(taskID,'_DailyRunoff_',uni_dates[dd],'.RDS'))
}

print(paste('Slurm Job Number ',taskID))

stopCluster(cl)





