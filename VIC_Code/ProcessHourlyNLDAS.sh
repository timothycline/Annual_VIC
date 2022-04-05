ssh tcline@yeti.cr.usgs.gov
TClinP@$$W0rd20211#!

cd ~/Annual_VIC
git pull

sbatch ~/Annual_VIC/VIC_Code/NLDASprocessing.slurm
scp tcline@yeti.cr.usgs.gov:~/Annual_VIC/NLDAS_Hourly_Processing.out ~/"OneDrive - DOI"/Annual_VIC
cat NLDAS_Hourly_Processing.out
scp tcline@yeti.cr.usgs.gov:~/Annual_VIC/NLDASdata/GRB_H/NLDAS_VIC0125_H.A20150705.0900.002.grb ~/"OneDrive - DOI"/Annual_VIC
sidle
scancel 5852330


salloc -p normal -A norock -N 1 -t 3-01:00:00

cd ~/Annual_VIC
git pull
module load R/4.1.1 gdal/3.1.0 geos/3.8.1 proj/7.0.1
srun Rscript VIC_Code/ProcessHourlyNLDAS_CreateDailyRunoff.R

#srun Rscript VIC_Code/detectCores.R
#srun Rscript VIC_Code/Download_VIC_2020_04_16_multiNode.R
#srun Rscript MPVA_ReddsOnly_Yeti.R

#exit

salloc -p normal -A norock -N 10 -t 3-01:00:00
cd ~/Annual_VIC
git pull
module load R/4.1.1 gdal/3.1.0 geos/3.8.1 proj/7.0.1
R

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
taskID <- 0#as.numeric(Sys.getenv('SLURM_PROCID')) #Get the Node number
dirsplit <- split(uni_dates,1:NumNodes) #split total number of directories into groups by

#vector of days to process on this node
task_dirlist <- dirsplit[[taskID + 1]]

cl <- makeCluster(detectCores())
registerDoParallel(cl)

which(task_dirlist == '20150705')

CorruptedFiles <- c('NLDAS_VIC0125_H.A20150705.0900.002.grb')
CorruptedDates <- lapply(CorruptedFiles,FUN=function(x){return(as.numeric(substr(x,18,25)))}) %>% unlist()
#Loop over days to process on this node in parallel
#foreach(dd=1:1,.packages=c('dplyr','raster','here')) %dopar% {
alloutput <- foreach(dd=1:length(task_dirlist),.packages=c('dplyr','raster','here')) %dopar% {
  dd<-13334
  print(paste('Slurm Job Number ',taskID,' :: starting ',task_dirlist[dd]))
  this.date <- which(all_dates==task_dirlist[dd]) #match all files that belong to this date
  
  #issue with loading one grib. NLDAS_VIC0125_H.A20150705.0900.002.RDS. I saved an RDS from Diane to the folder
  #Load all gribs that correspond to this date
  if(!file.exists(here('NLDASdata','DailyRunoffs',paste0('DailyRunoff_',task_dirlist[dd],'.RDS')))){
      
    GRIBS<-lapply(this.date,FUN=function(x){
      #CHECK FOR CORRUPTED FILE LIST 
      if(!(all_grb[x] %in% CorruptedFiles)){
        GRB1<-brick(here('NLDASdata','GRB_H',all_grb[x])) %>% as.array()
        #GRB1<-tryCatch(brick(here('NLDASdata','GRB_H',all_grb[x])) %>% as.array(),error=function(e){print(paste0('Error: brick failure on ',all_grb[x]))})
        return(GRB1)
      }
    })
    
    #Extract BGRUN
    BGRUNS <- lapply(GRIBS,FUN=function(x){
      BG1<-x[,,13]
      return(BG1)
    })
    #Extract SSRUN
    SSRUNS <- lapply(GRIBS,FUN=function(x){
      SS1<-x[,,12]
      return(SS1)
    })
    
    #If corrupted files for this day
    #Load RDS files for the corrupted files
    if(task_dirlist[dd] %in% CorruptedDates){
      CorruptedLists <- lapply(CorruptedFiles[CorruptedDates == task_dirlist[dd]],FUN=function(x){
        RDSname <- paste0(substr(x,1,nchar(x)-4),'.RDS')
        RDS1 <- readRDS(here('NLDASdata',RDSname))
        return(RDS1)
      })
      names(CorruptedLists) <- CorruptedFiles
      
      for(i in 1:length(CorruptedLists)){
        BGRUNS[[length(BGRUNS)]] <- CorruptedLists$BGRUN
        SSRUNS[[length(SSRUNS)]] <- CorruptedLists$SSRUN
      }
    }
    
    #Combine sum across all hourly data
    BGRUN_sum <- Reduce('+',BGRUNS)
    SSRUN_sum <- Reduce('+',SSRUNS)
    
    #Rebuild list
    # AllRUN <- array(data=NA,dim=c(nrow(BGRUN_sum),ncol(BGRUN_sum),2))
    # AllRUN[,,1] <- BGRUN_sum
    # AllRUN[,,2] <- SSRUN_sum
    
    AllRUN <- list(BGRUN = BGRUN_sum, SSRUN = SSRUN_sum)
    
    saveRDS(AllRUN,file=here('NLDASdata','DailyRunoffs',paste0('DailyRunoff_',task_dirlist[dd],'.RDS')))
    return(paste0(taskID,'_DailyRunoff_',task_dirlist[dd],'.RDS'))
  }
}

print(paste('Slurm Job Number ',taskID))

stopCluster(cl)

alldailyfiles <- list.files(here('NLDASdata','DailyRunoffs'))
length(alldailyfiles)==length(uni_dates)

q()
exit
