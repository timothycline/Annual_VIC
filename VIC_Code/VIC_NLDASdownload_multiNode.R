library(here)
library(dplyr)
library(doParallel)
library(foreach)

DLlist <- read.delim(here('NLDASdata','NLDASdownload_Hourly_01062022.txt'),header=F,stringsAsFactors = F) %>% pull(1)

NumNodes <- as.numeric(Sys.getenv('SLURM_JOB_NUM_NODES')) #Get the number of nodes assigned to the job
taskID <- as.numeric(Sys.getenv('SLURM_PROCID')) #Get the Node number
dirsplit <- split(DLlist,1:NumNodes) #split total number of directories into groups by
task_dirlist <- dirsplit[[taskID + 1]] 
length(task_dirlist)

cl <- makeCluster(detectCores())
registerDoParallel(cl)
#foreach(ff=1:length(task_dirlist)) %dopar% {
foreach(ff=1:1) %dopar% {
  system("cd ~/Annual_VIC/NLDASdata/GRB_H")
  system("touch .netrc")
  system('echo "machine urs.earthdata.nasa.gov login timothy_cline password %#%earthdata1542gnpMT" >> .netrc')
  system("touch .urs_cookies")
  system(paste0("wget --load-cookies .urs_cookies --save-cookies .urs_cookies --auth-no-challenge=on --no-check-certificate --keep-session-cookies ",task_dirlist[ff]," -P ~/Annual_VIC/NLDASdata/GRB_H"))
}
stopCluster(cl)



#https://hydro1.gesdisc.eosdis.nasa.gov/data/NLDAS/NLDAS_VIC0125_H.002/1979/002/NLDAS_VIC0125_H.A19790102.0000.002.grb