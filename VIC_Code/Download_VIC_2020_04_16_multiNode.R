# Nathan Walker
# Download VIC data
# April 16, 2020

# Clear workspace
rm(list = ls(all = TRUE))

# Load libraries
library(RCurl)
library(ncdf4)
library(here)
library(stringr)
library(doParallel)
library(foreach)

# Set working directory
outdir <- here('InputData')
#setwd(outdir)



# get list of folders in ftp site
site_url <- "ftp://gdo-dcp.ucllnl.org/pub/dcp/archive/cmip5/hydro/BCSD_daily_VIC_nc/"
#dirlist <- strsplit(getURL(site_url,ftp.use.epsv = FALSE,dirlistonly = TRUE),"\r\n")[[1]]
dirlist <- str_split(getURL(site_url,ftp.use.epsv = FALSE,dirlistonly = TRUE),"\n")[[1]]

# filter this to just RCP 8.5 directories
dirlist <- dirlist[grep("rcp85",dirlist, invert = FALSE)]

# filter to particular models:
# bcc-csm1-1, bcc-csm1-1-m, CanESM2, CCSM4, CNRM-CM5, CSIRO-Mk3-6-0, GFDL-ESM2G, GFDL-ESM2M, HadGEM2-CC365, 
# HadGEM2-ES365, inmcm4, IPSL-CM5A-MR, IPSL-CM5B-LR, MIROC5, MIROC-ESM, MIROC-ESM-CHEM, MRI-CGCM3, NorESM1-M
dirlist <- dirlist[grep("bcc-csm1-1|canesm2|ccsm4|cnrm-cm5|csiro-mk3-6|gfdl-esm|hadgem2-cc|hadgem2-es|inmcm4|ipsl-cm|miroc|mri-cgcm3|noresm1",dirlist, invert = FALSE)]

length(dirlist)*length(1975:2021)

NumNodes <- as.numeric(Sys.getenv('SLURM_JOB_NUM_NODES')) #Get the number of nodes assigned to the job
taskID <- as.numeric(Sys.getenv('SLURM_PROCID')) #Get the Node number
dirsplit <- split(dirlist,1:NumNodes) #split total number of directories into groups by
task_dirlist <- dirsplit[[taskID + 1]] #assign specific directories to nodes

cl <- makeCluster(detectCores())
registerDoParallel(cl)

# For each folder:
for(dd in 1:length(task_dirlist)){
#for (dd in 1:4){
  #for (dd in 1:20){
  #dd <- 1; ff <- 1
  
  # get list of files in folder
  filelist <- str_split(getURL(paste0(site_url,task_dirlist[dd],"/"),ftp.use.epsv = FALSE,dirlistonly = TRUE),"\n")[[1]]
  
  # filter to just total runoff
  filelist <- filelist[grep("total_runoff",filelist, invert = FALSE)]
  
  # find year of each file
  fileyrs <- as.integer(substr(filelist, nchar(filelist) - 6, nchar(filelist) - 3))
  
  # filter to just the years we want
  #filelist <- filelist[(fileyrs >= 1975 & fileyrs <= 2021) | (fileyrs >= 2030 & fileyrs <= 2059) | (fileyrs >= 2070 & fileyrs <= 2099)]
  filelist <- filelist[(fileyrs >= 1975 & fileyrs <= 2021)]
  
  
  # for each file:
  foreach(ff =  1:length(filelist)) %dopar% {
    
    # set file name to download
    url_sel <- paste0(site_url,task_dirlist[dd],"/",filelist[ff])
    
    # Set destination
    dest <- paste0(outdir, "/",filelist[ff])
    #dest <- here('InputData',filelist[ff])
   
    
    # skip files that have already been downloaded
    if (!file.exists(dest)){
      # download file: Unable to open nc files unless they were downloaded with 'mode='wb''
      # enclose in try wrapper, since this sometimes fails
      try(download.file(url=url_sel,destfile=dest,mode='wb'))
    }
    
    print(paste0("Downloaded file ", ff, " of ", length(filelist), " from folder ",dd," of ",length(task_dirlist)))
  }
}
stopCluster(cl)

# Test all downloads
nclist <- list.files(outdir)

# for each file:
for(nn in 1:length(nclist)){
  ncfile <- paste0(outdir, "/",nclist[nn])
  
  # check for error when opening file
  errorTest <- tryCatch(nc <- nc_open(ncfile,write=FALSE), 
                        error=function(e) {e})
  # if error returned, go to next value; otherwise, open file
  if(inherits(errorTest,"error")){
    print(paste0("Error reading file ", ncfile))
    next
  }
  
  print(paste0("Tested file ", nn, " of ", length(nclist), "."))
}

# If any fail, delete broken files and rerun download script, then test again
