ssh tcline@yeti.cr.usgs.gov
#TClinP@$$W0rd20211#!
TClinP@$$W0rd20212#!

#scp ~/"OneDrive - DOI"/Annual_VIC/NLDASdata/NLDAS_Lats.csv tcline@yeti.cr.usgs.gov:~/Annual_VIC/NLDASdata
#scp tcline@yeti.cr.usgs.gov:~/Annual_VIC/NLDAS_Routing_MultiNode_PN17.out ~/"OneDrive - DOI"/Annual_VIC/
  
#Start machine and load R
#salloc -p normal -A norock -N 1 -t 3-01:00:00
sidle
#salloc -p normal -A norock -N 1 -t 3-01:00:00
salloc -p normal -A norock -N 1 -t 3-01:00:00

cd ~/Annual_VIC
git pull

module load R/4.1.1 gdal/3.1.0 geos/3.8.1 proj/7.0.1

R

rm(list=ls())

suppressPackageStartupMessages(library(here))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(lubridate))

#Process multiple regions
regions <- c('PN17')

rr <- 1
rname <- regions[rr]

dir.create(here('NLDASdata','RoutedReformat',rname))

AllRoutedFlows <- list.files(here('NLDASdata','Routed',rname))
SplitNames <- str_split(AllRoutedFlows,pattern='_')
head(AllRoutedFlows)
head(SplitNames)
length(AllRoutedFlows)

#DoubleCheck the regions match
err1<-lapply(SplitNames,FUN=function(x,rrname=rname){
  if(x[1] != rrname){print('Error: Non-matching region names')}
})

#Load all flow files into the environment (~ 20 GB of data to hold in memory!)
temp1 <- lapply(AllRoutedFlows,FUN=function(ff){
  assign(x=ff,value=readRDS(here('NLDASdata','Routed',rname,ff)), envir = .GlobalEnv)
})
rm(temp1)

if(sum(is.na(match(AllRoutedFlows,ls()))) >0 ) print('Error: Not all files loaded into global environment properly')

#Apply over comids
#combine daily time series
#Add date labels
#Save 1 RDS per COMID
COMIDS <- names(PN17_Routed_19790102_19810225.RDS)
lapply(COMIDS,FUN=function(comid){
  #comid <- COMIDS[1]
  FullFlowList <- lapply(AllRoutedFlows,FUN=function(ff,comID=comid){
    #ff <- AllRoutedFlows[1]
    #comID <- comid
    g1<-get(ff)
    flowvals<-g1[[which(names(g1)==comID)]]
    
    #Create a list of dates
    name.split <- str_split(ff,pattern='_')[[1]][-c(1:2)]
    date.start <- ymd(name.split[1])
    date.end <- ymd(substr(name.split[2],1,8))
    date.seq <- seq(date.start,date.end,by=1)
    if(length(flowvals)!=length(date.seq)){print('Error:DateRange does not match number of flow values');break}
    
    return(data.frame(Date=date.seq,Flow=flowvals))
  }) %>% bind_rows()
  
  COMID_filename <- paste0('COMID_',comid,'_',paste0(str_split(min(FullFlowList$Date),pattern='-')[[1]],collapse=''),'_',paste0(str_split(max(FullFlowList$Date),pattern='-')[[1]],collapse=''),'.RDS')
  saveRDS(FullFlowList,here('NLDASdata','RoutedReformat',rname,COMID_filename))
  print(COMID_filename)
  
})

ls()
