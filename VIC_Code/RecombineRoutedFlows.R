rm(list=ls())

suppressPackageStartupMessages(library(here))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(doParallel))
suppressPackageStartupMessages(library(foreach))

#Process multiple regions
regions <- c('PN17')

rr <- 1
rname <- regions[rr]

dir.create(here('NLDASdata','Routed_ByCOMID',rname))

AllRoutedFlows <- list.files(here('NLDASdata','Routed_ByDate',rname))
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
  assign(x=ff,value=readRDS(here('NLDASdata','Routed_ByDate',rname,ff)), envir = .GlobalEnv)
})
rm(temp1)

if(sum(is.na(match(AllRoutedFlows,ls()))) >0 ) print('Error: Not all files loaded into global environment properly')

#Apply over comids
#combine daily time series
#Add date labels
#Save 1 RDS per COMID
#cl <- makeCluster(detectCores())
#registerDoParallel(cl)

COMIDS <- names(get(AllRoutedFlows[1]))

COMIDfiles_written <- lapply(COMIDS,FUN=function(cc){
  comid <- cc
  FullFlowList <- lapply(AllRoutedFlows,FUN=function(ff,comID=comid){
    #ff <- AllRoutedFlows[1]
    #comID <- comid
    g1<-get(ff)
    flowvals<-g1[[which(names(g1)==comID)]]
    return(flowvals)
  }) %>% bind_rows()
  
  COMID_filename <- paste0('COMID_',comid,'_',paste0(str_split(min(FullFlowList$Date),pattern='-')[[1]],collapse=''),'_',paste0(str_split(max(FullFlowList$Date),pattern='-')[[1]],collapse=''),'.RDS')
  saveRDS(FullFlowList,here('NLDASdata','Routed_ByCOMID',rname,COMID_filename))
  COMID_filename
})

# COMIDfiles_written <- foreach(cc = 1:length(COMIDS), .packages = c('dplyr')) %dopar% {
#   comid <- COMIDS[cc]
#   FullFlowList <- lapply(AllRoutedFlows,FUN=function(ff,comID=comid){
#     #ff <- AllRoutedFlows[1]
#     #comID <- comid
#     g1<-get(ff)
#     flowvals<-g1[[which(names(g1)==comID)]]
#     return(flowvals)
#   }) %>% bind_rows()
#   
#   COMID_filename <- paste0('COMID_',comid,'_',paste0(str_split(min(FullFlowList$Date),pattern='-')[[1]],collapse=''),'_',paste0(str_split(max(FullFlowList$Date),pattern='-')[[1]],collapse=''),'.RDS')
#   saveRDS(FullFlowList,here('NLDASdata','Routed_ByCOMID',rname,COMID_filename))
#   COMID_filename
# }

#stopCluster(cl)