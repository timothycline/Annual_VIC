rm(list=ls())

suppressPackageStartupMessages(library(here))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(doParallel))
suppressPackageStartupMessages(library(foreach))
suppressPackageStartupMessages(library(zoo))

regions <- c('PN17','MS10U')

# 1/2 of nodes to each region
rr <- taskID <- as.numeric(Sys.getenv('SLURM_PROCID')) + 1 #Get the Node number, SLURM returns 0 for first node, but we want 1 for indexing

rname <- regions[rr]
SubFiles <- list.files(here('NLDASdata','AnnualFlowStats',rname))
CombiFile <- lapply(1:length(SubFiles),FUN=function(x){
  r1<-readRDS(here('NLDASdata','AnnualFlowStats',rname,SubFiles[x]))
  return(r1)
}) %>% bind_rows()

saveRDS(CombiFile,here('NLDASdata','AnnualFlowStats',paste0(rname,'_AnnualFlowStats.RDS')))