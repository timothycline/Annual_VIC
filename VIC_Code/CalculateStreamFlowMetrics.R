rm(list=ls())

suppressPackageStartupMessages(library(here))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(doParallel))
suppressPackageStartupMessages(library(foreach))
suppressPackageStartupMessages(library(zoo))

#### DIVIDE TASKS AMONG SLURM NODES
NumNodes <- as.numeric(Sys.getenv('SLURM_JOB_NUM_NODES')) #Get the number of nodes assigned to the job
taskID <- as.numeric(Sys.getenv('SLURM_PROCID')) + 1 #Get the Node number, SLURM returns 0 for first node, but we want 1 for indexing

#Process multiple regions
regions <- c('PN17','MS10U')

# 1/2 of nodes to each region
rr <- ifelse(taskID <= (NumNodes/2),1,2)
rname <- regions[rr]

dir.create(here('NLDASdata','AnnualFlowStats',rname))

#Load all comid's by region
allCOMIDfiles <- list.files(here('NLDASdata','Routed_ByCOMID',rname))

dirsplit <- split(1:length(allCOMIDfiles),cut(1:length(allCOMIDfiles),(NumNodes/2),labels=F)) #split total number of directories into groups by
setID <- ifelse(taskID <= (NumNode/2),taskID, taskID-5 )
task_dirlist <- allCOMIDfiles[dirsplit[[setID]]] 

cl <- makeCluster(detectCores())
registerDoParallel(cl)

Node_FlowStats <- foreach(CMID = 1:length(task_dirlist),.packages=c('dplyr','stringr','lubridate','zoo','here')) %dopar% {
  FileName <- task_dirlist[CMID]
  COM1 <- readRDS(here('NLDASdata','Routed_ByCOMID',rname,FileName)) %>% 
    mutate(Year=year(Date),Month=month(Date),YearMonth=paste(Year,Month,sep='_'),DOY=format(Date,'%j'),WaterYear = ifelse(DOY>=274,Year+1,Year)) %>%
    mutate(Flow = Flow/86400)
  
  COMID1 <- str_split(FileName,pattern='_')[[1]][2] %>% as.numeric()
  
  #Compute Flow Metrics
  SummerFlow <- COM1 %>% filter(DOY %in% seq(152,273)) %>% group_by(Year) %>% summarize(SummerFlow=mean(Flow))
  AnnualFlow <- COM1 %>% group_by(Year) %>% summarize(AnnualFlow=mean(Flow))
  WaterYearFlow <- COM1 %>% group_by(WaterYear) %>% summarize(WaterYearFlow=mean(Flow)) %>% mutate(Year=WaterYear)
  #W95
  q95 <- COM1 %>% group_by(WaterYear) %>% summarize(q95=quantile(Flow,probs=c(0.95))) %>% mutate(Year=WaterYear)
  W95 <- COM1 %>% 
    filter(DOY %in% c(335:365,1:90)) %>% 
    mutate(q95 = q95$q95[match(WaterYear,q95$WaterYear)]) %>% 
    group_by(WaterYear) %>% 
    summarize(W95 = length(which(Flow>=q95))) %>% mutate(Year=WaterYear)
  BFI <- COM1 %>% filter(DOY %in% seq(152,273)) %>% 
    mutate(AvgFlow=WaterYearFlow$WaterYearFlow[match(WaterYear,WaterYearFlow$WaterYear)]) %>% 
    group_by(WaterYear) %>% summarize(BFI = min(rollmeanr(Flow,7),na.rm=T)/mean(AvgFlow)) %>% mutate(Year=WaterYear)
  
  COMID_FlowStats <- SummerFlow %>% 
    left_join(AnnualFlow) %>%
    left_join(WaterYearFlow %>% select(-WaterYear), by='Year') %>%
    left_join(W95 %>% select(-WaterYear), by='Year') %>%
    left_join(BFI %>% select(-WaterYear), by='Year') %>%
    mutate(COMID = COMID1)
  
  COMID_FlowStats
} %>% bind_rows()

stopCluster(cl)

saveRDS(Node_FlowStats,file=here('NLDASdata','AnnualFlowStats',rname,paste0(setID,'_outof_',NumNodes/2,'_AnnualFlowStats.RDS')))