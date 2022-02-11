rm(list = ls(all = TRUE))

suppressPackageStartupMessages(library(here))
suppressPackageStartupMessages(library(pryr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(foreign)) # to open .dbf's
suppressPackageStartupMessages(library(fastmatch))  # to speed up the matching (fmatch and %fin% instead of match and %in%)
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(doParallel))
suppressPackageStartupMessages(library(foreach))
suppressPackageStartupMessages(library(igraph))

print(here())

options(digits=22)
options(stringsAsFactors=FALSE) 

applyWeights <- function(wts_in, lats, longs, tr_arr){
  
  # Get index locations for each lat and long in the total runoff array
  #tr_lat <- fmatch(as.numeric(wts_in['Lat']), lats)
  #tr_long <- fmatch(as.numeric(wts_in['Long']), longs)
  tr_lat <- which.min(Mod(as.numeric(wts_in['Lat']) - lats))
  tr_long <- which.min(Mod(as.numeric(wts_in['Long']) - longs))
  
  # find total runoff values for each of these lat/longs, multiply by the proportion of the catchment in this grid cell
  wts_tr <- tr_arr[tr_lat,tr_long,] * as.numeric(wts_in['CatPctGrid'])
  
  return(wts_tr)
}

loc_filt <- function(raw_hydrograph){
  return(stats::filter(c(rep(raw_hydrograph[1],2),raw_hydrograph),c(0.9, 0.075, 0.025),sides=1)[3:(length(raw_hydrograph)+2)])
}


#get list of all daily runoff files
alldailyfiles <- list.files(here('NLDASdata','DailyRunoffs'))
#get dates from file names
alldates <- lapply(alldailyfiles,FUN=function(x){
  return(substr(x,13,20))
}) %>% unlist()

#Load in lats and longs for NLDAS data grid
lats <-  read.csv(here('NLDASdata','NLDAS_Lats.csv'),header=T,stringsAsFactors=F) %>% pull(1)
longs <- read.csv(here('NLDASdata','NLDAS_Longs.csv'),header=T,stringsAsFactors=F) %>% pull(1)

#Regions to be processed
rn_names <- c("PN17")

#### DIVIDE TASKS AMONG SLURM NODES
NumNodes <- as.numeric(Sys.getenv('SLURM_JOB_NUM_NODES')) #Get the number of nodes assigned to the job
taskID <- as.numeric(Sys.getenv('SLURM_PROCID')) + 1 #Get the Node number, SLURM returns 0 for first node, but we want 1 for indexing
dirsplit <- split(1:length(alldailyfiles),cut(1:length(alldailyfiles),NumNodes,labels=F)) #split total number of directories into groups by
if(taskID == 1){
  task_dirlist <- dirsplit[[taskID]] #If the first chunk load normally 
}else{
  #If not the first chunk load an extra 5 days because the runoff data is filtered with a 3 day lag
  #We will dump the extra days before saving
  startI <- dirsplit[[taskID]][1]
  task_dirlist <- c(seq(startI-5,startI-1,by=1),dirsplit[[taskID]])
}

#Read in all Runoff data as a list
AllRuns <- lapply(alldailyfiles[task_dirlist],FUN=function(x){
  RDS1<-readRDS(here('NLDASdata','DailyRunoffs',x))
  TOTRUN<-RDS1$BGRUN + RDS1$SSRUN
  return(TOTRUN)
})
names(AllRuns) <- alldates[task_dirlist]
datenames <- names(AllRuns)
startdate <- datenames[1]
enddate <- datenames[length(datenames)]


TotalRunoffArray<-array(unlist(AllRuns),dim=c(224,464,length(AllRuns)))

rm(AllRuns)

print(paste0(taskID,': Runoff data loaded'))

# processedRegions <- foreach(rr = 1:length(rn_names)) %dopar% {
#for(rr in 1:length(rn_names))
rr<-1
rname <- rn_names[rr]
dir.create(here('NLDASdata','Routed',rname), recursive = TRUE, showWarnings = FALSE)

# Load flowlines attributes (VAA): downloaded from here: https://nhdplus.com/NHDPlus/NHDPlusV2_data.php
vaa <- read.dbf(here('NHDdata',rname,"PlusFlowlineVAA.dbf"))
# Sort stream data (upstream to downstream)
vaa <- vaa[order(vaa$Hydroseq, decreasing=TRUE),]
# Secondary streams at a divergence will not "ask" for water
vaa$FromNode[vaa$Divergence==2] <- 0

processVPU <- TRUE
if(processVPU){
  # set up file and data frames to identify streams that go between VPUs; only has to be run once
  #vpux_name <- paste0(out_path, "vpux.csv")
  vpux_up <- data.frame(FromComID = integer(), FromVPU = character(), ToNode = integer(), stringsAsFactors = FALSE)
  vpux_dwn <- data.frame(ToComID = integer(), ToVPU = character(), FromNode = integer(), stringsAsFactors = FALSE)
  vv_u <- 1; vv_d <- 1
  
  ### Build VPU exchange file (tracking streams that flow into or out of this region); only has to be done on first run
  # fill in upstream VPU exchange file with any streams that flow out of this VPU and into another
  vaa_sel <- vaa[which(vaa$VPUOut == 1),]
  if(nrow(vaa_sel) > 0){
    vpux_up[vv_u:(vv_u + nrow(vaa_sel) - 1),"ToNode"] <- vaa_sel$ToNode
    vpux_up[vv_u:(vv_u + nrow(vaa_sel) - 1),"FromComID"] <- vaa_sel$ComID
    vpux_up[vv_u:(vv_u + nrow(vaa_sel) - 1),"FromVPU"] <- rname
    vv_u <- vv_u + nrow(vaa_sel)
  }
  
  # fill in downstream VPU exchange file with any streams that flow into this VPU from another
  vaa_sel <- vaa[which(vaa$VPUIn == 1),]
  if(nrow(vaa_sel) > 0){
    vpux_dwn[vv_d:(vv_d + nrow(vaa_sel) - 1),"FromNode"] <- vaa_sel$FromNode
    vpux_dwn[vv_d:(vv_d + nrow(vaa_sel) - 1),"ToComID"] <- vaa_sel$ComID
    vpux_dwn[vv_d:(vv_d + nrow(vaa_sel) - 1),"ToVPU"] <- rname
    vv_d <- vv_d + nrow(vaa_sel)
  }
  
  rm(vaa_sel)
  
  ### Merge upstream and downstream VPU exchange files; only has to be done on first run
  # Run with All = TRUE the first time to confirm that unmatched sections are okay to remove (i.e., divergences or no upstream area)
  vpux_merge <- merge(vpux_up, vpux_dwn, by.x = "ToNode", by.y = "FromNode", all = T)
  vpux_merge <- vpux_merge[,c(3,2,5,4,1)]
  names(vpux_merge)[5] <- "Node"
  write.csv(vpux_merge, here('NHDdata',rname,'VPUX.csv'), row.names = FALSE)
  
}#end VPU processing

# Load catchments, used for getting areas
# Downloaded catchments from here https://nhdplus.com/NHDPlus/NHDPlusV2_data.php, then exported files to .dbf
cats <- read.dbf(here('NHDdata',rname,"Catchment.dbf"))
# Load weight files; calculated using catchment data and Python script C:\Users\nathanwalker\Documents\USFS\Tools\Hydrology\VIC\Catchment_Weight_2020_08_01.py
wts <- read.csv(here('NHDdata',rname,"catch_wts.csv"))
#wts$Lat <- round(wts$Lat,3)
#wts$Long <- round(wts$Long,3)
# remove first column (empty); order by catchment
wts <- wts[order(wts$CatID),2:ncol(wts)]

# get IDs of nhd catchments from weights file: this will include some catchments without information in VAA file
nhd_list  <- unique(wts$CatID)
# order these according to order of catchments in VAA file
nhd_list <- nhd_list[order(fmatch(nhd_list, vaa$ComID))]

# Total number of catchments and grid cells
ncat <- length(nhd_list)
ncells <- length(unique(wts$GridID))   

# Load section of weights file, for this group of catchments
UniqueCatch<-unique(wts$CatID)
nwt <- length(UniqueCatch)
cat_st <- 1
cat_en <- nwt


wts_sel <- wts[wts$CatID %fin% UniqueCatch[cat_st:cat_en],]


# For each of these records, calculate the daily flow for that catchment using the function defined above
wts_tr <- t(apply(wts_sel, 1, function(x) applyWeights(wts=x, lats, longs, tr_arr=TotalRunoffArray)))


# summarize this by catchment ID (last column): there are multiple records for many catchments
cat_tr <- rowsum(wts_tr, as.numeric(wts_sel$CatID), na.rm = TRUE)

# get the row names (catchment IDs) and put these in the order of the NHD list
cat_list <- fmatch(as.numeric(row.names(cat_tr)), nhd_list)


print(paste0(taskID,': Catchment weighting applied'))


# Get catchment areas
cat_area <- cats$AreaSqKM[fmatch(row.names(cat_tr),cats$FEATUREID)]

#compute catchment daily flows
cl <- makeCluster(detectCores())
registerDoParallel(cl)
all_m3_day <- foreach(cc = 1:ncat) %dopar% {
  # get full input time series in kg/m2/day
  # Since we convert to m3/day below, the rest of the files have the same units between the two versions.
  
  kg_m2_day <- cat_tr[cc,]
  
  # Apply function to reduce flashiness in data
  # then convert from depth in mm/day to volume in m3/day: (depth of water in mm/1000) * (area in square km*1000000)
  m3_day <- loc_filt(kg_m2_day) * cat_area[cc] / 1000
  m3_day
}
names(all_m3_day) <- row.names(cat_tr)

print(paste0(taskID,': Catchment daily flows applied'))

# get list of streams from VAA file
strm_list <- vaa$ComID

# get list of days and catchments  
nc_days <- alldates
# number of days and streams
ndays <- length(nc_days)
nstrm <- length(strm_list)

# filter VPU exchange file to just this region
vpux_sel <- vpux_merge[vpux_merge$ToVPU == rname,]
vpux_sel <- vpux_sel[!is.na(vpux_sel$ToVPU),]
# For each stream segment, load flow in this segment; if there are any upstream segments, add in flow from all upstream segments
# The streams are ordered sequentially (upstream to downstream), so the flow added upstream is accumulated downstream
All_Acc <- foreach(ss = 1:nstrm,.packages=c('fastmatch')) %dopar% {
  #ss<-180937
  #ss <- 90622
  if(!(strm_list[ss] %in% nhd_list)){
    flow_cur <- rep(0,ndays)
    # otherwise, load data
  }else{
    # Load the data for this stream from the local netCDF file; convert NAs to 0
    #flow_cur  <- ncvar_get(nc_l,"NHDflow_local_m3",start=c(1,which(nhd_list == strm_list[ss])), count=c(ndays, 1))
    flow_cur  <- all_m3_day[[which(names(all_m3_day) == strm_list[ss])]]
    flow_cur[is.na(flow_cur)] <- 0
  }
  
  ### process flow from upstream VPUs, if they exist
  if(nrow(vpux_sel) > 0){
    
    # if this stream flows out of other VPUs:
    if(strm_list[ss] %in% vpux_sel$ToComID){
      
      # filter VPUX table to just this stream in the downstream region (may have two upstream regions)
      vpux_str <- vpux_sel[vpux_sel$ToComID == strm_list[ss],]
      
      # for each record in this table
      for(vv in 1:nrow(vpux_str)){
        
        # get the region that the stream flows out of
        fromVPU <- vpux_str[vv,"FromVPU"]
        
        # read in RDS file for the upstream region
        acc_up <- readRDS(here('NLDASdata','Routed',paste0(fromVPU,'_Routed_',startdate,'_',enddate,'.RDS')))
        
        # Get all upstream segments from the upstream VPU
        seg_ustrm <- fmatch(vpux_str[vv, "FromComID"], names(acc_up))
        
        # For each upstream segment
        for(uu in 1:length(seg_ustrm)){
          
          # load accumlated flow data for upstream segment (if the ordering is correct, this should have been created previously)
          #flow_ustrm  <- as.numeric(ncvar_get(nc_u,"NHDflow_acc_m3",start=c(1,seg_ustrm[uu]), count=c(ndays, 1)))
          flow_ustrm  <- acc_up[[seg_ustrm[uu]]]
          
          # Add flow from upstream segment into current segment
          flow_cur <- rowSums(cbind(flow_cur, flow_ustrm), na.rm = TRUE)
        }#end uu
        
        #nc_close(nc_u)
      }#end vv
    }
  }#End VPUX processing
  
  # Get all upstream segments within the current region
  seg_ustrm <- vaa$ComID[which(vaa[ss,"FromNode"] == vaa$ToNode)]
  
  # load data for upstream segment(s), if they exist for this stream
  if(length(seg_ustrm) > 0){
    for(uu in 1:length(seg_ustrm)){
      
      # load data for upstream segment: should have already been loaded into output netCDF
      #flow_ustrm  <- as.numeric(ncvar_get(nc_c,"NHDflow_acc_m3",start=c(1,seg_ustrm[uu]), count=c(ndays, 1)))
      if(seg_ustrm[uu] %in% nhd_list){
        flow_ustrm  <- all_m3_day[[ which(names(all_m3_day)==seg_ustrm[uu]) ]]
        
        # Add flow from upstream segment into current segment
        flow_cur <- rowSums(cbind(flow_cur, flow_ustrm), na.rm = TRUE)
      }
    }
  }
  
  flow_cur
}#EndForEachStreamSegment

names(All_Acc) <- strm_list

print(paste0(taskID,': Daily flows routed'))


if(taskID!=1){
  All_Acc <- lapply(All_Acc,FUN=function(x){
    return(x[-c(1:5)])
  })
  names(All_Acc) <- strm_list
  startdate <- datenames[6]
}

saveRDS(All_Acc,file=here('NLDASdata','Routed',rname,paste0(rname,'_Routed_',startdate,'_',enddate,'.RDS')))

print(paste0(taskID,': Routed datafile written'))
stopCluster(cl) 
#}