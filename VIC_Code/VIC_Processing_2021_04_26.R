# Script to take VIC cell and build hydrographs for all NHD segments and accumulate them downstream
# Initial versions by Seth Wenger and Pat Kormos  
# Revised by Charles Luce 1/26/2015-4/30/2015 to use new input files and correct accumulation calculations 
# Heavily modified/rewritten by Nathan Walker for national input (multiple models) and improved efficiency
# March 17, 2021

################################################################
### Setup for all regions and time periods; run prior to both loops

# Clear workspace
rm(list = ls(all = TRUE))

start_time <- proc.time()
start1 <- start_time

# Install libraries
library(ncdf4)  # for netcdf functions
library(foreign) # to open .dbf's
library(fastmatch)  # to speed up the matching (fmatch and %fin% instead of match and %in%)
library(lubridate) # interpreting dates
#library(igraph) # For ordering VPUs from upstream to downstream; only needed on first run

options(digits=22)
options(stringsAsFactors=FALSE)

# Set directories
in_path <- "G:/Hydrology/VIC_US_GriddedRunoff/"
m_path <- "G:/Hydrology/NHD/"
vaa_path <- paste0(m_path, "Flowline_Attributes/Attribute_Tables/")
cat_path <- paste0(m_path, "Catchments/Catchment_Tables/")
wt_path <- paste0(m_path, "Weights/Weight_Tables/")
out_path <- paste0("G:/Hydrology/VIC_US_5Model_FlowAcc/")
          
## set VPU (region) information
rn_names <- c("CA18","CO14","CO15","GB16","GL04","MA02","MS05","MS06","MS07","MS08","MS10L","MS10U","MS11","NE01","PN17",
              "RG13","SA03N","SA03S","SA03W","SR09","TX12")

# Set time period information
p_names <- c("hist","2040c","2080c")  # either historical period, 2040s composite period, or 2080s composite period
p_long <- c("Historical", "2040_composite", "2080_composite")
p_yrs <- list(seq(1977,2006,1),seq(2030,2059,1),seq(2070,2099,1))

# get model information
filelist <- list.files(in_path, pattern = ".nc")
mod_fls <- unique(sapply(strsplit(filelist, ""), function(x) paste0(x[10:(length(x)-27)], collapse="")))

# pick models to use; from RPA assessment: https://www.fs.fed.us/rm/pubs_series/rmrs/gtr/rmrs_gtr413.pdf
mod_fls <- mod_fls[c(9, 19, 21, 28, 29)]
mod_nms <- gsub("-","_",sub("_.*", "", mod_fls))


# set up file and data frames to identify streams that go between VPUs; only has to be run once
vpux_name <- paste0(out_path, "vpux.csv")
#vpux_up <- data.frame(FromComID = integer(), FromVPU = character(), ToNode = integer(), stringsAsFactors = FALSE)
#vpux_dwn <- data.frame(ToComID = integer(), ToVPU = character(), FromNode = integer(), stringsAsFactors = FALSE)
#vv_u <- 1; vv_d <- 1






#######################################################################################################
# First loop: for each model, time period and region, load and prepare data, distribute flow from grid cells
# into catchments, and then adjust that flow using a 3-day weighting calculation

# for each time period
#for(pp in 1:length(p_names)){
for(pp in 3){
  
  pname <- p_names[pp]
  
  # get years in this period
  yrs <- p_yrs[pp][[1]]
  
  # get count of days in these years
  date_strings = c(paste0("01.01.",yrs[1]), paste0("31.12.",yrs[length(yrs)]))
  datetimes = strptime(date_strings, format = "%d.%m.%Y") 
  ndays <- (as.numeric(difftime(datetimes[2], datetimes[1], units = "days"))) + 1
  
  # get day number of start of this period, relative to January 1, 1915
  date_strings = c("01.01.1915", paste0("01.01.",yrs[1]))
  datetimes = strptime(date_strings, format = "%d.%m.%Y") 
  stday <- (as.numeric(difftime(datetimes[2], datetimes[1], units = "days")))
  
  # for each model
  #for(mm in 1:length(mod_nms)){
  for(mm in 5){
    
    # Model name
    mname <- mod_nms[mm]
    
    #####################################################################################
    ### Read in VIC data (total runoff) for all years in this time period
    
    # for each year:
    for (yy in 1:length(yrs)){
      #yy <- 1
      
      # read in total runoff data for this year
      nc_file <- paste0("conus_c5.",mod_fls[mm],".daily.total_runoff.",yrs[yy],".nc")
      
      # check for error when opening file
      errorTest <- tryCatch(nc <- nc_open(paste0(in_path,nc_file),write=FALSE), error=function(e) {e})
      
      # if error returned, report this; otherwise, open file
      if(inherits(errorTest,"error")){
        print(paste0("Error reading file ", nc_file))
        break
      }
      
      # for first file, use dimensions to create array (lat * long * time)
      if(yy == 1){
        lats <- as.numeric(nc$dim$latitude$vals)
        longs <- as.numeric(nc$dim$longitude$vals)
        tr_arr <- array(NA,dim=c(length(longs),length(lats),ndays))
        dst <- 1
      }
      
      # add data from each year to the array
      dend <- dst + nc$dim$time$len - 1
      tr_arr[,,dst:dend] <- ncvar_get(nc,"total runoff")
      dst <- dend + 1
    }
    
    # Clean up workspace
    nc_close(nc)
    rm(nc, dst, dend)
    invisible(gc(verbose=FALSE,reset=TRUE))
    

    # For each region
    for(rr in 1:length(rn_names)){
    #for(rr in 17:length(rn_names)){
      #mm <- 5; pp <- 1; rr <- 15
      
      start_time <- proc.time()
  
      rname <- rn_names[rr]
      
      # Set output directory; create this folder if it doesn't yet exist
      outdir <- paste0(out_path, p_long[pp],"/",mname,"/",rname,"/")
      dir.create(outdir, recursive = TRUE, showWarnings = FALSE, )
      
      #####################################################################
      ### Read in and set up stream and catchment files for this region
      
      # Load flowlines attributes (VAA): downloaded from here: https://nhdplus.com/NHDPlus/NHDPlusV2_data.php
      vaa <- read.dbf(paste0(vaa_path, "PlusFlowlineVAA_", rname, ".dbf"))
      
      # Sort stream data (upstream to downstream)
      vaa <- vaa[order(vaa$Hydroseq, decreasing=TRUE),] 
      
      # Secondary streams at a divergence will not "ask" for water
      vaa$FromNode[vaa$Divergence==2] <- 0
      
      # Load catchments, used for getting areas
      # Downloaded catchments from here https://nhdplus.com/NHDPlus/NHDPlusV2_data.php, then exported files to .dbf
      cats <- read.dbf(paste0(cat_path, "Catchment_", rname, ".dbf"))
      
      # Load weight files; calculated using catchment data and Python script C:\Users\nathanwalker\Documents\USFS\Tools\Hydrology\VIC\Catchment_Weight_2020_08_01.py
      wts <- read.csv(paste0(wt_path, rname, "_wts.csv"))
      
      # remove first column (empty); order by catchment
      wts <- wts[order(wts$CatID),2:ncol(wts)]
      
      # get IDs of nhd catchments from weights file: this will include some catchments without information in VAA file
      nhd_list  <- unique(wts$CatID)
      
      # order these according to order of catchments in VAA file
      nhd_list <- nhd_list[order(fmatch(nhd_list, vaa$ComID))]
      
      # Total number of catchments and grid cells
      ncat <- length(nhd_list)
      ncells <- length(unique(wts$GridID))
      
      # output netCDF file with summed VIC inputs by catchment
      vsum_name <- paste0(outdir, rname,"_VIC.nc")
      
      # output netCDF file for 3-day weighted VIC inputs (m3)
      lcl_m3_name <- paste0(outdir,rname,"_local_m3.nc")
      
      ### Build VPU exchange file (tracking streams that flow into or out of this region); only has to be done on first run
      # fill in upstream VPU exchange file with any streams that flow out of this VPU and into another
      #vaa_sel <- vaa[which(vaa$VPUOut == 1),]
      #if(nrow(vaa_sel) > 0){
      #  vpux_up[vv_u:(vv_u + nrow(vaa_sel) - 1),"ToNode"] <- vaa_sel$ToNode
      #  vpux_up[vv_u:(vv_u + nrow(vaa_sel) - 1),"FromComID"] <- vaa_sel$ComID
      #  vpux_up[vv_u:(vv_u + nrow(vaa_sel) - 1),"FromVPU"] <- rname
      #  vv_u <- vv_u + nrow(vaa_sel)
      #}
      
      # fill in downstream VPU exchange file with any streams that flow into this VPU from another
      #vaa_sel <- vaa[which(vaa$VPUIn == 1),]
      #if(nrow(vaa_sel) > 0){
      #  vpux_dwn[vv_d:(vv_d + nrow(vaa_sel) - 1),"FromNode"] <- vaa_sel$FromNode
      #  vpux_dwn[vv_d:(vv_d + nrow(vaa_sel) - 1),"ToComID"] <- vaa_sel$ComID
      #  vpux_dwn[vv_d:(vv_d + nrow(vaa_sel) - 1),"ToVPU"] <- rname
      #  vv_d <- vv_d + nrow(vaa_sel)
      #}
      
      #rm(vaa_sel)


      
      ##################################################################
      ### Create netCDF Files from this information
      
      ## data defining the dimensions of the netcdf file for full catchment flows
      dimX <- ncdim_def("nhd_cat" ,"COMID for NHD catchment (unique identifier)",nhd_list)
      dimT <- ncdim_def("time","days since Jan. 1, 1915",stday:(stday + ndays - 1),unlim=FALSE)
      
      # create temporary variable definition, then ncdf4 file for summed VIC inputs
      TempDef  <- ncvar_def("NHD_Input","mm/day",list(dimT,dimX),prec="double",verbose=FALSE)
      if (!file.exists(vsum_name)) {
        nc <- nc_create(vsum_name,TempDef,force_v4=TRUE,verbose=FALSE)
        nc_close(nc)
      } 
      
      ## create nc file for local 3-day weighted NHD flow in cubic meters per day
      TempDef <- ncvar_def("NHDflow_local_m3","cubic meters per day",list(dimT,dimX),prec="double",verbose=FALSE)
      if (!file.exists(lcl_m3_name)){
        nc <- nc_create(lcl_m3_name,TempDef,force_v4=TRUE,verbose=FALSE)
        nc_close(nc)
      }
      
      # Clean Up Memory
      rm(dimX, dimT, TempDef)
      invisible(gc(verbose=FALSE, reset=TRUE))
      
  
      
        
        
      
      ################################################################
      ### Take VIC data and distribute flow among catchments, based on the proportion of the catchment in each grid cell
      
      # Function to look up lat/long for each record in the weights table, find the associated value in the VIC grid
      # and multiply this by the proportion of the grid cell occupied by this catchment
      applyWeights <- function(wts, lats, longs, tr_arr){
        
        # Get index locations for each lat and long in the total runoff array
        tr_lat <- fmatch(as.numeric(wts['Lat']), lats)
        tr_long <- fmatch(as.numeric(wts['Long']), longs)
        
        # find total runoff values for each of these lat/longs, multiply by the proportion of the catchment in this grid cell
        wts_tr <- tr_arr[tr_long, tr_lat,] * as.numeric(wts['CatPctGrid'])
      
        return(wts_tr)
      }
      
      # open nc file for writing
      nc <- nc_open(vsum_name,write=TRUE) 
      
      # Chunk this by catchments, to avoid running out of memory:
      nwt <- length(unique(wts$CatID))
      cat_st <- 1
      cat_en <- ifelse(nwt < 10000, nwt, 10000)
      
      while(cat_st < nwt){
        
        # Load section of weights file, for this group of catchments
        wts_sel <- wts[wts$CatID %fin% unique(wts$CatID)[cat_st:cat_en],]
      
        # For each of these records, calculate the daily flow for that catchment using the function defined above
        wts_tr <- t(apply(wts_sel, 1, function(x) applyWeights(x, lats, longs, tr_arr)))
        
        # Add catchment ID to this data frame
        wts_tr <- cbind(wts_tr, as.numeric(wts_sel$CatID))
        
        # summarize this by catchment ID (last column): there are multiple records for many catchments
        cat_tr <- rowsum(wts_tr[,1:ndays], wts_tr[,ncol(wts_tr)], na.rm = TRUE)
        
        # get the row names (catchment IDs) and put these in the order of the NHD list
        cat_list <- fmatch(as.numeric(row.names(cat_tr)), nhd_list)
        
        # put these values in the output nc file, one row at a time (entering each catchment by location in NHD list)
        for(cc in 1:nrow(cat_tr)){
          ncvar_put(nc, "NHD_Input", cat_tr[cc,], start = c(1,cat_list[cc]), count = c(ndays, 1), verbose=FALSE)
        }
      
        # Set up next group of catchments to process
        cat_st <- cat_en + 1
        cat_en <- ifelse((cat_en + 10000) > nwt, nwt, (cat_en + 10000))
      }
      
      # Clean up workspace
      nc_close(nc)
      rm(wts, wts_tr, cat_tr, wts_sel, nc, cat_list)
      invisible(gc(verbose=FALSE,reset=TRUE))
      
      
      
      #################################################################
      # Local routing (within local catchment, applying 3-day weighting to account for delays from input flow);
      # conversion to m3 for channel routing
      
      # open netCDF files
      nc_i <- nc_open(vsum_name, write=FALSE)
      nc_m <- nc_open(lcl_m3_name, write=TRUE)
      
      # Get catchment areas
      cat_area <- cats$AreaSqKM[fmatch(nhd_list,cats$FEATUREID)]
      
      ## local routing function: filter flow values and return
      # 3 day hydrograph: takes weighted average: multiplies most recent day by 0.9, adds to previous day multiplied by 0.075
      # and day before that by 0.025, to spread out flow resulting from each day's rainfall; reduces flashiness in results
      loc_filt <- function(raw_hydrograph){
        return(filter(c(rep(raw_hydrograph[1],2),raw_hydrograph),c(0.9, 0.075, 0.025),sides=1)[3:(length(raw_hydrograph)+2)])
      }
      
      # For each catchment:
      for(cc in 1:ncat){
        # cc <- 1
        
        # get full input time series in mm/day
        # note that nhd_inputs for this version (Mauger) are in mm/day; other versions of the data are in mm/sec
        # Since we convert to m3/day below, the rest of the files have the same units between the two versions.
        mm_day <- ncvar_get(nc_i,"NHD_Input",start=c(1,cc), count=c(ndays,1))
        
        # Apply function to reduce flashiness in data
        # then convert from depth in mm/day to volume in m3/day: (depth of water in mm/1000) * (area in square km*1000000)
        m3_day <- loc_filt(mm_day) * cat_area[cc] * 1000
        
        # Put this back in NC file:
        ncvar_put(nc_m,"NHDflow_local_m3",m3_day,start=c(1,cc), count=c(ndays,1))
      }
      
      # Clean up workspace
      nc_close(nc_m)
      nc_close(nc_i)
      rm(m3_day, mm_day, cat_area, cats, nc_i, nc_m)
      invisible(gc(verbose=FALSE, reset=TRUE))
    
      end_time <- proc.time() - start_time
      print(paste0("Stream segment processing of Period-",pname,"/Region-",rname,"/Model-",mname, " completed after ", round(end_time[3]/60,2), " minutes."))
    }
    
    # clean up workspace
    rm(tr_arr)
    invisible(gc(verbose=FALSE, reset=TRUE))
    
  }

    
  ### Merge upstream and downstream VPU exchange files; only has to be done on first run
  # Run with All = TRUE the first time to confirm that unmatched sections are okay to remove (i.e., divergences or no upstream area)
  #vpux_merge <- merge(vpux_up, vpux_dwn, by.x = "ToNode", by.y = "FromNode", all = FALSE)
  #vpux_merge <- vpux_merge[,c(3,2,5,4,1)]
  #names(vpux_merge)[5] <- "Node"
  #write.csv(vpux_merge, vpux_name, row.names = FALSE)
}

end_time <- proc.time() - start1
print(paste0("Stream segment processing of all regions and time periods completed after ", round(end_time[3]/3600,2), " hours"))













##############################################################################################
# Second loop: Accumulate flow downstream
# The prior loop must be run first to generate intermediate flow products for all regions, 
# such that we can calculate accumulated flow across region boundaries where necessary

start_time <- proc.time()
start2 <- start_time

# read in VPU exchange file
vpux <- read.csv(vpux_name)

### order regions such that all upstream regions will be processed before all downstream regions (only needed on first run)
# simplify VPU exchange table
#vpux_simp <- unique(vpux[,c("FromVPU", "ToVPU")])

# Use this to build and plot graph showing stream ordering
#vpux_gr <- graph.data.frame(vpux_simp)
#plot(vpux_gr)

# find any regions not in VPU exchange file
# rn_names[!rn_names %in% unique(c(vpux_simp$FromVPU, vpux$ToVPU))]

# There's probably a way to do all this programmatically, but I ended out just sorting regions manually based on the plot, 
# such that all upstream regions are first, followed by downstream, followed by any not in the exchange file
rn_names <- c("CO14", "CO15", "PN17", "CA18", "MS10U", "MS10L", "MS07", "RG13","TX12", "MS06", "MS05", "MS11",
            "MS08", "SA03W", "SA03S", "SA03N", "MA02", "NE01", "GB16", "GL04", "SR09")

# for each time period
#for(pp in 1:length(p_names)){
for(pp in 2){
  
  # for each model
  #for(mm in 1:length(mod_nms)){
  for(mm in 2){
  
    # For each region
    for(rr in 17:length(rn_names)){
    #for(rr in 14:length(rn_names)){
    #pp <- 2; rr <- 11; mm <- 1
    
      start_time <- proc.time()
      
      ### Set inputs and outputs
      
      pname <- p_names[pp]
      rname <- rn_names[rr]
      mname <- mod_nms[mm]
      
      # get years in this period
      yrs <- p_yrs[pp][[1]]
      
      # Load flowlines attributes (VAA)
      vaa <- read.dbf(paste0(vaa_path, "PlusFlowlineVAA_", rname, ".dbf"))
      
      # Sort stream data (upstream to downstream)
      vaa <- vaa[order(vaa$Hydroseq, decreasing=TRUE),] 
      
      # Secondary streams at a divergence will not "ask" for water
      vaa$FromNode[vaa$Divergence==2] <- 0
      
      # filter VPU exchange file to just this region
      vpux_sel <- vpux[vpux$ToVPU == rname,]
    
      # Set output directory
      outdir <- paste0(out_path, p_long[pp],"/",mname,"/",rname,"/")
      
      # input netcdf file for 3-day weighted VIC inputs, converted to m3 (created from first loop)
      lcl_m3_name <- paste0(outdir,rname,"_local_m3.nc")
      
      # output netcdf file for accumulated m3 information
      acc_name <- paste0(outdir,rname,"_acc_rt.nc")
      
      ### set up output netCDF files
      
      # open input netCDF file; get list of days and catchments
      nc_l <- nc_open(lcl_m3_name, write = FALSE)
      nc_days <- nc_l$dim$time$vals
      nhd_list <- nc_l$dim$nhd_cat$vals
      
      # get list of streams from VAA file
      strm_list <- vaa$ComID
      
      # number of days and streams
      ndays <- length(nc_days)
      nstrm <- length(strm_list)
      
      # data defining the dimensions of the netcdf file for full catchment flows
      dimX <- ncdim_def("nhd_cat" ,"COMID for NHD catchment (unique identifier)",strm_list)
      dimT <- ncdim_def("time","days since Jan. 1, 1915",nc_days,unlim=FALSE)
      
      # create nc file for NHD flow in cubic meters per day, accumulated (upstream to down)
      # if this has already been created, remove this and recreate it, to prevent any errors
      TempDef <- ncvar_def("NHDflow_acc_m3","cubic meters per day",list(dimT,dimX),prec="double",verbose=FALSE, missval = 0)
      if (file.exists(acc_name)){
        invisible(file.remove(acc_name))
      }
      
      nc <- nc_create(acc_name,TempDef,force_v4=TRUE,verbose=FALSE)
      nc_close(nc)
      rm(nc)
  
      #################################################################################################
      # Downstream accumulation: accumulate flow from upstream into downstream segments
      
      # Open output netCDF file for writing
      nc_c <- nc_open(acc_name,write=TRUE)
      
      # For each stream segment, load flow in this segment; if there are any upstream segments, add in flow from all upstream segments
      # The streams are ordered sequentially (upstream to downstream), so the flow added upstream is accumulated downstream
      for (ss in 1:nstrm){
        #ss <- which(strm_list == 14433983)
        
        # if this stream is not in the local flow file, just load zeros
        if(!strm_list[ss] %in% nhd_list){
          flow_cur <- rep(0,ndays)
          
          # otherwise, load data
        } else{
        
          # Load the data for this stream from the local netCDF file; convert NAs to 0
          flow_cur  <- ncvar_get(nc_l,"NHDflow_local_m3",start=c(1,which(nhd_list == strm_list[ss])), count=c(ndays, 1))
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
              
              # read in netCDF file for the upstream region
              acc_up_name <- paste0(out_path, p_long[pp],"/",mname,"/",fromVPU,"/",fromVPU,"_acc_rt.nc")
              nc_u <- nc_open(acc_up_name)
              
              # Get all upstream segments from the upstream VPU
              seg_ustrm <- fmatch(vpux_str[vv, "FromComID"], nc_u$dim$nhd_cat$vals)
              
              # For each upstream segment
              for(uu in 1:length(seg_ustrm)){
                
                # load accumlated flow data for upstream segment (if the ordering is correct, this should have been created previously)
                flow_ustrm  <- as.numeric(ncvar_get(nc_u,"NHDflow_acc_m3",start=c(1,seg_ustrm[uu]), count=c(ndays, 1)))
                
                # Add flow from upstream segment into current segment
                flow_cur <- rowSums(cbind(flow_cur, flow_ustrm), na.rm = TRUE)
              }
              
              nc_close(nc_u)
            }
          }
        }
          
        # Get all upstream segments within the current region
        seg_ustrm <- which(vaa[ss,"FromNode"] == vaa$ToNode)
        
        # load data for upstream segment(s), if they exist for this stream
        if(length(seg_ustrm) > 0){
          for(uu in 1:length(seg_ustrm)){
          
            # load data for upstream segment: should have already been loaded into output netCDF
            flow_ustrm  <- as.numeric(ncvar_get(nc_c,"NHDflow_acc_m3",start=c(1,seg_ustrm[uu]), count=c(ndays, 1)))
      
            # Add flow from upstream segment into current segment
            flow_cur <- rowSums(cbind(flow_cur, flow_ustrm), na.rm = TRUE)
          }
        }
        
      # Write the accumulated flow to the output file
      ncvar_put(nc_c,"NHDflow_acc_m3",flow_cur,start=c(1,ss),count=c(ndays,1))
      }
      
      # Clean up workspace
      nc_close(nc_l)
      nc_close(nc_c)
      rm(flow_cur, nc_l, nc_c)
      invisible(gc(verbose=FALSE, reset=TRUE)) 
  
      end_time <- proc.time() - start_time
      print(paste0("Stream accumulation of Period-",pname,"/Region-",rname, "/Model-", mname, " completed after ", round(end_time[3]/60,2), " minutes."))
    }
  }
}

end_time <- proc.time() - start2
print(paste0("Stream accumulation processing of all regions and time periods completed after ", round(end_time[3]/3600,2), " hours"))

# Delete intermediate files to save space; only do this once you're sure you no longer need them.
for(pp in 2){
  for(mm in 1){
    for(rr in 1:length(rn_names)){
      outdir <- paste0(out_path, p_long[pp],"/",mname,"/",rn_names[rr],"/")
      #file.remove(paste0(outdir,rn_names[rr],"_VIC.nc"))
      #file.remove(paste0(outdir,rn_names[rr],"_local_m3.nc"))
    }
  }
}

#sort( sapply(ls(),function(x){object.size(get(x))})) 