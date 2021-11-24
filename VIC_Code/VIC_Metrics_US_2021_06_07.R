# VIC METRICS for continguous U.S., historical and future projections
# Nathan Walker major revision of RMRS code; further revised to handle new contiguous U.S. VIC dataset, export as geodatabase
# January 14th, 2021

# Calculate the following metrics from VIC output for the historical (1977-2006), mid-century (2030-2059) and end-of-century (2070-2099) time periods
#	MA: Mean annual flow is calculated as the mean of the yearly cumulative discharge values. [Cubic feet per second]
#	MJan, MFeb, MMar, MApr, MMay, MJun, MJul, MAug, MSep, MOct, MNov, MDec: mean flow for each month. [Cubic feet per second]
#	HiQ1_5: The 1.5 year flood is calculated by first finding the annual maximum daily flow. The 33rd percentile of the annual maximum series defines the flow which occurs every 1.5 years, on average. [Cubic feet per second]
#	HiQ10 and HiQ25: the 10 and 25-year floods, respectively. [Cubic feet per second]
#	HiMax: Maximum modeled flood level. [Cubic feet per second]
#	Lo7Q1: average lowest 7-day flow during the year, calculated using either Jan-Dec or June-May, whichever has a lower standard deviation in the date of the low-flow week. [Cubic feet per second]
#	Lo7Q10: average lowest 7-day flow during the decade (calculated as the 10th percentile of the annual minimum weekly flows [Lo7Q1]). [Cubic feet per second]
#	BFI: The baseflow index is the ratio of the average daily flow during the lowest 7-day flow of the year to the average daily flow during the year overall. [Ratio]
#	CFM: Center of flow mass/center of timing is calculated using a weighted mean: CFM=(flow1*1+flow2*2+.flow365*365)/(flow1+flow2+.flow365), where flow is the flow volume on day pp of the water year. [Date of water year]
#	Lo7Q1Dt: Average date of center of lowest 7-day flow, calculated using either Jan-Dec or June-May, whichever has a lower standard deviation in the date of the low-flow week. [Calendar date]
#	W95: The average number of daily flows between December 1 and March 31 which exceed the 95th percentile of daily flows across the entire year. [Count]

# Clear workspace
rm(list = ls(all = TRUE))

main_start <- proc.time()

# Load libraries
library(ncdf4)  # for netcdf functions
library(fastmatch)  # to speed up the matching
library(zoo) # rolling averages; date conversion
library(lubridate) # handle dates
library(arcgisbinding) # For exporting to feature geodatabase; must be installed and updated from here: https://github.com/R-ArcGIS/r-bridge-install
library(foreign) # read .dbf files
library(tidyr) # tidy data
library(readr) # readcsvs more efficiently
library(abind) # merge arrays, to average results of different models

# Set number of significant digits; turn off string to factor conversion
options(digits=22)
options(stringsAsFactors = FALSE)
options(warn=2) # 1 = normal; 2 = convert warnings to errors

# Set directories
indir <- 'G:/Hydrology/VIC_US_5Model_FlowAcc/'
outdir <- 'G:/Hydrology/VIC_US_5Model_Summary/'

# get list of region names
regions <- c("CA18","CO14","CO15","GB16","GL04","MA02","MS05","MS06","MS07","MS08","MS10L",
             "MS10U","MS11","NE01","PN17","RG13","SA03N","SA03S","SA03W","SR09","TX12")

# time periods: either historical or future (composite)
periods <- c("Historical","2040_composite","2080_composite")

# set time periods (water years: water year 2005 goes into calendar year 2006)
s_yrs <- c(1977, 2030, 2070)
e_yrs <- c(2005, 2058, 2098)

# get model information
#filelist <- list.files('E:/VIC_US', pattern = ".nc")
#mod_fls <- unique(sapply(strsplit(filelist, ""), function(x) paste0(x[10:(length(x)-27)], collapse="")))

# pick models to use; from RPA assessment: https://www.fs.fed.us/rm/pubs_series/rmrs/gtr/rmrs_gtr413.pdf
#mod_fls <- mod_fls[c(9, 19, 21, 28, 29)]
#mod_nms <- gsub("-","_",sub("_.*", "", mod_fls))
mod_nms <- c("cnrm_cm5", "hadgem2_es", "ipsl_cm5a_mr", "mri_cgcm3", "noresm1_m")


# for each time period:
#for (pp in 1:length(periods)){
for (pp in 3){
  #pp <- 1; rr <- 15; mm <- 5; cc <- 1; yr <- 1; dd <- 1; mo <- 1
  
  ### Set up date values ###
  
  # get prefix for this time period
  period <- substr(periods[pp],1,4)
  
  # set start and end years for this time period
  s_yr <- s_yrs[pp]
  e_yr <- e_yrs[pp]
  yrs <- seq(s_yr,e_yr)
  nYr <- length(yrs)
  
  # number of days between start and end date
  dates <- seq(as.Date(paste0(s_yr,"/10/1")), as.Date(paste0((e_yr+1),"/9/30")), "day")
  
  # for each year, get year length in days; offset year to get this for water year (starting Oct 1)
  yrLen <- as.integer(as.Date(as.yearmon(yrs+1)+1) - as.Date(as.yearmon(yrs+1)))
  
  # get day numbers corresponding to first day of each water year
  daycounts <- c(0,cumsum(yrLen))
  
  # for each date, indicate which water year it's in
  waterYr <- rep(1:nYr,yrLen)
  
  # get day numbers (for each year) corresponding to dates we'll want access later
  mar31 <- which(match(dates,seq(as.Date(paste0((s_yr + 1),"/03/31")), as.Date(paste0((e_yr + 1),"/03/31")), "years"))>0)
  dec1 <- which(match(dates,seq(as.Date(paste0(s_yr,"/12/01")), as.Date(paste0(e_yr,"/12/01")), "years"))>0)
  jun1 <- which(match(dates,seq(as.Date(paste0((s_yr + 1),"/06/1")), as.Date(paste0((e_yr + 1),"/06/1")), "years"))>0)
  jan1 <- which(match(dates,seq(as.Date(paste0(s_yr + 1,"/01/01")), as.Date(paste0((e_yr+1),"/01/01")), "years"))>0)
  
  # for each region:
  for (rr in 1:length(regions)){
  #for(rr in c(1:14,16:21)){
    
    # for each model:
    for(mm in 1:length(mod_nms)){
    #for(mm in c(4)){
      
      # output file name
      flowmet_name <- paste0(outdir,"Regional/", regions[rr],"_",gsub("orical","",gsub("_composite","",periods[pp])),"_",mod_nms[mm],"_flow_met.csv")

      # If this output file has not yet been created, create it
      if(!file.exists(flowmet_name)){
      
        start_time <- proc.time()
    
        # set file name and location
        nc_name <- paste0(indir,periods[pp],"/",mod_nms[mm],"/",regions[rr],"/",regions[rr],"_acc_rt.nc")
        
        # open netcdf file
        nc <- nc_open(nc_name,write=FALSE) # open routed flow file to read data 
        
        # get list of NHD catchments
        nhd_list <- as.vector(ncvar_get(nc,"nhd_cat"))
        ncatch <- length(nhd_list)
        
        # get start and end day number
        nc_days <- ncvar_get(nc,"time")
        t1 <- fmatch(round(as.numeric(difftime(paste0(s_yr,"-10-01"),"1915-1-1"))),nc_days)
        t2 <- fmatch(round(as.numeric(difftime(paste0((e_yr + 1),"-09-30"),"1915-1-1"))),nc_days)
        
        # create variables, with length equal to number of catchments for each
        varlist <- c("flow_ann_avg", "Q1.5_avg", "Q10_avg", "Q25_avg", "MaxFlow_avg", "Ann7dMin_avg", 
                     "Dec7Q10_avg", "BFI_avg", "AnnMinDt_avg", "CFM_avg", "w95_avg")
        
        for(vv in 1:length(varlist)){
          assign(varlist[vv], rep(NA,ncatch))
          }
        
        # for each catchment
        for(cc in 1:ncatch){
        #for(cc in 1:1000){
          
          # load flow data for each day
          # start reading data at time 1, catchment cc; end after reading all days, 1 catchment
          # Note: I tried reading this in large blocks and processing using lapply instead of for loops to improve efficiency; didn't help
          flow_in  <- ncvar_get(nc,"NHDflow_acc_m3",start=c(t1,cc),count=c((t2-t1+1),1))
          
          #plot(flow_in, type ="l")
          
          # create temporary variables (for annual values, to be averaged together later)
          temp_vars <- c("Jan7dMin", "Jun7dMin", "JanMinDt", "JunMinDt", "JanBFI", "JunBFI", "CFM", "w95")
          for(vv in 1:length(temp_vars)){
            assign(temp_vars[vv], NA)
          }
          
          # calculate mean daily flow for each year; output is m3/day
          flow_mean <- aggregate(flow_in,by=list(waterYr),FUN=mean, na.rm = TRUE, na.action = NULL)
          
          # calculate mean daily flow for all years
          flow_ann_avg[cc] <- mean(flow_mean[,2], na.rm = TRUE)
          
          # calculate mean flow for each month
          for(mo in 1:12){
            mo_flow <- mean(flow_in[which(month(dates) == mo)], na.rm = TRUE)
            #assign(paste0("flow_",month.abb[mo],"_avg")[cc], mo_flow)
            mo_name <- paste0("flow_",month.abb[mo],"_avg")
            if(cc == 1){
              assign(mo_name, mo_flow)
            } else{
              assign(mo_name, c(get(mo_name), mo_flow))
            }
          }
          
          #for(mo in 1:12){print(get(paste0("flow_",month.abb[mo],"_avg")[cc]))}
          
          # if  annual flow = 0 or is NA, leave all summary variables as NA; otherwise, calculate variables
          if (!(flow_ann_avg[cc]==0 | is.na(flow_ann_avg[cc]) | is.nan(flow_ann_avg[cc]))) {
    
            # for each year
            for (yr in 1:nYr) {
          
              # get the flow for every day of this year (from day 1 of this year to day 0 of the next year)
              flow_yr <- flow_in[(daycounts[yr]+1):daycounts[yr+1]]
              #plot(flow_yr, type ="l")
              #abline(h = mean(flow_yr), col = "red")
              
              # if there is data for this year:
              if(any(!is.na(flow_yr))){
    
                # calculate center of timing, in days (mean of daily flows, weighted by day number)
                CFM[yr] <- sum(flow_yr*(1:yrLen[yr]), na.rm = TRUE)/sum(flow_yr, na.rm = TRUE)
                
                # get quantile associated with 95th percentile
                quant <- quantile(flow_yr,0.95, na.rm = TRUE)
                
                # calculate number of days over 95th percentile, in the winter of this year
                w95[yr] <- length(which(flow_in[dec1[yr]:mar31[yr]] > quant[1]))
                
                # Calculate lowest 7-day stretch of the year, starting on Jan 1;
                if(yr < nYr){
                  
                  flow_jan <- flow_in[jan1[yr]:(jan1[yr + 1]-1)]
                  
                  # if there is any data in this time period:
                  if(any(!is.na(flow_jan))){
                    Jan7d <- rollmean(flow_jan, 7, na.rm = TRUE)
                    Jan7dMin[yr] <- min(Jan7d, na.rm = TRUE)
                  
                    # find central date of this stretch (take median in case multiple dates have the same value)
                    JanMinDt[yr] <- median(which(Jan7d == Jan7dMin[yr]) + 3)
                  
                    # Find BFI (7-day low divided by annual average) for series starting on Jan 1
                    JanBFI[yr] <- Jan7dMin[yr]/mean(flow_jan, na.rm = TRUE)
                  }
                
                  # Calculate lowest 7-day stretch of the year and date of this stretch, starting on June 1
                  flow_jun <- flow_in[jun1[yr]:(jun1[yr + 1]-1)]
                  if(any(!is.na(flow_jun))){
                    Jun7d <- rollmean(flow_jun, 7, na.rm = TRUE)
                    Jun7dMin[yr] <- min(Jun7d, na.rm = TRUE)
                    JunMinDt[yr] <- median(which(Jun7d == Jun7dMin[yr]) + 3)
                    JunBFI[yr] <- Jun7dMin[yr]/mean(flow_jun, na.rm = TRUE)
                  }
                }
              }
            }
            
            # calculate mean values for all years
            CFM_avg[cc] <- mean(CFM, na.rm = TRUE)
            w95_avg[cc] <- mean(w95, na.rm = TRUE)
            
            # Take either mean of lowest 7-day stretches (value and date, plus BFI) starting Jan 1 or June, whichever has a 
            # lower standard deviation in the dates: if we just go from Jan 1-Dec 31, then parts of the country with
            # winter droughts could go from a value of Jan 1 (day number = 1) one year to Dec 31 (day = 365) the next
            # year; average = 182.5, July 1. In order to avoid this, we start either in summer or winter, whichever
            # gives more consistent values; in most of the country these will give identical answers
            if(sd(JanMinDt, na.rm = TRUE) < sd(JunMinDt, na.rm = TRUE)){
              Ann7dMin_avg[cc] <- mean(Jan7dMin, na.rm = TRUE)
              AnnMinDt_avg[cc] <- mean(JanMinDt, na.rm = TRUE)
              BFI_avg[cc] <- mean(JanBFI, na.rm = TRUE)
              
              # Calculate decadal low flow event (10% percentile of annual low flows)
              Dec7Q10_avg[cc] <- quantile(Jan7dMin, 0.1, na.rm = TRUE)
              
              
            } else{
              Ann7dMin_avg[cc] <- mean(Jun7dMin, na.rm = TRUE)
              BFI_avg[cc] <- mean(JunBFI, na.rm = TRUE)
              Dec7Q10_avg[cc] <- quantile(Jun7dMin, 0.1, na.rm = TRUE)
              
              # Adjust day numbers so that both series are counting from Jan 1; then adjust to keep values below 365.25
              AnnMinDt <- mean(JunMinDt, na.rm = TRUE) + mean(jun1[1:nYr-1] - jan1[1:nYr-1])
              if(AnnMinDt >= mean(yrLen[1:(nYr -1)])){
                AnnMinDt <- AnnMinDt - mean(yrLen[1:(nYr -1)])}
              AnnMinDt_avg[cc] <- AnnMinDt
            }
    
            # find maximum flow for each year; if all data in year are NA, return NA
            qmax <- aggregate(flow_in,list(waterYr), FUN=function(z) ifelse(any(!is.na(z)),max(z,na.rm=TRUE),NA))
            
            # based on maximum flow in all years, find quantile associated with 1.5 year flood
            # Change this value to get different flood level: e.g. 0.5 is surpassed every other year,
            # 0.75 is surpassed every 4th year, 0.33 is surpassed every year and a half (1/.67 = 1.5)
            Q1.5_avg[cc] <- quantile(qmax[,2],0.33, na.rm = TRUE) 
            # 10 and 25-year floods
            Q10_avg[cc] <- quantile(qmax[,2],0.9, na.rm = TRUE) 
            Q25_avg[cc] <- quantile(qmax[,2],0.96, na.rm = TRUE) 
            
            # maximum recorded flow
            MaxFlow_avg[cc] <- max(flow_in, na.rm = TRUE)
          }
          
          # report progress occasionally
          if (cc %% 10000 == 0){
            print(paste0("Completed ",cc," of ",ncatch))
          }
        }
        
        # close netCDF file
        nc_close(nc)
        
        # put results into data frame
        metrics_df <- data.frame(nhd_list, flow_ann_avg, lapply(paste0("flow_",month.abb,"_avg"), function(x) get(x)),
                                 Q1.5_avg, Q10_avg, Q25_avg, MaxFlow_avg, Ann7dMin_avg, Dec7Q10_avg, BFI_avg, 
                                 AnnMinDt_avg, CFM_avg, w95_avg)
        
        # rename fields
        names(metrics_df) <- c("COMID", paste0("MA_",period), paste0("M",month.abb,"_",period),
                             paste0("HiQ1_5_",period), paste0("HiQ10_",period), paste0("HiQ25_",period), paste0("HiMax_",period), 
                             paste0("Lo7Q1_",period), paste0("Lo7Q10_",period), paste0("BFI_",period),
                             paste0("Lo7Q1Dt_",period), paste0("CFM_",period), paste0("W95_",period))
        
        # Convert cubic meters/day to cubic feet/second:
        metrics_df[,2:20] <- metrics_df[,2:20] * 35.3146667/86400
        
        # Write csv file
        write.csv(metrics_df, flowmet_name, row.names = FALSE)
        
        # clean up workspace
        #sort( sapply(ls(),function(x){object.size(get(x))})) 
        rm(metrics_df, nc)
        invisible(gc(verbose=FALSE, reset=TRUE))
        
        # end time
        end_time <- proc.time() - start_time
        print(paste0("Region ",regions[rr]," (",periods[pp],"-",mod_nms[mm],") completed after ",round(end_time[3]/60,2)," minutes"))
    
      } else{
        print(paste0(flowmet_name, " already exists"))
      }
    }
    
    ### Merge all models together, if not already done
    flowmet_name <- paste0(outdir,"Regional/",regions[rr],"_",gsub("orical","",gsub("_composite","",periods[pp])),"_5model_flow_met.csv")
    if(!file.exists(flowmet_name)){
    
      # get paths to all models
      met_nms <- character()
      for(mm in 1:length(mod_nms)){
        met_nms <- c(met_nms, paste0(outdir,"Regional/",regions[rr],"_",gsub("orical","",gsub("_composite","",periods[pp])),"_",mod_nms[mm],"_flow_met.csv"))
      }
      
      # bind these together in array
      temp_array <- abind(lapply(met_nms, read.csv), along=3)
      
      # find the average of all models
      met_mean <- apply(temp_array, 1:2, function(x) mean(x, na.rm = TRUE))
      
      # write this output as a csv
      write.csv(met_mean, flowmet_name, row.names = FALSE)
      print(paste0("Region ",regions[rr]," (",periods[pp],"-5-model) completed after ",round(end_time[3]/60,2)," minutes"))
      
    } else{
      print(paste0(flowmet_name, " already exists"))
    }
  }
}

end_time <- proc.time() - main_start
print(paste0("Completed processing all time periods and regions after ", round(end_time[3]/3600,2)," hours"))


# merge all regions together
for(pp in 1:3){
  met_all <- read.csv(paste0(outdir,"Regional/",regions[1],"_",substr(periods[pp],1,4),"_5model_flow_met.csv"))
  for(rr in 2:length(regions)){
    met_all <- rbind(met_all, read.csv(paste0(outdir,"Regional/",regions[rr],"_",substr(periods[pp],1,4),"_5model_flow_met.csv")))
  }
  write.csv(met_all, paste0(outdir,substr(periods[pp],1,4),"_5model_flow_met.csv"), row.names = FALSE)
}










############################################################################################################
### Calculate absolute and percent change; merge all time periods together by region; export to geodatabase

main_start <- proc.time()

# Check license
arc.check_product()

# input/output locations for spatial data and VAA (value-added attributes)
fc_in <- "E:/NHD/Flowlines/NHD_Flowlines.gdb/"
fc_out <- "G:/Hydrology/VIC_US_5Model_Summary/VIC_US_FlowMetrics.gdb/"
vaa_in <- "E:/NHD/Flowline_Attributes/Attribute_Tables/"
regdir <- paste0(outdir,"Regional/")

# For each region
for (rr in 1:length(regions)){
#for (rr in c(1,3)){
  # rr <- 1
  
  rname <- regions[rr]
  start_time <- proc.time()
  
  # read in data
  t1 <- read.csv(paste0(regdir,rname,"_",gsub("orical","",periods[1]),"_5model_flow_met.csv"), as.is = TRUE)
  t2 <- read.csv(paste0(regdir,rname,"_",gsub("_composite","",periods[2]),"_5model_flow_met.csv"), as.is = TRUE)
  t3 <- read.csv(paste0(regdir,rname,"_",gsub("_composite","",periods[3]),"_5model_flow_met.csv"), as.is = TRUE)
  
  ## Calculate absolute and percent change between time periods
  
  # calculate absolute changes over time for 2040
  ch1 <- data.frame(t1)
  names(ch1) <- c("COMID", gsub("Hist", "a2040", names(t1)[2:ncol(ch1)]))
  ch1[,2:ncol(ch1)] <- t2[,c(2:ncol(t2))]-t1[,c(2:ncol(t1))]
  
  # revise change in dates to give as positive or negative number, whichever is closer
  # i.e., report change from Jan 1 to Dec 31 as -1, not +365)
  ch1[,22] <- ifelse(ch1[,22] > 182.625, ch1[,22] - 365.25, 
                     ifelse(ch1[,22] < -182.625, ch1[,22] + 365.25, ch1[,22]))
  
  # write to csv
  write.csv(ch1,paste0(regdir,rname,"_AbsHist2040_flow_met.csv"), row.names = FALSE)
  
  # Same for 2080
  ch2 <- data.frame(t1)
  names(ch2) <- c("COMID", gsub("Hist", "a2080", names(t1)[2:ncol(ch1)]))
  ch2[,2:ncol(ch1)] <- t3[,c(2:ncol(t3))]-t1[,c(2:ncol(t1))]
  ch2[,22] <- ifelse(ch2[,22] > 182.625, ch2[,22] - 365.25, ifelse(ch2[,22] < -182.625, ch2[,22] + 365.25, ch2[,22]))
  write.csv(ch2,paste0(regdir,rname,"_AbsHist2080_flow_met.csv"), row.names = FALSE)
  
  # percent changes; exclude dates
  chp1 <- data.frame(t1)[,c(1:21,24)]
  names(chp1) <- c("COMID", gsub("Hist", "p2040", names(t1)[c(2:21,24)]))
  chp1[,2:ncol(chp1)] <- ((t2[,c(2:21,24)]-t1[,c(2:21,24)])/t1[,c(2:21,24)]) * 100
  write.csv(chp1,paste0(regdir,rname,"_PctHist2040_flow_met.csv"), row.names = FALSE)
  
  chp2 <- data.frame(t1)[,c(1:21,24)]
  names(chp2) <- c("COMID", gsub("Hist", "p2080", names(t1)[c(2:21,24)]))
  chp2[,2:ncol(chp1)] <- ((t3[,c(2:21,24)]-t1[,c(2:21,24)])/t1[,c(2:21,24)]) * 100
  write.csv(chp2,paste0(regdir,rname,"_PctHist2080_flow_met.csv"), row.names = FALSE)
  
  # Merge all of these together
  met_all <- Reduce(function(x, y) merge(x, y, all=TRUE), list(t1, t2, t3, ch1, ch2, chp1, chp2))
    
  write.csv(met_all,paste0(outdir,rname,"_flow_met_all.csv"), row.names = FALSE)
  
  #met_all <- read.csv(paste0(outdir,rname,"_flow_met_all.csv"))
  
  # read in VAA (value added attributes) data; keep total upstream area (TotDASqKM), stream type (WBAreaType), 
  # and Tidal (whether stream is tidally influenced)
  vaa <- read.dbf(paste0(vaa_in, "PlusFlowlineVAA_",rname,".dbf"))
  #vaa <- vaa[,c(1,35,37,39)]
  vaa <- vaa[,c("ComID", "TotDASqKM", "Tidal", "WBAreaType")]
  
  # join total upstream catchment area from VAA file to stream metrics
  met_vaa <- merge(met_all, vaa, by.x = "COMID", by.y = "ComID", all.x = TRUE)

  # read in flowline feature class
  fc_name <- paste0(fc_in, "NHDFlowline_", rname)
  fc <- arc.select(arc.open(fc_name))
  fc <- fc[,c(1,2,6,7,11,13)]
  
  # join stream summary data to this feature class
  fc_join <- merge(fc, met_vaa, by.x = "Permanent_Identifier", by.y="COMID", all.x = TRUE)
  
  # return this to the original order
  fc_join <- fc_join[order(fc_join$OBJECTID),c(1,3:ncol(fc_join))]
  names(fc_join)[1] <- "COMID"
  
  # write to output file geodatabase
  arc.write(path = paste0(fc_out, "FlowMet_",rname), data=fc_join,  coords=arc.shape(fc), 
            shape_info=list(type='Polyline', hasZ=TRUE, WKID=attributes(arc.shape(fc))$shapeinfo[5][[1]]), 
            validate = TRUE, overwrite = TRUE)
  
  # Clean up workspace
  #sort( sapply(ls(),function(x){object.size(get(x))})) 
  #rm(fc_join, met_vaa, met_all, fc, t1, t2, t3, ch1, ch2, chp1, chp2, vaa)
  rm(fc_join, met_vaa, met_all, fc, vaa)
  invisible(gc(verbose=FALSE, reset=TRUE))       
  
  end_time <- proc.time() - start_time
  print(paste0("Summarization and exporting of region ", rname, " completed after ", round(end_time[3]/60,2)," minutes"))
  
}

end_time <- proc.time() - main_start
print(paste0("Summarization and exporting of all regions completed after ", round(end_time[3]/60,2)," minutes"))

# Subsequent steps performed in Python (merging, projecting, cleaning up data, rasterizing, and mapping)