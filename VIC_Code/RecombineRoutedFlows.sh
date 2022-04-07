ssh tcline@yeti.cr.usgs.gov
#TClinP@$$W0rd20211#!
TClinP@$$W0rd20213#! 

#scp ~/"OneDrive - DOI"/Annual_VIC/NLDASdata/NLDAS_Lats.csv tcline@yeti.cr.usgs.gov:~/Annual_VIC/NLDASdata
#scp tcline@yeti.cr.usgs.gov:~/Annual_VIC/NLDAS_Routing_MultiNode_PN17.out ~/"OneDrive - DOI"/Annual_VIC/

scp tcline@yeti.cr.usgs.gov:~/Annual_VIC/RecombineRoutedFlows.out ~/"OneDrive - DOI"/Annual_VIC/

#Download NorthFork
#Download ProspectCreek
#scp tcline@yeti.cr.usgs.gov:~/Annual_VIC/NLDASdata/Routed_ByCOMID/PN17/COMID_22949233_19790102_20220101.RDS ~/"OneDrive - DOI"/Annual_VIC/
scp tcline@yeti.cr.usgs.gov:~/Annual_VIC/NLDASdata/Routed_ByCOMID/PN17/COMID_22949229_19790102_20220101.RDS ~/"OneDrive - DOI"/Annual_VIC/
scp tcline@yeti.cr.usgs.gov:~/Annual_VIC/NLDASdata/Routed_ByCOMID/PN17/COMID_22976274_19790102_20220101.RDS ~/"OneDrive - DOI"/Annual_VIC/

cd ~/Annual_VIC
git pull

sbatch VIC_Code/RecombineRoutedFlows_PN17.slurm

sbatch VIC_Code/CalculateStreamFlowMetrics_PN17.slurm

#Start machine and load R
#salloc -p normal -A norock -N 1 -t 3-01:00:00
sidle
scancel 5887523
#salloc -p normal -A norock -N 1 -t 3-01:00:00
salloc -p normal -A norock -N 1 -t 3-01:00:00

cd ~/Annual_VIC
git pull

module load R/4.1.1 gdal/3.1.0 geos/3.8.1 proj/7.0.1

R
library(here)
rname <- 'PN17'
AllRoutedFlows <- list.files(here('NLDASdata','Routed_ByDate',rname))
ff <- AllRoutedFlows[1]
assign(x=ff,value=readRDS(here('NLDASdata','Routed_ByDate',rname,ff)), envir = .GlobalEnv)
COMIDS <- names(get(AllRoutedFlows[1]))

length(COMIDS)
list.files(here('NLDASdata','Routed_ByCOMID',rname))[1]
round(100*length(list.files(here('NLDASdata','Routed_ByCOMID',rname)))/length(COMIDS),2)

q()

