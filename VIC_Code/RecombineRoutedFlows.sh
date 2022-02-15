ssh tcline@yeti.cr.usgs.gov
#TClinP@$$W0rd20211#!
TClinP@$$W0rd20212#!

#scp ~/"OneDrive - DOI"/Annual_VIC/NLDASdata/NLDAS_Lats.csv tcline@yeti.cr.usgs.gov:~/Annual_VIC/NLDASdata
#scp tcline@yeti.cr.usgs.gov:~/Annual_VIC/NLDAS_Routing_MultiNode_PN17.out ~/"OneDrive - DOI"/Annual_VIC/

cd ~/Annual_VIC
git pull

sbatch VIC_Code/RecombineRoutedFlows_PN17.slurm

#Start machine and load R
#salloc -p normal -A norock -N 1 -t 3-01:00:00
sidle
#salloc -p normal -A norock -N 1 -t 3-01:00:00
salloc -p normal -A norock -N 1 -t 3-01:00:00

cd ~/Annual_VIC
git pull

module load R/4.1.1 gdal/3.1.0 geos/3.8.1 proj/7.0.1

R


