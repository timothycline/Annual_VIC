ssh tcline@yeti.cr.usgs.gov
TClinP@$$W0rd20211#!

cd ~/Annual_VIC
git pull

sbatch ~/Annual_VIC/VIC_Code/NLDASprocessing.slurm
cat NLDAS_Hourly_Processing.out

salloc -p normal -A norock -n 1 -N 1 -t 3-01:00:00

cd ~/Annual_VIC
#git pull
module load R/4.1.1 gdal/3.1.0 geos/3.8.1 proj/7.0.1

cd NLDASdata/DailyRunoffs
ls
#srun Rscript VIC_Code/detectCores.R
#srun Rscript VIC_Code/Download_VIC_2020_04_16_multiNode.R
#srun Rscript MPVA_ReddsOnly_Yeti.R

#exit