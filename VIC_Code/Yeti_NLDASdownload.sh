ssh tcline@yeti.cr.usgs.gov
TClinP@$$W0rd20211#!

#mkdir Annual_VIC
#git clone https://github.com/timothycline/Annual_VIC.git ~/Annual_VIC
git pull

#Transfer download file
#run in fresh terminal
#will require password
cd ~/"OneDrive - DOI"/Annual_VIC/NLDASdata
scp NLDASdownload_Hourly_01062022.txt tcline@yeti-dtn.cr.usgs.gov:~/Annual_VIC/NLDASdata

#load R
module load R/4.1.1

sbatch ~/Annual_VIC/VIC_Code/NLDASdownload.slurm
ls | wc -l

#cat NLDAS_Hourly_Download.out
#test code
#R
#system("cd ~/Annual_VIC")
#system("touch .netrc")
#system('echo "machine urs.earthdata.nasa.gov login timothy_cline password %#%earthdata1542gnpMT" >> .netrc')
#system("touch .urs_cookies")
#system(paste0("wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --auth-no-challenge=on --no-check-certificate --keep-session-cookies https://hydro1.gesdisc.eosdis.nasa.gov/data/NLDAS/NLDAS_VIC0125_H.002/1979/002/NLDAS_VIC0125_H.A19790102.0000.002.grb"))
