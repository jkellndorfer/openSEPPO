# openSEPPO CLI quick usage example 

# Search for NISAR data time series stacks across to alongtrack frames
seppo_nisar_search --track 105 --frame 17 18 -o la_urls.txt
# Inspect the first h5 file in the urls list for available grids
# Local files also work as input
seppo_nisar_gcov_convert --h5 la_urls.txt --list_grids
# Subset and convert the data to cloud optimized geotiffs, amplitude scaled with a target resolution of 50 m
# output to your s3 bucket (local path also possible)
seppo_nisar_gcov_convert -amp --h5 la_urls.txt --projwin 598146.587 3576347.040 750714.190 3428083.178 --output s3://seppo1-data/NISAR/test/LA_mosaic_50m/ -v -tr 50 50
# Also generate ancillary data for mask, number of looks, and gamma to sigma conversion factor
# Note: do not apply a scale conversion. 
 seppo_nisar_gcov_convert -i la_urls.txt --projwin 598146.587 3576347.040 750714.190 3428083.178 -o s3://seppo1-data/NISAR/test/LA_mosaic_anc_50m/ -v -tr 50 50 --vars mask numberOfLooks rtcGammaToSigmaFactor

# For integration in python programs see Jupyter notebook example
