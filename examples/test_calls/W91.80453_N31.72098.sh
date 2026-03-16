LON_LAT=-"91.80453 31.72098"

# Search data
seppo_nisar_search --point ${LON_LAT} -o myurls.txt --https

# Processing
TSRS="-t_srs 4326"
TR="-tr 0.0003 0.0003"
PROJWIN="--projwin -91.93513 31.84243 -91.58116 31.49518"
seppo_nisar_gcov_convert -dpratio -sigma0 -amp -vsis3 $TSRS $TR $PROJWIN -i myurls.txt -o s3://seppo1-data/NISAR/alltest -v
