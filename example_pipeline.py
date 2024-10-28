from get_bucket_files import getFiles
from avro_to_csv_with_ID import process_folder
from eda_metrics import processEDA

AVRO_DIR = "./avro"
CSV_DIR = "./csv"
METRICS_DIR = "./metrics"

# Get files from data bucket and store them in ./avro (files are sorted into folders based on participant id)
# If AVRO_DIR does not exist, function will create the directory
# If a file has already been downloaded from the data bucket, it will not be duplicated
getFiles(AVRO_DIR)

# Process .avro files to .csv files with participant_id and store in ./csv
# If CSV_DIR does not exist, function will create the directory
# If a file has already been processed, the name will be stored in a .txt file and it will not be re-processed
process_folder(AVRO_DIR, CSV_DIR)

# Process CSV_DIR/eda.csv file to extract EDA, SCR, and SCL metrics 
# If METRICS_DIR does not exist, function will create the directory
# Creates windows of time period window_size and calculates baseline SCL for first rest_period minutes of data
processEDA(CSV_DIR, METRICS_DIR, window_size='30s', rest_period=5)
