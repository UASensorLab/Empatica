import os.path
import sys
import pandas as pd
import glob

## Define the location of the folder containing csv files and the output folder.
# macOS example:
# csv_folder_path = "/Users/timmytommy/Data/Csv/"
# output_dir = "/Users/timmytommy/Data/Output/"
#
# Windows example:
# csv_folder_path = "C:/Data/Csv/"
# output_dir = "C:/Data/Output/"

def processTags(folderpath):
    tags_df = pd.DataFrame()
    pattern = os.path.join(folderpath, '**', '*.csv')
    tag_files = [f for f in glob.glob(pattern, recursive=True) if "tag" in os.path.basename(f).lower()]
    if tag_files:
        print("Tag Files Found:", tag_files)
        for filepath in tag_files:
            # Check that EDA file exists
            if os.path.isfile(filepath):
                tags_df = pd.concat([tags_df, pd.read_csv(filepath)])
            else:
                print("Tag file not found:", filepath)
                continue
    else:
        print("No Tag files found.")
        return tags_df
    tags_df['timestamp'] = pd.to_datetime(tags_df['tags_timestamp'] * 1000)
    return tags_df

def processTemperature(folderpath, output_dir, window_size='30s', tags=False):
    print("Processing Temperature data")

    # Find all .csv files containing "temperature" (case-insensitive)
    pattern = os.path.join(folderpath, '**', '*.csv')
    temp_files = [f for f in glob.glob(pattern, recursive=True) if "temperature" in os.path.basename(f).lower()]
    if temp_files:
        print("Temperature Files Found:", temp_files)
    else:
        print("No Temperature files found.")
        sys.exit()

    final_df = pd.DataFrame()
    
    for filepath in temp_files:

        # Check that temperature file exists
        if os.path.isfile(filepath):
            temperature_df = pd.read_csv(filepath)
        else:
            print("Temperature file not:", filepath)
            continue

        # Check that correct columns exist
        if not {'participant_id', 'unix_timestamp', 'temperature'}.issubset(temperature_df.columns):
            print('Error parsing', filepath, ":", temperature_df.columns)
            print('Skipping', filepath)
            continue
        
        # Set readable timestamp as index
        temperature_df['timestamp'] = pd.to_datetime(temperature_df['unix_timestamp'] * 1000)
        temperature_df = temperature_df.set_index(['timestamp'])
        temperature_df.index = pd.to_datetime(temperature_df.index) 

        # Divide data into windows (30s), get metrics for temperature
        temperature_df = temperature_df.groupby('participant_id').resample(window_size, origin='start').agg(
                                    mean_temp=('temperature', 'mean'), 
                                    max_temp=('temperature', 'max'),
                                    min_temp=('temperature', 'min'),
                                    std_temp=('temperature', 'std'))
        final_df = pd.concat([final_df, temperature_df])
    
    final_df.insert(0, 'window_id', final_df.groupby('participant_id').cumcount() + 1)


    final_df = final_df.dropna().reset_index()

    if tags:
        tags_df = processTags(folderpath)
        final_df.insert(3, 'tag', False)
        for _,row in tags_df.iterrows():
            final_df.loc[
                (final_df["timestamp"] <= row["timestamp"]) & 
                (final_df["timestamp"] + pd.Timedelta(window_size) > row["timestamp"]) & 
                (row['participant_id'] == final_df['participant_id']), "tag"] = True

    # Output to csv
    final_df.to_csv(os.path.join(output_dir, 'temperature_metrics.csv'))

    print("Outputting Temperature metrics:", os.path.join(output_dir, 'temperature_metrics.csv'))



# csv_folder_path = '/Users/maliaedmonds/Documents/SensorLab/Empatica/1-1-001_csv'
# output_dir = '/Users/maliaedmonds/Documents/SensorLab/Empatica/1-1-001_metrics'
# processTemperature(csv_folder_path, output_dir, window_size='30s')