import os.path
import sys
import pandas as pd
import biobss 
import numpy as np
import matplotlib.pyplot as plt
import glob

## Define the location of the eda csv file and the output folder.
# macOS example:
# csv_folder_path = "/Users/timmytommy/Data/Csv/eda.csv"
# output_dir = "/Users/timmytommy/Data/Output/"
#
# Windows example:
# csv_folder_path = "C:/Data/Csv/eda.csv"
# output_dir = "C:/Data/Output/"

## Define the window size for metric calculations
# 30 seconds -> '30s'
# 1 minute -> '1min'

## Define the length of the rest period (in minutes) to set SCL baseline
# 5 minues -> 5

''' Get basic EDA metrics by windows of window_size time '''
def getEDAWindows(df, window_size):
    # Divide data into windows (e.g. 30s), get metrics for eda
    eda_windows = df.groupby('participant_id').resample(window_size, label='left', origin='start').agg(
                                mean_eda=('eda', 'mean'), 
                                min_eda=('eda', 'min'),
                                max_eda=('eda', 'max'),
                                std_eda=('eda', 'std')).dropna().reset_index()
    
    # Get window ID
    eda_windows['window_id'] = eda_windows.groupby('participant_id').cumcount() + 1

    # Reorder columns to put window ID before timestamp
    eda_windows = eda_windows[['participant_id', 'timestamp', 'window_id', 'mean_eda', 'min_eda', 'max_eda', 'std_eda']]

    return eda_windows

''' Get SCR and SCL metrics by windows of window_size time '''
def getFeatureWindows(df, window_size, rest_period):
    # Decompose the EDA signal, set timestamp as index
    eda_decomp = biobss.edatools.eda_decompose(df['eda'], 4)
    eda_decomp['timestamp'] = df.index
    eda_decomp['participant_id'] = np.array(df['participant_id'])
    eda_decomp = eda_decomp.set_index(['timestamp'])

    # Get peak details
    array, info = biobss.edatools.eda_detectpeaks(eda_decomp['EDA_Phasic'], 4)
    array['timestamp'] = df.index
    array = array.set_index(['timestamp'])
    array.index = pd.to_datetime(array.index) 

    # Join decomposed signal and peak details
    eda_decomp = eda_decomp.join(array)
        
    # Get average scl for the first rest_period minutes (by participant)
    baseline = (
        eda_decomp.groupby('participant_id')
        .apply(lambda x: x[x.index <= (x.index[0] + pd.Timedelta(minutes=rest_period))]['EDA_Tonic'].mean())
    ).reset_index(name='scl_baseline')

    # Divide into windows, get metrics for SCR and SCL
    feature_windows = eda_decomp.groupby('participant_id').resample(window_size, label='left', origin='start').agg(
                                mean_scr=('EDA_Phasic', 'mean'),
                                min_scr=('EDA_Phasic', 'min'),
                                max_scr=('EDA_Phasic', 'max'),
                                num_scr_peaks=('SCR_Peaks', 'sum'),
                                max_scr_amplitude=('SCR_Amplitude', 'max'),
                                mean_scl=('EDA_Tonic', 'mean'),
                                min_scl=('EDA_Tonic', 'min'),
                                max_scl=('EDA_Tonic', 'max')).dropna().reset_index()
    
    # Add scl baseline and mean difference
    feature_windows = feature_windows.merge(baseline, on='participant_id', how='left')
    feature_windows['mean_diff_scl'] = feature_windows['mean_scl'] - feature_windows['scl_baseline']

    return feature_windows

''' Process all "tag" files in folderpath into a single dataframe '''
def processTags(folderpath):
    tags_df = pd.DataFrame()

    # Get all files in folderpath containing "tag" (case-insensitive)
    pattern = os.path.join(folderpath, '**', '*.csv')
    tag_files = [f for f in glob.glob(pattern, recursive=True) if "tag" in os.path.basename(f).lower()]
    
    # Process each tag file found
    if tag_files:
        print("Tag Files Found:", tag_files)
        for filepath in tag_files:
            file_df = pd.read_csv(filepath)

            # Check that correct columns exist
            if not {'participant_id', 'tags_timestamp'}.issubset(file_df.columns):
                print('Error parsing', filepath, ":", file_df.columns)
                print('Skipping', filepath)
                continue

            # Combine each tag dataframe
            tags_df = pd.concat([tags_df, file_df])
    
    # If no files were found, return and empty dataframe
    else:
        print("No Tag files found.")
        return tags_df
    
    # Add readable timestamp
    tags_df['timestamp'] = pd.to_datetime(tags_df['tags_timestamp'] * 1000)
    return tags_df


def getMetrics(eda_df, window_size, rest_period):
    # Check that correct columns exist
        if not {'participant_id', 'unix_timestamp', 'eda'}.issubset(eda_df.columns):
            print('Error parsing:', eda_df.columns)
            print('Skipping')
            return None
    
        # Set readable timestamp as index
        eda_df['timestamp'] = pd.to_datetime(eda_df.loc[:,'unix_timestamp'] * 1000)
        eda_df = eda_df.set_index(['timestamp'])
        eda_df.index = pd.to_datetime(eda_df.index) 
    
        # Resample EDA data and Phasic (SCR) and Tonic (SCL) components
        eda_windows = getEDAWindows(eda_df, window_size)
        feature_windows = getFeatureWindows(eda_df, window_size, rest_period)

        # Join EDA windows and feature windows, drop empty cells
        eda_windows = eda_windows.merge(feature_windows, on=['participant_id', 'timestamp'])

        return eda_windows

''' Processes EDA data files with participant_id, unix_timestamp, and eda data to output metrics by windows '''
def processEDA(folderpath, output_dir, window_size, rest_period, tags=False):
    print("Processing EDA data")
        
    # Find all .csv files containing "eda" (case-insensitive)
    pattern = os.path.join(folderpath, '**', '*.csv')
    eda_files = [f for f in glob.glob(pattern, recursive=True) if "eda" in os.path.basename(f).lower()]
    if eda_files:
        print("EDA Files Found:", eda_files)
    else:
        print("No EDA files found.")
        sys.exit()

    final_df = pd.DataFrame()
    
    for filepath in eda_files:

        # Check that EDA file exists
        if os.path.isfile(filepath):
            eda_df = pd.read_csv(filepath)
        else:
            print("EDA file not found:", filepath)
            continue

        eda_windows = getMetrics(eda_df, window_size, rest_period)

        if eda_windows is None:
            continue

        final_df = pd.concat([final_df, eda_windows])

    final_df = final_df.reset_index().drop(columns=['index'])

    # If tags is enabled, add boolean tags column from tags files
    if tags:
        tags_df = processTags(folderpath)
        final_df.insert(3, 'tag', "")
        for _,row in tags_df.iterrows():
            final_df.loc[
                (final_df["timestamp"] <= row["timestamp"]) & 
                (final_df["timestamp"] + pd.Timedelta(window_size) > row["timestamp"]) & 
                (row['participant_id'] == final_df['participant_id']), "tag"] = True

            
    # Print the result
    # print(final_df)

    # Output to csv
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    final_df.to_csv(os.path.join(output_dir, 'eda_metrics.csv'))

    print("Outputting EDA metrics:", os.path.join(output_dir, 'eda_metrics.csv'))





# Path to folder with extracted csv's (should have eda.csv)
# csv_folder_path = '/Users/maliaedmonds/Documents/SensorLab/Empatica/csv'
# # Path to output folder
# output_dir = '/Users/maliaedmonds/Documents/SensorLab/Empatica/metrics'
# # Window size (e.g. '30s' or '1min')
# window_size = '30s'
# # Rest period (minutes)
# rest_period = 5

# # eda_csv = os.path.join(csv_folder_path, 'eda.csv')
# processEDA(csv_folder_path, output_dir, window_size, rest_period)

# acc_csv = os.path.join(csv_folder_path, 'accelerometer.csv')
# processAcc(acc_csv, output_dir, window_size)