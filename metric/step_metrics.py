import os
import glob
import pandas as pd
import numpy as np
import sys

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

def getMetrics(steps_df, window_size):
    # Check that correct columns exist
        if not {'participant_id', 'unix_timestamp', 'steps'}.issubset(steps_df.columns):
            print('Error parsing:', steps_df.columns)
            print('Skipping')
            return None
    
        # Set readable timestamp as index
        steps_df['timestamp'] = pd.to_datetime(steps_df.loc[:,'unix_timestamp'] * 1000)
        steps_df = steps_df.set_index(['timestamp'])
        steps_df.index = pd.to_datetime(steps_df.index) 
    
        # Resample steps data 
        steps_windows = steps_df.groupby('participant_id').resample(window_size, label='left', origin='start').agg(
                                step_count=('steps', 'sum')).dropna().reset_index()
        
        steps_windows.insert(2, 'window_id', steps_windows.groupby('participant_id').cumcount() + 1)

        return steps_windows

''' Processes steps data files with participant_id, unix_timestamp, and steps data to output metrics by windows '''
def processSteps(folderpath, output_dir, window_size, tags=False):
    print("Processing steps data")
        
    # Find all .csv files containing "steps" (case-insensitive)
    pattern = os.path.join(folderpath, '**', '*.csv')
    steps_files = [f for f in glob.glob(pattern, recursive=True) if "steps" in os.path.basename(f).lower()]
    if steps_files:
        print("steps Files Found:", steps_files)
    else:
        print("No steps files found.")
        sys.exit()

    final_df = pd.DataFrame()
    
    for filepath in steps_files:

        # Check that steps file exists
        if os.path.isfile(filepath):
            steps_df = pd.read_csv(filepath)
        else:
            print("steps file not found:", filepath)
            continue

        steps_windows = getMetrics(steps_df, window_size)

        if steps_windows is None:
            continue

        final_df = pd.concat([final_df, steps_windows])

    final_df = final_df.reset_index().drop(columns=['index'])

    final_df['window_id'] = final_df.groupby('participant_id').cumcount() + 1

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
    final_df.to_csv(os.path.join(output_dir, 'steps_metrics.csv'))

    print("Outputting steps metrics:", os.path.join(output_dir, 'steps_metrics.csv'))


csv_path = '/Users/maliaedmonds/Documents/SensorLab/Empatica/csv'
output_dir = '/Users/maliaedmonds/Documents/SensorLab/Empatica/metrics'

processSteps(csv_path, output_dir, '1min')