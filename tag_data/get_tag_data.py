import os
import glob
import sys
import pandas as pd

''' Process all "tag" files in folderpath into a single dataframe '''
def processTags(folderpath):
    print("Processing tags")
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

''' Process each file containing metric name in folderpath into a single dataframe '''
def processMetric(folderpath, metric):
    print("Processing", metric)

    # Get all files in folderpath containing metric name (case-insensitive)
    pattern = os.path.join(folderpath, '**', '*.csv')
    met_files = [f for f in glob.glob(pattern, recursive=True) if metric.lower() in os.path.basename(f).lower()]

    # Return None if no metric files were found
    if met_files:
        print(metric, "Files Found:", met_files)
    else:
        print("No", metric, "files found.")
        return None

    met_df = pd.DataFrame()

    # Process each metric file found
    for filepath in met_files:
        file_df = pd.read_csv(filepath)

        # Check that correct columns exist
        if not {'participant_id', 'unix_timestamp', metric.lower()}.issubset(file_df.columns):
            print('Error parsing', filepath, ":", file_df.columns)
            print('Skipping', filepath)
            continue

        # Combine each metric dataframe
        met_df = pd.concat([met_df, file_df])

    # Add readable timestamp 
    met_df.insert(2, 'timestamp', pd.to_datetime(met_df['unix_timestamp'] * 1000))

    return met_df

''' Print each metric in metrics list (default 'eda') for timeframe seconds before and after each tag in csv_folderpath '''
def getTagData(csv_folderpath, timeframe, metrics=['eda']):

    # Get all tags in csv_folderpath
    tags_df = processTags(csv_folderpath)

    # Get a list of dataframes for each metric, stored in a dictionary with metric name as the key
    metric_dfs = {}
    for metric in metrics:
        met_df = processMetric(csv_folderpath, metric)
        metric_dfs.update({metric: met_df})

    # Print metrics for each tag
    for _, row in tags_df.iterrows():
        id = row['participant_id']
        time = row['timestamp']

        # Print each metric
        for key in metric_dfs:
            met_df = metric_dfs[key]
            print()
            print("*******************************************************************************")
            print(key + " (" + str(time) + ")")
            print("*******************************************************************************")

            # If no data was found, print message
            if met_df is None:
                print("No", key, "data found.")

            # Print data timeframe seconds before and after tag (for respective participant id)
            else:
                tag_window = met_df[(met_df['participant_id'] == id)
                            & (met_df['timestamp'] >= (time - pd.Timedelta(seconds=timeframe)))
                            & (met_df['timestamp'] <= (time + pd.Timedelta(seconds=timeframe)))]
                # Check that data matches tag
                if tag_window.empty:
                    print(key, "data does not match tag.")
                else:
                    print(tag_window)
    print()
            

csv_folderpath = '/Users/maliaedmonds/Documents/SensorLab/Empatica/csv'
timeframe = 30

getTagData(csv_folderpath, timeframe, metrics=['eda', 'temperature', 'bvp', 'pulse'])