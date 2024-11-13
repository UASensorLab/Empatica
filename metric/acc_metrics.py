import os
import glob
import pandas as pd
import numpy as np
import sys
import biobss

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

def calculateAI(accx, accy, accz, window_size):
    if window_size == '30s':
        size = 30
    elif window_size == '1min': 
        size = 60
    else:
        size = 30
    
    try:
        if accx.size == 0 or accy.size == 0 or accz.size == 0:
            return pd.Series({'UFXYZ': np.nan, 'FXYZ': np.nan})
        
        #Filter ACC signal by using predefined filters
        f_accx=biobss.preprocess.filter_signal(sig=accx, sampling_rate=32, signal_type='ACC', method='lowpass')
        f_accy=biobss.preprocess.filter_signal(sig=accy, sampling_rate=32, signal_type='ACC', method='lowpass')
        f_accz=biobss.preprocess.filter_signal(sig=accz, sampling_rate=32, signal_type='ACC', method='lowpass')

        ai = biobss.imutools.calc_activity_index(accx, accy, accz, signal_length=size, sampling_rate=32, metric='AI', baseline_variance=[0.05,0.05,0.05])

        return pd.Series(ai)

    except Exception as e:
        print(str(e))
        return pd.Series({'UFXYZ': np.nan, 'FXYZ': np.nan})


def getMetrics(acc_df, window_size):
    # Check that correct columns exist
        if not {'participant_id', 'unix_timestamp', 'x', 'y', 'z'}.issubset(acc_df.columns):
            print('Error parsing:', acc_df.columns)
            print('Skipping')
            return None
    
        # Set readable timestamp as index
        acc_df['timestamp'] = pd.to_datetime(acc_df.loc[:,'unix_timestamp'] * 1000)
        acc_df = acc_df.set_index(['timestamp'])
        acc_df.index = pd.to_datetime(acc_df.index) 
    
        # Resample acc data 
        acc_windows = (acc_df.groupby('participant_id')
                            .resample(window_size, label='left', origin='start')
                            .apply(lambda x: calculateAI(np.asarray(x['x']), np.asarray(x['y']), np.asarray(x['z']), window_size))
                            .dropna()
                            .reset_index()
        )
        
        acc_windows.insert(2, 'window_id', acc_windows.groupby('participant_id').cumcount() + 1)

        print(acc_windows)

        return acc_windows

''' Processes acc data files with participant_id, unix_timestamp, and acc data to output metrics by windows '''
def processAcc(folderpath, output_dir, window_size, tags=False):
    print("Processing acc data")
        
    # Find all .csv files containing "acc" (case-insensitive)
    pattern = os.path.join(folderpath, '**', '*.csv')
    acc_files = [f for f in glob.glob(pattern, recursive=True) if "acc" in os.path.basename(f).lower()]
    if acc_files:
        print("acc Files Found:", acc_files)
    else:
        print("No acc files found.")
        sys.exit()

    final_df = pd.DataFrame()
    
    for filepath in acc_files:

        # Check that acc file exists
        if os.path.isfile(filepath):
            acc_df = pd.read_csv(filepath)
        else:
            print("acc file not found:", filepath)
            continue

        acc_windows = getMetrics(acc_df, window_size)

        if acc_windows is None:
            continue

        final_df = pd.concat([final_df, acc_windows])

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
    final_df.to_csv(os.path.join(output_dir, 'acc_metrics.csv'))

    print("Outputting acc metrics:", os.path.join(output_dir, 'acc_metrics.csv'))


csv_path = '/Users/maliaedmonds/Documents/SensorLab/Empatica/csv'
output_dir = '/Users/maliaedmonds/Documents/SensorLab/Empatica/metrics'

processAcc(csv_path, output_dir, '30s')