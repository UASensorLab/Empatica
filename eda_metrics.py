import os.path
import sys
import pandas as pd
import biobss as bio
import numpy as np
import matplotlib.pyplot as plt

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
    eda_windows = df.groupby('participant_id').resample(window_size, origin='start').agg(
                                mean_eda=('eda', 'mean'), 
                                min_eda=('eda', 'min'),
                                max_eda=('eda', 'max'),
                                std_eda=('eda', 'std')).dropna().reset_index()
    
    # Get window ID
    eda_windows['window_id'] = eda_windows.groupby('participant_id').cumcount() + 1

    # Reorder columns to put window ID before timestamp
    eda_windows = eda_windows[['participant_id', 'window_id', 'timestamp', 'mean_eda', 'min_eda', 'max_eda', 'std_eda']]

    return eda_windows

''' Get SCR and SCL metrics by windows of window_size time '''
def getFeatureWindows(df, window_size, rest_period):
    # Decompose the EDA signal, set timestamp as index
    eda_decomp = bio.edatools.eda_decompose(df['eda'], 4)
    eda_decomp['timestamp'] = df.index
    eda_decomp['participant_id'] = np.array(df['participant_id'])
    eda_decomp = eda_decomp.set_index(['timestamp'])

    # Get peak details
    array, info = bio.edatools.eda_detectpeaks(eda_decomp['EDA_Phasic'], 4)
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
    feature_windows = eda_decomp.groupby('participant_id').resample(window_size, origin='start').agg(
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

''' Processes an EDA data file with participant_id, unix_timestamp, and eda data to output metrics by windows '''
def processEDA(folder, output_dir, window_size, rest_period):
    print("Processing EDA data")
    filepath = os.path.join(folder, 'eda.csv')

    # Check that EDA file exists
    if os.path.isfile(filepath):
        eda_df = pd.read_csv(filepath)
    else:
        print("EDA file not found")
        sys.exit()

    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    
    # Set readable timestamp as index
    eda_df['timestamp'] = pd.to_datetime(eda_df['unix_timestamp'] * 1000)
    eda_df = eda_df.set_index(['timestamp'])
    eda_df.index = pd.to_datetime(eda_df.index) 
    
    # Resample EDA data and Phasic (SCR) and Tonic (SCL) components
    eda_windows = getEDAWindows(eda_df, window_size)
    feature_windows = getFeatureWindows(eda_df, window_size, rest_period)

    # Join EDA windows and feature windows, drop empty cells
    eda_windows = eda_windows.merge(feature_windows, on=['participant_id', 'timestamp'])
    eda_windows = eda_windows.dropna().reset_index().drop(columns=['index'])

    # Print the result
    # print(eda_windows)

    # Output to csv
    eda_windows.to_csv(os.path.join(output_dir, 'eda_metrics.csv'))

    print("Output EDA metrics:", os.path.join(output_dir, 'eda_metrics.csv'))


# def classify_activity(magnitude):
#     if magnitude < 0.1:
#         return 1
#     else:
#         return 2
#     # elif magnitude < 0.3:
#     #     return 3
#     # else:
#     #     return 4

# ## NOT COMPLETE
# def processAcc(filepath, output_dir, window_size):
#     # Check that ACC file exists
#     if os.path.isfile(filepath):
#         acc_df = pd.read_csv(filepath)
#     else:
#         print("ACC file not found")
#         sys.exit()

#     # Set readable timestamp as index
#     acc_df['timestamp'] = pd.to_datetime(acc_df['unix_timestamp'] * 1000)
#     acc_df = acc_df.set_index(['timestamp'])
#     acc_df.index = pd.to_datetime(acc_df.index) 

#     acc_df = acc_df[(acc_df.index <= '2024-10-05')]

#     acc_df['fmpre'] = bio.imutools.generate_dataset(acc_df['x'],acc_df['y'],acc_df['z'],32,filtering=True,filtering_order='pre',magnitude=True,normalize=False,modify=False)[0]
#     print(acc_df.head())
    
#     acc_df = acc_df.resample(window_size, origin='start').agg(mean_fmpre=('fmpre', 'mean'))
#     acc_df['activity_level'] = acc_df['mean_fmpre'].apply(classify_activity)
  
#     acc_df = acc_df.dropna()

#     plt.figure(figsize=(12, 8))

#     # plt.plot(acc_df.index, acc_df['mean_fmpre'], label='ACC')
#     plt.plot(acc_df.index, acc_df['activity_level'], label="Activity")
#     plt.title('ACC')
#     plt.grid(True)

#     plt.show()

#     print(acc_df)
#     # print(acc_df['mean_magnitude'].max(), acc_df['mean_magnitude'].min())

#     acc_df.to_csv(os.path.join(output_dir, 'acc_metrics.csv'))




# Path to folder with extracted csv's (should have eda.csv)
csv_folder_path = '/Users/maliaedmonds/Documents/SensorLab/Empatica'
# Path to output folder
output_dir = '/Users/maliaedmonds/Documents/SensorLab/empatica_test/1-1-001_metrics'
# Window size (e.g. '30s' or '1min')
window_size = '30s'
# Rest period (minutes)
rest_period = 5

# eda_csv = os.path.join(csv_folder_path, 'eda.csv')
processEDA(csv_folder_path, output_dir, window_size, rest_period)

# acc_csv = os.path.join(csv_folder_path, 'accelerometer.csv')
# processAcc(acc_csv, output_dir, window_size)