import os
import glob
import pandas as pd
import sys
import biobss
import numpy as np

def calculate_hrv(bvp_segment):
    # Compute HRV metrics using the bvp_segment
    try:
        # print(bvp_segment)
        # if 'bvp' not in segment.columns:
        #     print("Warning: 'bvp' column is missing.")
        #     return pd.Series({key: np.nan for key in ('hrv_mean_hr', 'hrv_min_hr', 'hrv_max_hr', 'hrv_std_hr', 'hrv_mean_nni')})  # Return NaNs or appropriate defaults

        # # Extract the 'bvp' data
        # bvp_segment = segment['bvp']
        # # print(bvp_segment)
        # bvp_segment.reset_index().to_numpy()
        # print(bvp_segment)
        # print('bvp' in bvp_segment.columns)
        info=biobss.preprocess.peak_detection(bvp_segment,64,'peakdet',delta=0.01)

        print(info)
        locs_peaks=info['Peak_locs']
        locs_onsets=info['Trough_locs']

        hrv_features = biobss.hrvtools.get_hrv_features(sampling_rate=64, signal_length=30, input_type='peaks', peaks_locs=locs_peaks)
        return pd.Series({key: hrv_features[key] for key in ('hrv_mean_hr', 'hrv_min_hr', 'hrv_max_hr', 'hrv_std_hr', 'hrv_mean_nni')})
    except Exception as e:
        return None

def processBVP(folderpath, output_dir, window_size):
    print("Processing BVP data")
        
    # Find all .csv files containing "eda" (case-insensitive)
    pattern = os.path.join(folderpath, '**', '*.csv')
    bvp_files = [f for f in glob.glob(pattern, recursive=True) if "bvp" in os.path.basename(f).lower()]
    if bvp_files:
        print("BVP Files Found:", bvp_files)
    else:
        print("No BVP files found.")
        sys.exit()

    final_df = pd.DataFrame()
    
    for filepath in bvp_files:
        # Check that BVP file exists
        if os.path.isfile(filepath):
            bvp_df = pd.read_csv(filepath)
        else:
            print("BVP file not found:", filepath)
            continue

        # Check that correct columns exist
        if not {'participant_id', 'unix_timestamp', 'bvp'}.issubset(bvp_df.columns):
            print('Error parsing', filepath, ":", bvp_df.columns)
            print('Skipping', filepath)
            continue

        # Set readable timestamp as index
        bvp_df['timestamp'] = pd.to_datetime(bvp_df['unix_timestamp'] * 1000)
        bvp_df = bvp_df.set_index(['timestamp'])
        bvp_df.index = pd.to_datetime(bvp_df.index)

        bvp_df = bvp_df.dropna()
        bvp_df = bvp_df[bvp_df.index <= '10-05-2024']

        # hrv_mets = (
        #     bvp_df.groupby('participant_id', as_index=False) 
        #     .resample(window_size, origin='start') 
        #     .apply(lambda x: calculate_hrv(x)) 
        #     .dropna()
        #     .reset_index() 
        # )

        # print(hrv_mets)

        bvp_mets = bvp_df.groupby('participant_id').resample(window_size, origin='start').agg(
                                mean_bvp=('bvp', 'mean'), 
                                min_bvp=('bvp', 'min'),
                                max_bvp=('bvp', 'max'),
                                std_bvp=('bvp', 'std')).dropna().reset_index()
        
        # bvp_windows = bvp_mets.merge(hrv_mets, on=['participant_id', 'timestamp'])

        # print(bvp_windows)

        # bvp_df = bvp_df[bvp_df.index <= '10-05-2024'] 

        # bvp_df['filtered_bvp'] = biobss.preprocess.filter_signal(bvp_df['bvp'], sampling_rate=64, signal_type='PPG', method='bandpass')

        # info=biobss.preprocess.peak_detection(bvp_df['bvp'],64,'peakdet',delta=0.01)

        # locs_peaks=info['Peak_locs']
        # peaks=bvp_df['bvp'][locs_peaks]
        # locs_onsets=info['Trough_locs']
        # onsets=bvp_df['bvp'][locs_onsets]

        # ppg_hrv = biobss.hrvtools.get_hrv_features(sampling_rate=64, signal_length=100, input_type='peaks', peaks_locs=locs_peaks)
        # print(ppg_hrv)

csv_path = '/Users/maliaedmonds/Documents/SensorLab/Empatica/csv'
output_dir = '/Users/maliaedmonds/Documents/SensorLab/Empatica/metrics'

processBVP(csv_path, output_dir, '30s')