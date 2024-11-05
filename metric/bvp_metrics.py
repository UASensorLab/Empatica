import os
import glob
import pandas as pd
import sys
import biobss
import numpy as np
import traceback
import warnings

def calculateHRV(bvp_segment, window_size, mets):
    # Compute HRV metrics using the bvp_segment
    if window_size == '30s':
        size = 30
    elif window_size == '1min': 
        size = 60
    else:
        size = 30


    try:
        if bvp_segment.size == 0:
            return pd.Series({key: np.nan for key in mets})
        
        info=biobss.preprocess.peak_detection(bvp_segment,64,'peakdet',delta=0.01)
        
        locs_peaks=info['Peak_locs']
        hrv_features = biobss.hrvtools.get_hrv_features(sampling_rate=64, signal_length=size, input_type='peaks', peaks_locs=locs_peaks)
        return pd.Series({key: hrv_features[key] for key in mets})
    except Exception as e:
        return None
    
def calculateResp(bvp):
    peaks=biobss.preprocess.peak_detection(bvp,64,'peakdet',delta=0.01)
    print(bvp.shape)
    peaks['Trough_locs'] = np.append(peaks['Trough_locs'], [0])
    print(peaks['Trough_locs'].shape)
    print(peaks['Peak_locs'].shape)

    info=biobss.resptools.extract_resp_sig(sig=bvp, peaks_locs=peaks['Peak_locs'][0:75246], troughs_locs=peaks['Trough_locs'][0:75246], sampling_rate=64, mod_type=['AM','FM','BW'], resampling_rate=10)

    y_am=info['am_y']
    x_am=info['am_x']

    y_fm=info['fm_y']
    x_fm=info['fm_x']

    y_bw=info['bw_y']
    x_bw=info['bw_x']

    print(info)

def getMetrics(bvp_df, window_size, metrics=('hrv_mean_hr', 'hrv_min_hr', 'hrv_max_hr', 'hrv_std_hr', 'hrv_mean_nni')):
    warnings.filterwarnings('ignore')

    # Check that correct columns exist
    if not {'participant_id', 'unix_timestamp', 'bvp'}.issubset(bvp_df.columns):
        print('Error parsing:', bvp_df.columns)
        print('Skipping')
        return None

    # Set readable timestamp as index
    bvp_df['timestamp'] = pd.to_datetime(bvp_df['unix_timestamp'] * 1000)
    bvp_df = bvp_df.set_index(['timestamp'])
    bvp_df.index = pd.to_datetime(bvp_df.index)

    bvp_df = bvp_df.dropna()

    hrv_mets = (
        bvp_df.groupby('participant_id') 
        .resample(window_size, label='left', origin='start') 
        .apply(lambda x: calculateHRV(np.asarray(x['bvp']), window_size, metrics)) 
        .dropna()
        .reset_index(drop=False) 
    )

    bvp_mets = bvp_df.groupby('participant_id').resample(window_size, label='left', origin='start').agg(
                                mean_bvp=('bvp', 'mean'), 
                                min_bvp=('bvp', 'min'),
                                max_bvp=('bvp', 'max'),
                                std_bvp=('bvp', 'std')).dropna().reset_index()
    bvp_mets.insert(2, 'window_id', bvp_mets.groupby('participant_id').cumcount() + 1)
        
    bvp_windows = bvp_mets.merge(hrv_mets, on=['participant_id', 'timestamp'])

    return bvp_windows

def processBVP(folderpath, output_dir, window_size, metrics):
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

        bvp_windows = getMetrics(bvp_df, window_size, metrics)

        if bvp_windows is None:
            continue

        final_df = pd.concat([final_df, bvp_windows])
        
    # final_df.to_csv(os.path.join(output_dir, 'bvp_metrics.csv'))

        
csv_path = '/Users/maliaedmonds/Documents/SensorLab/Empatica/csv'
output_dir = '/Users/maliaedmonds/Documents/SensorLab/Empatica/metrics'
mets = ('hrv_mean_hr', 'hrv_min_hr', 'hrv_max_hr', 'hrv_std_hr', 'hrv_mean_nni')

# processBVP(csv_path, output_dir, '1min', mets)