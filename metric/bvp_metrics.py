import os
import glob
import pandas as pd
import sys
import biobss
import numpy as np
import traceback
import warnings

''' Compute HRV metrics using the bvp_segment '''
def calculateHRV(bvp_segment, window_size, mets):
    
    if window_size == '30s':
        size = 30
    elif window_size == '1min': 
        size = 60
    else:
        size = 30

    try:
        if bvp_segment.size == 0:
            return pd.Series({key: np.nan for key in mets})
        
        # Extract peaks
        info=biobss.preprocess.peak_detection(bvp_segment,64,'peakdet',delta=0.01)
        locs_peaks=info['Peak_locs']

        # Calculate hrv metrics for the given window
        hrv_features = biobss.hrvtools.get_hrv_features(sampling_rate=64, signal_length=size, input_type='peaks', peaks_locs=locs_peaks)

        # Return dictionary of metrics listed in 'mets'
        return pd.Series({key: hrv_features[key] for key in mets})
    
    except Exception as e:
        return None

# TAKES A LONG TIME  
''' Compute Respiratory metrics using the bvp_segment ''' 
def calculateResp(bvp):

    # Extract peaks
    peaks=biobss.preprocess.peak_detection(bvp,64,'peakdet',delta=0.01)
    peak_locs = peaks['Peak_locs']
    trough_locs = peaks['Trough_locs']

    # Correct peak_locs shape (should be one less than trough_locs length)
    if len(peak_locs) == len(trough_locs) + 1:
        peak_locs = peak_locs[1:-1]
    elif len(peak_locs) == len(trough_locs):
        if peak_locs[0] < trough_locs[0]:
            peak_locs = peak_locs[1:]
        else:
            peak_locs = peak_locs[:-1]
    elif len(peak_locs) != len(trough_locs) - 1:
        print("Error: (" + str(len(peak_locs)) +" != " + str(len(trough_locs) - 1))

    # Extract respiratory signal
    info=biobss.resptools.extract_resp_sig(sig=bvp, 
                                           peaks_locs=peak_locs, 
                                           troughs_locs=trough_locs, 
                                           sampling_rate=64, 
                                           mod_type=['AM','FM','BW'], 
                                           resampling_rate=10)
    y_am=info['am_y']
    y_fm=info['fm_y']
    y_bw=info['bw_y']

    # Calculate respiratory signal quality index
    rqi_am=biobss.resptools.calc_rqi(y_am,resampling_rate=10)
    rqi_fm=biobss.resptools.calc_rqi(y_fm,resampling_rate=10)
    rqi_bw=biobss.resptools.calc_rqi(y_bw,resampling_rate=10)

    # Estimate respiratory rate based on modulation type
    rr_am=biobss.resptools.estimate_rr(y_am,10,method='xcorr')
    rr_fm=biobss.resptools.estimate_rr(y_fm,10,method='xcorr')
    rr_bw=biobss.resptools.estimate_rr(y_bw,10,method='peakdet')

    # Return series of respiratory quality indices and estimated respiratory rates
    resp_mets = {'rqi_am': rqi_am, 'rqi_fm': rqi_fm, 'rqi_bw': rqi_bw, 'rr_am': rr_am, 'rr_fm': rr_fm, 'rr_bw': rr_bw}
    return pd.Series(resp_mets)

''' Return metrics for a given BVP dataframe '''
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
    # bvp_df = bvp_df[bvp_df.index <= "10-05-24"]

    # calculateResp(np.asarray(bvp_df['bvp']))

    # Calculate HRV metrics by windows
    hrv_mets = (
        bvp_df.groupby('participant_id') 
        .resample(window_size, label='left', origin='start') 
        .apply(lambda x: calculateHRV(np.asarray(x['bvp']), window_size, metrics)) 
        .dropna()
        .reset_index(drop=False) 
    )

    # Calculate respiratory metrics by windows
    # resp_mets = (
    #     bvp_df.groupby('participant_id') 
    #     .resample(window_size, label='left', origin='start') 
    #     .apply(lambda x: calculateResp(np.asarray(x['bvp']))) 
    #     .dropna()
    #     .reset_index(drop=False) 
    # )

    # Calculate BVP metrics by windows
    bvp_mets = bvp_df.groupby('participant_id').resample(window_size, label='left', origin='start').agg(
                                mean_bvp=('bvp', 'mean'), 
                                min_bvp=('bvp', 'min'),
                                max_bvp=('bvp', 'max'),
                                std_bvp=('bvp', 'std')).dropna().reset_index()
    bvp_mets.insert(2, 'window_id', bvp_mets.groupby('participant_id').cumcount() + 1)
    
    # Merge all metric dataframes
    bvp_windows = bvp_mets.merge(hrv_mets, on=['participant_id', 'timestamp'])
    # bvp_windows = bvp_windows.merge(resp_mets, on=['participant_id', 'timestamp'])

    return bvp_windows

''' Processes BVP data files with participant_id, unix_timestamp, and bvp data to output metrics by windows '''
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
    
    # Get metrics for each file, concatenate into final_df
    for filepath in bvp_files:
        bvp_df = pd.read_csv(filepath)

        bvp_windows = getMetrics(bvp_df, window_size, metrics)

        if bvp_windows is None:
            continue

        final_df = pd.concat([final_df, bvp_windows])
    
    print(final_df.head())
    
    # Output to CSV
    final_df.to_csv(os.path.join(output_dir, 'bvp_metrics.csv'))

        
csv_path = '/Users/maliaedmonds/Documents/SensorLab/Empatica/csv'
output_dir = '/Users/maliaedmonds/Documents/SensorLab/Empatica/metrics'
mets = ('hrv_mean_hr', 'hrv_min_hr', 'hrv_max_hr', 'hrv_std_hr', 'hrv_mean_nni')

# processBVP(csv_path, output_dir, '1min', mets)