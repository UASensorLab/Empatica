import pandas as pd
import matplotlib.pyplot as plt
from biobss.edatools.eda_features import from_signal
from biobss.preprocess.signal_filter import filter_signal
from biobss.preprocess.signal_detectpeaks import peak_detection
import numpy as np

# Load the EDA data from the CSV file
eda_data = pd.read_csv("C:/Users/q1n/Documents/Empatica/Output2/eda.csv")
eda_signal = eda_data['eda']
timestamps = eda_data['unix_timestamp']

# Apply lowpass filtering to the EDA signal
filtered_eda = filter_signal(eda_signal, filter_type='lowpass', N=2, f_lower=0.05, f_upper=1.0, sampling_rate=4)

# Extract EDA features
features = from_signal(filtered_eda, sampling_rate=4)
print(features)

# Detect peaks in the filtered EDA signal
peaks_indices = peak_detection(filtered_eda, sampling_rate=4, method='scipy')

print("peak detection Results:", peaks_indices)

if isinstance(peaks_indices, dict):
    peaks_results = peaks_indices['peaks']
else:
    peaks_results = peaks_indices

peaks_results = np.array(peaks_results)

# Plot the raw and filtered EDA signals with detected peaks
plt.figure(figsize=(10, 6))
plt.plot(timestamps, eda_signal, label="Raw EDA")
plt.plot(timestamps, filtered_eda, label="Filtered EDA", linestyle="--")

valid_indices = peaks_indices[(peaks_indices >= 0) & (peaks_indices < len(timestamps))]
plt.scatter(timestamps.iloc[valid_indices], filtered_eda[valid_indices], color='r', label="Detected Peaks", marker='o')

plt.xlabel("Time (s)")
plt.ylabel("EDA (µS)")
plt.title("EDA Signal with Detected Peaks")
plt.legend()
plt.show()