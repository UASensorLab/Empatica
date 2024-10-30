import pandas as pd
import matplotlib.pyplot as plt
from biobss.edatools.eda_features import from_signal
from biobss.preprocess.signal_filter import filter_signal
from biobss.edatools.eda_peaks import eda_detectpeaks

eda_data = pd.read_csv("C:/Users/q1n/Documents/Empatica/Output2/eda.csv")
eda_signal = eda_data['eda']
timestamps = eda_data['unix_timestamp']

filtered_eda_bandpass = filter_signal(eda_signal, filter_type='bandpass', N=2, f_lower=0.05, f_upper=0.5, sampling_rate=4)
scr_peaks = eda_detectpeaks(filtered_eda_bandpass, sampling_rate=4)

print(scr_peaks)




plt.plot(timestamps, filtered_eda_bandpass, label="Bandpass Filtered EDA", linestyle="--")
plt.show()

plt.figure(figsize=(10, 6))
plt.plot(timestamps, filtered_eda_bandpass, label="Filtered EDA")
plt.scatter(scr_peaks, filtered_eda_bandpass[scr_peaks], color='r', label="SCR Peaks")
plt.xlabel("Time (s)")
plt.ylabel("EDA (µS)")
plt.title("Filtered EDA with SCR Peaks")
plt.legend()
plt.show()