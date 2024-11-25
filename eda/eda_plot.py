import pandas as pd
import matplotlib.pyplot as plt
from biobss.edatools.eda_features import from_signal
from biobss.preprocess.signal_filter import filter_signal

eda_data = pd.read_csv("C:/Users/q1n/Documents/Empatica/Output2/eda.csv")
eda_signal = eda_data['eda']
timestamps = eda_data['unix_timestamp']

filtered_eda = filter_signal(eda_signal, filter_type='lowpass', N=2, f_lower=0.05, f_upper=1.0, sampling_rate=4)

features = from_signal(filtered_eda, sampling_rate=4)

print(features)

plt.figure(figsize=(10, 6))
plt.plot(timestamps, eda_signal, label="Raw EDA")
plt.plot(timestamps, filtered_eda, label="Filtered EDA", linestyle="--")
plt.xlabel("Time (s)")
plt.ylabel("EDA (µS)")
plt.title("EDA Signal")
plt.legend()
plt.show()