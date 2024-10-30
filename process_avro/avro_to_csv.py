from avro.datafile import DataFileReader
from avro.io import DatumReader
import json
import csv
import os
import glob
## Define the location of the folder containing Avro files and the output folder.
# macOS example:
# avro_folder_path = "/Users/timmytommy/Data/Avros/"
# output_dir = "/Users/timmytommy/Data/Output/"
#
# Windows example:
# avro_folder_path = "C:/Data/Avros/"
# output_dir = "C:/Data/Output/"
def append_to_csv(filename, headers, rows, output_dir):
    """Helper function to append data to CSV file."""
    file_path = os.path.join(output_dir, filename)
    # Check if file exists to determine if headers need to be written
    write_header = not os.path.exists(file_path)
    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(headers)
        writer.writerows(rows)


def process_accelerometer(data, output_dir):
    """Process and append accelerometer data."""
    acc = data["rawData"]["accelerometer"]
    timestamp = [round(acc["timestampStart"] + i * (1e6 / acc["samplingFrequency"]))
                for i in range(len(acc["x"]))]
    delta_physical = acc["imuParams"]["physicalMax"] - acc["imuParams"]["physicalMin"]
    delta_digital = acc["imuParams"]["digitalMax"] - acc["imuParams"]["digitalMin"]
    x_g = [val * delta_physical / delta_digital for val in acc["x"]]
    y_g = [val * delta_physical / delta_digital for val in acc["y"]]
    z_g = [val * delta_physical / delta_digital for val in acc["z"]]
    append_to_csv('accelerometer.csv', ["unix_timestamp", "x", "y", "z"],
                  [[ts, x, y, z] for ts, x, y, z in zip(timestamp, x_g, y_g, z_g)], output_dir)
    
    
def process_gyroscope(data, output_dir):
    """Process and append gyroscope data."""
    gyro = data["rawData"]["gyroscope"]
    timestamp = [round(gyro["timestampStart"] + i * (1e6 / gyro["samplingFrequency"]))
                for i in range(len(gyro["x"]))]
    delta_physical = gyro["imuParams"]["physicalMax"] - gyro["imuParams"]["physicalMin"]
    delta_digital = gyro["imuParams"]["digitalMax"] - gyro["imuParams"]["digitalMin"]
    x_dps = [val * delta_physical / delta_digital for val in gyro["x"]]
    y_dps = [val * delta_physical / delta_digital for val in gyro["y"]]
    z_dps = [val * delta_physical / delta_digital for val in gyro["z"]]
    append_to_csv('gyroscope.csv', ["unix_timestamp", "x", "y", "z"],
                  [[ts, x, y, z] for ts, x, y, z in zip(timestamp, x_dps, y_dps, z_dps)], output_dir)
def process_eda(data, output_dir):
    """Process and append EDA data."""
    eda = data["rawData"]["eda"]
    timestamp = [round(eda["timestampStart"] + i * (1e6 / eda["samplingFrequency"]))
                for i in range(len(eda["values"]))]
    append_to_csv('eda.csv', ["unix_timestamp", "eda"],
                  [[ts, eda] for ts, eda in zip(timestamp, eda["values"])], output_dir)
def process_temperature(data, output_dir):
    """Process and append temperature data."""
    tmp = data["rawData"]["temperature"]
    timestamp = [round(tmp["timestampStart"] + i * (1e6 / tmp["samplingFrequency"]))
                for i in range(len(tmp["values"]))]
    append_to_csv('temperature.csv', ["unix_timestamp", "temperature"],
                  [[ts, tmp] for ts, tmp in zip(timestamp, tmp["values"])], output_dir)
def process_tags(data, output_dir):
    """Process and append tags data."""
    tags = data["rawData"]["tags"]
    append_to_csv('tags.csv', ["tags_timestamp"],
                  [[tag] for tag in tags["tagsTimeMicros"]], output_dir)
def process_bvp(data, output_dir):
    """Process and append BVP data."""
    bvp = data["rawData"]["bvp"]
    timestamp = [round(bvp["timestampStart"] + i * (1e6 / bvp["samplingFrequency"]))
                for i in range(len(bvp["values"]))]
    append_to_csv('bvp.csv', ["unix_timestamp", "bvp"],
                  [[ts, bvp] for ts, bvp in zip(timestamp, bvp["values"])], output_dir)
def process_systolic_peaks(data, output_dir):
    """Process and append systolic peaks data."""
    sps = data["rawData"]["systolicPeaks"]
    append_to_csv('systolic_peaks.csv', ["systolic_peak_timestamp"],
                  [[sp] for sp in sps["peaksTimeNanos"]], output_dir)
def process_steps(data, output_dir):
    """Process and append steps data."""
    steps = data["rawData"]["steps"]
    timestamp = [round(steps["timestampStart"] + i * (1e6 / steps["samplingFrequency"]))
                for i in range(len(steps["values"]))]
    append_to_csv('steps.csv', ["unix_timestamp", "steps"],
                  [[ts, step] for ts, step in zip(timestamp, steps["values"])], output_dir)
def process_all_sensors(data, output_dir):
    """Call all processing functions for each sensor and append to CSV."""
    process_accelerometer(data, output_dir)
    process_gyroscope(data, output_dir)
    process_eda(data, output_dir)
    process_temperature(data, output_dir)
    process_tags(data, output_dir)
    process_bvp(data, output_dir)
    process_systolic_peaks(data, output_dir)
    process_steps(data, output_dir)
def process_avro_file(avro_file_path, output_dir):
    """Process a single Avro file and append to CSV files."""
    reader = DataFileReader(open(avro_file_path, "rb"), DatumReader())
    data = next(reader)
    process_all_sensors(data, output_dir)
    reader.close()
def process_folder(folder_path, output_dir):
    """Scan the given folder and process all Avro files recursively."""
    avro_files = glob.glob(os.path.join(folder_path, '**', '*.avro'), recursive=True)
    if not avro_files:
        print("No Avro files found.")
        return
    for avro_file in avro_files:
        print(f"Processing {avro_file}...")
        process_avro_file(avro_file, output_dir)
        print(f"Finished processing {avro_file}.")
# Example usage:
# Replace the paths below with the actual folder containing Avro files and the output folder.
avro_folder_path = "C:/Users/q1n/Documents/Empatica"
output_dir = "C:/Users/q1n/Documents/Empatica/Output"
process_folder(avro_folder_path, output_dir)