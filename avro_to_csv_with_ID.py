from avro.datafile import DataFileReader
from avro.io import DatumReader
import json
import csv
import os
import glob
from datetime import datetime
import pandas as pd

PROCESSED_FILES_LOG = './processed_files3.txt'


def load_processed_files():
    """Load the list of processed files from a text file."""
    if os.path.exists(PROCESSED_FILES_LOG):
        with open(PROCESSED_FILES_LOG, 'r') as f:
            return set(line.strip() for line in f)
    return set()
def save_processed_file(avro_file_path):
    """Append a processed file to the log."""
    print(f"Saving processed file: {avro_file_path}")
    with open(PROCESSED_FILES_LOG, 'a') as f:
        f.write(avro_file_path + '\n')


def append_to_csv(filename, headers, rows, output_dir, timestamp_col='unix_timestamp'):
    """Helper function to append data to CSV file and remove duplicates based on the specific timestamp column."""
    file_path = os.path.join(output_dir, filename)

    # Convert the new rows to a DataFrame
    new_data = pd.DataFrame(rows, columns=headers)

    if os.path.exists(file_path):
        # Load existing data
        existing_data = pd.read_csv(file_path)
        # Combine existing data with the new data
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
    else:
        # If the file doesn't exist, just use the new data
        combined_data = new_data

    # Remove duplicates based on 'participant_id' and the correct timestamp column
    if 'participant_id' in combined_data.columns and timestamp_col in combined_data.columns:
        combined_data.drop_duplicates(subset=['participant_id', timestamp_col], inplace=True)

    # Save back to the CSV, overwriting the file
    combined_data.to_csv(file_path, index=False)


def extract_participant_id(avro_file_path):
    """Extract participant ID from Avro file name."""
    file_name = os.path.basename(avro_file_path)
    participant_id = file_name.split('_')[0]
    return participant_id

def process_accelerometer(data, participant_id, output_dir):
    """Process and append accelerometer data."""
    acc = data["rawData"]["accelerometer"]
    timestamp = [round(acc["timestampStart"] + i * (1e6 / acc["samplingFrequency"]))
                for i in range(len(acc["x"]))]
    delta_physical = acc["imuParams"]["physicalMax"] - acc["imuParams"]["physicalMin"]
    delta_digital = acc["imuParams"]["digitalMax"] - acc["imuParams"]["digitalMin"]
    x_g = [val * delta_physical / delta_digital for val in acc["x"]]
    y_g = [val * delta_physical / delta_digital for val in acc["y"]]
    z_g = [val * delta_physical / delta_digital for val in acc["z"]]
    append_to_csv('accelerometer.csv', ["participant_id", "unix_timestamp", "x", "y", "z"],
                  [[participant_id, ts, x, y, z] for ts, x, y, z in zip(timestamp, x_g, y_g, z_g)], output_dir)

def process_gyroscope(data, participant_id, output_dir):
    """Process and append gyroscope data."""
    gyro = data["rawData"]["gyroscope"]
    timestamp = [round(gyro["timestampStart"] + i * (1e6 / gyro["samplingFrequency"]))
                for i in range(len(gyro["x"]))]
    delta_physical = gyro["imuParams"]["physicalMax"] - gyro["imuParams"]["physicalMin"]
    delta_digital = gyro["imuParams"]["digitalMax"] - gyro["imuParams"]["digitalMin"]
    x_dps = [val * delta_physical / delta_digital for val in gyro["x"]]
    y_dps = [val * delta_physical / delta_digital for val in gyro["y"]]
    z_dps = [val * delta_physical / delta_digital for val in gyro["z"]]
    append_to_csv('gyroscope.csv', ["participant_id", "unix_timestamp", "x", "y", "z"],
                  [[participant_id, ts, x, y, z] for ts, x, y, z in zip(timestamp, x_dps, y_dps, z_dps)], output_dir)

def process_eda(data, participant_id, output_dir):
    """Process and append EDA data."""
    eda = data["rawData"]["eda"]
    timestamp = [round(eda["timestampStart"] + i * (1e6 / eda["samplingFrequency"]))
                for i in range(len(eda["values"]))]
    append_to_csv('eda.csv', ["participant_id", "unix_timestamp", "eda"],
                  [[participant_id, ts, eda_val] for ts, eda_val in zip(timestamp, eda["values"])], output_dir)

def process_temperature(data, participant_id, output_dir):
    """Process and append temperature data."""
    tmp = data["rawData"]["temperature"]
    timestamp = [round(tmp["timestampStart"] + i * (1e6 / tmp["samplingFrequency"]))
                for i in range(len(tmp["values"]))]
    append_to_csv('temperature.csv', ["participant_id", "unix_timestamp", "temperature"],
                  [[participant_id, ts, tmp_val] for ts, tmp_val in zip(timestamp, tmp["values"])], output_dir)

def process_tags(data, participant_id, output_dir):
    """Process and append tags data."""
    tags = data["rawData"]["tags"]
    append_to_csv('tags.csv', ["participant_id", "tags_timestamp"],
                  [[participant_id, tag] for tag in tags["tagsTimeMicros"]], output_dir, timestamp_col = 'tags_timestamp')

def process_bvp(data, participant_id, output_dir):
    """Process and append BVP data."""
    bvp = data["rawData"]["bvp"]
    timestamp = [round(bvp["timestampStart"] + i * (1e6 / bvp["samplingFrequency"]))
                for i in range(len(bvp["values"]))]
    append_to_csv('bvp.csv', ["participant_id", "unix_timestamp", "bvp"],
                  [[participant_id, ts, bvp_val] for ts, bvp_val in zip(timestamp, bvp["values"])], output_dir)

def process_systolic_peaks(data, participant_id, output_dir):
    """Process and append systolic peaks data."""
    sps = data["rawData"]["systolicPeaks"]
    append_to_csv('systolic_peaks.csv', ["participant_id", "systolic_peak_timestamp"],
                  [[participant_id, sp] for sp in sps["peaksTimeNanos"]], output_dir, timestamp_col= 'systolic_peak_timestamp')

def process_steps(data, participant_id, output_dir):
    """Process and append steps data."""
    steps = data["rawData"]["steps"]
    timestamp = [round(steps["timestampStart"] + i * (1e6 / steps["samplingFrequency"]))
                for i in range(len(steps["values"]))]
    append_to_csv('steps.csv', ["participant_id", "unix_timestamp", "steps"],
                  [[participant_id, ts, step_val] for ts, step_val in zip(timestamp, steps["values"])], output_dir)

def process_all_sensors(data, participant_id, output_dir):
    """Call all processing functions for each sensor and append to CSV."""
    process_accelerometer(data, participant_id, output_dir)
    process_gyroscope(data, participant_id, output_dir)
    process_eda(data, participant_id, output_dir)
    process_temperature(data, participant_id, output_dir)
    process_tags(data, participant_id, output_dir)
    process_bvp(data, participant_id, output_dir)
    process_systolic_peaks(data, participant_id, output_dir)
    process_steps(data, participant_id, output_dir)

def process_avro_file(avro_file_path, output_dir):
    """Process a single Avro file and append to CSV files."""
    reader = DataFileReader(open(avro_file_path, "rb"), DatumReader())
    data = next(reader)
    participant_id = extract_participant_id(avro_file_path)  # Extract participant ID
    process_all_sensors(data, participant_id, output_dir)
    reader.close()

def process_folder(folder_path, output_dir):
    """Scan the given folder and process all Avro files recursively."""
    avro_files = glob.glob(os.path.join(folder_path, '**', '*.avro'), recursive=True)
    processed_files = load_processed_files()

    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    if not avro_files:
        print("No Avro files found.")
        return

    for avro_file in avro_files:
        if avro_file in processed_files:
            print(f"Skipping already procedded file: {avro_file}")
            continue
            
        print(f"Processing {avro_file}...")

        process_avro_file(avro_file, output_dir)
        print(f"Finished processing {avro_file}.")

        save_processed_file(avro_file)
        print(f"Finished processing {avro_file}")

# Example usage:
# Replace the paths below with the actual folder containing Avro files and the output folder.
folder_path = "./avro"
output_dir = "./csv"
process_folder(folder_path, output_dir)