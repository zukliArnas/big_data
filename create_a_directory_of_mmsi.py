import pandas as pd
import os

file_path = "/Users/arnaszuklija/Desktop/data_science_2/big_data/assigment_nr1/aisdk-2024-12-28/aisdk-2024-12-28.csv"
df = pd.read_csv(file_path)

important_columns = ["# Timestamp", "MMSI", "Latitude", "Longitude", "SOG", "COG"]
filtered_df = df[important_columns]

output_file_path = 'filtered_aisdk-2024-12-28.csv'

filtered_df.to_csv(output_file_path, index=False)


filtered_file_path = '/Users/arnaszuklija/Desktop/data_science_2/big_data/assigment_nr1/filtered_aisdk-2024-12-28.csv'  # Replace with your filtered file path
df = pd.read_csv(filtered_file_path)

grouped = df.groupby('MMSI')

output_directory = 'mmsi_files'
os.makedirs(output_directory, exist_ok=True)

for mmsi, group in grouped:
    output_file_path = os.path.join(output_directory, f'mmsi_{mmsi}.csv')
    group.to_csv(output_file_path, index=False)
    print(f"Saved file for MMSI {mmsi}")
