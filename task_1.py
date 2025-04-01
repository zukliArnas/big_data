import os
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor

# Directory containing the MMSI files
directory = 'mmsi_files'

# Vectorized Haversine formula to calculate distance between consecutive points
def vectorized_haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    lat1, lon1, lat2, lon2 = np.radians([lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def process_chunk(chunk):
    if '# Timestamp' in chunk.columns:
        chunk.rename(columns={'# Timestamp': 'Timestamp'}, inplace=True)

    # Compute distances (skip the first row since shift() creates NaN)
    chunk['Distance'] = vectorized_haversine(
        chunk['Latitude'].shift(), chunk['Longitude'].shift(),
        chunk['Latitude'], chunk['Longitude']
    )
    chunk = chunk.dropna(subset=['Distance'])  # Remove first NaN row

    # Identify anomalies
    return chunk[chunk['Distance'] > 50]

def process_file(file_path):
    try:
        anomalies = pd.DataFrame()
        
        # Read in chunks
        chunk_size = 100  
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunk_anomalies = process_chunk(chunk)
            anomalies = pd.concat([anomalies, chunk_anomalies], ignore_index=True)

        if not anomalies.empty:
            print(f"Anomalies detected in file: {file_path}")
            print(anomalies[['Timestamp', 'Distance']])
        else:
            print(f"No anomalies detected in file: {file_path}")
            
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def main():
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]

    # Use multiprocessing efficiently
    with ProcessPoolExecutor(max_workers=7) as executor:
        executor.map(process_file, files)

if __name__ == "__main__":
    main()
