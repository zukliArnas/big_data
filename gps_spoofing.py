import json
import numpy as np
import pandas as pd


def split_csv_into_chunks(csv_file_path, num_chunks):
    df = pd.read_csv(csv_file_path)
    chunk_size = int(np.ceil(len(df) / num_chunks))
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    return chunks


def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c


def calculate_speed_vectorized(group):
    """Calculate speeds using vectorized operations."""
    lat1 = group['Latitude'].shift(1)
    lon1 = group['Longitude'].shift(1)
    lat2 = group['Latitude']
    lon2 = group['Longitude']
    time_diff = group['# Timestamp'].diff().dt.total_seconds() / 3600  # in hours

    time_diff.replace(0, np.nan, inplace=True)

    valid_idx = time_diff.notna() & lat1.notna() & lon1.notna()
    distances = np.zeros(len(group))
    distances[valid_idx] = calculate_distance(lat1[valid_idx], lon1[valid_idx], lat2[valid_idx], lon2[valid_idx])

    speeds = distances / time_diff
    speeds = speeds.fillna(0)
    return speeds


def detect_spoofing_in_chunk(chunk, speed_threshold=50.0):
    chunk = chunk.sort_values(by=['MMSI', '# Timestamp'])
    anomalies_by_mmsi = {}

    for mmsi, group in chunk.groupby('MMSI'):
        speeds = calculate_speed_vectorized(group)

        prev_lat = group['Latitude'].shift(1)
        prev_lon = group['Longitude'].shift(1)
        prev_timestamp = group['# Timestamp'].shift(1)
        
        implausible_speeds = speeds[speeds > speed_threshold]

        for idx in implausible_speeds.index:
            if speeds.loc[idx] == np.inf:
                print(f"Infinity speed detected at index {idx}, skipping this entry.")
                continue

            timestamp = group.loc[idx, '# Timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            latitude = group.loc[idx, 'Latitude']
            longitude = group.loc[idx, 'Longitude']
            speed = float(speeds.loc[idx])

            prev_time = prev_timestamp.loc[idx]
            prev_lat_val = prev_lat.loc[idx]
            prev_lon_val = prev_lon.loc[idx]

            if pd.isna(prev_time) or pd.isna(prev_lat_val) or pd.isna(prev_lon_val):
                continue

            prev_time_str = prev_time.strftime('%Y-%m-%d %H:%M:%S')
            distance = calculate_distance(prev_lat_val, prev_lon_val, latitude, longitude)

            if mmsi not in anomalies_by_mmsi:
                anomalies_by_mmsi[mmsi] = {
                    'Anomalies': []
                }

            anomalies_by_mmsi[mmsi]['Anomalies'].append({
                'Previous Timestamp': prev_time_str,
                'Current Timestamp': timestamp,
                'Previous Latitude': prev_lat_val,
                'Previous Longitude': prev_lon_val,
                'Current Latitude': latitude,
                'Current Longitude': longitude,
                'Distance (km)': distance,
                'Reason': 'Implausible speed',
                'Speed': speed
            })

            print(f"Detected anomaly for MMSI {mmsi} at {timestamp}: Speed {speed}")

    return anomalies_by_mmsi


def process_chunks_with_print(chunk):
    anomalies = []

    if chunk.empty:
        return anomalies

    chunk['# Timestamp'] = pd.to_datetime(chunk['# Timestamp'], format='%d/%m/%Y %H:%M:%S')
    chunk_anomalies = detect_spoofing_in_chunk(chunk)

    if chunk_anomalies:
        anomalies.append(chunk_anomalies)
    return anomalies


def save_anomalies_to_json(anomalies, output_file):
    with open(output_file, 'w') as f:
        json.dump(anomalies, f, indent=4)
