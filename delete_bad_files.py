import os
import pandas as pd
from multiprocessing import Pool

directory = 'mmsi_files'

# Function to process a single file
def process_file(file_path):
    try:
        df = pd.read_csv(file_path)

        # Remove bad data
        df = df[~((df['Latitude'] == 91.0) & (df['Longitude'] == 0.0))]

        if df['Latitude'].nunique() == 1 and df['Longitude'].nunique() == 1:
            # Remove the file if location does not change
            os.remove(file_path)
            print(f"Removed file: {file_path}")
        else:
            # Save cleaned data back to the file
            df.to_csv(file_path, index=False)
            print(f"Cleaned file: {file_path}")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

# Main function to process files in parallel
def main():
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    with Pool(os.cpu_count()-1) as executor:
        executor.map(process_file, files)

if __name__ == "__main__":
    main()
