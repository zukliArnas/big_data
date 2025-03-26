import argparse
import time
import sys
import json

from multiprocessing import Pool

from collect_the_data import *
from gps_spoofing import process_chunks_with_print, save_anomalies_to_json
from logger_config import get_logger

logger = get_logger("task.log")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--url", type=str, required=True, 
                        help="Link from where should data come from.")
    parser.add_argument("-o", "--output", type=str, default=None, 
                        help="Output file name.")
    parser.add_argument("-l", "--log-file", default='task.log', 
                        help="Specify the file name, where logs should be kept.")
    parser.add_argument("-f", "--force", action="store_true", 
                        help="Force redownload if the file exists.")
    return parser.parse_args()

def main():
    logger.info(" >>> Running the script\n")

    args = parse_args()
    file_path = download_the_dataset(args.url, args.output, args.force)

    if file_path is None:
        logger.error("Download failed. Exiting program.")
        sys.exit(1)


    extract_folder = unzip_the_file(file_path)
    if extract_folder:
        logger.info(f"Files are available in: {extract_folder}")
    else:
        logger.warning("No extraction was performed.")

    csv_file_path = find_csv_file(extract_folder)
    if csv_file_path is None:
        logger.error("No CSV file found. Exiting program.")
        sys.exit(1)

    
    with Pool(processes=os.cpu_count()-1) as pool:
        results = pool.apply_async(process_chunks_with_print, (csv_file_path,))
        all_anomalies = results.get()

    # Save anomalies to a JSON file
    output_json_file = os.path.join(extract_folder, "anomalies.json")
    save_anomalies_to_json(all_anomalies, output_json_file)

    with open(output_json_file, "w") as f:
        json.dump(all_anomalies, f, indent=4)

    with open(output_json_file, 'r') as file:
        data = json.load(file)

# Count the number of unique MMSI elements
    num_mmsi_elements = len(data)

    print(f"Number of unique MMSI elements: {num_mmsi_elements}")


if __name__ == "__main__":
    start = time.time()
    main()
    end_time = time.time()
    print(end_time - start)
