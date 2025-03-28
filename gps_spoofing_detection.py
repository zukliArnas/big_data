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

    logger.info("Starting dataset download...")
    file_path = download_the_dataset(args.url, args.output, args.force)
    if file_path is None:
        logger.error("Download failed. Exiting program.")
        sys.exit(1)
    logger.info(f"Download completed: {file_path}")


    logger.info(f"Starting extraction of: {file_path}")
    extract_folder = unzip_the_file(file_path)
    if extract_folder:
        logger.info(f"Extraction completed. Files available in: {extract_folder}")
    else:
        logger.warning("No extraction was performed.")

    logger.info("Searching for CSV file in extracted folder...")
    csv_file_path = find_csv_file(extract_folder)
    if csv_file_path is None:
        logger.error("No CSV file found. Exiting program.")
        sys.exit(1)
    logger.info(f"CSV file found: {csv_file_path}")


    logger.info("Starting data processing and anomaly detection...")
    start_processing_time = time.time()

    with Pool(processes=1) as pool:
        results = pool.apply_async(process_chunks_with_print, (csv_file_path,))
        all_anomalies = results.get()

    processing_time = time.time() - start_processing_time
    logger.info(f"Data processing completed in {processing_time:.2f} seconds.")


    # Save anomalies to a JSON file
    logger.info("Saving anomalies to JSON file...")
    output_json_file = os.path.join(extract_folder, "anomalies.json")
    save_anomalies_to_json(all_anomalies, output_json_file)
    logger.info(f"Anomalies saved to: {output_json_file}")

    logger.info(">>> Script execution completed.\n")

    with open(output_json_file, "w") as f:
        json.dump(all_anomalies, f, indent=4)


if __name__ == "__main__":
    start_time = time.time()
    main()
    total_time = time.time() - start_time
    logger.info(f"Total execution time: {total_time:.2f} seconds.")
    print(f"Total execution time: {total_time:.2f} seconds.")
