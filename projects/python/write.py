# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import argparse
import logging
import time
from xmlrpc import client
from azure.identity import AzureCliCredential
from openmirroring_operations import OpenMirroringClient

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - [%(module)s.%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s'
)

logging.getLogger(__name__).setLevel(logging.INFO)
logging.getLogger('openmirroring_operations').setLevel(logging.INFO)

def parse_args():
    parser = argparse.ArgumentParser(description="Open Mirroring Benchmarker.")
    parser.add_argument("--landing-zone-fqdn", type=str, required=True, help="Landing Zone FQDN (e.g. 'https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/f0a2c69e-ad20-4cd1-b35b-409776de3d66/Files/LandingZone').")
    parser.add_argument("--schema-name", type=str, required=True, help="Schema name for the table (e.g. 'microsoft').")
    parser.add_argument("--table-name", type=str, required=True, help="Table name (e.g. 'source_employees').")
    parser.add_argument("--key-cols", type=str, nargs='+', required=True, help="List of key column names (e.g. 'Column1' 'Column2').")
    parser.add_argument("--local-file-path", type=str, required=True, help="Path to the local parquet file to upload (e.g. 'C:\\path\\to\\file.parquet').")
    parser.add_argument("--continuous", action="store_true", help="Keep uploading the same file continuously in a loop.")
    parser.add_argument("--interval", type=int, default=5, help="Interval in seconds between uploads when using --continuous (default: 5 seconds).")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds for continuous mode (default: 60 seconds). Use 0 for infinite duration.")
    
    return parser.parse_args()

def main():
    args = parse_args()
    logger = logging.getLogger(__name__)
    
    credential = AzureCliCredential()
    
    mirroringClient = OpenMirroringClient(
        credential=credential,
        host=args.landing_zone_fqdn,
        logger=logger
    )
    
    logger.info(f"Creating table '{args.table_name}' in schema '{args.schema_name}' with key columns: {args.key_cols}")
    mirroringClient.create_table(schema_name=args.schema_name, table_name=args.table_name, key_cols=args.key_cols)
    
    if args.continuous:
        duration_text = f"for {args.duration} seconds" if args.duration > 0 else "indefinitely"
        logger.info(f"Starting continuous upload mode with {args.interval} second intervals, running {duration_text}. Press Ctrl+C to stop.")
        
        upload_count = 0
        start_time = time.time()
        
        try:
            while True:
                if args.duration > 0 and (time.time() - start_time) >= args.duration:
                    logger.info(f"Duration of {args.duration} seconds reached. Stopping continuous upload.")
                    break
                
                upload_count += 1
                upload_start_time = time.time()
                mirroringClient.upload_data_file(schema_name=args.schema_name, table_name=args.table_name, local_file_path=args.local_file_path)
                upload_duration = time.time() - upload_start_time
                
                logger.info(f"Upload #{upload_count} completed in {upload_duration:.2f} seconds.")
                
                if args.interval > 0:
                    logger.info(f"Waiting {args.interval} seconds before next upload...")
                    time.sleep(args.interval)

        except KeyboardInterrupt:
            logger.info(f"\nContinuous upload stopped by user. Total uploads completed: {upload_count}")
        
        elapsed_time = time.time() - start_time
        logger.info(f"Continuous mode completed. Total uploads: {upload_count}, Total time: {elapsed_time:.1f} seconds")
    else:
        logger.info(f"Starting upload of file '{args.local_file_path}' to table '{args.schema_name}.{args.table_name}'")
        
        upload_start_time = time.time()
        mirroringClient.upload_data_file(schema_name=args.schema_name, table_name=args.table_name, local_file_path=args.local_file_path)
        upload_duration = time.time() - upload_start_time
        
        logger.info(f"Upload completed in {upload_duration:.2f} seconds.")

    logger.info(f"Mirrored database status: {mirroringClient.get_mirrored_database_status()}")
    logger.info(f"All table status: {mirroringClient.get_table_status()}")
    logger.info(f"Status for table '{args.schema_name}.{args.table_name}': {mirroringClient.get_table_status(schema_name=args.schema_name, table_name=args.table_name)}")
    

if __name__ == "__main__":
    main()