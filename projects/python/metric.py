# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import argparse
import logging
import pandas as pd
from tabulate import tabulate

from azure.identity import AzureCliCredential
from openmirroring_operations import OpenMirroringClient

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - [%(module)s.%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s",
)

logging.getLogger(__name__).setLevel(logging.INFO)
logging.getLogger("openmirroring_operations").setLevel(logging.INFO)


def bytes_to_mb(bytes_value: int) -> float:
    """
    Converts bytes to megabytes.

    :param bytes_value: Size in bytes
    :return: Size in megabytes (rounded to 2 decimal places)
    """
    return round(bytes_value / (1024 * 1024), 2)


def parse_args():
    parser = argparse.ArgumentParser(description="Open Mirroring Metrics Collector.")
    parser.add_argument("--host-root-fqdn", type=str, required=True, help="Host Root FQDN ending with GUID (e.g. 'https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/f0a2c69e-ad20-4cd1-b35b-409776de3d66').")
    parser.add_argument("--schema-name", type=str, required=True, help="Schema name for the table (e.g. 'microsoft').")
    parser.add_argument("--table-name", type=str, required=True, help="Table name (e.g. 'employees').")

    return parser.parse_args()


def main():
    args = parse_args()
    logger = logging.getLogger(__name__)

    credential = AzureCliCredential()
    mirroring_client = OpenMirroringClient(credential=credential, host=args.host_root_fqdn, logger=logger)

    logger.info(f"Collecting metrics for table '{args.table_name}' in schema '{args.schema_name}'")

    latest_landing_zone_file = None
    latest_tables_file = None
    landing_zone_size_bytes = 0
    tables_size_bytes = 0

    try:
        latest_landing_zone_file = mirroring_client.get_latest_parquet_file_landing_zone(schema_name=args.schema_name, table_name=args.table_name)
        landing_zone_file_path = f"LandingZone/{args.schema_name}.schema/{args.table_name}/{latest_landing_zone_file}"
        landing_zone_size_bytes = mirroring_client.get_parquet_file_size(landing_zone_file_path, file_system="Files")

    except Exception as e:
        logger.warning(f"Could not get latest LandingZone parquet file: {e}")

    try:
        latest_tables_file = mirroring_client.get_latest_parquet_file_tables(schema_name=args.schema_name, table_name=args.table_name)
        tables_file_path = f"{args.schema_name}/{args.table_name}/{latest_tables_file}" if args.schema_name else f"{args.table_name}/{latest_tables_file}"
        tables_size_bytes = mirroring_client.get_parquet_file_size(tables_file_path, file_system="Tables")

    except Exception as e:
        logger.warning(f"Could not get latest Tables parquet file: {e}")

    landing_zone_size_mb = bytes_to_mb(landing_zone_size_bytes)
    tables_size_mb = bytes_to_mb(tables_size_bytes)

    metrics_data = {"metric_key": ["latest_parquet_file_landing_zone_size_mb", "latest_parquet_file_tables_size_mb", "latest_parquet_file_landing_zone_name", "latest_parquet_file_tables_name"], "metric_value": [landing_zone_size_mb, tables_size_mb, latest_landing_zone_file or "Not found", latest_tables_file or "Not found"]}

    df = pd.DataFrame(metrics_data)

    logger.info(f"Metrics collected for table: {args.schema_name}.{args.table_name}")
    logger.info(f"\n{tabulate(df, headers='keys', tablefmt='grid', showindex=False)}")


if __name__ == "__main__":
    main()
