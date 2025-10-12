# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import argparse
import logging
import pandas as pd
from tabulate import tabulate
import duckdb

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


def get_max_writer_timestamp(host: str, file_path: str, logger: logging.Logger):
    """
    Gets the MAX WriterTimestamp from a parquet file using DuckDB with Azure credentials.

    :param host: The Azure host URL
    :param file_path: The path to the parquet file
    :param logger: Logger instance
    :return: MAX WriterTimestamp as string, or None if error
    """
    try:
        host_parts = host.replace("https://", "").split("/")
        domain = host_parts[0]
        container_id = host_parts[1]
        lakehouse_id = host_parts[2]
        abfss_url = f"abfss://{container_id}@{domain}/{lakehouse_id}/{file_path}"

        conn = duckdb.connect()
        conn.execute("INSTALL azure")
        conn.execute("LOAD azure")
        conn.execute("SET azure_transport_option_type = 'default'")
        conn.execute(
            """
            CREATE SECRET (
                TYPE AZURE,
                PROVIDER CREDENTIAL_CHAIN,
                CHAIN 'cli',
                ACCOUNT_NAME 'msit-onelake'
            )
        """
        )
        if file_path.endswith('.zstd.parquet'):
            query = f"SELECT MAX(WriterTimestamp AT TIME ZONE 'UTC') as max_timestamp FROM parquet_scan('{abfss_url}')"
        else:
            query = f"SELECT MAX(WriterTimestamp) as max_timestamp FROM parquet_scan('{abfss_url}')"
        
        result = conn.execute(query).fetchone()

        if result and result[0]:
            return str(result[0])
        else:
            return None

    except Exception as e:
        logger.warning(f"Could not get MAX WriterTimestamp from '{file_path}': {e}")
        return None
    finally:
        if "conn" in locals():
            conn.close()


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
    landing_zone_last_modified = None
    tables_last_modified = None
    landing_zone_max_timestamp = None
    tables_max_timestamp = None

    if latest_landing_zone_file:
        try:
            landing_zone_file_path = f"LandingZone/{args.schema_name}.schema/{args.table_name}/{latest_landing_zone_file}"
            landing_zone_last_modified = mirroring_client.get_parquet_file_last_modified(landing_zone_file_path, file_system="Files")
        except Exception as e:
            logger.warning(f"Could not get LastModifiedTimestamp for LandingZone file: {e}")

    if latest_tables_file:
        try:
            tables_file_path = f"{args.schema_name}/{args.table_name}/{latest_tables_file}" if args.schema_name else f"{args.table_name}/{latest_tables_file}"
            tables_last_modified = mirroring_client.get_parquet_file_last_modified(tables_file_path, file_system="Tables")
        except Exception as e:
            logger.warning(f"Could not get LastModifiedTimestamp for Tables file: {e}")

    if latest_landing_zone_file:
        landing_zone_full_path = f"Files/LandingZone/{args.schema_name}.schema/{args.table_name}/{latest_landing_zone_file}"
        landing_zone_max_timestamp = get_max_writer_timestamp(args.host_root_fqdn, landing_zone_full_path, logger)

    if latest_tables_file:
        tables_full_path = f"Tables/{args.schema_name}/{args.table_name}/{latest_tables_file}" if args.schema_name else f"Tables/{args.table_name}/{latest_tables_file}"
        tables_max_timestamp = get_max_writer_timestamp(args.host_root_fqdn, tables_full_path, logger)

    # fmt: off
    metrics_data = {
        "metric_key": [
            "latest_parquet_file_landing_zone_size_mb", 
            "latest_parquet_file_tables_size_mb", 
            "latest_parquet_file_landing_zone_name", 
            "latest_parquet_file_tables_name", 
            "latest_parquet_file_landing_zone_last_modified", 
            "latest_parquet_file_tables_last_modified",
            "latest_parquet_file_landing_zone_max_timestamp", 
            "latest_parquet_file_tables_max_timestamp"
        ], 
        "metric_value": [
            landing_zone_size_mb, 
            tables_size_mb, 
            latest_landing_zone_file or "Not found", 
            latest_tables_file or "Not found", 
            str(landing_zone_last_modified) if landing_zone_last_modified else "Not found", 
            str(tables_last_modified) if tables_last_modified else "Not found",
            landing_zone_max_timestamp or "Not found", 
            tables_max_timestamp or "Not found"
        ]
    }
    # fmt: on

    df = pd.DataFrame(metrics_data)

    logger.info(f"Metrics collected for table: {args.schema_name}.{args.table_name}")
    logger.info(f"\n{tabulate(df, headers='keys', tablefmt='grid', showindex=False)}")


if __name__ == "__main__":
    main()
