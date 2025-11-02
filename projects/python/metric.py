# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import argparse
import logging
import pandas as pd

from azure.identity import AzureCliCredential
from openmirroring_operations import OpenMirroringClient
from metric_operations import MetricOperationsClient
from tabulate import tabulate

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - [%(module)s.%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s",
)

logging.getLogger(__name__).setLevel(logging.INFO)
logging.getLogger("openmirroring_operations").setLevel(logging.INFO)
logging.getLogger("metric_operations").setLevel(logging.INFO)


def parse_args():
    parser = argparse.ArgumentParser(description="Open Mirroring Metrics Collector.")
    parser.add_argument("--host-root-fqdn", type=str, required=True, help="Host Root FQDN ending with GUID (e.g. 'https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/f0a2c69e-ad20-4cd1-b35b-409776de3d66').")
    parser.add_argument("--schema-name", type=str, required=True, help="Schema name for the table (e.g. 'microsoft').")
    parser.add_argument("--table-name", type=str, required=True, help="Table name (e.g. 'employees').")
    parser.add_argument("--fabric-sql-connection-string", type=str, required=True, help="Fabric SQL endpoint connection string (e.g. 'Driver={ODBC Driver 18 for SQL Server};Server=your-endpoint.datawarehouse.fabric.microsoft.com;Database=your-db;Encrypt=yes;TrustServerCertificate=no;').")
    parser.add_argument("--fabric-sql-database-name", type=str, required=True, help="Fabric SQL database name (e.g. 'open_mirroring_benchmark').")

    return parser.parse_args()


def main():
    args = parse_args()
    logger = logging.getLogger(__name__)

    credential = AzureCliCredential()
    mirroring_client = OpenMirroringClient(credential=credential, host=args.host_root_fqdn, logger=logger)
    metric_client = MetricOperationsClient(mirroring_client=mirroring_client, host=args.host_root_fqdn, logger=logger, fabric_sql_connection_string=args.fabric_sql_connection_string, fabric_sql_database_name=args.fabric_sql_database_name)
    all_metrics = metric_client.get_all_metrics(schema_name=args.schema_name, table_name=args.table_name)
    metrics_data = {"metric_key": list(all_metrics.keys()), "metric_value": list(all_metrics.values())}

    df = pd.DataFrame(metrics_data)

    logger.info(f"Metrics collected for table: {args.schema_name}.{args.table_name}")
    logger.info(f"\n{tabulate(df, headers='keys', tablefmt='grid', showindex=False)}")


if __name__ == "__main__":
    main()
