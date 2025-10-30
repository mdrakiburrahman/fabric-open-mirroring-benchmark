# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

"""
Launcher script for the Open Mirroring Metrics Monitor Streamlit app.

Usage:
python metric_monitor_launcher.py --host-root-fqdns "https://host1/.../guid1,https://host2/.../guid2" --schema-names "microsoft,contoso" --table-names "employees,products" --poll 30 --metrics "metric1,metric2,metric3"

This script starts the Streamlit application with the provided arguments.
Host FQDNs, schema names and table names should be provided as comma-separated lists in corresponding order.
Each host FQDN corresponds to the schema-table pair at the same index position.
"""

import subprocess
import sys
import argparse
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def parse_args():
    parser = argparse.ArgumentParser(description="Open Mirroring Metrics Monitor Launcher")
    parser.add_argument(
        "--host-root-fqdns", 
        type=str, 
        required=True, 
        help="Comma-separated list of Host Root FQDNs ending with GUID, corresponding to each schema (e.g. 'https://msit-onelake.dfs.fabric.microsoft.com/.../guid1,https://msit-onelake.dfs.fabric.microsoft.com/.../guid2')"
    )
    parser.add_argument(
        "--schema-names", 
        type=str, 
        required=True, 
        help="Comma-separated list of schema names (e.g. 'microsoft,contoso')"
    )
    parser.add_argument(
        "--table-names", 
        type=str, 
        required=True, 
        help="Comma-separated list of table names, corresponding to schema names (e.g. 'employees,products')"
    )
    parser.add_argument(
        "--poll", 
        type=int, 
        default=30, 
        help="Poll interval in seconds (default: 30)"
    )
    parser.add_argument(
        "--metrics", 
        type=str, 
        default="latest_parquet_file_landing_zone_size_mb,latest_parquet_file_tables_size_mb,latest_delta_committed_file_size_mb",
        help="Comma-separated list of metrics to plot (default: size metrics)"
    )
    parser.add_argument(
        "--port", 
        type=int, 
        default=8501, 
        help="Streamlit port (default: 8501)"
    )
    
    return parser.parse_args()

def main():
    logger = logging.getLogger(__name__)
    args = parse_args()
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    streamlit_app = os.path.join(script_dir, "metric_monitor.py")
    
    # Build the command to run Streamlit
    cmd = [
        sys.executable, "-m", "streamlit", "run", streamlit_app,
        "--server.port", str(args.port),
        "--",
        "--host-root-fqdns", args.host_root_fqdns,
        "--schema-names", args.schema_names,
        "--table-names", args.table_names,
        "--poll", str(args.poll),
        "--metrics", args.metrics
    ]
    
    logger.info("Starting Streamlit app with the following configuration:")
    logger.info(f"  Hosts: {args.host_root_fqdns}")
    logger.info(f"  Schemas: {args.schema_names}")
    logger.info(f"  Tables: {args.table_names}")
    logger.info(f"  Poll Interval: {args.poll}s")
    logger.info(f"  Metrics: {args.metrics}")
    logger.info(f"  Port: {args.port}")
    logger.info(f"Running command: {' '.join(cmd)}")
    logger.info(f"Streamlit app will be available at: http://localhost:{args.port}")
    logger.info("Press Ctrl+C to stop the application")
    
    try:
        # Run the Streamlit app
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running Streamlit app: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()