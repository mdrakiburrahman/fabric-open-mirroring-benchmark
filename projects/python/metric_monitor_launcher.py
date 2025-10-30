# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

"""
Launcher script for the Open Mirroring Metrics Monitor Streamlit app.

Usage:
python metric_monitor_launcher.py --host-root-fqdn "https://..." --schema-name "microsoft" --table-name "employees" --poll 30 --metrics "metric1,metric2,metric3"

This script starts the Streamlit application with the provided arguments.
"""

import subprocess
import sys
import argparse
import os

def parse_args():
    parser = argparse.ArgumentParser(description="Open Mirroring Metrics Monitor Launcher")
    parser.add_argument(
        "--host-root-fqdn", 
        type=str, 
        required=True, 
        help="Host Root FQDN ending with GUID (e.g. 'https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/f0a2c69e-ad20-4cd1-b35b-409776de3d66')"
    )
    parser.add_argument(
        "--schema-name", 
        type=str, 
        required=True, 
        help="Schema name for the table (e.g. 'microsoft')"
    )
    parser.add_argument(
        "--table-name", 
        type=str, 
        required=True, 
        help="Table name (e.g. 'employees')"
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
    args = parse_args()
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    streamlit_app = os.path.join(script_dir, "metric_monitor.py")
    
    # Build the command to run Streamlit
    cmd = [
        sys.executable, "-m", "streamlit", "run", streamlit_app,
        "--server.port", str(args.port),
        "--",
        "--host-root-fqdn", args.host_root_fqdn,
        "--schema-name", args.schema_name,
        "--table-name", args.table_name,
        "--poll", str(args.poll),
        "--metrics", args.metrics
    ]
    
    print(f"Starting Streamlit app with the following configuration:")
    print(f"  Host: {args.host_root_fqdn}")
    print(f"  Schema: {args.schema_name}")
    print(f"  Table: {args.table_name}")
    print(f"  Poll Interval: {args.poll}s")
    print(f"  Metrics: {args.metrics}")
    print(f"  Port: {args.port}")
    print(f"\nRunning command: {' '.join(cmd)}")
    print(f"\nStreamlit app will be available at: http://localhost:{args.port}")
    print("Press Ctrl+C to stop the application")
    
    try:
        # Run the Streamlit app
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print("\nApplication stopped by user")
    except subprocess.CalledProcessError as e:
        print(f"Error running Streamlit app: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()