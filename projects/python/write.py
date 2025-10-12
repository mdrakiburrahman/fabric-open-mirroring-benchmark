# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import argparse
import duckdb
import logging
import os
import pandas as pd
import tempfile
import threading
import time
import uuid

from azure.identity import AzureCliCredential
from concurrent.futures import ThreadPoolExecutor
from openmirroring_operations import OpenMirroringClient
from tabulate import tabulate

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - [%(module)s.%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s",
)

logging.getLogger(__name__).setLevel(logging.INFO)
logging.getLogger("openmirroring_operations").setLevel(logging.INFO)

upload_counter = threading.Lock()
global_upload_count = 0

upload_stats = {}
upload_stats_lock = threading.Lock()


def get_next_upload_number() -> int:
    global global_upload_count
    with upload_counter:
        global_upload_count += 1
        return global_upload_count


def record_successful_upload(writer_id: int, num_rows: int) -> None:
    """Record a successful upload with timestamp, writer_id, and num_rows."""
    global upload_stats
    timestamp = time.time()
    with upload_stats_lock:
        upload_stats[timestamp] = (writer_id, num_rows)


def calculate_metrics(start_time: float, elapsed_time: float) -> dict:
    """Calculate upload metrics and return as a dictionary."""
    global upload_stats

    metrics = {}

    if not upload_stats:
        # fmt: off
        metrics.update({
            "total_uploads_recorded": 0, 
            "total_rows_uploaded": 0, 
            "total_minutes": 0, 
            "average_rows_per_minute": 0, 
            "upload_rate_per_second": 0, 
            "total_elapsed_time_seconds": elapsed_time
        })
        # fmt: on
        return metrics

    minute_stats = {}
    total_rows = 0
    writer_breakdown = {}

    with upload_stats_lock:
        for timestamp, (writer_id, num_rows) in upload_stats.items():
            minutes_elapsed = int((timestamp - start_time) // 60)
            if minutes_elapsed not in minute_stats:
                minute_stats[minutes_elapsed] = 0
            minute_stats[minutes_elapsed] += num_rows
            total_rows += num_rows
            if writer_id not in writer_breakdown:
                writer_breakdown[writer_id] = {"uploads": 0, "rows": 0}
            writer_breakdown[writer_id]["uploads"] += 1
            writer_breakdown[writer_id]["rows"] += num_rows

    total_minutes = len(minute_stats) if minute_stats else 0
    avg_rows_per_minute = total_rows / total_minutes if total_minutes > 0 else 0
    upload_rate_per_second = global_upload_count / elapsed_time if elapsed_time > 0 else 0

    # fmt: off
    metrics.update({
        "total_uploads_recorded": len(upload_stats), 
        "total_rows_uploaded": total_rows, 
        "total_minutes": total_minutes, 
        "average_rows_per_minute": round(avg_rows_per_minute, 2), 
        "upload_rate_per_second": round(upload_rate_per_second, 2), 
        "total_elapsed_time_seconds": round(elapsed_time, 1)
    })
    # fmt: on

    for minute, rows in sorted(minute_stats.items()):
        metrics[f"minute_{minute}_rows"] = rows
    for writer_id, stats in writer_breakdown.items():
        metrics[f"writer_{writer_id}_uploads"] = stats["uploads"]
        metrics[f"writer_{writer_id}_rows"] = stats["rows"]

    return metrics


def generate_parquet_file(num_rows: int = 100) -> str:
    """
    Generate a parquet file with random employee data using DuckDB.

    :return: Path to the generated parquet file
    """
    temp_dir = tempfile.mkdtemp()
    file_guid = str(uuid.uuid4())
    parquet_path = os.path.join(temp_dir, f"{file_guid}.parquet")
    sql_query = f"""
    COPY (
        SELECT 
            NOW() AT TIME ZONE 'UTC' AS WriterTimestamp,
            'E' || LPAD(CAST((RANDOM() * 999 + 1)::INT AS VARCHAR), 3, '0') AS EmployeeID,
            CASE 
                WHEN RANDOM() < 0.25 THEN 'Redmond'
                WHEN RANDOM() < 0.50 THEN 'Seattle'
                WHEN RANDOM() < 0.75 THEN 'Bellevue'
                WHEN RANDOM() < 0.90 THEN 'Toronto'
                ELSE 'Kirkland'
            END AS EmployeeLocation,
            0 AS __rowMarker__
        FROM generate_series(1, {num_rows})
    ) TO '{parquet_path}'
    """

    duckdb.sql(sql_query)

    return parquet_path


def writer_task(
    writer_id: int,
    total_writers: int,
    mirroring_client: OpenMirroringClient,
    schema_name: str,
    table_name: str,
    num_rows: int,
    start_time: float,
    duration: int,
    interval: int,
    stop_event: threading.Event,
) -> int:
    logger = logging.getLogger(f"writer_{writer_id}")
    writer_uploads = 0

    try:
        while not stop_event.is_set():
            if duration > 0 and (time.time() - start_time) >= duration:
                break

            upload_number = get_next_upload_number()
            remaining = 0
            if duration > 0:
                elapsed = time.time() - start_time
                remaining = duration - elapsed

            parquet_file_path = generate_parquet_file(num_rows)
            try:
                upload_start_time = time.time()
                mirroring_client.upload_data_file(
                    schema_name=schema_name,
                    table_name=table_name,
                    local_file_path=parquet_file_path,
                )
                upload_duration = time.time() - upload_start_time
                writer_uploads += 1

                record_successful_upload(writer_id, num_rows)

                logger.info(f"[{writer_id}/{total_writers-1}] Upload #{upload_number} completed in {upload_duration:.2f} seconds, remaining: {remaining:.1f} seconds.")

            finally:
                try:
                    os.remove(parquet_file_path)
                    os.rmdir(os.path.dirname(parquet_file_path))
                except Exception as cleanup_error:
                    logger.warning(f"[{writer_id}/{total_writers-1}] Failed to clean up temp file {parquet_file_path}: {cleanup_error}")

            if interval > 0 and not stop_event.is_set():
                logger.info(f"[{writer_id}/{total_writers-1}] Waiting {interval} seconds before next upload")
                stop_event.wait(interval)

    except Exception as e:
        logger.error(f"[{writer_id}/{total_writers-1}] Writer error: {e}")

    return writer_uploads


def parse_args():
    parser = argparse.ArgumentParser(description="Open Mirroring Benchmarker.")
    parser.add_argument("--host-root-fqdn", type=str, required=True, help="Host Root FQDN ending with GUID (e.g. 'https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/f0a2c69e-ad20-4cd1-b35b-409776de3d66').")
    parser.add_argument("--schema-name", type=str, required=True, help="Schema name for the table (e.g. 'microsoft').")
    parser.add_argument("--table-name", type=str, required=True, help="Table name (e.g. 'source_employees').")
    parser.add_argument("--key-cols", type=str, nargs="+", required=True, help="List of key column names (e.g. 'Column1' 'Column2').")
    parser.add_argument("--interval", type=int, default=5, help="Interval in seconds between uploads when using --continuous (default: 5 seconds).")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds for continuous mode (default: 60 seconds). Use 0 for infinite duration.")
    parser.add_argument("--concurrent-writers", type=int, default=2, help="Number of concurrent writer threads (default: 2).")
    parser.add_argument("--num-rows", type=int, default=100, help="Number of rows to generate in each parquet file (default: 100).")
    parser.add_argument("--timeout", type=int, default=60, help="Timeout in seconds for waiting for worker threads to complete (default: 60).")

    return parser.parse_args()


def main():
    global global_upload_count

    args = parse_args()
    logger = logging.getLogger(__name__)

    credential = AzureCliCredential()
    mirroringClient = OpenMirroringClient(credential=credential, host=args.host_root_fqdn, logger=logger)

    logger.info(f"Creating table '{args.table_name}' in schema '{args.schema_name}' with key columns: {args.key_cols}")
    mirroringClient.create_table(schema_name=args.schema_name, table_name=args.table_name, key_cols=args.key_cols)

    duration_text = f"for {args.duration} seconds" if args.duration > 0 else "indefinitely"
    logger.info(f"Starting concurrent upload mode with {args.concurrent_writers} writers, {args.interval} second intervals, running {duration_text}. Press Ctrl+C to stop.")

    start_time = time.time()
    stop_event = threading.Event()

    mirroring_clients = []
    for i in range(args.concurrent_writers):
        thread_logger = logging.getLogger(f"writer_{i}")
        thread_logger.setLevel(logging.INFO)
        thread_client = OpenMirroringClient(credential=credential, host=args.host_root_fqdn, logger=thread_logger)
        mirroring_clients.append(thread_client)

    try:
        with ThreadPoolExecutor(max_workers=args.concurrent_writers, thread_name_prefix="Writer") as executor:
            futures = []
            for writer_id in range(args.concurrent_writers):
                future = executor.submit(
                    writer_task,
                    writer_id=writer_id,
                    total_writers=args.concurrent_writers,
                    mirroring_client=mirroring_clients[writer_id],
                    schema_name=args.schema_name,
                    table_name=args.table_name,
                    num_rows=args.num_rows,
                    start_time=start_time,
                    duration=args.duration,
                    interval=args.interval,
                    stop_event=stop_event,
                )
                futures.append(future)

            try:
                if args.duration > 0:
                    time.sleep(args.duration)
                    stop_event.set()
                    logger.info("Duration reached, stopping all writers...")
                else:
                    while not stop_event.is_set():
                        time.sleep(1)

            except KeyboardInterrupt:
                logger.info("\nInterrupt received, stopping all writers...")
                stop_event.set()

            total_writer_uploads = 0
            for i, future in enumerate(futures):
                try:
                    writer_uploads = future.result(timeout=args.timeout)
                    total_writer_uploads += writer_uploads
                    logger.info(f"Writer {i} completed {writer_uploads} uploads")
                except Exception as e:
                    logger.error(f"Writer {i} failed: {e}")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        stop_event.set()

    elapsed_time = time.time() - start_time

    metrics = calculate_metrics(start_time, elapsed_time)
    # fmt: off
    metrics.update({
        "all_table_status": str(mirroringClient.get_table_status()), 
        "concurrent_writers": args.concurrent_writers, 
        "duration_seconds": args.duration, 
        "interval_seconds": args.interval, 
        "mirrored_database_status": str(mirroringClient.get_mirrored_database_status()), 
        "num_rows_per_upload": args.num_rows,
        "schema_name": args.schema_name, 
        "table_name": args.table_name, 
        "specific_table_status": str(mirroringClient.get_table_status(schema_name=args.schema_name, table_name=args.table_name)), 
    })
    metrics_data = {"metric_key": list(metrics.keys()), "metric_value": list(metrics.values())}
    # fmt: on

    df = pd.DataFrame(metrics_data)

    logger.info(f"Upload benchmarking completed for table: {args.schema_name}.{args.table_name}")
    logger.info(f"\n{tabulate(df, headers='keys', tablefmt='grid', showindex=False)}")


if __name__ == "__main__":
    main()
