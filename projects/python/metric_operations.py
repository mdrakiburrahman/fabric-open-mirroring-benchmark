# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import duckdb
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from openmirroring_operations import OpenMirroringClient


class MetricOperationsClient:
    def __init__(self, mirroring_client: OpenMirroringClient, host: str, logger: logging.Logger):
        """
        Initialize the MetricOperationsClient.

        :param mirroring_client: Instance of OpenMirroringClient
        :param host: The Azure host URL
        :param logger: Logger instance
        """
        self.mirroring_client = mirroring_client
        self.host = host
        self.logger = logger

    @staticmethod
    def bytes_to_mb(bytes_value: int) -> float:
        """
        Converts bytes to megabytes.

        :param bytes_value: Size in bytes
        :return: Size in megabytes (rounded to 2 decimal places)
        """
        return round(bytes_value / (1024 * 1024), 2)

    @staticmethod
    def calculate_lag_seconds(start_timestamp, end_timestamp):
        """
        Calculates lag in seconds between two timestamps.

        :param start_timestamp: Earlier timestamp (string or datetime object)
        :param end_timestamp: Later timestamp (string or datetime object)
        :return: Lag in seconds (float), or None if calculation fails
        """
        try:
            if start_timestamp is None or end_timestamp is None:
                return None

            if isinstance(start_timestamp, str):
                if "." in start_timestamp and "+" in start_timestamp:
                    if start_timestamp.endswith("+00:00"):
                        start_dt = datetime.fromisoformat(start_timestamp.replace("+00:00", "+00:00"))
                    else:
                        start_dt = datetime.fromisoformat(start_timestamp)
                else:
                    start_dt = datetime.fromisoformat(start_timestamp)
            else:
                start_dt = start_timestamp

            if isinstance(end_timestamp, str):
                if "." in end_timestamp and "+" in end_timestamp:
                    if end_timestamp.endswith("+00:00"):
                        end_dt = datetime.fromisoformat(end_timestamp.replace("+00:00", "+00:00"))
                    else:
                        end_dt = datetime.fromisoformat(end_timestamp)
                else:
                    end_dt = datetime.fromisoformat(end_timestamp)
            else:
                end_dt = end_timestamp

            diff = (end_dt - start_dt).total_seconds()
            return round(diff, 2)

        except Exception:
            return None

    def get_max_writer_timestamp(self, file_path: str) -> Optional[str]:
        """
        Gets the MAX WriterTimestamp from a parquet file using DuckDB with Azure credentials.

        :param file_path: The path to the parquet file
        :return: MAX WriterTimestamp as string, or None if error
        """
        try:
            host_parts = self.host.replace("https://", "").split("/")
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
            if file_path.endswith(".zstd.parquet"):
                query = f"SELECT MAX(WriterTimestamp AT TIME ZONE 'UTC') as max_timestamp FROM parquet_scan('{abfss_url}')"
            else:
                query = f"SELECT MAX(WriterTimestamp) as max_timestamp FROM parquet_scan('{abfss_url}')"

            result = conn.execute(query).fetchone()

            if result and result[0]:
                return str(result[0])
            else:
                return None

        except Exception as e:
            self.logger.warning(f"Could not get MAX WriterTimestamp from '{file_path}': {e}")
            return None
        finally:
            if "conn" in locals():
                conn.close()

    def calculate_landing_zone_metrics(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Calculate metrics for landing zone files.

        :param schema_name: Schema name for the table
        :param table_name: Table name
        :return: Dictionary containing landing zone metrics
        """
        metrics = {
            "latest_parquet_file_landing_zone_name": "Not found",
            "latest_parquet_file_landing_zone_size_mb": 0,
            "latest_parquet_file_landing_zone_last_modified": "Not found",
            "latest_parquet_file_landing_zone_max_timestamp": "Not found"
        }

        try:
            latest_landing_zone_file = self.mirroring_client.get_latest_parquet_file_landing_zone(
                schema_name=schema_name, table_name=table_name
            )
            metrics["latest_parquet_file_landing_zone_name"] = latest_landing_zone_file

            landing_zone_file_path = f"LandingZone/{schema_name}.schema/{table_name}/{latest_landing_zone_file}"
            landing_zone_size_bytes = self.mirroring_client.get_parquet_file_size(
                landing_zone_file_path, file_system="Files"
            )
            metrics["latest_parquet_file_landing_zone_size_mb"] = self.bytes_to_mb(landing_zone_size_bytes)

            landing_zone_last_modified = self.mirroring_client.get_parquet_file_last_modified(
                landing_zone_file_path, file_system="Files"
            )
            metrics["latest_parquet_file_landing_zone_last_modified"] = str(landing_zone_last_modified) if landing_zone_last_modified else "Not found"

            landing_zone_full_path = f"Files/LandingZone/{schema_name}.schema/{table_name}/{latest_landing_zone_file}"
            landing_zone_max_timestamp = self.get_max_writer_timestamp(landing_zone_full_path)
            metrics["latest_parquet_file_landing_zone_max_timestamp"] = landing_zone_max_timestamp or "Not found"

        except Exception as e:
            self.logger.warning(f"Could not get latest LandingZone parquet file: {e}")

        return metrics

    def calculate_tables_metrics(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Calculate metrics for tables files.

        :param schema_name: Schema name for the table
        :param table_name: Table name
        :return: Dictionary containing tables metrics
        """
        metrics = {
            "latest_parquet_file_tables_name": "Not found",
            "latest_parquet_file_tables_size_mb": 0,
            "latest_parquet_file_tables_last_modified": "Not found",
            "latest_parquet_file_tables_max_timestamp": "Not found"
        }

        try:
            latest_tables_file = self.mirroring_client.get_latest_parquet_file_tables(
                schema_name=schema_name, table_name=table_name
            )
            metrics["latest_parquet_file_tables_name"] = latest_tables_file

            tables_file_path = f"{schema_name}/{table_name}/{latest_tables_file}" if schema_name else f"{table_name}/{latest_tables_file}"
            tables_size_bytes = self.mirroring_client.get_parquet_file_size(
                tables_file_path, file_system="Tables"
            )
            metrics["latest_parquet_file_tables_size_mb"] = self.bytes_to_mb(tables_size_bytes)

            tables_last_modified = self.mirroring_client.get_parquet_file_last_modified(
                tables_file_path, file_system="Tables"
            )
            metrics["latest_parquet_file_tables_last_modified"] = str(tables_last_modified) if tables_last_modified else "Not found"

            tables_full_path = f"Tables/{schema_name}/{table_name}/{latest_tables_file}" if schema_name else f"Tables/{table_name}/{latest_tables_file}"
            tables_max_timestamp = self.get_max_writer_timestamp(tables_full_path)
            metrics["latest_parquet_file_tables_max_timestamp"] = tables_max_timestamp or "Not found"

        except Exception as e:
            self.logger.warning(f"Could not get latest Tables parquet file: {e}")

        return metrics

    def calculate_delta_metrics(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Calculate metrics for delta files.

        :param schema_name: Schema name for the table
        :param table_name: Table name
        :return: Dictionary containing delta metrics
        """
        metrics = {
            "latest_delta_committed_file_name": "Not found",
            "latest_delta_committed_file_size_mb": 0,
            "latest_delta_committed_file_last_modified": "Not found",
            "latest_delta_committed_file_landing_zone_max_timestamp": "Not found"
        }

        try:
            latest_delta_committed_files = self.mirroring_client.get_latest_delta_committed_file(
                schema_name=schema_name, table_name=table_name
            )

            if latest_delta_committed_files:
                first_file = latest_delta_committed_files[0]
                metrics["latest_delta_committed_file_name"] = first_file

                delta_file_path = f"{schema_name}/{table_name}/{first_file}" if schema_name else f"{table_name}/{first_file}"
                latest_delta_committed_file_last_modified = self.mirroring_client.get_parquet_file_last_modified(
                    delta_file_path, file_system="Tables"
                )
                metrics["latest_delta_committed_file_last_modified"] = latest_delta_committed_file_last_modified or "Not found"

                delta_size_bytes = self.mirroring_client.get_parquet_file_size(
                    delta_file_path, file_system="Tables"
                )
                metrics["latest_delta_committed_file_size_mb"] = self.bytes_to_mb(delta_size_bytes)

                max_timestamps = []
                with ThreadPoolExecutor(max_workers=min(len(latest_delta_committed_files), 10)) as executor:
                    future_to_file = {}
                    for delta_file in latest_delta_committed_files:
                        delta_full_path = f"Tables/{schema_name}/{table_name}/{delta_file}" if schema_name else f"Tables/{table_name}/{delta_file}"
                        future = executor.submit(self.get_max_writer_timestamp, delta_full_path)
                        future_to_file[future] = delta_file

                    for future in as_completed(future_to_file):
                        delta_file = future_to_file[future]
                        try:
                            timestamp = future.result()
                            if timestamp:
                                max_timestamps.append(timestamp)
                        except Exception as exc:
                            self.logger.warning(f"Failed to get timestamp for {delta_file}: {exc}")

                if max_timestamps:
                    latest_delta_committed_file_landing_zone_max_timestamp = max(max_timestamps)
                    metrics["latest_delta_committed_file_landing_zone_max_timestamp"] = latest_delta_committed_file_landing_zone_max_timestamp
                    self.logger.info(f"Found {len(max_timestamps)} timestamps from {len(latest_delta_committed_files)} Delta files, using max: {latest_delta_committed_file_landing_zone_max_timestamp}")

        except Exception as e:
            self.logger.warning(f"Could not get Delta table metrics: {e}")

        return metrics

    def calculate_lag_metrics(self, landing_zone_metrics: Dict[str, Any], tables_metrics: Dict[str, Any], delta_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate lag metrics between different timestamp sources.

        :param landing_zone_metrics: Landing zone metrics dictionary
        :param tables_metrics: Tables metrics dictionary
        :param delta_metrics: Delta metrics dictionary
        :return: Dictionary containing lag metrics
        """
        landing_zone_last_modified = landing_zone_metrics.get("latest_parquet_file_landing_zone_last_modified")
        tables_last_modified = tables_metrics.get("latest_parquet_file_tables_last_modified")
        delta_last_modified = delta_metrics.get("latest_delta_committed_file_last_modified")
        
        landing_zone_max_timestamp = landing_zone_metrics.get("latest_parquet_file_landing_zone_max_timestamp")
        tables_max_timestamp = tables_metrics.get("latest_parquet_file_tables_max_timestamp")
        delta_max_timestamp = delta_metrics.get("latest_delta_committed_file_landing_zone_max_timestamp")

        if landing_zone_last_modified == "Not found":
            landing_zone_last_modified = None
        if tables_last_modified == "Not found":
            tables_last_modified = None
        if delta_last_modified == "Not found":
            delta_last_modified = None
        if landing_zone_max_timestamp == "Not found":
            landing_zone_max_timestamp = None
        if tables_max_timestamp == "Not found":
            tables_max_timestamp = None
        if delta_max_timestamp == "Not found":
            delta_max_timestamp = None

        lag_landing_zone_to_tables = self.calculate_lag_seconds(landing_zone_last_modified, tables_last_modified)
        lag_landing_zone_to_delta = self.calculate_lag_seconds(landing_zone_last_modified, delta_last_modified)
        lag_max_timestamp_landing_zone_to_tables = self.calculate_lag_seconds(landing_zone_max_timestamp, tables_max_timestamp)
        lag_max_timestamp_landing_zone_to_delta = self.calculate_lag_seconds(landing_zone_max_timestamp, delta_max_timestamp)

        return {
            "lag_seconds_parquet_file_landing_zone_to_parquet_file_table": abs(lag_landing_zone_to_tables) if lag_landing_zone_to_tables is not None else "Not available",
            "lag_seconds_parquet_file_landing_zone_to_delta_committed_file": abs(lag_landing_zone_to_delta) if lag_landing_zone_to_delta is not None else "Not available",
            "lag_seconds_max_timestamp_parquet_file_landing_zone_to_parquet_file_table": abs(lag_max_timestamp_landing_zone_to_tables) if lag_max_timestamp_landing_zone_to_tables is not None else "Not available",
            "lag_seconds_max_timestamp_parquet_file_landing_zone_to_delta_committed_file": abs(lag_max_timestamp_landing_zone_to_delta) if lag_max_timestamp_landing_zone_to_delta is not None else "Not available"
        }

    def get_metric(self, metric_name: str, schema_name: str, table_name: str) -> Any:
        """
        Get a specific metric by name - calculates ONLY the exact data needed for that specific metric.

        :param metric_name: Name of the metric to calculate
        :param schema_name: Schema name for the table
        :param table_name: Table name
        :return: The calculated metric value
        """
        try:
            if metric_name == "latest_parquet_file_landing_zone_size_mb":
                latest_file = self.mirroring_client.get_latest_parquet_file_landing_zone(schema_name=schema_name, table_name=table_name)
                file_path = f"LandingZone/{schema_name}.schema/{table_name}/{latest_file}"
                size_bytes = self.mirroring_client.get_parquet_file_size(file_path, file_system="Files")
                return self.bytes_to_mb(size_bytes)
                
            elif metric_name == "latest_parquet_file_landing_zone_name":
                return self.mirroring_client.get_latest_parquet_file_landing_zone(schema_name=schema_name, table_name=table_name)
                
            elif metric_name == "latest_parquet_file_landing_zone_last_modified":
                latest_file = self.mirroring_client.get_latest_parquet_file_landing_zone(schema_name=schema_name, table_name=table_name)
                file_path = f"LandingZone/{schema_name}.schema/{table_name}/{latest_file}"
                last_modified = self.mirroring_client.get_parquet_file_last_modified(file_path, file_system="Files")
                return str(last_modified) if last_modified else "Not found"
                
            elif metric_name == "latest_parquet_file_landing_zone_max_timestamp":
                latest_file = self.mirroring_client.get_latest_parquet_file_landing_zone(schema_name=schema_name, table_name=table_name)
                full_path = f"Files/LandingZone/{schema_name}.schema/{table_name}/{latest_file}"
                return self.get_max_writer_timestamp(full_path) or "Not found"
                
            elif metric_name == "latest_parquet_file_tables_size_mb":
                latest_file = self.mirroring_client.get_latest_parquet_file_tables(schema_name=schema_name, table_name=table_name)
                file_path = f"{schema_name}/{table_name}/{latest_file}" if schema_name else f"{table_name}/{latest_file}"
                size_bytes = self.mirroring_client.get_parquet_file_size(file_path, file_system="Tables")
                return self.bytes_to_mb(size_bytes)
                
            elif metric_name == "latest_parquet_file_tables_name":
                return self.mirroring_client.get_latest_parquet_file_tables(schema_name=schema_name, table_name=table_name)
                
            elif metric_name == "latest_parquet_file_tables_last_modified":
                latest_file = self.mirroring_client.get_latest_parquet_file_tables(schema_name=schema_name, table_name=table_name)
                file_path = f"{schema_name}/{table_name}/{latest_file}" if schema_name else f"{table_name}/{latest_file}"
                last_modified = self.mirroring_client.get_parquet_file_last_modified(file_path, file_system="Tables")
                return str(last_modified) if last_modified else "Not found"
                
            elif metric_name == "latest_parquet_file_tables_max_timestamp":
                latest_file = self.mirroring_client.get_latest_parquet_file_tables(schema_name=schema_name, table_name=table_name)
                full_path = f"Tables/{schema_name}/{table_name}/{latest_file}" if schema_name else f"Tables/{table_name}/{latest_file}"
                return self.get_max_writer_timestamp(full_path) or "Not found"
                
            elif metric_name == "latest_delta_committed_file_size_mb":
                latest_files = self.mirroring_client.get_latest_delta_committed_file(schema_name=schema_name, table_name=table_name)
                if latest_files:
                    first_file = latest_files[0]
                    file_path = f"{schema_name}/{table_name}/{first_file}" if schema_name else f"{table_name}/{first_file}"
                    size_bytes = self.mirroring_client.get_parquet_file_size(file_path, file_system="Tables")
                    return self.bytes_to_mb(size_bytes)
                return 0
                
            elif metric_name == "latest_delta_committed_file_name":
                latest_files = self.mirroring_client.get_latest_delta_committed_file(schema_name=schema_name, table_name=table_name)
                return latest_files[0] if latest_files else "Not found"
                
            elif metric_name == "latest_delta_committed_file_last_modified":
                latest_files = self.mirroring_client.get_latest_delta_committed_file(schema_name=schema_name, table_name=table_name)
                if latest_files:
                    first_file = latest_files[0]
                    file_path = f"{schema_name}/{table_name}/{first_file}" if schema_name else f"{table_name}/{first_file}"
                    last_modified = self.mirroring_client.get_parquet_file_last_modified(file_path, file_system="Tables")
                    return str(last_modified) if last_modified else "Not found"
                return "Not found"
                
            elif metric_name == "latest_delta_committed_file_landing_zone_max_timestamp":
                latest_files = self.mirroring_client.get_latest_delta_committed_file(schema_name=schema_name, table_name=table_name)
                if latest_files:
                    max_timestamps = []
                    with ThreadPoolExecutor(max_workers=min(len(latest_files), 10)) as executor:
                        future_to_file = {}
                        for delta_file in latest_files:
                            full_path = f"Tables/{schema_name}/{table_name}/{delta_file}" if schema_name else f"Tables/{table_name}/{delta_file}"
                            future = executor.submit(self.get_max_writer_timestamp, full_path)
                            future_to_file[future] = delta_file
                        
                        for future in as_completed(future_to_file):
                            try:
                                timestamp = future.result()
                                if timestamp:
                                    max_timestamps.append(timestamp)
                            except Exception as exc:
                                self.logger.warning(f"Failed to get timestamp: {exc}")
                    
                    return max(max_timestamps) if max_timestamps else "Not found"
                return "Not found"
                
            elif metric_name == "lag_seconds_parquet_file_landing_zone_to_parquet_file_table":
                lz_file = self.mirroring_client.get_latest_parquet_file_landing_zone(schema_name=schema_name, table_name=table_name)
                lz_path = f"LandingZone/{schema_name}.schema/{table_name}/{lz_file}"
                lz_modified = self.mirroring_client.get_parquet_file_last_modified(lz_path, file_system="Files")
                
                tables_file = self.mirroring_client.get_latest_parquet_file_tables(schema_name=schema_name, table_name=table_name)
                tables_path = f"{schema_name}/{table_name}/{tables_file}" if schema_name else f"{table_name}/{tables_file}"
                tables_modified = self.mirroring_client.get_parquet_file_last_modified(tables_path, file_system="Tables")
                
                lag = self.calculate_lag_seconds(lz_modified, tables_modified)
                return abs(lag) if lag is not None else "Not available"
                
            elif metric_name == "lag_seconds_parquet_file_landing_zone_to_delta_committed_file":
                lz_file = self.mirroring_client.get_latest_parquet_file_landing_zone(schema_name=schema_name, table_name=table_name)
                lz_path = f"LandingZone/{schema_name}.schema/{table_name}/{lz_file}"
                lz_modified = self.mirroring_client.get_parquet_file_last_modified(lz_path, file_system="Files")
                
                delta_files = self.mirroring_client.get_latest_delta_committed_file(schema_name=schema_name, table_name=table_name)
                if delta_files:
                    delta_path = f"{schema_name}/{table_name}/{delta_files[0]}" if schema_name else f"{table_name}/{delta_files[0]}"
                    delta_modified = self.mirroring_client.get_parquet_file_last_modified(delta_path, file_system="Tables")
                    lag = self.calculate_lag_seconds(lz_modified, delta_modified)
                    return abs(lag) if lag is not None else "Not available"
                return "Not available"
                
            elif metric_name == "lag_seconds_max_timestamp_parquet_file_landing_zone_to_parquet_file_table":
                lz_file = self.mirroring_client.get_latest_parquet_file_landing_zone(schema_name=schema_name, table_name=table_name)
                lz_full_path = f"Files/LandingZone/{schema_name}.schema/{table_name}/{lz_file}"
                lz_max_timestamp = self.get_max_writer_timestamp(lz_full_path)
                
                tables_file = self.mirroring_client.get_latest_parquet_file_tables(schema_name=schema_name, table_name=table_name)
                tables_full_path = f"Tables/{schema_name}/{table_name}/{tables_file}" if schema_name else f"Tables/{table_name}/{tables_file}"
                tables_max_timestamp = self.get_max_writer_timestamp(tables_full_path)
                
                lag = self.calculate_lag_seconds(lz_max_timestamp, tables_max_timestamp)
                return abs(lag) if lag is not None else "Not available"
                
            elif metric_name == "lag_seconds_max_timestamp_parquet_file_landing_zone_to_delta_committed_file":
                current_utc_timestamp = datetime.now(timezone.utc)
                
                delta_files = self.mirroring_client.get_latest_delta_committed_file(schema_name=schema_name, table_name=table_name)
                if delta_files:
                    max_timestamps = []
                    with ThreadPoolExecutor(max_workers=min(len(delta_files), 10)) as executor:
                        future_to_file = {}
                        for delta_file in delta_files:
                            delta_full_path = f"Tables/{schema_name}/{table_name}/{delta_file}" if schema_name else f"Tables/{table_name}/{delta_file}"
                            future = executor.submit(self.get_max_writer_timestamp, delta_full_path)
                            future_to_file[future] = delta_file
                        
                        for future in as_completed(future_to_file):
                            try:
                                timestamp = future.result()
                                if timestamp:
                                    max_timestamps.append(timestamp)
                            except Exception as exc:
                                self.logger.warning(f"Failed to get timestamp: {exc}")
                    
                    if max_timestamps:
                        delta_max_timestamp = max(max_timestamps)
                        lag = self.calculate_lag_seconds(current_utc_timestamp, delta_max_timestamp)
                        return abs(lag) if lag is not None else "Not available"
                
                return "Not available"
                
            else:
                raise ValueError(f"Unknown metric: {metric_name}. Available metrics: {self.get_available_metrics()}")
                
        except Exception as e:
            self.logger.error(f"Error calculating metric {metric_name}: {e}")
            return "Error"

    def get_all_metrics(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Calculate and return all available metrics.

        :param schema_name: Schema name for the table
        :param table_name: Table name
        :return: Dictionary containing all calculated metrics
        """
        self.logger.info(f"Calculating metrics for table '{table_name}' in schema '{schema_name}'")

        landing_zone_metrics = self.calculate_landing_zone_metrics(schema_name, table_name)
        tables_metrics = self.calculate_tables_metrics(schema_name, table_name)
        delta_metrics = self.calculate_delta_metrics(schema_name, table_name)
        lag_metrics = self.calculate_lag_metrics(landing_zone_metrics, tables_metrics, delta_metrics)

        all_metrics = {}
        all_metrics.update(landing_zone_metrics)
        all_metrics.update(tables_metrics)
        all_metrics.update(delta_metrics)
        all_metrics.update(lag_metrics)

        return all_metrics

    def get_available_metrics(self) -> List[str]:
        """
        Get a list of all available metric names.

        :return: List of metric names
        """
        return [
            "latest_parquet_file_landing_zone_size_mb",
            "latest_parquet_file_tables_size_mb",
            "latest_delta_committed_file_size_mb",
            "latest_parquet_file_landing_zone_name",
            "latest_parquet_file_tables_name",
            "latest_delta_committed_file_name",
            "latest_parquet_file_landing_zone_last_modified",
            "latest_parquet_file_tables_last_modified",
            "latest_delta_committed_file_last_modified",
            "latest_parquet_file_landing_zone_max_timestamp",
            "latest_parquet_file_tables_max_timestamp",
            "latest_delta_committed_file_landing_zone_max_timestamp",
            "lag_seconds_parquet_file_landing_zone_to_parquet_file_table",
            "lag_seconds_parquet_file_landing_zone_to_delta_committed_file",
            "lag_seconds_max_timestamp_parquet_file_landing_zone_to_parquet_file_table",
            "lag_seconds_max_timestamp_parquet_file_landing_zone_to_delta_committed_file"
        ]