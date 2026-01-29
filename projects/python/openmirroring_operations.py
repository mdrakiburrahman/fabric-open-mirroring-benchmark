# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import requests
import json
import os
import logging
import uuid
import re
import time
from datetime import datetime, timedelta
from enum import Enum

from azure.core.credentials import TokenCredential
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake import DeltaTable
from typing import List, Dict, Any


class FileDetectionStrategy(Enum):
    """Enum for file detection strategies in Open Mirroring."""

    SEQUENTIAL_FILE_NAME = "SequentialFileName"
    LAST_UPDATE_TIME_FILE_DETECTION = "LastUpdateTimeFileDetection"


class OpenMirroringClient:
    def __init__(self, credential: TokenCredential, host: str, logger: logging.Logger):
        self.credential = credential
        self.host = self.validate_path(host)
        self.logger = logger
        self.service_client = self._create_service_client()
        self._cached_token = None
        self._token_expires_at = None

    def validate_path(self, path: str) -> str:
        """
        Validates that the given path ends with a GUID and returns the cleaned path.

        :param path: The original path that should end with a GUID.
        :return: The validated path with trailing slashes removed.
        :raises ValueError: If the path doesn't end with a valid GUID format.
        """
        path = path.rstrip("/")
        guid_pattern = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        path_segments = path.split("/")
        last_segment = path_segments[-1] if path_segments else ""
        if not re.match(guid_pattern, last_segment.lower()):
            raise ValueError(f"Path must end with a valid GUID. Invalid path: {path}")

        return path

    def _create_service_client(self):
        """Creates and returns a DataLakeServiceClient."""
        try:
            return DataLakeServiceClient(account_url=self.host, credential=self.credential)
        except Exception as e:
            raise Exception(f"Failed to create DataLakeServiceClient: {e}")

    def get_access_token(self) -> str:
        """
        Returns a memoized access token for Azure Storage API calls.
        Automatically refreshes the token when it expires or is close to expiring.

        :return: Valid access token string.
        """
        current_time = datetime.utcnow()
        if self._cached_token is None or self._token_expires_at is None or current_time >= (self._token_expires_at - timedelta(minutes=5)):

            try:
                token_credential = self.credential.get_token("https://storage.azure.com/.default")
                self._cached_token = token_credential.token
                if hasattr(token_credential, "expires_on") and token_credential.expires_on:
                    self._token_expires_at = datetime.utcfromtimestamp(token_credential.expires_on)
                else:
                    self._token_expires_at = current_time + timedelta(hours=1)

                self.logger.debug(f"Token refreshed, expires at: {self._token_expires_at}")

            except Exception as e:
                self.logger.error(f"Failed to get access token: {e}")
                raise Exception(f"Failed to get access token: {e}")

        return self._cached_token

    def create_table(self, schema_name: str = None, table_name: str = "", key_cols: list = [], file_detection_strategy: FileDetectionStrategy = FileDetectionStrategy.SEQUENTIAL_FILE_NAME, is_partition_enabled: bool = False):
        """
        Creates a folder in OneLake storage and a _metadata.json file inside it.
        This method is idempotent - if the table already exists, it will not recreate it.

        :param schema_name: Optional schema name.
        :param table_name: Name of the table.
        :param key_cols: List of key column names.
        :param file_detection_strategy: File detection strategy.
        :param is_partition_enabled: Whether partitioning is enabled for this table.
        """
        if not table_name:
            raise ValueError("table_name cannot be empty.")

        folder_path = f"{schema_name}.schema/{table_name}" if schema_name else f"{table_name}"

        try:
            file_system_client = self.service_client.get_file_system_client(file_system="Files")
            directory_client = file_system_client.get_directory_client(f"LandingZone/{folder_path}")

            metadata_file_client = directory_client.get_file_client("_metadata.json")
            if metadata_file_client.exists():
                self.logger.warning(f"Table '{folder_path}' already exists with _metadata.json. Skipping creation.")
                return

            if not directory_client.exists():
                directory_client.create_directory()
                self.logger.debug(f"Directory created at: {folder_path}")

            metadata_content = {"keyColumns": [f"{col}" for col in key_cols]}
            if file_detection_strategy == FileDetectionStrategy.LAST_UPDATE_TIME_FILE_DETECTION:
                metadata_content["fileDetectionStrategy"] = "LastUpdateTimeFileDetection"
                metadata_content["isUpsertDefaultRowMarker"] = False
            if is_partition_enabled:
                metadata_content["isPartitionEnabled"] = "true"

            file_client = directory_client.create_file("_metadata.json")
            file_client.append_data(
                data=json.dumps(metadata_content),
                offset=0,
                length=len(json.dumps(metadata_content)),
            )
            file_client.flush_data(len(json.dumps(metadata_content)))

            self.logger.debug(f"_metadata.json created successfully at: {folder_path}")

        except Exception as e:
            raise Exception(f"Failed to create table: {e}")

    def remove_table(
        self,
        schema_name: str = None,
        table_name: str = "",
        remove_schema_folder: bool = False,
    ):
        """
        Deletes a folder in the OneLake storage.

        :param schema_name: Optional schema name.
        :param table_name: Name of the table.
        :param remove_schema_folder: If True, removes the schema folder as well.
        """
        if not table_name:
            raise ValueError("table_name cannot be empty.")

        folder_path = f"{schema_name}.schema/{table_name}" if schema_name else f"{table_name}"

        try:
            file_system_client = self.service_client.get_file_system_client(file_system="Files")
            directory_client = file_system_client.get_directory_client(f"LandingZone/{folder_path}")

            if not directory_client.exists():
                self.logger.warning(f"Folder '{folder_path}' not found.")
                return

            directory_client.delete_directory()
            self.logger.debug(f"Folder '{folder_path}' deleted successfully.")

            if remove_schema_folder and schema_name:
                schema_folder_path = f"LandingZone/{schema_name}.schema"
                schema_directory_client = file_system_client.get_directory_client(schema_folder_path)
                if schema_directory_client.exists():
                    schema_directory_client.delete_directory()
                    self.logger.debug(f"Schema folder '{schema_folder_path}' deleted successfully.")
                else:
                    self.logger.warning(f"Schema folder '{schema_folder_path}' not found.")

        except Exception as e:
            raise Exception(f"Failed to delete table: {e}")

    def get_next_file_name(self, schema_name: str = None, table_name: str = "") -> str:
        """
        Finds the next file name for a folder in OneLake storage.

        :param schema_name: Optional schema name.
        :param table_name: Name of the table.
        :return: The next file name padded to 20 digits.
        """
        if not table_name:
            raise ValueError("table_name cannot be empty.")

        folder_path = f"Files/LandingZone/{schema_name}.schema/{table_name}" if schema_name else f"Files/LandingZone/{table_name}"

        try:
            file_system_client = self.service_client.get_file_system_client(file_system=folder_path)
            file_list = file_system_client.get_paths(recursive=False)
            parquet_files = []

            for file in file_list:
                file_name = os.path.basename(file.name)
                if not file.is_directory and file_name.endswith(".parquet") and not file_name.startswith("_"):
                    if not file_name[:-8].isdigit() or len(file_name[:-8]) != 20:  # Exclude ".parquet"
                        raise ValueError(f"Invalid file name pattern: {file_name}")
                    parquet_files.append(int(file_name[:-8]))

            if parquet_files:
                next_file_number = max(parquet_files) + 1
            else:
                next_file_number = 1

            return f"{next_file_number:020}.parquet"

        except Exception as e:
            raise Exception(f"Failed to get next file name: {e}")

    def upload_data_file(
        self,
        schema_name: str = None,
        table_name: str = "",
        local_file_path: str = "",
        retry_on_conflict: int = 30,
        partition_path: str = "",
    ):
        """
        Uploads a file to OneLake storage with support for concurrent writers.

        :param schema_name: Optional schema name.
        :param table_name: Name of the table.
        :param local_file_path: Path to the local file to be uploaded.
        :param retry_on_conflict: Number of times to retry getting next file name and renaming (default: 30).
        :param partition_path: Optional partition path (e.g., "YearMonthDate=20260128/Region=eastus").
        """
        if not table_name:
            raise ValueError("table_name cannot be empty.")
        if not local_file_path or not os.path.isfile(local_file_path):
            raise ValueError("Invalid local file path.")

        folder_path = f"{schema_name}.schema/{table_name}" if schema_name else f"{table_name}"
        if partition_path:
            folder_path = f"{folder_path}/{partition_path}"

        try:
            file_system_client = self.service_client.get_file_system_client(file_system="Files")
            directory_client = file_system_client.get_directory_client(f"LandingZone/{folder_path}")

            if not directory_client.exists():
                directory_client.create_directory()
                self.logger.debug(f"Created partition directory: {folder_path}")

            temp_directory_path = f"LandingZone/{folder_path}/_Temp"
            temp_directory_client = file_system_client.get_directory_client(temp_directory_path)
            if not temp_directory_client.exists():
                temp_directory_client.create_directory()
                self.logger.debug(f"Created _Temp directory in {folder_path}")

            temp_file_name = f"_temp_{uuid.uuid4()}.parquet"
            temp_file_client = temp_directory_client.create_file(temp_file_name)
            with open(local_file_path, "rb") as file_data:
                file_contents = file_data.read()
                temp_file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
                temp_file_client.flush_data(len(file_contents))

            self.logger.debug(f"File uploaded successfully as '{temp_file_name}'.")

            for attempt in range(retry_on_conflict):
                try:
                    next_file_name = self.get_next_file_name(schema_name, table_name)
                    rename_success = self.rename_file(f"Files/{temp_directory_path}", temp_file_name, f"Files/LandingZone/{folder_path}", next_file_name)

                    if rename_success:
                        self.logger.debug(f"File renamed successfully to '{next_file_name}' on attempt {attempt + 1}.")
                        return
                    else:
                        if attempt < retry_on_conflict - 1:
                            self.logger.warning(f"Rename attempt {attempt + 1} failed, retrying...")
                            time.sleep(0.1 * (attempt + 1))
                        continue

                except Exception as e:
                    if attempt < retry_on_conflict - 1:
                        self.logger.warning(f"Error on rename attempt {attempt + 1}: {e}, retrying...")
                        time.sleep(0.1 * (attempt + 1))
                        continue
                    else:
                        try:
                            cleanup_temp_file_client = temp_directory_client.get_file_client(temp_file_name)
                            cleanup_temp_file_client.delete_file()
                            self.logger.warning(f"Cleaned up temp file '{temp_file_name}' after failed rename attempts.")
                        except:
                            pass
                        raise

            try:
                cleanup_temp_file_client = temp_directory_client.get_file_client(temp_file_name)
                cleanup_temp_file_client.delete_file()
                self.logger.warning(f"Cleaned up temp file '{temp_file_name}' after exhausting retry attempts.")
            except:
                pass

            raise Exception(f"Failed to rename file after {retry_on_conflict} attempts. Concurrent write conflict.")

        except Exception as e:
            raise Exception(f"Failed to upload data file: {e}")

    def upload_data_file_direct(
        self,
        schema_name: str = None,
        table_name: str = "",
        local_file_path: str = "",
        partition_path: str = "",
    ):
        """
        Uploads a file directly to OneLake storage with a GUID filename.
        Used for LastUpdateTimeFileDetection strategy - no _Temp folder or sequential renaming.

        :param schema_name: Optional schema name.
        :param table_name: Name of the table.
        :param local_file_path: Path to the local file to be uploaded.
        :param partition_path: Optional partition path (e.g., "YearMonthDate=20260128/Region=eastus").
        """
        if not table_name:
            raise ValueError("table_name cannot be empty.")
        if not local_file_path or not os.path.isfile(local_file_path):
            raise ValueError("Invalid local file path.")

        folder_path = f"{schema_name}.schema/{table_name}" if schema_name else f"{table_name}"
        if partition_path:
            folder_path = f"{folder_path}/{partition_path}"

        try:
            file_system_client = self.service_client.get_file_system_client(file_system="Files")
            directory_client = file_system_client.get_directory_client(f"LandingZone/{folder_path}")

            if not directory_client.exists():
                directory_client.create_directory()
                self.logger.debug(f"Created partition directory: {folder_path}")

            guid_file_name = f"{uuid.uuid4()}.parquet"
            file_client = directory_client.create_file(guid_file_name)
            with open(local_file_path, "rb") as file_data:
                file_contents = file_data.read()
                file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
                file_client.flush_data(len(file_contents))

            self.logger.debug(f"File uploaded directly as '{guid_file_name}' to {folder_path}.")

        except Exception as e:
            raise Exception(f"Failed to upload data file directly: {e}")

    def rename_file(self, source_folder_path: str, old_file_name: str, dest_folder_path: str, new_file_name: str) -> bool:
        """
        Renames/moves a file using the REST API from source folder to destination folder.

        :param source_folder_path: The source folder path containing the file.
        :param old_file_name: The current file name.
        :param dest_folder_path: The destination folder path.
        :param new_file_name: The desired new file name.
        :return: True if rename was successful, False otherwise.
        """
        token = self.get_access_token()
        rename_url = f"{self.host}/{dest_folder_path}/{new_file_name}"
        source_path = f"{self.host}/{source_folder_path}/{old_file_name}"
        headers = {
            "Authorization": f"Bearer {token}",
            "x-ms-rename-source": source_path,
            "x-ms-version": "2020-06-12",
        }
        response = requests.put(rename_url, headers=headers)

        if response.status_code in [200, 201]:
            self.logger.debug(f"File moved from {source_folder_path}/{old_file_name} to {dest_folder_path}/{new_file_name} successfully.")
            return True
        else:
            self.logger.debug(f"Failed to move file. Status code: {response.status_code}, Error: {response.text}")
            return False

    def get_mirrored_database_status(self) -> str:
        """
        Retrieves and returns the status of the mirrored database from Monitoring/replicator.json.

        :return: JSON string of the mirrored database status.
        :raises Exception: If the status file or path does not exist.
        """
        file_system_client = self.service_client.get_file_system_client(file_system="Files")
        try:
            file_client = file_system_client.get_file_client("Monitoring/replicator.json")
            if not file_client.exists():
                raise Exception("No status of mirrored database has been found. Please check whether the mirrored database has been started properly.")

            download = file_client.download_file()
            content = download.readall()
            status_json = json.loads(content)
            return json.dumps(status_json, indent=4)

        except Exception:
            raise Exception("No status of mirrored database has been found. Please check whether the mirrored database has been started properly.")

    def get_table_status(self, schema_name: str = None, table_name: str = None) -> str:
        """
        Retrieves and returns the status of tables from Monitoring/table.json.

        :param schema_name: Optional schema name to filter.
        :param table_name: Optional table name to filter.
        :return: JSON string of the table status.
        :raises Exception: If the status file or path does not exist.
        """
        file_system_client = self.service_client.get_file_system_client(file_system="Files")
        try:
            file_client = file_system_client.get_file_client("Monitoring/tables.json")
            if not file_client.exists():
                raise Exception("No status of mirrored database has been found. Please check whether the mirrored database has been started properly.")

            download = file_client.download_file()
            content = download.readall()
            status_json = json.loads(content)
            schema_name = schema_name or ""
            table_name = table_name or ""

            if not schema_name and not table_name:
                return json.dumps(status_json, indent=4)
            else:
                filtered_tables = [t for t in status_json.get("tables", []) if t.get("sourceSchemaName", "") == schema_name and t.get("sourceTableName", "") == table_name]
                return json.dumps({"tables": filtered_tables}, indent=4)

        except Exception:
            raise Exception("No status of mirrored database has been found. Please check whether the mirrored database has been started properly.")

    def get_latest_parquet_file(self, table_path: str, file_system: str = "Files") -> str:
        """
        Returns the name of the latest parquet file in the specified path based on ADLS LastModifiedTimestamp.

        :param table_path: The full path to the table directory.
        :param file_system: The file system to use (default: "Files").
        :return: The name of the latest parquet file.
        :raises Exception: If no parquet files are found or if the table path doesn't exist.
        """
        if not table_path:
            raise ValueError("table_path cannot be empty.")

        try:
            file_system_client = self.service_client.get_file_system_client(file_system=table_path)
            file_list = file_system_client.get_paths(recursive=False)
            parquet_files = []

            for file in file_list:
                file_name = os.path.basename(file.name)
                if not file.is_directory and file_name.endswith(".parquet") and not file_name.startswith("_"):
                    parquet_files.append({"name": file_name, "last_modified": file.last_modified})

            if not parquet_files:
                raise FileNotFoundError(f"No parquet files found in '{table_path}'.")

            latest_file = max(parquet_files, key=lambda x: x["last_modified"])
            self.logger.debug(f"Latest parquet file in '{table_path}': {latest_file['name']} (modified: {latest_file['last_modified']})")

            return latest_file["name"]

        except Exception as e:
            raise Exception(f"Failed to get latest parquet file: {e}")

    def get_latest_parquet_file_tables(self, schema_name: str = None, table_name: str = "") -> str:
        """
        Returns the name of the latest parquet file in the Tables folder based on ADLS LastModifiedTimestamp.

        :param schema_name: Optional schema name.
        :param table_name: Name of the table.
        :return: The name of the latest parquet file.
        :raises Exception: If no parquet files are found or if the table path doesn't exist.
        """
        if not table_name:
            raise ValueError("table_name cannot be empty.")

        table_path = f"Tables/{schema_name}/{table_name}" if schema_name else f"Tables/{table_name}"
        return self.get_latest_parquet_file(table_path=table_path)

    def get_latest_parquet_file_landing_zone(self, schema_name: str = None, table_name: str = "") -> str:
        """
        Returns the name of the latest parquet file in the LandingZone folder based on ADLS LastModifiedTimestamp.

        :param schema_name: Optional schema name.
        :param table_name: Name of the table.
        :return: The name of the latest parquet file.
        :raises Exception: If no parquet files are found or if the table path doesn't exist.
        """
        if not table_name:
            raise ValueError("table_name cannot be empty.")

        table_path = f"Files/LandingZone/{schema_name}.schema/{table_name}" if schema_name else f"Files/LandingZone/{table_name}"
        return self.get_latest_parquet_file(table_path=table_path)

    def get_parquet_file_size(self, file_path: str, file_system: str = "Files") -> int:
        """
        Returns the size of a parquet file in bytes.

        :param file_path: The full path to the parquet file (e.g., "Tables/microsoft/employees/file.parquet").
        :param file_system: The file system to use (default: "Files").
        :return: The file size in bytes as an integer.
        :raises Exception: If the file doesn't exist or cannot be accessed.
        """
        if not file_path:
            raise ValueError("file_path cannot be empty.")

        try:
            file_system_client = self.service_client.get_file_system_client(file_system=file_system)
            file_client = file_system_client.get_file_client(file_path)

            if not file_client.exists():
                raise FileNotFoundError(f"File '{file_path}' not found.")

            file_properties = file_client.get_file_properties()
            file_size = file_properties.size

            self.logger.debug(f"File '{file_path}' size: {file_size} bytes")

            return file_size

        except Exception as e:
            raise Exception(f"Failed to get file size: {e}")

    def get_parquet_file_last_modified(self, file_path: str, file_system: str = "Files"):
        """
        Returns the ADLS LastModifiedTimestamp of a parquet file.

        :param file_path: The full path to the parquet file (e.g., "Tables/microsoft/employees/file.parquet").
        :param file_system: The file system to use (default: "Files").
        :return: The LastModifiedTimestamp as a datetime object.
        :raises Exception: If the file doesn't exist or cannot be accessed.
        """
        if not file_path:
            raise ValueError("file_path cannot be empty.")

        try:
            file_system_client = self.service_client.get_file_system_client(file_system=file_system)
            file_client = file_system_client.get_file_client(file_path)

            if not file_client.exists():
                raise FileNotFoundError(f"File '{file_path}' not found.")

            file_properties = file_client.get_file_properties()
            last_modified = file_properties.last_modified

            self.logger.debug(f"File '{file_path}' last modified: {last_modified}")

            return last_modified

        except Exception as e:
            raise Exception(f"Failed to get file last modified timestamp: {e}")

    def get_delta_table_files_and_metadata(self, schema_name: str = None, table_name: str = "") -> List[Dict[str, Any]]:
        """
        Returns the Delta table files and metadata from the transaction log.

        :param schema_name: Optional schema name.
        :param table_name: Name of the table.
        :return: List of dictionaries containing file information and metadata.
        :raises Exception: If the Delta table doesn't exist or cannot be accessed.
        """
        if not table_name:
            raise ValueError("table_name cannot be empty.")

        table_path = f"Tables/{schema_name}/{table_name}" if schema_name else f"Tables/{table_name}"

        try:
            host_parts = self.host.replace("https://", "").split("/")
            domain = host_parts[0]
            container_id = host_parts[1]
            lakehouse_id = host_parts[2]

            delta_table_url = f"abfss://{container_id}@{domain}/{lakehouse_id}/{table_path}"
            self.logger.debug(f"Accessing Delta table at: {delta_table_url}")

            storage_options = {"token": self.get_access_token()}
            dt = DeltaTable(delta_table_url, storage_options=storage_options)
            files = dt.files()
            metadata = dt.metadata()
            schema = dt.schema()
            version = dt.version()
            result = []

            for file_path in files:
                file_info = {"file_path": file_path, "table_version": version, "partition_columns": metadata.partition_columns if metadata else [], "created_time": str(metadata.created_time) if metadata and metadata.created_time else None, "schema_string": str(schema) if schema else None}
                result.append(file_info)

            self.logger.debug(f"Found {len(result)} files in Delta table '{table_path}' at version {version}")

            return result

        except Exception as e:
            raise Exception(f"Failed to get Delta table files and metadata: {e}")

    def get_latest_delta_committed_file(self, schema_name: str = None, table_name: str = "") -> List[str]:
        """
        Returns the names of all committed parquet files in the Delta table with the latest created_time metadata.
        Since multiple files can have the same created_time, this returns an array of all such files.

        :param schema_name: Optional schema name.
        :param table_name: Name of the table.
        :return: List of the latest Delta committed parquet file names.
        :raises Exception: If no Delta files are found or if the table doesn't exist.
        """
        if not table_name:
            raise ValueError("table_name cannot be empty.")

        try:
            delta_files_metadata = self.get_delta_table_files_and_metadata(schema_name=schema_name, table_name=table_name)

            if not delta_files_metadata:
                raise FileNotFoundError(f"No Delta committed files found for table '{table_name}' in schema '{schema_name}'.")

            # Find the latest created_time from Delta metadata
            latest_created_time = max(delta_files_metadata, key=lambda x: x.get("created_time", "") if x.get("created_time") else "").get("created_time", "")

            if not latest_created_time:
                raise FileNotFoundError(f"Could not determine latest created_time for Delta committed files for table '{table_name}' in schema '{schema_name}'.")

            # Find all files with the latest created_time
            latest_files = [file_info.get("file_path", "") for file_info in delta_files_metadata if file_info.get("created_time", "") == latest_created_time]

            # Filter out any empty file paths
            latest_files = [file_path for file_path in latest_files if file_path]

            if not latest_files:
                raise FileNotFoundError(f"Could not determine latest Delta committed files for table '{table_name}' in schema '{schema_name}'.")

            self.logger.debug(f"Latest Delta committed files ({len(latest_files)}): {latest_files} (created: {latest_created_time})")
            return latest_files

        except Exception as e:
            raise Exception(f"Failed to get latest Delta committed files: {e}")

    def get_delta_committed_file_created_time(self, schema_name: str = None, table_name: str = ""):
        """
        Returns the created_time from Delta metadata of the latest committed Delta parquet files.

        :param schema_name: Optional schema name.
        :param table_name: Name of the table.
        :return: The created_time as a string from Delta metadata.
        :raises Exception: If the Delta table doesn't exist or cannot be accessed.
        """
        if not table_name:
            raise ValueError("table_name cannot be empty.")

        try:
            delta_files_metadata = self.get_delta_table_files_and_metadata(schema_name=schema_name, table_name=table_name)

            if not delta_files_metadata:
                raise FileNotFoundError(f"No Delta committed files found for table '{table_name}' in schema '{schema_name}'.")

            # Find the latest created_time from Delta metadata
            latest_created_time = max(delta_files_metadata, key=lambda x: x.get("created_time", "") if x.get("created_time") else "").get("created_time", None)

            if not latest_created_time:
                raise FileNotFoundError(f"Could not get created_time for latest Delta committed files for table '{table_name}' in schema '{schema_name}'.")

            self.logger.debug(f"Latest Delta committed files created_time: {latest_created_time}")
            return latest_created_time

        except Exception as e:
            raise Exception(f"Failed to get Delta committed files created_time: {e}")
