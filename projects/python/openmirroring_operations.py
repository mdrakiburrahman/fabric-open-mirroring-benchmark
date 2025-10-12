# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import requests
import json
import os
import logging
import uuid
import re
import time

from azure.core.credentials import TokenCredential
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake import DeltaTable
from typing import List, Dict, Any


class OpenMirroringClient:
    def __init__(self, credential: TokenCredential, host: str, logger: logging.Logger):
        self.credential = credential
        self.host = self.validate_path(host)
        self.logger = logger
        self.service_client = self._create_service_client()

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

    def create_table(self, schema_name: str = None, table_name: str = "", key_cols: list = []):
        """
        Creates a folder in OneLake storage and a _metadata.json file inside it.
        This method is idempotent - if the table already exists, it will not recreate it.

        :param schema_name: Optional schema name.
        :param table_name: Name of the table.
        :param key_cols: List of key column names.
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
    ):
        """
        Uploads a file to OneLake storage with support for concurrent writers.

        :param schema_name: Optional schema name.
        :param table_name: Name of the table.
        :param local_file_path: Path to the local file to be uploaded.
        :param retry_on_conflict: Number of times to retry getting next file name and renaming (default: 30).
        """
        if not table_name:
            raise ValueError("table_name cannot be empty.")
        if not local_file_path or not os.path.isfile(local_file_path):
            raise ValueError("Invalid local file path.")

        folder_path = f"{schema_name}.schema/{table_name}" if schema_name else f"{table_name}"

        try:
            file_system_client = self.service_client.get_file_system_client(file_system="Files")
            directory_client = file_system_client.get_directory_client(f"LandingZone/{folder_path}")

            if not directory_client.exists():
                raise FileNotFoundError(f"Folder '{folder_path}' not found.")

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

    def rename_file(self, source_folder_path: str, old_file_name: str, dest_folder_path: str, new_file_name: str) -> bool:
        """
        Renames/moves a file using the REST API from source folder to destination folder.

        :param source_folder_path: The source folder path containing the file.
        :param old_file_name: The current file name.
        :param dest_folder_path: The destination folder path.
        :param new_file_name: The desired new file name.
        :return: True if rename was successful, False otherwise.
        """
        token = self.credential.get_token("https://storage.azure.com/.default").token
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

            storage_options = {"token": self.credential.get_token("https://storage.azure.com/.default").token}
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

    def get_latest_delta_committed_file(self, schema_name: str = None, table_name: str = "") -> str:
        """
        Returns the name of the latest committed parquet file in the Delta table based on Delta created_time metadata.

        :param schema_name: Optional schema name.
        :param table_name: Name of the table.
        :return: The name of the latest Delta committed parquet file.
        :raises Exception: If no Delta files are found or if the table doesn't exist.
        """
        if not table_name:
            raise ValueError("table_name cannot be empty.")

        try:
            delta_files_metadata = self.get_delta_table_files_and_metadata(schema_name=schema_name, table_name=table_name)

            if not delta_files_metadata:
                raise FileNotFoundError(f"No Delta committed files found for table '{table_name}' in schema '{schema_name}'.")

            # Find the file with the latest created_time from Delta metadata
            latest_file = max(delta_files_metadata, key=lambda x: x.get("created_time", "") if x.get("created_time") else "")

            latest_file_path = latest_file.get("file_path", "")
            if not latest_file_path:
                raise FileNotFoundError(f"Could not determine latest Delta committed file for table '{table_name}' in schema '{schema_name}'.")

            self.logger.debug(f"Latest Delta committed file: {latest_file_path} (created: {latest_file.get('created_time', 'N/A')})")
            return latest_file_path

        except Exception as e:
            raise Exception(f"Failed to get latest Delta committed file: {e}")

    def get_delta_committed_file_created_time(self, schema_name: str = None, table_name: str = ""):
        """
        Returns the created_time from Delta metadata of the latest committed Delta parquet file.

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

            latest_file = max(delta_files_metadata, key=lambda x: x.get("created_time", "") if x.get("created_time") else "")

            created_time = latest_file.get("created_time", None)
            if not created_time:
                raise FileNotFoundError(f"Could not get created_time for latest Delta committed file for table '{table_name}' in schema '{schema_name}'.")

            self.logger.debug(f"Latest Delta committed file created_time: {created_time}")
            return created_time

        except Exception as e:
            raise Exception(f"Failed to get Delta committed file created_time: {e}")
