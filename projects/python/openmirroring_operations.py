# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.credentials import TokenCredential
import requests
import json
import os
import logging
import uuid
import time


class OpenMirroringClient:
    def __init__(self, credential: TokenCredential, host: str, logger: logging.Logger):
        self.credential = credential
        self.host = self._normalize_path(host)
        self.logger = logger
        self.service_client = self._create_service_client()

    def _normalize_path(self, path: str) -> str:
        """
        Normalizes the given path by removing the 'LandingZone' segment if it ends with it.

        :param path: The original path.
        :return: The normalized path.
        """
        if path.endswith("LandingZone"):
            return path[: path.rfind("/LandingZone")]
        elif path.endswith("LandingZone/"):
            return path[: path.rfind("/LandingZone/")]
        return path

    def _create_service_client(self):
        """Creates and returns a DataLakeServiceClient."""
        try:
            return DataLakeServiceClient(
                account_url=self.host, credential=self.credential
            )
        except Exception as e:
            raise Exception(f"Failed to create DataLakeServiceClient: {e}")

    def create_table(
        self, schema_name: str = None, table_name: str = "", key_cols: list = []
    ):
        """
        Creates a folder in OneLake storage and a _metadata.json file inside it.
        This method is idempotent - if the table already exists, it will not recreate it.

        :param schema_name: Optional schema name.
        :param table_name: Name of the table.
        :param key_cols: List of key column names.
        """
        if not table_name:
            raise ValueError("table_name cannot be empty.")

        folder_path = (
            f"{schema_name}.schema/{table_name}" if schema_name else f"{table_name}"
        )

        try:
            file_system_client = self.service_client.get_file_system_client(
                file_system="LandingZone"
            )
            directory_client = file_system_client.get_directory_client(folder_path)

            metadata_file_client = directory_client.get_file_client("_metadata.json")
            if metadata_file_client.exists():
                self.logger.warning(
                    f"Table '{folder_path}' already exists with _metadata.json. Skipping creation."
                )
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

        folder_path = (
            f"{schema_name}.schema/{table_name}" if schema_name else f"{table_name}"
        )

        try:
            file_system_client = self.service_client.get_file_system_client(
                file_system="LandingZone"
            )
            directory_client = file_system_client.get_directory_client(folder_path)

            if not directory_client.exists():
                self.logger.warning(f"Folder '{folder_path}' not found.")
                return

            directory_client.delete_directory()
            self.logger.debug(f"Folder '{folder_path}' deleted successfully.")

            if remove_schema_folder and schema_name:
                schema_folder_path = f"{schema_name}.schema"
                schema_directory_client = file_system_client.get_directory_client(
                    schema_folder_path
                )
                if schema_directory_client.exists():
                    schema_directory_client.delete_directory()
                    self.logger.debug(
                        f"Schema folder '{schema_folder_path}' deleted successfully."
                    )
                else:
                    self.logger.warning(
                        f"Schema folder '{schema_folder_path}' not found."
                    )

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

        folder_path = (
            f"LandingZone/{schema_name}.schema/{table_name}"
            if schema_name
            else f"LandingZone/{table_name}"
        )

        try:
            file_system_client = self.service_client.get_file_system_client(
                file_system=folder_path
            )
            file_list = file_system_client.get_paths(recursive=False)
            parquet_files = []

            for file in file_list:
                file_name = os.path.basename(file.name)
                if (
                    not file.is_directory
                    and file_name.endswith(".parquet")
                    and not file_name.startswith("_")
                ):
                    if (
                        not file_name[:-8].isdigit() or len(file_name[:-8]) != 20
                    ):  # Exclude ".parquet"
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

        folder_path = (
            f"{schema_name}.schema/{table_name}" if schema_name else f"{table_name}"
        )

        try:
            file_system_client = self.service_client.get_file_system_client(
                file_system="LandingZone"
            )
            directory_client = file_system_client.get_directory_client(folder_path)

            if not directory_client.exists():
                raise FileNotFoundError(f"Folder '{folder_path}' not found.")

            temp_file_name = f"_temp_{uuid.uuid4()}.parquet"
            file_client = directory_client.create_file(temp_file_name)
            with open(local_file_path, "rb") as file_data:
                file_contents = file_data.read()
                file_client.append_data(
                    data=file_contents, offset=0, length=len(file_contents)
                )
                file_client.flush_data(len(file_contents))

            self.logger.debug(f"File uploaded successfully as '{temp_file_name}'.")

            for attempt in range(retry_on_conflict):
                try:
                    next_file_name = self.get_next_file_name(schema_name, table_name)
                    rename_success = self.rename_file(
                        f"LandingZone/{folder_path}", temp_file_name, next_file_name
                    )

                    if rename_success:
                        self.logger.debug(
                            f"File renamed successfully to '{next_file_name}' on attempt {attempt + 1}."
                        )
                        return
                    else:
                        if attempt < retry_on_conflict - 1:
                            self.logger.warning(
                                f"Rename attempt {attempt + 1} failed, retrying..."
                            )
                            time.sleep(0.1 * (attempt + 1))
                        continue

                except Exception as e:
                    if attempt < retry_on_conflict - 1:
                        self.logger.warning(
                            f"Error on rename attempt {attempt + 1}: {e}, retrying..."
                        )
                        time.sleep(0.1 * (attempt + 1))
                        continue
                    else:
                        try:
                            temp_file_client = directory_client.get_file_client(
                                temp_file_name
                            )
                            temp_file_client.delete_file()
                            self.logger.warning(
                                f"Cleaned up temp file '{temp_file_name}' after failed rename attempts."
                            )
                        except:
                            pass
                        raise

            try:
                temp_file_client = directory_client.get_file_client(temp_file_name)
                temp_file_client.delete_file()
                self.logger.warning(
                    f"Cleaned up temp file '{temp_file_name}' after exhausting retry attempts."
                )
            except:
                pass

            raise Exception(
                f"Failed to rename file after {retry_on_conflict} attempts. Concurrent write conflict."
            )

        except Exception as e:
            raise Exception(f"Failed to upload data file: {e}")

    def rename_file(
        self, folder_path: str, old_file_name: str, new_file_name: str
    ) -> bool:
        """
        Renames a file using the REST API.

        :param folder_path: The folder path containing the file.
        :param old_file_name: The current file name.
        :param new_file_name: The desired new file name.
        :return: True if rename was successful, False otherwise.
        """
        token = self.credential.get_token("https://storage.azure.com/.default").token
        rename_url = f"{self.host}/{folder_path}/{new_file_name}"
        source_path = f"{self.host}/{folder_path}/{old_file_name}"
        headers = {
            "Authorization": f"Bearer {token}",
            "x-ms-rename-source": source_path,
            "x-ms-version": "2020-06-12",
        }
        response = requests.put(rename_url, headers=headers)

        if response.status_code in [200, 201]:
            self.logger.debug(
                f"File renamed from {old_file_name} to {new_file_name} successfully."
            )
            return True
        else:
            self.logger.debug(
                f"Failed to rename file. Status code: {response.status_code}, Error: {response.text}"
            )
            return False

    def get_mirrored_database_status(self) -> str:
        """
        Retrieves and returns the status of the mirrored database from Monitoring/replicator.json.

        :return: JSON string of the mirrored database status.
        :raises Exception: If the status file or path does not exist.
        """
        file_system_client = self.service_client.get_file_system_client(
            file_system="Monitoring"
        )
        try:
            file_client = file_system_client.get_file_client("replicator.json")
            if not file_client.exists():
                raise Exception(
                    "No status of mirrored database has been found. Please check whether the mirrored database has been started properly."
                )

            download = file_client.download_file()
            content = download.readall()
            status_json = json.loads(content)
            return json.dumps(status_json, indent=4)

        except Exception:
            raise Exception(
                "No status of mirrored database has been found. Please check whether the mirrored database has been started properly."
            )

    def get_table_status(self, schema_name: str = None, table_name: str = None) -> str:
        """
        Retrieves and returns the status of tables from Monitoring/table.json.

        :param schema_name: Optional schema name to filter.
        :param table_name: Optional table name to filter.
        :return: JSON string of the table status.
        :raises Exception: If the status file or path does not exist.
        """
        file_system_client = self.service_client.get_file_system_client(
            file_system="Monitoring"
        )
        try:
            file_client = file_system_client.get_file_client("tables.json")
            if not file_client.exists():
                raise Exception(
                    "No status of mirrored database has been found. Please check whether the mirrored database has been started properly."
                )

            download = file_client.download_file()
            content = download.readall()
            status_json = json.loads(content)
            schema_name = schema_name or ""
            table_name = table_name or ""

            if not schema_name and not table_name:
                return json.dumps(status_json, indent=4)
            else:
                filtered_tables = [
                    t
                    for t in status_json.get("tables", [])
                    if t.get("sourceSchemaName", "") == schema_name
                    and t.get("sourceTableName", "") == table_name
                ]
                return json.dumps({"tables": filtered_tables}, indent=4)

        except Exception:
            raise Exception(
                "No status of mirrored database has been found. Please check whether the mirrored database has been started properly."
            )
