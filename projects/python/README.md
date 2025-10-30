# Python Writer

## References

* [Python SDK of Microsoft Fabric Open Mirroring](https://github.com/microsoft/fabric-toolbox/tree/main/tools/OpenMirroringPythonSDK)

## Run in Windows

Setup:

```powershell
$GIT_ROOT = git rev-parse --show-toplevel
cd "$GIT_ROOT\projects\python"

python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Format:

```powershell
python -m black --line-length 2000 .
```

Run:

```powershell
$PARUQET_GENERATOR_QUERY = @"
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
"@

python write.py `
  --host-root-fqdn "https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/24bc8931-db5e-4a59-afd9-74e8182b454b" `
  --schema-name "contoso" `
  --table-name "employees" `
  --key-cols "EmployeeID" `
  --interval 5 `
  --duration 30000 `
  --concurrent-writers 1 `
  --num-rows 625000 `
  --timeout 60 `
  --custom-sql $PARUQET_GENERATOR_QUERY
```

Get metrics:

```powershell
python metric.py `
  --host-root-fqdn "https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/24bc8931-db5e-4a59-afd9-74e8182b454b" `
  --schema-name "contoso" `
  --table-name "employees"

python metric_monitor_launcher.py `
  --host-root-fqdn "https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/24bc8931-db5e-4a59-afd9-74e8182b454b" `
  --schema-name "contoso" `
  --table-name "employees" `
  --metrics "lag_seconds_max_timestamp_parquet_file_landing_zone_to_parquet_file_table,lag_seconds_max_timestamp_parquet_file_landing_zone_to_delta_committed_file" `
  --poll 5 `
  --port 8501
```


In SQL Endpoint, get the lag via:

```sql
SELECT MIN(DATEDIFF(SECOND, [WriterTimestamp], SYSUTCDATETIME())) AS LastWriteAgoInSeconds
FROM [open_mirroring_benchmark_1].[microsoft].[employees];
```

And in the Monitoring KQL database, get errors via:

```kql
MirroredDatabaseTableExecutionLogs
| where ItemName == 'open_mirroring_benchmark_1'
| where SourceSchemaName == 'microsoft'
| where SourceTableName == 'employees'
| order by Timestamp desc 
| extend SecondsAgo = datetime_diff('second', now(), Timestamp)
| project Timestamp, SecondsAgo, OperationName, OperationStartTime, OperationEndTime, ProcessedRows, ProcessedBytes, ReplicatorBatchLatency, ErrorType, ErrorMessage
| take 100
```