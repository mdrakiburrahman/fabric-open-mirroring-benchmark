# Python Writer

## References

* [Python SDK of Microsoft Fabric Open Mirroring](https://github.com/microsoft/fabric-toolbox/tree/main/tools/OpenMirroringPythonSDK)
* [ODBC driver installed for Fabric SQL](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16).

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
  --schema-name "direct_staging_off" `
  --table-name "employees" `
  --key-cols "EmployeeID" `
  --interval 0 `
  --duration 300000 `
  --concurrent-writers 16 `
  --num-rows 625000 `
  --timeout 60 `
  --custom-sql $PARUQET_GENERATOR_QUERY

python write.py `
  --host-root-fqdn "https://msit-onelake.dfs.fabric.microsoft.com/81c0bc17-7c2d-4ad4-9f00-47c7b126d80d/b374c99c-04df-4a57-b1ff-04174e75dbfe" `
  --schema-name "direct_staging_on" `
  --table-name "employees" `
  --key-cols "EmployeeID" `
  --interval 0 `
  --duration 300000 `
  --concurrent-writers 16 `
  --num-rows 625000 `
  --timeout 60 `
  --custom-sql $PARUQET_GENERATOR_QUERY
```

Get metrics:

```powershell
python metric.py `
  --host-root-fqdn "https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/24bc8931-db5e-4a59-afd9-74e8182b454b" `
  --schema-name "direct_staging_off" `
  --table-name "employees" `
  --fabric-sql-connection-string "Driver={ODBC Driver 18 for SQL Server};Server=x6eps4xrq2xudenlfv6naeo3i4-2aarsbuljwiuzn4pf4irrh7fga.msit-datawarehouse.fabric.microsoft.com;Database=open_mirroring_benchmark;Encrypt=yes;TrustServerCertificate=no;" `
  --fabric-sql-database-name "open_mirroring_benchmark"

python metric_monitor_launcher.py `
  --host-root-fqdns "https://msit-onelake.dfs.fabric.microsoft.com/81c0bc17-7c2d-4ad4-9f00-47c7b126d80d/b374c99c-04df-4a57-b1ff-04174e75dbfe,https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/24bc8931-db5e-4a59-afd9-74e8182b454b" `
  --schema-names "direct_staging_on,direct_staging_off" `
  --table-names "employees,employees" `
  --metrics "lag_seconds_delta_committed_sql_endpoint" `
  --fabric-sql-connection-string-base64s "RHJpdmVyPXtPREJDIERyaXZlciAxOCBmb3IgU1FMIFNlcnZlcn07U2VydmVyPXg2ZXBzNHhycTJ4dWRlbmxmdjZuYWVvM2k0LWM2Nm1iYWpucHRrZXZoeWFpN2QzY2p3eWJ1Lm1zaXQtZGF0YXdhcmVob3VzZS5mYWJyaWMubWljcm9zb2Z0LmNvbTtEYXRhYmFzZT1vcGVuX21pcnJvcmluZ19iZW5jaG1hcms7RW5jcnlwdD15ZXM7VHJ1c3RTZXJ2ZXJDZXJ0aWZpY2F0ZT1ubzs=,RHJpdmVyPXtPREJDIERyaXZlciAxOCBmb3IgU1FMIFNlcnZlcn07U2VydmVyPXg2ZXBzNHhycTJ4dWRlbmxmdjZuYWVvM2k0LTJhYXJzYnVsandpdXpuNHBmNGlycmg3ZmdhLm1zaXQtZGF0YXdhcmVob3VzZS5mYWJyaWMubWljcm9zb2Z0LmNvbTtEYXRhYmFzZT1vcGVuX21pcnJvcmluZ19iZW5jaG1hcms7RW5jcnlwdD15ZXM7VHJ1c3RTZXJ2ZXJDZXJ0aWZpY2F0ZT1ubzs=" `
  --fabric-sql-database-name "open_mirroring_benchmark,open_mirroring_benchmark" `
  --poll 30 `
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