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

Create the mirrored database via [REST](https://learn.microsoft.com/en-us/fabric/mirroring/mirrored-database-rest-api#create-mirrored-database):

```powershell
$workspaceId = "b6d561c2-5df2-4161-90ef-2b1532ab6642"
$databaseName = "openmirroring_python_test_2"
$schemaName = "dbo"
$tableName = "employees_no_marker_no_keys_1"

$token = az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv

$mirroringJson = @{
  properties = @{
    source = @{
      type = "GenericMirror"
      typeProperties = $null
    }
    target = @{
      type = "MountedRelationalDatabase"
      typeProperties = @{
        format = "Delta"
        defaultSchema = $schemaName
        compactionFrequency = 0
      }
    }
  }
} | ConvertTo-Json -Depth 10 -Compress

$mirroringJsonBase64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($mirroringJson))

$body = @{
  displayName = $databaseName
  description = "A mirrored database description"
  definition = @{
    parts = @(
      @{
        path = "mirroring.json"
        payload = $mirroringJsonBase64
        payloadType = "InlineBase64"
      }
    )
  }
} | ConvertTo-Json -Depth 10

$uri = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/mirroredDatabases"
$headers = @{
  "Authorization" = "Bearer $token"
  "Content-Type" = "application/json"
}

$response = Invoke-RestMethod -Uri $uri -Method Post -Headers $headers -Body $body
$mirroredDatabaseId = $response.id
Write-Host "Created mirrored database with ID: $mirroredDatabaseId"

$startMirroringUri = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/mirroredDatabases/$mirroredDatabaseId/startMirroring"
Invoke-RestMethod -Uri $startMirroringUri -Method Post -Headers $headers
Write-Host "Mirroring started for database: $databaseName"
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
        END AS EmployeeLocation
    FROM generate_series(1, {num_rows})
) TO '{parquet_path}'
"@

python write.py `
  --host-root-fqdn "https://msit-onelake.dfs.fabric.microsoft.com/b6d561c2-5df2-4161-90ef-2b1532ab6642/7ea9c97c-083f-4acc-980d-3bfc4a1cbf29" `
  --schema-name $schemaName `
  --table-name $tableName `
  --key-cols "" `
  --interval 60 `
  --duration 300000 `
  --concurrent-writers 16 `
  --num-rows 6250000 `
  --timeout 60 `
  --file-detection-strategy "LastUpdateTimeFileDetection" `
  --partition-json '{"YearMonthDate": "get_year_month_date()", "Region": "global"}' `
  --custom-sql $PARUQET_GENERATOR_QUERY
```

Get metrics:

```powershell
python metric.py `
  --host-root-fqdn "https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/706f0686-1cda-4069-8c18-647f530b603e" `
  --schema-name "direct_staging_off" `
  --table-name "employees_no_marker_no_keys" `
  --fabric-sql-connection-string "Driver={ODBC Driver 18 for SQL Server};Server=x6eps4xrq2xudenlfv6naeo3i4-2aarsbuljwiuzn4pf4irrh7fga.msit-datawarehouse.fabric.microsoft.com;Database=open_mirroring_benchmark;Encrypt=yes;TrustServerCertificate=no;" `
  --fabric-sql-database-name "open_mirroring_benchmark"

python metric_monitor_launcher.py `
  --host-root-fqdns "https://msit-onelake.dfs.fabric.microsoft.com/81c0bc17-7c2d-4ad4-9f00-47c7b126d80d/dbf986a3-f739-466f-a22e-c7781f768e16,https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/706f0686-1cda-4069-8c18-647f530b603e" `
  --schema-names "direct_staging_on,direct_staging_off" `
  --table-names "employees_no_marker_no_keys,employees_no_marker_no_keys" `
  --metrics "lag_seconds_delta_committed_sql_endpoint" `
  --fabric-sql-connection-string-base64s "RHJpdmVyPXtPREJDIERyaXZlciAxOCBmb3IgU1FMIFNlcnZlcn07U2VydmVyPXg2ZXBzNHhycTJ4dWRlbmxmdjZuYWVvM2k0LWM2Nm1iYWpucHRrZXZoeWFpN2QzY2p3eWJ1Lm1zaXQtZGF0YXdhcmVob3VzZS5mYWJyaWMubWljcm9zb2Z0LmNvbTtEYXRhYmFzZT1vcGVuX21pcnJvcmluZ19iZW5jaG1hcms7RW5jcnlwdD15ZXM7VHJ1c3RTZXJ2ZXJDZXJ0aWZpY2F0ZT1ubzs=,RHJpdmVyPXtPREJDIERyaXZlciAxOCBmb3IgU1FMIFNlcnZlcn07U2VydmVyPXg2ZXBzNHhycTJ4dWRlbmxmdjZuYWVvM2k0LTJhYXJzYnVsandpdXpuNHBmNGlycmg3ZmdhLm1zaXQtZGF0YXdhcmVob3VzZS5mYWJyaWMubWljcm9zb2Z0LmNvbTtEYXRhYmFzZT1vcGVuX21pcnJvcmluZ19iZW5jaG1hcms7RW5jcnlwdD15ZXM7VHJ1c3RTZXJ2ZXJDZXJ0aWZpY2F0ZT1ubzs=" `
  --fabric-sql-database-name "open_mirroring_benchmark,open_mirroring_benchmark" `
  --poll 30 `
  --port 8501
```

In SQL Endpoint, get the lag via:

```sql
SELECT MIN(DATEDIFF(SECOND, [WriterTimestamp], SYSUTCDATETIME())) AS LastWriteAgoInSeconds
FROM [open_mirroring_benchmark].[microsoft].[employees];
```

And in the Monitoring KQL database, get errors via:

```kql
MirroredDatabaseTableExecutionLogs
| where ItemName == 'open_mirroring_benchmark'
| where SourceSchemaName == 'direct_staging_on'
| where SourceTableName == 'employees'
| order by Timestamp desc 
| extend SecondsAgo = datetime_diff('second', now(), Timestamp)
| project Timestamp, SecondsAgo, OperationName, OperationStartTime, OperationEndTime, ProcessedRows, ProcessedBytes, ReplicatorBatchLatency, ErrorType, ErrorMessage
| take 100
```