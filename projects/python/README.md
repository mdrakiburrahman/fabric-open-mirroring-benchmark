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
python write.py `
  --host-root-fqdn "https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/83185a57-3b3c-4802-8e19-94fc046e5d4a" `
  --schema-name "microsoft" `
  --table-name "employees" `
  --key-cols "EmployeeID" `
  --interval 0 `
  --duration 3000 `
  --concurrent-writers 16 `
  --num-rows 625000 `
  --timeout 60
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