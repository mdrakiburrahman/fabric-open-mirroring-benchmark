# Python Writer

## References

* [Python SDK of Microsoft Fabric Open Mirroring](https://github.com/microsoft/fabric-toolbox/tree/main/tools/OpenMirroringPythonSDK)

## Run in Windows

```powershell
$GIT_ROOT = git rev-parse --show-toplevel
cd "$GIT_ROOT\projects\python"

# Setup
#
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Run:

```powershell
python write.py `
  --landing-zone-fqdn "https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/f0a2c69e-ad20-4cd1-b35b-409776de3d66/Files/LandingZone" `
  --schema-name "microsoft" `
  --table-name "employees" `
  --key-cols "EmployeeID" `
  --local-file-path "C:\Users\mdrrahman\Downloads\Open Mirroring\source_employees\00000000000000000001.parquet" `
  --continuous `
  --interval 0 `
  --duration 30
```
