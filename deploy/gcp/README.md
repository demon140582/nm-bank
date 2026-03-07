# NM-Bank: GCP e2-micro Free Deploy

PowerShell script for one-shot deploy to Google Cloud `e2-micro` VM:

- creates project (if needed)
- links billing
- enables Compute API
- creates firewall for port `5000`
- creates VM in free-tier settings
- installs NM-Bank as systemd service

## Run

```powershell
cd C:\Users\dimat\Documents\bank
powershell -ExecutionPolicy Bypass -File .\deploy\gcp\deploy_e2_micro_free.ps1
```

Optional params:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\gcp\deploy_e2_micro_free.ps1 `
  -ProjectId "your-project-id" `
  -Zone "us-west1-a" `
  -InstanceName "nm-bank"
```

## Prerequisites

- Google Cloud account logged in (`gcloud auth login`)
- accepted Google Cloud Terms of Service
- active billing account linked to the profile
