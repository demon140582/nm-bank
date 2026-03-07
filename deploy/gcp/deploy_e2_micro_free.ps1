param(
    [string]$ProjectId = "",
    [string]$Zone = "us-west1-a",
    [string]$InstanceName = "nm-bank"
)

$ErrorActionPreference = "Stop"

function Get-GcloudCmd {
    $candidates = @(
        "gcloud",
        "$env:LOCALAPPDATA\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
        "C:\Program Files\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
        "C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"
    )
    foreach ($candidate in $candidates) {
        if ($candidate -eq "gcloud") {
            $cmd = Get-Command gcloud -ErrorAction SilentlyContinue
            if ($cmd) { return $cmd.Source }
        } elseif (Test-Path $candidate) {
            return $candidate
        }
    }
    throw "gcloud not found. Install Google Cloud SDK first."
}

function Invoke-Gcloud {
    param([string[]]$Args)
    & $script:GCloudCmd @Args
}

$script:GCloudCmd = Get-GcloudCmd
Write-Host "Using gcloud: $script:GCloudCmd"

Write-Host "[1/7] Checking auth..."
$activeAccount = (Invoke-Gcloud @("auth", "list", "--filter=status:ACTIVE", "--format=value(account)")).Trim()
if (-not $activeAccount) {
    throw "No active gcloud account. Run: gcloud auth login"
}
Write-Host "Active account: $activeAccount"

if (-not $ProjectId) {
    $currentProject = (Invoke-Gcloud @("config", "get-value", "project")).Trim()
    if ($currentProject -and $currentProject -ne "(unset)") {
        $ProjectId = $currentProject
        Write-Host "Using current project: $ProjectId"
    } else {
        $ProjectId = "nm-bank-$((Get-Random -Minimum 10000000 -Maximum 99999999))"
        Write-Host "Creating project: $ProjectId"
        Invoke-Gcloud @("projects", "create", $ProjectId, "--name=NM Bank")
    }
}

Write-Host "[2/7] Setting active project..."
Invoke-Gcloud @("config", "set", "project", $ProjectId) | Out-Null

Write-Host "[3/7] Linking billing (required by GCP)..."
$billing = (Invoke-Gcloud @("billing", "accounts", "list", "--filter=open=true", "--format=value(ACCOUNT_ID)")).Trim()
if (-not $billing) {
    throw "No open billing account. Add billing in GCP Console, then rerun script."
}
Invoke-Gcloud @("billing", "projects", "link", $ProjectId, "--billing-account=$billing") | Out-Null

Write-Host "[4/7] Enabling Compute API..."
Invoke-Gcloud @("services", "enable", "compute.googleapis.com") | Out-Null

Write-Host "[5/7] Creating firewall rule..."
$ruleExists = (Invoke-Gcloud @("compute", "firewall-rules", "list", "--filter=name=nmbank-allow-5000", "--format=value(name)")).Trim()
if (-not $ruleExists) {
    Invoke-Gcloud @(
        "compute", "firewall-rules", "create", "nmbank-allow-5000",
        "--allow=tcp:5000",
        "--direction=INGRESS",
        "--source-ranges=0.0.0.0/0",
        "--target-tags=nm-bank"
    ) | Out-Null
}

Write-Host "[6/7] Creating VM if not exists..."
$existing = (Invoke-Gcloud @("compute", "instances", "list", "--filter=name=$InstanceName AND zone:($Zone)", "--format=value(name)")).Trim()
if (-not $existing) {
    Invoke-Gcloud @(
        "compute", "instances", "create", $InstanceName,
        "--zone=$Zone",
        "--machine-type=e2-micro",
        "--image-family=ubuntu-2204-lts",
        "--image-project=ubuntu-os-cloud",
        "--boot-disk-type=pd-standard",
        "--boot-disk-size=30GB",
        "--tags=nm-bank"
    ) | Out-Null
}

Write-Host "[7/7] Installing NM-Bank on VM..."
$installCmd = "curl -fsSL https://raw.githubusercontent.com/demon140582/nm-bank/main/deploy/oracle/install_free_24_7.sh -o install.sh && chmod +x install.sh && sudo bash install.sh https://github.com/demon140582/nm-bank.git"
Invoke-Gcloud @("compute", "ssh", $InstanceName, "--zone=$Zone", "--command=$installCmd") | Out-Null

$ip = (Invoke-Gcloud @("compute", "instances", "describe", $InstanceName, "--zone=$Zone", "--format=value(networkInterfaces[0].accessConfigs[0].natIP)")).Trim()
Write-Host ""
Write-Host "DONE"
Write-Host "APP:    http://$ip`:5000/"
Write-Host "HEALTH: http://$ip`:5000/healthz"
