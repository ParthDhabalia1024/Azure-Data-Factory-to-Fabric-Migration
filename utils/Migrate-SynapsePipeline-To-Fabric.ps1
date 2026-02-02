# <#
# .SYNOPSIS
#     Migrate Synapse Analytics pipelines to Microsoft Fabric.

# .DESCRIPTION
#     - Uses Azure CLI token for Synapse Dev API (required)
#     - Lists pipelines and filters by name (GET by name not supported)
#     - Converts pipeline JSON to Fabric resources
#     - Applies resolutions
#     - Exports to Fabric workspace
#     - Logs all actions

# .REQUIREMENTS
#     PowerShell 7+
#     Azure CLI (az login required)
#     Az.Accounts
#     MigrateFactoryToFabric
# #>

# param(
#     [Parameter(Mandatory = $true)]
#     [string]$SubscriptionId,

#     [Parameter(Mandatory = $true)]
#     [string]$ResourceGroupName,

#     [Parameter(Mandatory = $true)]
#     [string]$SynapseWorkspaceName,

#     [Parameter(Mandatory = $true)]
#     [string[]]$PipelineNames,

#     [Parameter(Mandatory = $true)]
#     [string]$FabricWorkspaceId,

#     [Parameter(Mandatory = $true)]
#     [string]$ResolutionsFile,

#     [string]$Region = "prod"
# )

# # --------------------------------------------------
# # Logging
# # --------------------------------------------------
# $LogFolder = Join-Path $PSScriptRoot "Logs"
# if (-not (Test-Path $LogFolder)) {
#     New-Item -ItemType Directory -Path $LogFolder | Out-Null
# }

# $LogFile = Join-Path $LogFolder ("SynapseMigration_{0}.txt" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
# "--- Starting Synapse → Fabric Migration ---" | Out-File $LogFile

# function Log {
#     param([string]$Message, [string]$Color = "Gray")
#     Write-Host $Message -ForegroundColor $Color
#     Add-Content $LogFile ("[{0}] {1}" -f (Get-Date -Format "HH:mm:ss"), $Message)
# }

# # --------------------------------------------------
# # Azure context (Az is used ONLY for Fabric)
# # --------------------------------------------------
# Log "Logging into Azure (Az)..." "Yellow"
# Add-AzAccount | Out-Null
# Select-AzSubscription -SubscriptionId $SubscriptionId | Out-Null
# Log "Active subscription set." "Green"

# # --------------------------------------------------
# # Tokens
# # --------------------------------------------------
# Log "Retrieving tokens..." "Yellow"

# try {
#     # 🔑 Synapse Dev API token (MUST come from Azure CLI)
#     $synapseToken = az account get-access-token `
#         --resource https://dev.azuresynapse.net `
#         --query accessToken -o tsv

#     if (-not $synapseToken) {
#         throw "Failed to acquire Synapse token via Azure CLI. Run 'az login'."
#     }

#     # 🔑 Fabric token (Az works fine)
#     $fabricToken = (Get-AzAccessToken `
#         -ResourceUrl "https://analysis.windows.net/powerbi/api"
#     ).Token

#     Log "Tokens acquired successfully." "Green"
# }
# catch {
#     Log "Token acquisition failed: $($_.Exception.Message)" "Red"
#     exit 1
# }

# # --------------------------------------------------
# # Validate resolutions file
# # --------------------------------------------------
# if (-not (Test-Path $ResolutionsFile)) {
#     Log "Resolutions file not found: $ResolutionsFile" "Red"
#     exit 1
# }

# # --------------------------------------------------
# # Normalize pipeline names
# # --------------------------------------------------
# if ($PipelineNames.Count -eq 1 -and $PipelineNames[0] -like "*,*") {
#     $PipelineNames = $PipelineNames[0].Split(",") | ForEach-Object { $_.Trim() }
# }

# # --------------------------------------------------
# # Synapse Dev API setup
# # --------------------------------------------------
# $headers = @{
#     Authorization = "Bearer $synapseToken"
#     "Content-Type" = "application/json"
# }

# $baseUri = "https://$SynapseWorkspaceName.dev.azuresynapse.net"

# # --------------------------------------------------
# # List ALL pipelines once (required)
# # --------------------------------------------------
# Log "Listing pipelines from Synapse workspace '$SynapseWorkspaceName'..." "Yellow"

# try {
#     $listUri = "$baseUri/pipelines?api-version=2020-12-01"
#     $allPipelines = Invoke-RestMethod `
#         -Method GET `
#         -Uri $listUri `
#         -Headers $headers `
#         -ErrorAction Stop
# }
# catch {
#     Log "Failed to list Synapse pipelines: $($_.Exception.Message)" "Red"
#     exit 1
# }

# if (-not $allPipelines.value) {
#     Log "No pipelines found in Synapse workspace." "Red"
#     exit 1
# }

# # --------------------------------------------------
# # Migrate selected pipelines
# # --------------------------------------------------
# foreach ($PipelineName in $PipelineNames) {

#     try {
#         Log "Processing pipeline '$PipelineName'..." "Yellow"

#         # 🔍 Filter pipeline from list
#         $pipeline = $allPipelines.value |
#             Where-Object { $_.name -eq $PipelineName }

#         if (-not $pipeline) {
#             throw "Pipeline '$PipelineName' not found in Synapse workspace."
#         }

#         if (-not $pipeline.properties.activities) {
#             throw "Pipeline '$PipelineName' has no activities."
#         }

#         # Normalize to Fabric-compatible structure
#         $fabricPipeline = @{
#             name       = $pipeline.name
#             properties = $pipeline.properties
#         }

#         Log "Converting pipeline '$PipelineName' to Fabric resources..." "Yellow"

#         $fabricPipeline |
#             ConvertTo-Json -Depth 100 |
#             ConvertFrom-Json |
#             ConvertTo-FabricResources |
#             Import-FabricResolutions -ResolutionsFilename $ResolutionsFile |
#             Export-FabricResources `
#                 -Region $Region `
#                 -Workspace $FabricWorkspaceId `
#                 -Token $fabricToken

#         Log "Migration complete for pipeline '$PipelineName'" "Green"
#     }
#     catch {
#         Log "Migration failed for '$PipelineName': $($_.Exception.Message)" "Red"
#     }
# }

# # --------------------------------------------------
# # Wrap up
# # --------------------------------------------------
# Log "All Synapse pipeline migrations complete." "Green"
# Log "Logs saved at: $LogFile" "Gray"

<#
.SYNOPSIS
  Migrate Synapse pipelines to Microsoft Fabric
  (using classic Synapse Dev API that is proven to work).

.DESCRIPTION
  - Uses Azure CLI token for Synapse Dev API
  - Uses Az token for Fabric
  - Lists pipelines using /pipelines?api-version=2020-12-01
  - Fetches referenced datasets + linked services
  - Exports full graph to Fabric (LS → DS → Pipeline)

.REQUIREMENTS
  PowerShell 7+
  Azure CLI (az login)
  Az.Accounts
  MigrateFactoryToFabric
#>

param(
    [Parameter(Mandatory)]
    [string]$SubscriptionId,

    [Parameter(Mandatory)]
    [string]$ResourceGroupName,

    [Parameter(Mandatory)]
    [string]$SynapseWorkspaceName,

    [Parameter(Mandatory)]
    [string[]]$PipelineNames,

    [Parameter(Mandatory)]
    [string]$FabricWorkspaceId,

    [Parameter(Mandatory)]
    [string]$ResolutionsFile,

    [string]$Region = "prod"
)

# ==================================================
# Logging
# ==================================================
$LogFolder = Join-Path $PSScriptRoot "Logs"
if (-not (Test-Path $LogFolder)) { New-Item -ItemType Directory -Path $LogFolder | Out-Null }

$LogFile = Join-Path $LogFolder ("SynapseMigration_{0}.txt" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
"--- Starting Synapse → Fabric Migration ---" | Out-File $LogFile

function Log {
    param([string]$Message, [string]$Color = "Gray")
    Write-Host $Message -ForegroundColor $Color
    Add-Content $LogFile ("[{0}] {1}" -f (Get-Date -Format "HH:mm:ss"), $Message)
}

# ==================================================
# Azure context (Fabric token only)
# ==================================================
Log "Logging into Azure (Az)..." "Yellow"
Add-AzAccount | Out-Null
Select-AzSubscription -SubscriptionId $SubscriptionId | Out-Null
Log "Active subscription set." "Green"

# ==================================================
# Tokens
# ==================================================
Log "Retrieving tokens..." "Yellow"

try {
    # Synapse Dev API token (CLI)
    $synapseToken = az account get-access-token `
        --resource https://dev.azuresynapse.net `
        --query accessToken -o tsv

    if (-not $synapseToken) {
        throw "Failed to acquire Synapse token. Run 'az login'."
    }

    # Fabric token
    $fabricToken = (Get-AzAccessToken `
        -ResourceUrl "https://analysis.windows.net/powerbi/api"
    ).Token

    Log "Tokens acquired successfully." "Green"
}
catch {
    Log "Token acquisition failed: $($_.Exception.Message)" "Red"
    exit 1
}

# ==================================================
# Validate resolutions file
# ==================================================
if (-not (Test-Path $ResolutionsFile)) {
    Log "Resolutions file not found: $ResolutionsFile" "Red"
    exit 1
}

# ==================================================
# Normalize pipeline names
# ==================================================
if ($PipelineNames.Count -eq 1 -and $PipelineNames[0] -like "*,*") {
    $PipelineNames = $PipelineNames[0].Split(",") | ForEach-Object { $_.Trim() }
}

# ==================================================
# Synapse Dev API (OLD, WORKING LOGIC)
# ==================================================
$headers = @{
    Authorization = "Bearer $synapseToken"
    "Content-Type" = "application/json"
}

$baseUri    = "https://$SynapseWorkspaceName.dev.azuresynapse.net"
$apiVersion = "2020-12-01"

function SynGet {
    param([string]$Path)
    $uri = "$baseUri/$Path?api-version=$apiVersion"
    Invoke-RestMethod -Method GET -Uri $uri -Headers $headers -ErrorAction Stop
}

# ==================================================
# 1️⃣ LIST PIPELINES (PROVEN TO WORK)
# ==================================================
Log "Listing pipelines from Synapse workspace '$SynapseWorkspaceName'..." "Yellow"

try {
    $allPipelines = SynGet "pipelines"
}
catch {
    Log "Failed to list pipelines: $($_.Exception.Message)" "Red"
    exit 1
}

if (-not $allPipelines.value -or $allPipelines.value.Count -eq 0) {
    Log "No pipelines found in Synapse workspace." "Red"
    exit 1
}

$pipelineList = $allPipelines.value

# ==================================================
# 2️⃣ PROCESS SELECTED PIPELINES
# ==================================================
foreach ($PipelineName in $PipelineNames) {
    try {
        Log "Processing pipeline '$PipelineName'..." "Yellow"

        $pipeline = $pipelineList | Where-Object { $_.name -eq $PipelineName }
        if (-not $pipeline) {
            throw "Pipeline '$PipelineName' not found."
        }

        # --------------------------------------------------
        # Extract dataset references
        # --------------------------------------------------
        $datasetNames = New-Object System.Collections.Generic.HashSet[string]
        foreach ($act in $pipeline.properties.activities) {
            foreach ($i in ($act.inputs  | ForEach-Object { $_ })) {
                if ($i.referenceName) { [void]$datasetNames.Add($i.referenceName) }
            }
            foreach ($o in ($act.outputs | ForEach-Object { $_ })) {
                if ($o.referenceName) { [void]$datasetNames.Add($o.referenceName) }
            }
        }

        # --------------------------------------------------
        # Fetch datasets
        # --------------------------------------------------
        $datasets = @()
        foreach ($dsName in $datasetNames) {
            Log "Fetching dataset '$dsName'..." "Gray"
            $ds = SynGet "datasets/$dsName"

            $datasets += [pscustomobject]@{
                type       = "Microsoft.DataFactory/factories/datasets"
                name       = $ds.name
                properties = $ds.properties
            }
        }

        # --------------------------------------------------
        # Extract linked services
        # --------------------------------------------------
        $lsNames = New-Object System.Collections.Generic.HashSet[string]
        foreach ($d in $datasets) {
            $ls = $d.properties.linkedServiceName.referenceName
            if ($ls) { [void]$lsNames.Add($ls) }
        }

        # --------------------------------------------------
        # Fetch linked services
        # --------------------------------------------------
        $linkedServices = @()
        foreach ($lsName in $lsNames) {
            Log "Fetching linked service '$lsName'..." "Gray"
            $ls = SynGet "linkedservices/$lsName"

            $linkedServices += [pscustomobject]@{
                type       = "Microsoft.DataFactory/factories/linkedservices"
                name       = $ls.name
                properties = $ls.properties
            }
        }

        # --------------------------------------------------
        # Build pipeline resource
        # --------------------------------------------------
        $pipelineResource = [pscustomobject]@{
            # type       = "Microsoft.DataFactory/factories/pipelines"
            type = "Microsoft.Synapse/workspaces/pipelines"
            name       = $pipeline.name
            properties = $pipeline.properties
        }

        # --------------------------------------------------
        # Export to Fabric (LS → DS → Pipeline)
        # --------------------------------------------------
        Log "Exporting pipeline '$PipelineName' to Fabric..." "Yellow"

        $resources = @()
        $resources += $linkedServices
        $resources += $datasets
        $resources += $pipelineResource

        $result = $resources |
            ConvertTo-Json -Depth 100 |
            ConvertFrom-Json |
            ConvertTo-FabricResources |
            Import-FabricResolutions -ResolutionsFilename $ResolutionsFile |
            Export-FabricResources `
                -Region $Region `
                -Workspace $FabricWorkspaceId `
                -Token $fabricToken

        if ($result.state -ne "Succeeded") {
            throw "Fabric export failed: $($result | ConvertTo-Json -Depth 10)"
        }

        Log "Migration complete for pipeline '$PipelineName'" "Green"
    }
    catch {
        Log "Migration failed for '$PipelineName': $($_.Exception.Message)" "Red"
    }
}

Log "All Synapse pipeline migrations complete." "Green"
Log "Logs saved at: $LogFile" "Gray"
