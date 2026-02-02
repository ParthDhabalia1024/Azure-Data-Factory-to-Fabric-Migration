<#
.SYNOPSIS
    Azure Data Factory 3 Microsoft Fabric migration tool with logging and safe error handling.
 
.DESCRIPTION
    - Logs into Azure
    - Uses provided Subscription, Resource Group, ADF, and Pipeline(s)
    - Migrates selected pipelines to Fabric
    - Handles common errors gracefully
    - Logs actions and messages to /Logs folder
 
.REQUIREMENTS
    PowerShell 7+
    Modules: Az.Accounts, Az.DataFactory, MigrateFactoryToFabric
#>
 
param(
    [Parameter(Mandatory = $true)]
    [string]$FabricWorkspaceId,
 
    [Parameter(Mandatory = $true)]
    [string]$ResolutionsFile,
 
    [Parameter(Mandatory = $false)]
    [string]$Region = "prod",
 
    [Parameter(Mandatory = $true)]
    [string]$SubscriptionId,
 
    [Parameter(Mandatory = $true)]
    [string]$ResourceGroupName,
 
    [Parameter(Mandatory = $true)]
    [string]$DataFactoryName,
 
    [Parameter(Mandatory = $true)]
    [string[]]$PipelineNames
)
 
# ----------------------------
# Setup logging
# ----------------------------
$LogFolder = "$PSScriptRoot\Logs"
if (-not (Test-Path $LogFolder)) {
    New-Item -ItemType Directory -Path $LogFolder | Out-Null
}
$LogFile = "$LogFolder\MigrationLog_{0}.txt" -f (Get-Date -Format "yyyyMMdd_HHmmss")
"--- Starting Migration Session ---" | Out-File -FilePath $LogFile
 
function Log {
    param(
        [string]$message,
        [string]$color = "Gray"
    )
    Write-Host $message -ForegroundColor $color
    Add-Content -Path $LogFile -Value ("[{0}] {1}" -f (Get-Date -Format "HH:mm:ss"), $message)
}
 
# ----------------------------
# Step 1. Login to Azure
# ----------------------------
Log "510 Logging into Azure..." "Yellow"
try {
    Add-AzAccount | Out-Null
    Select-AzSubscription -SubscriptionId $SubscriptionId | Out-Null
    $context = Get-AzContext
    if (-not $context) { throw "Azure login failed. No context found." }
    Log "4d8 Active Subscription: $($context.Subscription.Name) ($($context.Subscription.Id))" "Green"
}
catch {
    Log "6d1 Azure login failed: $($_.Exception.Message)" "Red"
    exit 1
}
 
# ----------------------------
# Step 2. Validate resource group and data factory
# ----------------------------
Log "Using Resource Group: $ResourceGroupName" "Yellow"
try {
    $null = Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction Stop
    Log "197 Resource Group found: $ResourceGroupName" "Green"
}
catch {
    Log "6d1 Failed to find Resource Group '$ResourceGroupName': $($_.Exception.Message)" "Red"
    exit 1
}
 
Log "Using Data Factory: $DataFactoryName" "Yellow"
try {
    $factory = Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $DataFactoryName -ErrorAction Stop
    if (-not $factory) {
        throw "Data Factory not found."
    }
    Log "197 Data Factory resolved: $DataFactoryName" "Green"
}
catch {
    Log "6d1 Error retrieving Data Factory '$DataFactoryName': $($_.Exception.Message)" "Red"
    exit 1
}
 
# ----------------------------
# Step 3. Resolve pipelines to migrate
# ----------------------------
if ($null -eq $PipelineNames) {
    $PipelineNames = @()
}
elseif ($PipelineNames.Count -eq 1 -and $PipelineNames[0] -is [string] -and $PipelineNames[0] -like "*,*") {
    $PipelineNames = @(
        $PipelineNames[0].Split(",", [System.StringSplitOptions]::RemoveEmptyEntries) |
        ForEach-Object { $_.Trim() } |
        Where-Object { $_ }
    )
}
Log "Retrieving pipelines from '$DataFactoryName'..." "Yellow"
try {
    $allPipelines = Get-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -ErrorAction Stop
    if (-not $allPipelines) {
        throw "No pipelines found in '$DataFactoryName'."
    }
 
    if ($allPipelines -isnot [System.Collections.IEnumerable]) {
        $allPipelines = @($allPipelines)
    }
 
    $allNames = @($allPipelines | ForEach-Object { $_.Name })
    $selected = @()
 
    foreach ($p in $PipelineNames) {
        if ($allNames -contains $p) {
            $selected += $p
        }
        else {
            Log "6a03f38van Warning: pipeline '$p' not found in factory and will be skipped." "Red"
        }
    }
 
    if (-not $selected) {
        Log "6d1 No valid pipelines to migrate after validation." "Red"
        exit 1
    }
 
    $PipelineNames = $selected
    Log "197 Pipelines selected for migration: $($PipelineNames -join ', ')" "Green"
}
catch {
    Log "6d1 Error retrieving or validating pipelines: $($_.Exception.Message)" "Red"
    exit 1
}
 
# ----------------------------
# Step 4. Tokens
# ----------------------------
Log "Retrieving secure tokens..." "Yellow"
try {
    $adfSecureToken = (Get-AzAccessToken -ResourceUrl "https://management.azure.com/").Token
    $fabricSecureToken = (Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api").Token
    Log "197 Tokens acquired successfully." "Green"
}
catch {
    Log "6d1 Failed to acquire tokens: $($_.Exception.Message)" "Red"
    exit 1
}
 
# ----------------------------
# Step 5. Validate resolutions file
# ----------------------------
if (-not (Test-Path $ResolutionsFile)) {
    Log "6a0e0f Resolutions file not found: $ResolutionsFile" "Red"
    exit 1
}
 
# ----------------------------
# Step 6. Run migration(s)
# ----------------------------
foreach ($PipelineName in $PipelineNames) {
    try {
        Log "Migrating pipeline '$PipelineName' from ADF '$DataFactoryName'..." "Yellow"
        Import-AdfFactory `
            -SubscriptionId $SubscriptionId `
            -ResourceGroupName $ResourceGroupName `
            -FactoryName $DataFactoryName `
            -PipelineName $PipelineName `
            -AdfToken $adfSecureToken |
        ConvertTo-FabricResources |
        Import-FabricResolutions -ResolutionsFilename $ResolutionsFile |
        Export-FabricResources `
            -Region $Region `
            -Workspace $FabricWorkspaceId `
            -Token $fabricSecureToken
        Log "197 Migration complete for pipeline: $PipelineName" "Green"
    }
    catch {
        Log "6d1 Migration failed for pipeline '$PipelineName': $($_.Exception.Message)" "Red"
    }
}
 
# ----------------------------
# Step 7. Wrap up
# ----------------------------
Log "Migration complete! Check your Fabric workspace (ID: $FabricWorkspaceId)." "Green"
Log "4c3 Logs saved at: $LogFile" "Gray"


# <#
# .SYNOPSIS
#     Azure Data Factory OR Synapse Pipeline → Microsoft Fabric migration tool

# .DESCRIPTION
#     - Supports ADF and Synapse pipelines
#     - ADF: uses Import-AdfFactory (native)
#     - Synapse: pulls pipeline via Synapse Dev API and wraps into an Import-AdfFactory-like bundle
#     - Reuses ConvertTo-FabricResources / Import-FabricResolutions / Export-FabricResources
#     - Logging and safe error handling

# .REQUIREMENTS
#     PowerShell 7+
#     Modules: Az.Accounts, Az.DataFactory, MigrateFactoryToFabric
# #>

# param(
#     [Parameter(Mandatory = $true)]
#     [ValidateSet("ADF", "Synapse")]
#     [string]$SourceType,

#     [Parameter(Mandatory = $true)]
#     [string]$FabricWorkspaceId,

#     [Parameter(Mandatory = $true)]
#     [string]$ResolutionsFile,

#     [Parameter(Mandatory = $false)]
#     [string]$Region = "prod",

#     [Parameter(Mandatory = $true)]
#     [string]$SubscriptionId,

#     [Parameter(Mandatory = $true)]
#     [string]$ResourceGroupName,

#     # ADF name OR Synapse workspace name
#     [Parameter(Mandatory = $true)]
#     [string]$SourceName,

#     [Parameter(Mandatory = $true)]
#     [string[]]$PipelineNames,

#     # Optional: dumps the wrapped object to logs for troubleshooting
#     [Parameter(Mandatory = $false)]
#     [switch]$DebugBundle
# )

# # -------------------------------------------------
# # Logging
# # -------------------------------------------------
# $LogFolder = Join-Path $PSScriptRoot "Logs"
# if (-not (Test-Path $LogFolder)) { New-Item -ItemType Directory -Path $LogFolder | Out-Null }

# $LogFile = Join-Path $LogFolder ("MigrationLog_{0}.txt" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
# "--- Starting Migration Session ---" | Out-File $LogFile

# function Log {
#     param([string]$Message,[string]$Color="Gray")
#     Write-Host $Message -ForegroundColor $Color
#     Add-Content -Path $LogFile -Value ("[{0}] {1}" -f (Get-Date -Format "HH:mm:ss"), $Message)
# }

# # -------------------------------------------------
# # Login
# # -------------------------------------------------
# Log "Logging into Azure..." "Yellow"
# try {
#     Add-AzAccount | Out-Null
#     Select-AzSubscription -SubscriptionId $SubscriptionId | Out-Null
#     Log "Azure login successful." "Green"
# }
# catch {
#     Log "Azure login failed: $($_.Exception.Message)" "Red"
#     exit 1
# }

# # -------------------------------------------------
# # Tokens
# # -------------------------------------------------
# try {
#     $adfToken    = (Get-AzAccessToken -ResourceUrl "https://management.azure.com/").Token
#     $fabricToken = (Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api").Token
# }
# catch {
#     Log "Failed to acquire tokens: $($_.Exception.Message)" "Red"
#     exit 1
# }

# # -------------------------------------------------
# # Normalize pipeline list
# # -------------------------------------------------
# if ($PipelineNames.Count -eq 1 -and $PipelineNames[0] -like "*,*") {
#     $PipelineNames = $PipelineNames[0].Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
# }

# # =================================================
# # ADF MIGRATION (native)
# # =================================================
# if ($SourceType -eq "ADF") {

#     Log "Source Type: Azure Data Factory" "Cyan"
#     Log "Data Factory: $SourceName" "Yellow"

#     try {
#         Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $SourceName -ErrorAction Stop | Out-Null
#         Log "ADF resolved successfully." "Green"
#     }
#     catch {
#         Log "ADF not found: $($_.Exception.Message)" "Red"
#         exit 1
#     }

#     foreach ($PipelineName in $PipelineNames) {
#         try {
#             Log "Migrating ADF pipeline: $PipelineName" "Yellow"

#             Import-AdfFactory `
#                 -SubscriptionId $SubscriptionId `
#                 -ResourceGroupName $ResourceGroupName `
#                 -FactoryName $SourceName `
#                 -PipelineName $PipelineName `
#                 -AdfToken $adfToken |
#             ConvertTo-FabricResources |
#             Import-FabricResolutions -ResolutionsFilename $ResolutionsFile |
#             Export-FabricResources `
#                 -Region $Region `
#                 -Workspace $FabricWorkspaceId `
#                 -Token $fabricToken

#             Log "ADF migration completed: $PipelineName" "Green"
#         }
#         catch {
#             Log "ADF migration failed for ${PipelineName}: $($_.Exception.Message)" "Red"
#         }
#     }
# }

# # =================================================
# # SYNAPSE MIGRATION (bundle wrapper)
# # =================================================
# elseif ($SourceType -eq "Synapse") {

#     Log "Source Type: Synapse Analytics" "Cyan"
#     Log "Synapse Workspace: $SourceName" "Yellow"

#     foreach ($PipelineName in $PipelineNames) {

#         try {
#             Log "Fetching Synapse pipeline: $PipelineName" "Yellow"

#             # Pull Synapse pipeline via Dev API using current Az context
#             $pipelineResponse = Invoke-AzRestMethod `
#                 -Method GET `
#                 -Uri "https://${SourceName}.dev.azuresynapse.net/pipelines/${PipelineName}?api-version=2020-12-01"

#             $synapsePipeline = $pipelineResponse.Content | ConvertFrom-Json

#             # ----------------------------
#             # Build an Import-AdfFactory-like bundle
#             # ----------------------------

#             # Pipeline as an ARM-like resource (commonly expected by converters)
#             $pipelineArm = [PSCustomObject]@{
#                 id         = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.Synapse/workspaces/$SourceName/pipelines/$PipelineName"
#                 name       = "$SourceName/$PipelineName"
#                 type       = "Microsoft.DataFactory/factories/pipelines"
#                 properties = $synapsePipeline.properties
#             }

#             # Factory as an ARM-like resource (some converters expect it to exist)
#             $factoryArm = [PSCustomObject]@{
#                 id   = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$SourceName"
#                 name = $SourceName
#                 type = "Microsoft.DataFactory/factories"
#                 properties = [PSCustomObject]@{}
#             }

#             # Bundle that mimics Import-AdfFactory output (key fix)
#             $bundle = [PSCustomObject]@{
#                 SubscriptionId     = $SubscriptionId
#                 ResourceGroupName  = $ResourceGroupName
#                 FactoryName        = $SourceName
#                 Factory            = $factoryArm
#                 Pipelines          = @($pipelineArm)
#                 LinkedServices     = @()
#                 Datasets           = @()
#                 Triggers           = @()
#                 IntegrationRuntimes= @()
#             }

#             if ($DebugBundle) {
#                 $bundleJson = $bundle | ConvertTo-Json -Depth 50
#                 Log "---- DEBUG BUNDLE START ----" "DarkGray"
#                 Add-Content -Path $LogFile -Value $bundleJson
#                 Log "---- DEBUG BUNDLE END ----" "DarkGray"
#             }

#             # Now run the same Fabric conversion pipeline
#             $bundle |
#                 ConvertTo-FabricResources |
#                 Import-FabricResolutions -ResolutionsFilename $ResolutionsFile |
#                 Export-FabricResources `
#                     -Region $Region `
#                     -Workspace $FabricWorkspaceId `
#                     -Token $fabricToken

#             Log "Synapse migration completed: $PipelineName" "Green"
#         }
#         catch {
#             Log "Synapse migration failed for ${PipelineName}: $($_.Exception.Message)" "Red"
#         }
#     }
# }

# Log "Migration completed." "Green"
# Log "Logs saved to: $LogFile" "Gray"

# # <#
# # .SYNOPSIS
# #     Azure Data Factory OR Synapse Pipeline → Microsoft Fabric migration tool

# # .DESCRIPTION
# #     - Supports ADF and Synapse pipelines
# #     - Uses ARM APIs for ADF
# #     - Uses Synapse DEV REST API for Synapse (via Invoke-AzRestMethod)
# #     - Reuses existing Fabric conversion logic
# #     - Safe logging and error handling

# # .REQUIREMENTS
# #     PowerShell 7+
# #     Az.Accounts
# #     Az.DataFactory
# #     MigrateFactoryToFabric
# # #>

# # param(
# #     [Parameter(Mandatory = $true)]
# #     [ValidateSet("ADF", "Synapse")]
# #     [string]$SourceType,

# #     [Parameter(Mandatory = $true)]
# #     [string]$FabricWorkspaceId,

# #     [Parameter(Mandatory = $true)]
# #     [string]$ResolutionsFile,

# #     [Parameter(Mandatory = $false)]
# #     [string]$Region = "prod",

# #     [Parameter(Mandatory = $true)]
# #     [string]$SubscriptionId,

# #     [Parameter(Mandatory = $true)]
# #     [string]$ResourceGroupName,

# #     # ADF name OR Synapse workspace name
# #     [Parameter(Mandatory = $true)]
# #     [string]$SourceName,

# #     [Parameter(Mandatory = $true)]
# #     [string[]]$PipelineNames
# # )

# # # -------------------------------------------------
# # # Logging setup
# # # -------------------------------------------------
# # $LogFolder = Join-Path $PSScriptRoot "Logs"
# # if (-not (Test-Path $LogFolder)) {
# #     New-Item -ItemType Directory -Path $LogFolder | Out-Null
# # }

# # $LogFile = Join-Path $LogFolder ("MigrationLog_{0}.txt" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
# # "--- Starting Migration Session ---" | Out-File -FilePath $LogFile

# # function Log {
# #     param(
# #         [string]$Message,
# #         [string]$Color = "Gray"
# #     )
# #     Write-Host $Message -ForegroundColor $Color
# #     Add-Content -Path $LogFile -Value ("[{0}] {1}" -f (Get-Date -Format "HH:mm:ss"), $Message)
# # }

# # # -------------------------------------------------
# # # Login
# # # -------------------------------------------------
# # Log "Logging into Azure..." "Yellow"
# # try {
# #     Add-AzAccount | Out-Null
# #     Select-AzSubscription -SubscriptionId $SubscriptionId | Out-Null
# #     Log "Azure login successful." "Green"
# # }
# # catch {
# #     Log "Azure login failed: $($_.Exception.Message)" "Red"
# #     exit 1
# # }

# # # -------------------------------------------------
# # # Tokens (ADF + Fabric only)
# # # -------------------------------------------------
# # try {
# #     $adfToken = (Get-AzAccessToken -ResourceUrl "https://management.azure.com/").Token
# #     $fabricToken = (Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api").Token
# # }
# # catch {
# #     Log "Failed to acquire tokens: $($_.Exception.Message)" "Red"
# #     exit 1
# # }

# # # -------------------------------------------------
# # # Normalize pipeline list
# # # -------------------------------------------------
# # if ($PipelineNames.Count -eq 1 -and $PipelineNames[0] -like "*,*") {
# #     $PipelineNames = $PipelineNames[0].Split(",") | ForEach-Object { $_.Trim() }
# # }

# # # =================================================
# # # ADF MIGRATION (ARM)
# # # =================================================
# # if ($SourceType -eq "ADF") {

# #     Log "Source Type: Azure Data Factory" "Cyan"
# #     Log "Data Factory: $SourceName" "Yellow"

# #     try {
# #         Get-AzDataFactoryV2 `
# #             -ResourceGroupName $ResourceGroupName `
# #             -Name $SourceName `
# #             -ErrorAction Stop | Out-Null

# #         Log "ADF resolved successfully." "Green"
# #     }
# #     catch {
# #         Log "ADF not found: $($_.Exception.Message)" "Red"
# #         exit 1
# #     }

# #     foreach ($PipelineName in $PipelineNames) {
# #         try {
# #             Log "Migrating ADF pipeline: $PipelineName" "Yellow"

# #             Import-AdfFactory `
# #                 -SubscriptionId $SubscriptionId `
# #                 -ResourceGroupName $ResourceGroupName `
# #                 -FactoryName $SourceName `
# #                 -PipelineName $PipelineName `
# #                 -AdfToken $adfToken |
# #             ConvertTo-FabricResources |
# #             Import-FabricResolutions -ResolutionsFilename $ResolutionsFile |
# #             Export-FabricResources `
# #                 -Region $Region `
# #                 -Workspace $FabricWorkspaceId `
# #                 -Token $fabricToken

# #             Log "ADF migration completed: $PipelineName" "Green"
# #         }
# #         catch {
# #             Log "ADF migration failed for ${PipelineName}: $($_.Exception.Message)" "Red"
# #         }
# #     }
# # }

# # # =================================================
# # # SYNAPSE MIGRATION (DEV API – CORRECT WAY)
# # # =================================================
# # elseif ($SourceType -eq "Synapse") {

# #     Log "Source Type: Synapse Analytics" "Cyan"
# #     Log "Synapse Workspace: $SourceName" "Yellow"

# #     foreach ($PipelineName in $PipelineNames) {

# #         try {
# #             Log "Fetching Synapse pipeline: $PipelineName" "Yellow"

# #             # IMPORTANT:
# #             # Use Invoke-AzRestMethod to avoid 401 issues
# #             $pipelinePath = "/pipelines/${PipelineName}?api-version=2020-12-01"

# #             $pipelineJson = Invoke-AzRestMethod `
# #                 -Method GET `
# #                 -Uri "https://${SourceName}.dev.azuresynapse.net$pipelinePath"

# #             $pipelineJson |
# #                 ConvertTo-FabricResources |
# #                 Import-FabricResolutions -ResolutionsFilename $ResolutionsFile |
# #                 Export-FabricResources `
# #                     -Region $Region `
# #                     -Workspace $FabricWorkspaceId `
# #                     -Token $fabricToken

# #             Log "Synapse migration completed: $PipelineName" "Green"
# #         }
# #         catch {
# #             Log "Synapse migration failed for ${PipelineName}: $($_.Exception.Message)" "Red"
# #         }
# #     }
# # }

# # # -------------------------------------------------
# # # Wrap up
# # # -------------------------------------------------
# # Log "Migration completed." "Green"
# # Log "Logs saved to: $LogFile" "Gray"
