<#
.SYNOPSIS
    Migrate an ADF-compatible pipeline JSON directly to Microsoft Fabric with logging and safe error handling.

.DESCRIPTION
    - Logs into Azure
    - Validates inputs and tokens
    - Converts provided ADF-compatible pipeline JSON to Fabric resources
    - Applies resolutions and exports to Fabric workspace
    - Logs actions and messages to /Logs folder

.REQUIREMENTS
    PowerShell 7+
    Modules: Az.Accounts, MigrateFactoryToFabric
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
    [string]$PipelineName,

    [Parameter(Mandatory = $true)]
    [string]$PipelineDefinitionFile
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
Log "🔐 Logging into Azure..." "Yellow"
try {
    if (-not (Get-AzContext)) { Add-AzAccount | Out-Null }
    Select-AzSubscription -SubscriptionId $SubscriptionId | Out-Null
    $context = Get-AzContext
    if (-not $context) { throw "Azure login failed. No context found." }
    Log "✅ Active Subscription: $($context.Subscription.Name) ($($context.Subscription.Id))" "Green"
}
catch {
    Log "❌ Azure login failed: $($_.Exception.Message)" "Red"
    exit 1
}

# ----------------------------
# Step 2. Tokens
# ----------------------------
Log "Retrieving secure tokens..." "Yellow"
try {
    $fabricSecureToken = (Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api").Token
    Log "✅ Tokens acquired successfully." "Green"
}
catch {
    Log "❌ Failed to acquire Fabric token: $($_.Exception.Message)" "Red"
    exit 1
}

# ----------------------------
# Step 3. Validate inputs
# ----------------------------
if (-not (Test-Path $ResolutionsFile)) {
    Log "❌ Resolutions file not found: $ResolutionsFile" "Red"
    exit 1
}
if (-not (Test-Path $PipelineDefinitionFile)) {
    Log "❌ Pipeline JSON not found: $PipelineDefinitionFile" "Red"
    exit 1
}

# ----------------------------
# Step 4. Load ADF-compatible JSON and migrate
# ----------------------------
try {
    Log "🚀 Migrating pipeline '$PipelineName' from JSON..." "Yellow"
    $adfObject = Get-Content -Path $PipelineDefinitionFile -Raw | ConvertFrom-Json

    $adfObject |
      ConvertTo-FabricResources |
      Import-FabricResolutions -ResolutionsFilename $ResolutionsFile |
      Export-FabricResources `
        -Region $Region `
        -Workspace $FabricWorkspaceId `
        -Token $fabricSecureToken

    Log "✅ Migration complete for pipeline: $PipelineName" "Green"
}
catch {
    Log "❌ Migration failed for pipeline '$PipelineName': $($_.Exception.Message)" "Red"
    exit 1
}

# ----------------------------
# Step 5. Wrap up
# ----------------------------
Log "Migration complete! Check your Fabric workspace (ID: $FabricWorkspaceId)." "Green"
Log "📄 Logs saved at: $LogFile" "Gray"
