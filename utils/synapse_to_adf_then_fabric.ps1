# <#
# .SYNAPSE
#     Synapse → ADF (rehydrate) → Fabric migration wrapper

# .DESCRIPTION
#     - Fetches Synapse pipeline JSON
#     - Creates a temporary ADF
#     - Deploys pipeline into ADF
#     - Calls existing adf_to_fabric_migration.ps1
# #>

# param(
#     [Parameter(Mandatory = $true)]
#     [string]$SubscriptionId,

#     [Parameter(Mandatory = $true)]
#     [string]$ResourceGroupName,

#     [Parameter(Mandatory = $true)]
#     [string]$SynapseWorkspaceName,

#     [Parameter(Mandatory = $true)]
#     [string]$PipelineName,

#     [Parameter(Mandatory = $true)]
#     [string]$FabricWorkspaceId,

#     [Parameter(Mandatory = $true)]
#     [string]$ResolutionsFile,

#     [Parameter(Mandatory = $false)]
#     [string]$Region = "prod",

#     [Parameter(Mandatory = $false)]
#     [string]$TempAdfName = "temp-adf-for-synapse-migration"
# )

# # -------------------------------------------------
# # Login
# # -------------------------------------------------
# Add-AzAccount | Out-Null
# Select-AzSubscription -SubscriptionId $SubscriptionId | Out-Null
# (Legacy commented section)

# # -------------------------------------------------
# # Tokens
# # -------------------------------------------------
# $armToken = (Get-AzAccessToken -ResourceUrl "https://management.azure.com/").Token
# $synapseToken = (Get-AzAccessToken -ResourceUrl "https://dev.azuresynapse.net").Token

# # -------------------------------------------------
# # Ensure temp ADF exists
# # -------------------------------------------------
# $adf = Get-AzDataFactoryV2 `
#     -ResourceGroupName $ResourceGroupName `
#     -Name $TempAdfName `
#     -ErrorAction SilentlyContinue

# if (-not $adf) {
#     Write-Host "Creating temporary ADF: $TempAdfName" -ForegroundColor Yellow
#     $adf = New-AzDataFactoryV2 `
#         -ResourceGroupName $ResourceGroupName `
#         -Name $TempAdfName `
#         -Location "East US"
# }

# # -------------------------------------------------
# # Fetch Synapse pipeline JSON
# # -------------------------------------------------
# $pipelineUrl = "https://$SynapseWorkspaceName.dev.azuresynapse.net/pipelines/$PipelineName?api-version=2020-12-01"
# $pipelineJson = Invoke-RestMethod `
#     -Method GET `
#     -Uri $pipelineUrl `
#     -Headers @{ Authorization = "Bearer $synapseToken" }

# # -------------------------------------------------
# # Deploy pipeline into ADF
# # -------------------------------------------------
# Write-Host "Deploying pipeline into temporary ADF..." -ForegroundColor Cyan

# Set-AzDataFactoryV2Pipeline `
#     -ResourceGroupName $ResourceGroupName `
#     -DataFactoryName $TempAdfName `
#     -Name $PipelineName `
#     -Definition $pipelineJson `
#     -Force

# # -------------------------------------------------
# # Call EXISTING migration script (UNCHANGED)
# # -------------------------------------------------
# Write-Host "Starting Fabric migration using existing script..." -ForegroundColor Green

# & "$PSScriptRoot\adf_to_fabric_migration.ps1" `
#     -FabricWorkspaceId $FabricWorkspaceId `
#     -ResolutionsFile $ResolutionsFile `
#     -Region $Region `
#     -SubscriptionId $SubscriptionId `
#     -ResourceGroupName $ResourceGroupName `
#     -DataFactoryName $TempAdfName `
#     -PipelineNames $PipelineName

# Write-Host "Migration completed." -ForegroundColor Green




<#
.SYNOPSIS
    Synapse → ADF-compatible JSON → Fabric migration

.DESCRIPTION
    - Fetches Synapse pipeline via Dev API
    - Normalizes to ADF v2 schema
    - Feeds JSON into existing Fabric migration script
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$SubscriptionId,

    [Parameter(Mandatory = $true)]
    [string]$ResourceGroupName,

    [Parameter(Mandatory = $true)]
    [string]$SynapseWorkspaceName,

    [Parameter(Mandatory = $true)]
    [string]$PipelineName,

    [Parameter(Mandatory = $true)]
    [string]$FabricWorkspaceId,

    [Parameter(Mandatory = $true)]
    [string]$ResolutionsFile,

    [Parameter(Mandatory = $false)]
    [string]$Region = "prod",

    [Parameter(Mandatory = $false)]
    [string]$TempAdfName = "temp-adf-for-synapse-migration",

    # If Synapse workspace lives in a different RG than the ADF temp RG, specify it here. Defaults to ResourceGroupName.
    [Parameter(Mandatory = $false)]
    [string]$SynapseResourceGroupName,

    # When true, if the fetched Synapse pipeline has no activities, inject a 1s Wait so ADF accepts the pipeline.
    # Default is false to prevent masking fetch/permission issues.
    [Parameter(Mandatory = $false)]
    [switch]$InjectWaitIfEmpty,

    # When true, delete the temporary ADF after migration completes successfully.
    [Parameter(Mandatory = $false)]
    [switch]$CleanupTempAdf,

    # Optional explicit overrides when Synapse dataset fetch is blocked (e.g., 401) or datasets are parameterized
    [Parameter(Mandatory = $false)]
    [string]$SourceDatasetName,
    [Parameter(Mandatory = $false)]
    [string]$SourceContainer,
    [Parameter(Mandatory = $false)]
    [string]$SourceFolderPath,
    [Parameter(Mandatory = $false)]
    [string]$SourceFileName,

    [Parameter(Mandatory = $false)]
    [string]$SinkDatasetName,
    [Parameter(Mandatory = $false)]
    [string]$SinkContainer,
    [Parameter(Mandatory = $false)]
    [string]$SinkFolderPath,
    [Parameter(Mandatory = $false)]
    [string]$SinkFileName
)

Write-Host "🔐 Ensuring Azure login..." -ForegroundColor Yellow
if (-not (Get-AzContext)) {
    Connect-AzAccount | Out-Null
}
Select-AzSubscription -SubscriptionId $SubscriptionId | Out-Null

# Default SynapseResourceGroupName to the provided ADF ResourceGroupName when not explicitly supplied
if (-not $SynapseResourceGroupName -or [string]::IsNullOrWhiteSpace($SynapseResourceGroupName)) {
    $SynapseResourceGroupName = $ResourceGroupName
    Write-Host ("ℹ️ Using ADF Resource Group as Synapse RG: {0}" -f $SynapseResourceGroupName) -ForegroundColor DarkGray
}

# Helper: ARM auth headers using active tenant
function Get-ArmHeaders {
    $ctx = Get-AzContext
    if (-not $ctx) { throw "No Az context. Please login." }
    # Important: no trailing slash on ResourceUrl. Also, omit -TenantId to let Az select the correct authority for the active subscription (avoids InvalidAuthenticationToken in some cross-tenant contexts)
    $tok = (Get-AzAccessToken -ResourceUrl "https://management.azure.com").Token
    return @{ Authorization = "Bearer $tok"; "Content-Type" = "application/json" }
}

# Helper: Synapse Dev API headers (tries Az token first, can fall back to Azure CLI if needed)
function Get-DevApiHeaders {
    param([switch]$UseAzCli)
    # Prefer Azure CLI token when available (proved to work in your environment).
    $azCmd = Get-Command az -ErrorAction SilentlyContinue
    if ($UseAzCli.IsPresent -or $azCmd) {
        try {
            if (-not $azCmd) { throw "Azure CLI ('az') not found on PATH" }
            $tok = az account get-access-token --resource https://dev.azuresynapse.net --query accessToken -o tsv
            if (-not $tok) { throw "az CLI did not return a token" }
            Write-Host "🔑 Using Azure CLI token for Synapse Dev API." -ForegroundColor DarkGray
            return @{ Authorization = "Bearer $tok" }
        } catch {
            Write-Host ("ℹ️ Azure CLI token acquisition failed: {0}. Falling back to Az token..." -f $_.Exception.Message) -ForegroundColor Yellow
        }
    }
    try {
        $tok = (Get-AzAccessToken -ResourceUrl "https://dev.azuresynapse.net").Token
        Write-Host "🔑 Using Az account token for Synapse Dev API." -ForegroundColor DarkGray
        return @{ Authorization = "Bearer $tok" }
    } catch { throw "Failed to acquire Dev API token via Az: $($_.Exception.Message)" }
}

# ------------------------------------------------
# Fetch Synapse pipeline (Az.Synapse first, then ARM, then Dev API)
# ------------------------------------------------
Write-Host "📥 Fetching Synapse pipeline JSON..." -ForegroundColor Yellow

$synapsePipeline = $null
$rawSynPath = Join-Path $PSScriptRoot "$PipelineName.synapse.raw.json"

# 1) Try Az.Synapse cmdlet if module is available (handles auth and versioning)
if (Get-Module -ListAvailable -Name Az.Synapse) {
    try {
        Import-Module Az.Synapse -ErrorAction Stop | Out-Null
        $spObj = Get-AzSynapsePipeline -WorkspaceName $SynapseWorkspaceName -Name $PipelineName -ErrorAction Stop
        if ($spObj) {
            # Keep Az.Synapse object for activity extraction, but prefer saving a full ARM/Dev JSON for the raw pipeline file
            $synapsePipeline = $spObj
            Write-Host "✔ Retrieved Synapse pipeline via Az.Synapse (will also try to save full ARM/Dev JSON)." -ForegroundColor Green
        }

    function Get-ActivityParamsForDataset($datasetName) {
        $dn = $datasetName.ToLower()
        $paramBag = @{}
        foreach ($a in $activities) {
            $refs = @()
            if ($a.inputs) { $refs += $a.inputs }
            if ($a.outputs) { $refs += $a.outputs }
            foreach ($r in $refs) {
                if (($r.referenceName) -and ($r.referenceName.ToLower() -eq $dn)) {
                    if ($r.parameters) {
                        foreach ($p in $r.parameters.PSObject.Properties) {
                            $paramBag[$p.Name] = $p.Value
                        }
                    }
                }
            }
        }
        return $paramBag
    }
    }
    catch {
        Write-Host "ℹ️ Az.Synapse fetch did not succeed: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

# 1b) Always try to save a canonical raw pipeline JSON via ARM/Dev for fidelity
function Save-PipelineRawJson {
    try {
        $armPipelineId = "/subscriptions/$SubscriptionId/resourceGroups/$SynapseResourceGroupName/providers/Microsoft.Synapse/workspaces/$SynapseWorkspaceName/pipelines/$PipelineName?api-version=2020-12-01"
        $armUri = "https://management.azure.com$armPipelineId"
        $armResp = Invoke-AzRestMethod -Method GET -Uri $armUri -ErrorAction Stop
        if ($armResp -and $armResp.Content) {
            ($armResp.Content) | Out-File -FilePath $rawSynPath -Encoding utf8
            Write-Host "✔ Saved pipeline JSON via ARM to $rawSynPath" -ForegroundColor Green
            return
        }
    } catch {
        Write-Host ("ℹ️ ARM pipeline fetch failed: {0}" -f $_.Exception.Message) -ForegroundColor Yellow
    }
    try {
        $devUrl = "https://$SynapseWorkspaceName.dev.azuresynapse.net/pipelines/$PipelineName?api-version=2020-12-01"
        $synHeaders = Get-DevApiHeaders
        $synContent = $null
        try {
            $synContent = Invoke-RestMethod -Method GET -Uri $devUrl -Headers $synHeaders -ErrorAction Stop
        } catch {
            $msg = $_.Exception.Message
            if ($msg -match 'AuthenticationFailed' -or $msg -match 'Token Authentication failed') {
                Write-Host "ℹ️ Dev API rejected Az token; retrying with Azure CLI token..." -ForegroundColor Yellow
                try {
                    $synHeaders = Get-DevApiHeaders -UseAzCli
                    $synContent = Invoke-RestMethod -Method GET -Uri $devUrl -Headers $synHeaders -ErrorAction Stop
                } catch {
                    Write-Host ("❌ Dev API retry with Azure CLI token failed: {0}" -f $_.Exception.Message) -ForegroundColor Red
                    throw
                }
            } else { throw }
        }
        if ($synContent) {
            ($synContent | ConvertTo-Json -Depth 100) | Out-File -FilePath $rawSynPath -Encoding utf8
            Write-Host "✔ Saved pipeline JSON via Dev API to $rawSynPath" -ForegroundColor Green
            return
        }
    } catch {
        Write-Host ("ℹ️ Dev API pipeline fetch failed: {0}" -f $_.Exception.Message) -ForegroundColor Yellow
    }
    if ($synapsePipeline) {
        ($synapsePipeline | ConvertTo-Json -Depth 100) | Out-File -FilePath $rawSynPath -Encoding utf8
        Write-Host "ℹ️ Saved Az.Synapse object JSON to $rawSynPath (reduced fidelity)" -ForegroundColor Yellow
    }
}
Save-PipelineRawJson

# 2) Try ARM management-plane for activities if pipeline not yet set (works with standard ARM token and RBAC)
if (-not $synapsePipeline) {
    $armPipelineId = "/subscriptions/$SubscriptionId/resourceGroups/$SynapseResourceGroupName/providers/Microsoft.Synapse/workspaces/$SynapseWorkspaceName/pipelines/$PipelineName?api-version=2020-12-01"
    $armUri = "https://management.azure.com$armPipelineId"
    try {
        $armResp = Invoke-AzRestMethod -Method GET -Uri $armUri -ErrorAction Stop
        if ($armResp -and $armResp.Content) {
            ($armResp.Content) | Out-File -FilePath $rawSynPath -Encoding utf8
            $armObj = $armResp.Content | ConvertFrom-Json
            if ($armObj.PSObject.Properties.Name -contains 'error') {
                Write-Host "ℹ️ ARM returned error object. Will try Dev API." -ForegroundColor Yellow
            }
            else {
                $synapsePipeline = $armObj
                Write-Host "✔ Retrieved Synapse pipeline via ARM." -ForegroundColor Green
            }
        }
        # Always try to persist the original Synapse dataset JSON next to script for troubleshooting
        Save-SynapseDatasetJson -datasetName $dsName
    }
    catch {
        Write-Host "ℹ️ ARM fetch did not succeed (will try Dev API): $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

# 3) Fallback to Synapse Dev API
if (-not $synapsePipeline) {
    $devUrl = "https://$SynapseWorkspaceName.dev.azuresynapse.net/pipelines/$PipelineName?api-version=2020-12-01"
    try {
        $synToken = (Get-AzAccessToken -ResourceUrl "https://dev.azuresynapse.net").Token
        $synHeaders = @{ Authorization = "Bearer $synToken" }
        $synContent = Invoke-RestMethod -Method GET -Uri $devUrl -Headers $synHeaders -ErrorAction Stop
        if ($synContent) {
            ($synContent | ConvertTo-Json -Depth 100) | Out-File -FilePath $rawSynPath -Encoding utf8
            if ($synContent.PSObject.Properties.Name -contains 'error') {
                throw "Dev API returned error object."
            }
            $synapsePipeline = $synContent
            Write-Host "✔ Retrieved Synapse pipeline via Dev API." -ForegroundColor Green
        }
    }
    catch {
        Write-Host "❌ Failed to fetch via Synapse Dev API: $($_.Exception.Message)" -ForegroundColor Red
        throw "Failed to fetch Synapse pipeline JSON from Az.Synapse, ARM and Dev API. Check workspace/pipeline names and permissions."
    }
}

# ------------------------------------------------
# Normalize to ADF v2 schema (with safe defaults)
# ------------------------------------------------
Write-Host "🧹 Normalizing pipeline to ADF v2 schema..." -ForegroundColor Cyan

# Prefer properties node if present (ARM/Dev API shape). If fetching via Az.Synapse, fields are top-level (Activities, Parameters, Variables, Annotations).
$sp = $synapsePipeline.properties

# Initialize with safe defaults
$activitiesRaw  = @()
$parameters  = @{}
$variables   = @{}
$annotations = @()
$description = $null

function Get-PropCI($obj, $name) {
    if (-not $obj) { return $null }
    foreach ($k in $obj.PSObject.Properties.Name) {
        if ($k -and ($k.ToString().ToLower() -eq $name.ToLower())) { return $obj.$k }
    }
    return $null
}

# Normalize variables into ADF shape { type = 'String'; defaultValue = <val> }
function Normalize_Variables {
    param([hashtable]$vars)
    if (-not $vars) { return @{} }
    $out = @{}
    foreach ($k in $vars.Keys) {
        $v = $vars[$k]
        $typeVal = $null
        $defaultVal = $null
        if ($v -is [hashtable] -or $v -is [pscustomobject]) {
            $typeVal = (Get-PropCI $v 'type'); if (-not $typeVal) { $typeVal = (Get-PropCI $v 'Type') }
            $defaultVal = (Get-PropCI $v 'defaultValue'); if (-not $defaultVal) { $defaultVal = (Get-PropCI $v 'DefaultValue') }
        }
        if (-not $typeVal) { $typeVal = 'String' }
        $out[$k] = @{ type = $typeVal; defaultValue = $defaultVal }
    }
    return $out
}

if ($sp) {
    $acts = (Get-PropCI $sp 'activities'); if ($acts) { $activitiesRaw = $acts } else { $activitiesRaw = @() }
    $pars = (Get-PropCI $sp 'parameters'); if ($pars) { $parameters  = $pars } else { $parameters  = @{} }
    $vars = (Get-PropCI $sp 'variables');  if ($vars) { $variables   = $vars } else { $variables   = @{} }
    $ann  = (Get-PropCI $sp 'annotations');if ($ann) { $annotations = $ann } else { $annotations = @() }
    $desc = (Get-PropCI $sp 'description');if ($desc){ $description = $desc }
}
else {
    # Az.Synapse object shape
    $acts = (Get-PropCI $synapsePipeline 'activities'); if ($acts) { $activitiesRaw = $acts } else { $activitiesRaw = @() }
    $pars = (Get-PropCI $synapsePipeline 'parameters'); if ($pars) { $parameters  = $pars } else { $parameters  = @{} }
    $vars = (Get-PropCI $synapsePipeline 'variables');  if ($vars) { $variables   = $vars } else { $variables   = @{} }
    $ann  = (Get-PropCI $synapsePipeline 'annotations');if ($ann) { $annotations = $ann } else { $annotations = @() }
    $desc = (Get-PropCI $synapsePipeline 'description');if ($desc){ $description = $desc }
}

# Transform Synapse activities to ADF activities (handle Copy shape)
function Convert-SynapseActivityToAdf($a) {
    $name = $a.Name
    # Heuristic: presence of Source/Sink implies Copy
    $hasCopyShape = ($a.PSObject.Properties.Name -contains 'Source') -or ($a.PSObject.Properties.Name -contains 'Sink')
    if ($hasCopyShape) {
        $inputs = @()
        if ($a.Inputs) {
            foreach ($inp in $a.Inputs) {
                $inputs += @{ referenceName = $inp.ReferenceName; type = 'DatasetReference'; parameters = ($inp.Parameters ? $inp.Parameters : @{}) }
            }
        }
        $outputs = @()
        if ($a.Outputs) {
            foreach ($outp in $a.Outputs) {
                $outputs += @{ referenceName = $outp.ReferenceName; type = 'DatasetReference'; parameters = ($outp.Parameters ? $outp.Parameters : @{}) }
            }
        }
        $typeProps = @{}
        if ($a.TypeProperties) {
            $typeProps = $a.TypeProperties
        }
        # Ensure source/sink present; some shapes expose them at top level, others inside TypeProperties
        if (-not $typeProps.source -and $a.Source) { $typeProps.source = $a.Source }
        if (-not $typeProps.sink   -and $a.Sink)   { $typeProps.sink   = $a.Sink }
        if ($a.Translator) { $typeProps.translator = $a.Translator }
        return @{
            name = $name
            type = 'Copy'
            inputs = $inputs
            outputs = $outputs
            typeProperties = $typeProps
            policy = $a.Policy
            dependsOn = ($a.DependsOn ? $a.DependsOn : @())
            userProperties = ($a.UserProperties ? $a.UserProperties : @())
        }
    }
    elseif (
        (($a.Type) -and ($a.Type.ToString().ToLower() -in @('setvariable','set variable','set_variable'))) -or
        (($a.type) -and ($a.type.ToString().ToLower() -in @('setvariable','set variable','set_variable'))) -or
        (($a.TypeProperties) -and ((Get-PropCI $a.TypeProperties 'variableName') -or (Get-PropCI $a.TypeProperties 'value')))
    ) {
        # Map SetVariable activity to ADF shape
        $varName = $null; $val = $null
        if ($a.TypeProperties) {
            $varName = Get-PropCI $a.TypeProperties 'variableName'
            $val = Get-PropCI $a.TypeProperties 'value'
        }
        if (-not $varName) { $varName = Get-PropCI $a 'variableName' }
        if (-not $val) { $val = Get-PropCI $a 'value' }
        if ($val -is [string]) {
            if ($val.Length -ge 2 -and $val.StartsWith('"') -and $val.EndsWith('"')) {
                $val = $val.Substring(1, $val.Length - 2)
            }
        }
        # Prefer ADF's native expression object: { type: 'Expression', value: <expr> }
        $exprValue = $null
        if ($val -is [hashtable] -or $val -is [pscustomobject]) {
            if ($val.type -and $val.value) { $exprValue = $val } else { $exprValue = @{ type = 'Expression'; value = ([string]$val) } }
        }
        elseif ($val -is [string]) {
            $escaped = $val.Replace("'", "''")
            $exprValue = @{ type = 'Expression'; value = ("'{0}'" -f $escaped) }
        }
        elseif ($val -is [int] -or $val -is [double] -or $val -is [decimal]) {
            $exprValue = @{ type = 'Expression'; value = ([string]$val) }
        }
        else {
            $s = [string]$val
            $escaped = $s.Replace("'", "''")
            $exprValue = @{ type = 'Expression'; value = ("'{0}'" -f $escaped) }
        }
        return @{
            name = ($a.Name ? $a.Name : 'SetVariable')
            type = 'SetVariable'
            typeProperties = @{ variableName = $varName; value = $exprValue }
            dependsOn = ($a.DependsOn ? $a.DependsOn : @())
            userProperties = ($a.UserProperties ? $a.UserProperties : @())
            policy = ($a.Policy ? $a.Policy : $null)
        }
    }
    else {
        # Fallback: pass-through minimal shape
        return @{
            name = ($a.Name ? $a.Name : ($a.name ? $a.name : 'Activity'))
            type = ($a.Type ? $a.Type : ($a.type ? $a.type : 'Execute'))
        }
    }
}

$activities = @()
foreach ($a in $activitiesRaw) {
    $activities += (Convert-SynapseActivityToAdf -a $a)
}

# Normalize variables now that we have them
$variables = Normalize_Variables -vars $variables

# If there are still no activities, decide based on InjectWaitIfEmpty
if (-not $activities -or ($activities | Measure-Object).Count -eq 0) {
    if ($InjectWaitIfEmpty.IsPresent) {
        $activities = @(
            @{ 
                name = "NoOp_Wait_1s";
                type = "Wait";
                typeProperties = @{ waitTimeInSeconds = 1 };
            }
        )
        Write-Host "ℹ️ No activities found; injected NoOp_Wait_1s due to -InjectWaitIfEmpty." -ForegroundColor Yellow
    }
    else {
        Write-Host "❌ No activities found in Synapse pipeline JSON. See $rawSynPath for details." -ForegroundColor Red
        throw "Synapse pipeline has no activities or fetch returned error object."
    }
}

# Build properties block for ADF schema
$properties = @{
    activities  = $activities
    parameters  = $parameters
    variables   = $variables
    annotations = $annotations
}
if ($description) { $properties.description = $description }

# Diagnostics: log normalized activities types to help troubleshoot mappings
try {
    Write-Host "📄 Normalized activities:" -ForegroundColor DarkCyan
    foreach ($act in $activities) {
        $t = ($act.type ? $act.type : 'unknown')
        $n = ($act.name ? $act.name : 'unnamed')
        Write-Host ("  - {0} :: {1}" -f $n, $t) -ForegroundColor DarkGray
    }
    $activitiesDebugPath = Join-Path $PSScriptRoot ("{0}.activities.debug.json" -f $PipelineName)
    ($activities | ConvertTo-Json -Depth 100) | Out-File -FilePath $activitiesDebugPath -Encoding utf8
    Write-Host ("📝 Wrote activities debug JSON to {0}" -f $activitiesDebugPath) -ForegroundColor DarkGray
} catch { }

# Ensure the ADF pipeline object carries properties
$adfPipeline = @{
    name       = $PipelineName
    properties = $properties
}

# ------------------------------------------------
# Save normalized JSON
# ------------------------------------------------
$tempJsonPath = Join-Path $PSScriptRoot "$PipelineName.adf.json"

# Also build and save properties-only body used for deployment
$propsBody = @{ properties = $properties } | ConvertTo-Json -Depth 100
$propsJsonPath = Join-Path $PSScriptRoot "$PipelineName.adf.properties.json"
$propsBody | Out-File -FilePath $propsJsonPath -Encoding utf8

$adfPipeline |
    ConvertTo-Json -Depth 100 |
    Out-File -FilePath $tempJsonPath -Encoding utf8

Write-Host "✅ ADF-compatible pipeline JSON written to $tempJsonPath" -ForegroundColor Green

# Guard: ensure properties exists (activities is guaranteed above)
if (-not $adfPipeline.properties) {
    Write-Host "❌ Normalized pipeline has no 'properties' block. Cannot deploy to ADF." -ForegroundColor Red
    throw "Normalized ADF pipeline is missing required 'properties'."
}

# ------------------------------------------------
# Deploy into temporary ADF, then call proven ADF migration path
# ------------------------------------------------
Write-Host "🚀 Migrating to Fabric via temporary ADF..." -ForegroundColor Green

try {
    # Ensure temp ADF exists in the same RG/location
    $rg = Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction Stop
    $location = $rg.Location
    $adf = Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $TempAdfName -ErrorAction SilentlyContinue
    if (-not $adf) {
        Write-Host "Creating temporary ADF: $TempAdfName in $location" -ForegroundColor Yellow
        $adf = New-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $TempAdfName -Location $location
    }

    # Ensure placeholder linked service and datasets referenced by activities exist in temp ADF
    function Ensure-AdfLinkedService($rg,$factory,$lsName) {
        Write-Host "Creating/ensuring linked service: $lsName" -ForegroundColor Gray
        $lsObj = @{ name = $lsName; properties = @{ type = "AzureBlobStorage"; typeProperties = @{ connectionString = "DefaultEndpointsProtocol=https;AccountName=placeholder;AccountKey=Kg==;EndpointSuffix=core.windows.net" } } }
        $lsPath = Join-Path $env:TEMP ("{0}.ls.json" -f $lsName)
        ($lsObj | ConvertTo-Json -Depth 50) | Out-File -FilePath $lsPath -Encoding utf8
        try {
            Set-AzDataFactoryV2LinkedService -ResourceGroupName $rg -DataFactoryName $factory -Name $lsName -DefinitionFile $lsPath -Force -ErrorAction Stop | Out-Null
        }
        catch {
            $msg = $_.Exception.Message
            if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $msg = $_.ErrorDetails.Message }
            Write-Host ("LS create via cmdlet failed, trying REST: {0}" -f $msg) -ForegroundColor Yellow
            $uri = ("https://management.azure.com/subscriptions/{0}/resourceGroups/{1}/providers/Microsoft.DataFactory/factories/{2}/linkedservices/{3}?api-version=2018-06-01" -f $SubscriptionId,$rg,$factory,$lsName)
            $headers = Get-ArmHeaders
            $body = $lsObj | ConvertTo-Json -Depth 50
            try { Invoke-RestMethod -Method PUT -Uri $uri -Headers $headers -Body $body -ErrorAction Stop | Out-Null }
            catch {
                $msg2 = $_.Exception.Message
                if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $msg2 = $_.ErrorDetails.Message }
                Write-Host ("LS create failed: {0}" -f $msg2) -ForegroundColor Yellow
            }
        }
    }

    function Get-SynapseDatasetLocation($datasetName) {
        try {
            function Try-Parse-DatasetLocationFrom($tp) {
                if (-not $tp) { return $null }
                $location = $tp.location
                $container = $null; $folderPath = $null; $fileName = $null
                if ($location) {
                    if ($location.container) { $container = [string]$location.container }
                    if ($location.fileSystem) { $container = [string]$location.fileSystem }
                    if ($location.folderPath) { $folderPath = [string]$location.folderPath }
                    if ($location.filePath) { $fp = [string]$location.filePath; $lastSlash = $fp.LastIndexOf('/'); if ($lastSlash -ge 0) { $folderPath = $fp.Substring(0,$lastSlash); $fileName = $fp.Substring($lastSlash+1) } else { $fileName = $fp } }
                    if ($location.fileName) { $fileName = [string]$location.fileName }
                } else {
                    if ($tp.container) { $container = [string]$tp.container }
                    if ($tp.fileSystem) { $container = [string]$tp.fileSystem }
                    if ($tp.folderPath) { $folderPath = [string]$tp.folderPath }
                    if ($tp.fileName) { $fileName = [string]$tp.fileName }
                    if ($tp.filePath) { $fp = [string]$tp.filePath; $lastSlash = $fp.LastIndexOf('/'); if ($lastSlash -ge 0) { $folderPath = $fp.Substring(0,$lastSlash); $fileName = $fp.Substring($lastSlash+1) } else { $fileName = $fp } }
                }
                if ($container -or $folderPath -or $fileName) { return @{ container = $container; folderPath = $folderPath; fileName = $fileName } }
                return $null
            }

            function Try-GetArmDatasetLocation($datasetName) {
                $encodedName = [System.Uri]::EscapeDataString($datasetName)
                $armIds = @(
                    "/subscriptions/$SubscriptionId/resourceGroups/$SynapseResourceGroupName/providers/Microsoft.Synapse/workspaces/$SynapseWorkspaceName/artifacts/datasets/$encodedName",
                    "/subscriptions/$SubscriptionId/resourceGroups/$SynapseResourceGroupName/providers/Microsoft.Synapse/workspaces/$SynapseWorkspaceName/datasets/$encodedName"
                )
                $apiVersions = @("2021-06-01", "2020-12-01")
                foreach ($rid in $armIds) {
                    foreach ($ver in $apiVersions) {
                        # Use subexpression to avoid PowerShell parsing "$rid?api-version" as a variable name
                        $uri = "https://management.azure.com$($rid)?api-version=$ver"
                        try {
                            Write-Host ("ARM GET: {0}" -f $uri) -ForegroundColor DarkGray
                            # Prefer Invoke-AzRestMethod to use the active Az context and a valid ARM token automatically
                            $azResp = Invoke-AzRestMethod -Method GET -Uri $uri -ErrorAction Stop
                            if ($azResp -and $azResp.Content) {
                                $resp = $azResp.Content | ConvertFrom-Json
                                if ($resp -and $resp.properties -and $resp.properties.typeProperties) {
                                    $loc = Try-Parse-DatasetLocationFrom -tp $resp.properties.typeProperties
                                    if ($loc) { Write-Host ("↪ ARM dataset resolved via {0}" -f $ver) -ForegroundColor DarkGray; return $loc }
                                }
                                elseif ($resp -and $resp.error) {
                                    $em = ($resp.error | ConvertTo-Json -Depth 5)
                                    Write-Host ("ARM GET error body ({0}): {1}" -f $ver, $em) -ForegroundColor Yellow
                                }
                            }
                        }
                        catch {
                            $msg = $_.Exception.Message
                            if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $msg = $_.ErrorDetails.Message }
                            # If it's a 404, datasets might not be exposed via ARM in this tenant/version; continue quietly.
                            if ($msg -match 'StatusCode\s*:\s*404' -or $msg -match 'NotFound') {
                                Write-Host ("ARM GET 404 ({0}) for dataset resource; skipping to next variant." -f $ver) -ForegroundColor DarkGray
                            } else {
                                Write-Host ("ARM GET failed ({0}): {1}" -f $ver, $msg) -ForegroundColor Yellow
                            }
                        }
                    }
                }
                return $null
            }

            # 0) Try ARM (management plane) first for reliability
            $armLoc = Try-GetArmDatasetLocation -datasetName $datasetName
            if ($armLoc) { return $armLoc }

            # Prefer Az.Synapse cmdlet (uses current Az context) to avoid 401s
            if (Get-Module -ListAvailable -Name Az.Synapse) {
                try {
                    Import-Module Az.Synapse -ErrorAction Stop | Out-Null
                    $ds = Get-AzSynapseDataset -WorkspaceName $SynapseWorkspaceName -Name $datasetName -ErrorAction Stop
                    if ($ds -and $ds.Properties -and $ds.Properties.TypeProperties) {
                        $tp = $ds.Properties.TypeProperties
                        $loc = Try-Parse-DatasetLocationFrom -tp $tp
                        if ($loc) { return $loc }
                    }
                }
                catch {
                    Write-Host ("ℹ️ Az.Synapse dataset fetch failed for '{0}': {1}. Will try Dev API." -f $datasetName, $_.Exception.Message) -ForegroundColor Yellow
                }
            }

            # Finally, try Synapse Dev API (GET, then LIST as fallback)
            $encodedName = [System.Uri]::EscapeDataString($datasetName)
            $headers = Get-DevApiHeaders
            $devApiVersions = @("2021-06-01", "2020-12-01")
            $tp = $null
            foreach ($v in $devApiVersions) {
                try {
                    $devUrl = "https://$SynapseWorkspaceName.dev.azuresynapse.net/datasets/$encodedName?api-version=$v"
                    $resp = $null
                    try { $resp = Invoke-RestMethod -Method GET -Uri $devUrl -Headers $headers -ErrorAction Stop }
                    catch {
                        $msg = $_.Exception.Message
                        if ($msg -match 'AuthenticationFailed' -or $msg -match 'Token Authentication failed') {
                            Write-Host "ℹ️ Dev API rejected Az token for dataset GET; retrying with Azure CLI token..." -ForegroundColor Yellow
                            try {
                                $headers = Get-DevApiHeaders -UseAzCli
                                $resp = Invoke-RestMethod -Method GET -Uri $devUrl -Headers $headers -ErrorAction Stop
                            } catch {
                                Write-Host ("❌ Dev API dataset GET retry with Azure CLI token failed: {0}" -f $_.Exception.Message) -ForegroundColor Red
                                throw
                            }
                        } else { throw }
                    }
                    if ($resp -and $resp.properties -and $resp.properties.typeProperties) { $tp = $resp.properties.typeProperties; break }
                }
                catch { continue }
            }
            if (-not $tp) {
                foreach ($v in $devApiVersions) {
                    try {
                        $listUrl = "https://$SynapseWorkspaceName.dev.azuresynapse.net/datasets?api-version=$v"
                        $lresp = $null
                        try { $lresp = Invoke-RestMethod -Method GET -Uri $listUrl -Headers $headers -ErrorAction Stop }
                        catch {
                            $msg = $_.Exception.Message
                            if ($msg -match 'AuthenticationFailed' -or $msg -match 'Token Authentication failed') {
                                Write-Host "ℹ️ Dev API rejected Az token for dataset LIST; retrying with Azure CLI token..." -ForegroundColor Yellow
                                try {
                                    $headers = Get-DevApiHeaders -UseAzCli
                                    $lresp = Invoke-RestMethod -Method GET -Uri $listUrl -Headers $headers -ErrorAction Stop
                                } catch {
                                    Write-Host ("❌ Dev API dataset LIST retry with Azure CLI token failed: {0}" -f $_.Exception.Message) -ForegroundColor Red
                                    throw
                                }
                            } else { throw }
                        }
                        $items = $null
                        if ($lresp.value) { $items = $lresp.value } else { $items = $lresp }
                        foreach ($it in $items) {
                            if (($it.name) -and ($it.name.ToString().ToLower() -eq $datasetName.ToLower())) {
                                if ($it.properties -and $it.properties.typeProperties) { $tp = $it.properties.typeProperties }
                                break
                            }
                        }
                        if ($tp) { break }
                    }
                    catch { continue }
                }
            }
            if (-not $tp) { return $null }

            # Common Synapse fields for blob-like datasets
            $location = $tp.location
            $container = $null; $folderPath = $null; $fileName = $null
            if ($location) {
                if ($location.container) { $container = [string]$location.container }
                if ($location.fileSystem) { $container = [string]$location.fileSystem }
                if ($location.folderPath) { $folderPath = [string]$location.folderPath }
                if ($location.filePath) {
                    # filePath may include folder/file; split if present
                    $fp = [string]$location.filePath
                    $lastSlash = $fp.LastIndexOf('/')
                    if ($lastSlash -ge 0) {
                        $folderPath = $fp.Substring(0, $lastSlash)
                        $fileName = $fp.Substring($lastSlash + 1)
                    } else {
                        $fileName = $fp
                    }
                }
                if ($location.fileName) { $fileName = [string]$location.fileName }
            } else {
                # Some Synapse dataset shapes store container/folder/file at top-level typeProperties
                if ($tp.container) { $container = [string]$tp.container }
                if ($tp.fileSystem) { $container = [string]$tp.fileSystem }
                if ($tp.folderPath) { $folderPath = [string]$tp.folderPath }
                if ($tp.fileName) { $fileName = [string]$tp.fileName }
                if ($tp.filePath) {
                    $fp = [string]$tp.filePath
                    $lastSlash = $fp.LastIndexOf('/')
                    if ($lastSlash -ge 0) {
                        $folderPath = $fp.Substring(0, $lastSlash)
                        $fileName = $fp.Substring($lastSlash + 1)
                    } else {
                        $fileName = $fp
                    }
                }
            }
            if (-not $container -and -not $folderPath -and -not $fileName) { return $null }
            return @{ container = $container; folderPath = $folderPath; fileName = $fileName }
        }
        catch {
            Write-Host ("⚠️ Could not fetch Synapse dataset '{0}' details: {1}" -f $datasetName, $_.Exception.Message) -ForegroundColor Yellow
            return $null
        }
    }

    function Load-SynapseDatasetLocationsMap {
        $map = @{}
        try {
            if (Get-Module -ListAvailable -Name Az.Synapse) {
                try {
                    Import-Module Az.Synapse -ErrorAction Stop | Out-Null
                    $all = Get-AzSynapseDataset -WorkspaceName $SynapseWorkspaceName -ErrorAction Stop
                    foreach ($ds in $all) {
                        $name = $ds.Name
                        if (-not $name) { continue }
                        $tp = $ds.Properties.TypeProperties
                        if (-not $tp) { continue }
                        $location = $tp.location
                        $container = $null; $folderPath = $null; $fileName = $null
                        if ($location) {
                            if ($location.container) { $container = [string]$location.container }
                            if ($location.fileSystem) { $container = [string]$location.fileSystem }
                            if ($location.folderPath) { $folderPath = [string]$location.folderPath }
                            if ($location.filePath) { $fp = [string]$location.filePath; $lastSlash = $fp.LastIndexOf('/'); if ($lastSlash -ge 0) { $folderPath = $fp.Substring(0,$lastSlash); $fileName = $fp.Substring($lastSlash+1) } else { $fileName = $fp } }
                            if ($location.fileName) { $fileName = [string]$location.fileName }
                        } else {
                            if ($tp.container) { $container = [string]$tp.container }
                            if ($tp.fileSystem) { $container = [string]$tp.fileSystem }
                            if ($tp.folderPath) { $folderPath = [string]$tp.folderPath }
                            if ($tp.fileName) { $fileName = [string]$tp.fileName }
                            if ($tp.filePath) { $fp = [string]$tp.filePath; $lastSlash = $fp.LastIndexOf('/'); if ($lastSlash -ge 0) { $folderPath = $fp.Substring(0,$lastSlash); $fileName = $fp.Substring($lastSlash+1) } else { $fileName = $fp } }
                        }
                        if ($container -or $folderPath -or $fileName) {
                            $map[$name.ToLower()] = @{ container = $container; folderPath = $folderPath; fileName = $fileName }
                        }
                    }
                }
                catch {
                    Write-Host ("ℹ️ Listing datasets via Az.Synapse failed: {0}. Will try Dev API." -f $_.Exception.Message) -ForegroundColor Yellow
                }
            }
            if ($map.Count -eq 0) {
                # ARM bulk list is not straightforward for datasets; fall back to Dev API list (prefer CLI token)
                try {
                    $headers = Get-DevApiHeaders -UseAzCli
                } catch {
                    $headers = Get-DevApiHeaders
                }
                $apiVers = @("2021-06-01","2020-12-01")
                foreach ($ver in $apiVers) {
                    try {
                        $listUrl = "https://$SynapseWorkspaceName.dev.azuresynapse.net/datasets?api-version=$ver"
                        $resp = Invoke-RestMethod -Method GET -Uri $listUrl -Headers $headers -ErrorAction Stop
                        $items = $null
                        if ($resp.value) { $items = $resp.value } else { $items = $resp }
                        foreach ($it in $items) {
                            $name = $it.name
                            if (-not $name) { continue }
                            $props = $it.properties
                            if (-not $props) { continue }
                            $tp = $props.typeProperties
                            if (-not $tp) { continue }
                            $location = $tp.location
                            $container = $null; $folderPath = $null; $fileName = $null
                            if ($location) {
                                if ($location.container) { $container = [string]$location.container }
                                if ($location.fileSystem) { $container = [string]$location.fileSystem }
                                if ($location.folderPath) { $folderPath = [string]$location.folderPath }
                                if ($location.filePath) { $fp = [string]$location.filePath; $lastSlash = $fp.LastIndexOf('/'); if ($lastSlash -ge 0) { $folderPath = $fp.Substring(0,$lastSlash); $fileName = $fp.Substring($lastSlash+1) } else { $fileName = $fp } }
                                if ($location.fileName) { $fileName = [string]$location.fileName }
                            } else {
                                if ($tp.container) { $container = [string]$tp.container }
                                if ($tp.fileSystem) { $container = [string]$tp.fileSystem }
                                if ($tp.folderPath) { $folderPath = [string]$tp.folderPath }
                                if ($tp.fileName) { $fileName = [string]$tp.fileName }
                                if ($tp.filePath) { $fp = [string]$tp.filePath; $lastSlash = $fp.LastIndexOf('/'); if ($lastSlash -ge 0) { $folderPath = $fp.Substring(0,$lastSlash); $fileName = $fp.Substring($lastSlash+1) } else { $fileName = $fp } }
                            }
                            if ($container -or $folderPath -or $fileName) {
                                $map[$name.ToLower()] = @{ container = $container; folderPath = $folderPath; fileName = $fileName }
                            }
                        }
                        if ($map.Count -gt 0) { break }
                    } catch {
                        $em = $_.Exception.Message
                        Write-Host ("ℹ️ Dev API dataset LIST failed for {0}: {1}" -f $ver, $em) -ForegroundColor Yellow
                        continue
                    }
                }
            }
        }
        catch {
            Write-Host ("⚠️ Could not list Synapse datasets: {0}" -f $_.Exception.Message) -ForegroundColor Yellow
        }
        return $map
    }

    $synDsMap = Load-SynapseDatasetLocationsMap

    # Build explicit overrides map from parameters if provided
    $overrideMap = @{}
    if ($SourceDatasetName -and ($SourceContainer -or $SourceFolderPath -or $SourceFileName)) {
        $overrideMap[$SourceDatasetName.ToLower()] = @{ container = $SourceContainer; folderPath = $SourceFolderPath; fileName = $SourceFileName }
        Write-Host ("⬆ Using explicit override for source dataset '{0}'" -f $SourceDatasetName) -ForegroundColor Yellow
    }
    if ($SinkDatasetName -and ($SinkContainer -or $SinkFolderPath -or $SinkFileName)) {
        $overrideMap[$SinkDatasetName.ToLower()] = @{ container = $SinkContainer; folderPath = $SinkFolderPath; fileName = $SinkFileName }
        Write-Host ("⬆ Using explicit override for sink dataset '{0}'" -f $SinkDatasetName) -ForegroundColor Yellow
    }

    function Derive-Location-From-Activities($datasetName) {
        function Get-PropsCI($obj) { if (-not $obj) { return @() } return $obj.PSObject.Properties }
        function Get-ChildValues($obj) { if (-not $obj) { return @() } return ($obj.PSObject.Properties | ForEach-Object { $_.Value }) }
        function Collect-LocationFromObj($o,[ref]$acc) {
            if (-not $o) { return }
            foreach ($p in (Get-PropsCI $o)) {
                $n = ($p.Name + '').ToLower()
                $v = $p.Value
                if (-not $v) { continue }
                switch ($n) {
                    'filepath' { $fp = [string]$v; $parts = $fp.Split('/'); if ($parts.Length -ge 1 -and -not $acc.Value.container) { $acc.Value.container = $parts[0] }; if ($parts.Length -ge 2) { if ($parts.Length -gt 2 -and -not $acc.Value.folderPath) { $acc.Value.folderPath = [string]::Join('/', $parts[1..($parts.Length-2)]) }; if (-not $acc.Value.fileName) { $acc.Value.fileName = $parts[$parts.Length-1] } } elseif ($parts.Length -eq 1 -and -not $acc.Value.fileName) { $acc.Value.fileName = $parts[0] } }
                    'path'     { $fp = [string]$v; $parts = $fp.Split('/'); if ($parts.Length -ge 1 -and -not $acc.Value.container) { $acc.Value.container = $parts[0] }; if ($parts.Length -ge 2) { if ($parts.Length -gt 2 -and -not $acc.Value.folderPath) { $acc.Value.folderPath = [string]::Join('/', $parts[1..($parts.Length-2)]) }; if (-not $acc.Value.fileName) { $acc.Value.fileName = $parts[$parts.Length-1] } } }
                    'folderpath' { if (-not $acc.Value.folderPath) { $acc.Value.folderPath = [string]$v } }
                    'filename'   { if (-not $acc.Value.fileName) { $acc.Value.fileName = [string]$v } }
                    'container'  { if (-not $acc.Value.container) { $acc.Value.container = [string]$v } }
                    'filesystem' { if (-not $acc.Value.container) { $acc.Value.container = [string]$v } }
                    default {
                        # Recurse into nested objects (e.g., location)
                        if ($v -is [System.Management.Automation.PSObject] -or $v -is [hashtable]) {
                            Collect-LocationFromObj -o $v -acc ([ref]$acc.Value)
                        }
                    }
                }
            }
        }

        $dnLower = $datasetName.ToLower()
        foreach ($orig in $activitiesRaw) {
            $inRefs = @(); if ($orig.Inputs) { $inRefs = @($orig.Inputs | ForEach-Object { $_.ReferenceName }) }
            $outRefs = @(); if ($orig.Outputs) { $outRefs = @($orig.Outputs | ForEach-Object { $_.ReferenceName }) }
            $matches = $false
            foreach ($r in $inRefs + $outRefs) { if ($r -and ($r.ToString().ToLower() -eq $dnLower)) { $matches = $true; break } }
            if (-not $matches) { continue }

            $candidates = @()
            if ($orig.Source) { $candidates += $orig.Source }
            if ($orig.Sink)   { $candidates += $orig.Sink }
            foreach ($cand in $candidates) {
                $acc = @{ container = $null; folderPath = $null; fileName = $null }
                Collect-LocationFromObj -o $cand -acc ([ref]$acc)
                if ($acc.container -or $acc.folderPath -or $acc.fileName) {
                    Write-Host ("↪ Derived location from activity for dataset '{0}': container={1}, folder={2}, file={3}" -f $datasetName, $acc.container, $acc.folderPath, $acc.fileName) -ForegroundColor DarkGray
                    return $acc
                }
            }
        }
        return $null
    }

    function Save-SynapseDatasetJson($datasetName) {
        $encodedName = [System.Uri]::EscapeDataString($datasetName)
        $outPath = Join-Path $PSScriptRoot ("{0}.synapse.dataset.json" -f $datasetName)
        # Try ARM then Dev, then Az.Synapse
        try {
            $armUrl = "https://management.azure.com/subscriptions/$SubscriptionId/resourceGroups/$SynapseResourceGroupName/providers/Microsoft.Synapse/workspaces/$SynapseWorkspaceName/artifacts/datasets/$encodedName?api-version=2020-12-01"
            $armDs = Invoke-AzRestMethod -Method GET -Uri $armUrl -ErrorAction Stop
            if ($armDs -and $armDs.Content) {
                ($armDs.Content) | Out-File -FilePath $outPath -Encoding utf8
                Write-Host ("💾 Saved dataset JSON via ARM: {0}" -f $datasetName) -ForegroundColor Gray
                return
            }
        } catch {}
        try {
            $devUrl = "https://$SynapseWorkspaceName.dev.azuresynapse.net/datasets/$encodedName?api-version=2020-12-01"
            $token = (Get-AzAccessToken -ResourceUrl "https://dev.azuresynapse.net").Token
            $headers = @{ Authorization = "Bearer $token" }
            $devDs = Invoke-RestMethod -Method GET -Uri $devUrl -Headers $headers -ErrorAction Stop
            if ($devDs) {
                ($devDs | ConvertTo-Json -Depth 100) | Out-File -FilePath $outPath -Encoding utf8
                Write-Host ("💾 Saved dataset JSON via Dev API: {0}" -f $datasetName) -ForegroundColor Gray
                return
            }
        } catch {}
        try {
            if (Get-Module -ListAvailable -Name Az.Synapse) {
                $ds = Get-AzSynapseDataset -WorkspaceName $SynapseWorkspaceName -Name $datasetName -ErrorAction Stop
                if ($ds) {
                    ($ds | ConvertTo-Json -Depth 100) | Out-File -FilePath $outPath -Encoding utf8
                    Write-Host ("💾 Saved dataset JSON via Az.Synapse: {0}" -f $datasetName) -ForegroundColor Gray
                }
            }
        } catch {}
    }

    function Ensure-AdfDataset($rg,$factory,$dsName,$lsName) {
        Write-Host "Creating/ensuring dataset: $dsName" -ForegroundColor Gray
        $loc = $null
        if ($overrideMap.ContainsKey($dsName.ToLower())) {
            $loc = $overrideMap[$dsName.ToLower()]
            Write-Host ("↪ Using explicit override for dataset '{0}'" -f $dsName) -ForegroundColor Yellow
        }
        elseif ($synDsMap.ContainsKey($dsName.ToLower())) {
            $loc = $synDsMap[$dsName.ToLower()]
            Write-Host ("↪ Using Synapse dataset location for '{0}' from preloaded map" -f $dsName) -ForegroundColor DarkGray
        }
        else {
            $loc = Get-SynapseDatasetLocation -datasetName $dsName
            if (-not $loc) { $loc = Derive-Location-From-Activities -datasetName $dsName }
        }
        if ($loc) {
            $useFs = $false
            $containerVal = $null
            if ($loc.container) { $containerVal = $loc.container }
            if (-not $containerVal -and $loc.folderPath) { $containerVal = "placeholder" }
            # Heuristic: if original dataset had fileSystem (captured into container when present), prefer AzureBlobFSLocation
            # We can't perfectly detect here, so switch to FS when folderPath is present and container name likely a filesystem.
            if ($loc.container -and ($loc.container -match '^[a-z0-9-]+$')) { $useFs = $false }
            $locationObj = @{ }
            if ($useFs) {
                $locationObj.type = "AzureBlobFSLocation"
                $locationObj.fileSystem = ($containerVal ? $containerVal : "placeholder")
            } else {
                $locationObj.type = "AzureBlobStorageLocation"
                $locationObj.container = ($containerVal ? $containerVal : "placeholder")
            }
            if ($loc.folderPath) { $locationObj.folderPath = $loc.folderPath }
            if ($loc.fileName) { $locationObj.fileName = $loc.fileName }
            $dsObj = @{ 
                name = $dsName; 
                properties = @{ 
                    type = "DelimitedText"; 
                    linkedServiceName = @{ referenceName = $lsName; type = "LinkedServiceReference" }; 
                    typeProperties = @{ 
                        location = $locationObj; 
                        firstRowAsHeader = $true; 
                        columnDelimiter = "," 
                    } 
                } 
            }
            Write-Host ("✔ Resolved dataset '{0}' location → type={1}, container/fs={2}, folder={3}, file={4}" -f $dsName, $locationObj.type, ($locationObj.container?$locationObj.container:$locationObj.fileSystem), ($locationObj.folderPath), ($locationObj.fileName)) -ForegroundColor Green
        }
        else {
            # Try parameterized dataset pattern based on activity-supplied parameters
            $p = Get-ActivityParamsForDataset -datasetName $dsName
            if ($p.Count -gt 0) {
                $paramDefs = @{}
                $tpLoc = @{}
                $isFs = $false
                foreach ($k in $p.Keys) {
                    $kk = $k.ToString()
                    $paramDefs[$kk] = @{ type = "String"; defaultValue = [string]$p[$kk] }
                }
                if ($p.ContainsKey('fileSystem')) { $isFs = $true; $tpLoc.fileSystem = "@{dataset().fileSystem}" }
                if ($p.ContainsKey('container')) { $tpLoc.container = "@{dataset().container}" }
                if ($p.ContainsKey('folderPath')) { $tpLoc.folderPath = "@{dataset().folderPath}" }
                if ($p.ContainsKey('fileName')) { $tpLoc.fileName = "@{dataset().fileName}" }
                if ($p.ContainsKey('filePath')) {
                    # Split default filePath into folder/file defaults
                    $fp = [string]$p['filePath']
                    $lastSlash = $fp.LastIndexOf('/')
                    if ($lastSlash -ge 0) {
                        if (-not $p.ContainsKey('folderPath')) { $paramDefs['folderPath'] = @{ type = "String"; defaultValue = $fp.Substring(0,$lastSlash) } }
                        if (-not $p.ContainsKey('fileName')) { $paramDefs['fileName'] = @{ type = "String"; defaultValue = $fp.Substring($lastSlash+1) } }
                        $tpLoc.folderPath = "@{dataset().folderPath}"; $tpLoc.fileName = "@{dataset().fileName}"
                    } else {
                        if (-not $p.ContainsKey('fileName')) { $paramDefs['fileName'] = @{ type = "String"; defaultValue = $fp } }
                        $tpLoc.fileName = "@{dataset().fileName}"
                    }
                }
                $locObj = if ($isFs) { @{ type = "AzureBlobFSLocation" } } else { @{ type = "AzureBlobStorageLocation" } }
                foreach ($prop in $tpLoc.GetEnumerator()) { $locObj[$prop.Key] = $prop.Value }
                $dsObj = @{ 
                    name = $dsName; 
                    properties = @{ 
                        type = "DelimitedText"; 
                        parameters = $paramDefs; 
                        linkedServiceName = @{ referenceName = $lsName; type = "LinkedServiceReference" }; 
                        typeProperties = @{ 
                            location = $locObj; 
                            firstRowAsHeader = $true; 
                            columnDelimiter = "," 
                        } 
                    } 
                }
                Write-Host ("✔ Built parameterized dataset for '{0}' from activity parameters" -f $dsName) -ForegroundColor Green
            }
            else {
                $dsObj = @{ 
                    name = $dsName; 
                    properties = @{ 
                        type = "DelimitedText"; 
                        linkedServiceName = @{ referenceName = $lsName; type = "LinkedServiceReference" }; 
                        typeProperties = @{ 
                            location = @{ type = "AzureBlobStorageLocation"; container = "placeholder"; fileName = "placeholder.txt" }; 
                            firstRowAsHeader = $true; 
                            columnDelimiter = "," 
                        } 
                    } 
                }
                Write-Host ("⚠️ Using placeholder location for dataset '{0}'" -f $dsName) -ForegroundColor Yellow
            }
        }
        $dsPath = Join-Path $env:TEMP ("{0}.ds.json" -f $dsName)
        ($dsObj | ConvertTo-Json -Depth 50) | Out-File -FilePath $dsPath -Encoding utf8
        try {
            Set-AzDataFactoryV2Dataset -ResourceGroupName $rg -DataFactoryName $factory -Name $dsName -DefinitionFile $dsPath -Force -ErrorAction Stop | Out-Null
        }
        catch {
            $msg = $_.Exception.Message
            if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $msg = $_.ErrorDetails.Message }
            Write-Host ("DS create via cmdlet failed, trying REST: {0}" -f $msg) -ForegroundColor Yellow
            $encoded = [System.Uri]::EscapeDataString($dsName)
            $armUri = ("https://management.azure.com/subscriptions/{0}/resourceGroups/{1}/providers/Microsoft.DataFactory/factories/{2}/datasets/{3}?api-version=2018-06-01" -f $SubscriptionId,$rg,$factory,$encoded)
            $headers = Get-ArmHeaders
            $body = $dsObj | ConvertTo-Json -Depth 50
            try { Invoke-RestMethod -Method PUT -Uri $armUri -Headers $headers -Body $body -ErrorAction Stop | Out-Null }
            catch {
                $msg2 = $_.Exception.Message
                if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $msg2 = $_.ErrorDetails.Message }
                Write-Host ("DS create failed for {0}: {1}" -f $dsName, $msg2) -ForegroundColor Yellow
            }
        }
    }

    $referencedDatasets = New-Object System.Collections.Generic.HashSet[string]
    foreach ($a in $activities) {
        if ($a.inputs)  { foreach ($i in $a.inputs)  { if ($i.referenceName) { [void]$referencedDatasets.Add($i.referenceName) } } }
        if ($a.outputs) { foreach ($o in $a.outputs) { if ($o.referenceName) { [void]$referencedDatasets.Add($o.referenceName) } } }
    }
    if ($referencedDatasets.Count -gt 0) {
        # Prefer creating/using the actual ADF LS name so resolutions can map it directly in Fabric
        $preferredLsName = "AzureBlobStorage1"
        Ensure-AdfLinkedService -rg $ResourceGroupName -factory $TempAdfName -lsName $preferredLsName
        foreach ($ds in $referencedDatasets) {
            Ensure-AdfDataset -rg $ResourceGroupName -factory $TempAdfName -dsName $ds -lsName $preferredLsName
            $lower = $ds.ToLower()
            if ($lower -ne $ds) { Ensure-AdfDataset -rg $ResourceGroupName -factory $TempAdfName -dsName $lower -lsName $preferredLsName }
        }
        # Brief delay and list to confirm
        Start-Sleep -Seconds 3
        try {
            $listUri = "https://management.azure.com/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$TempAdfName/datasets?api-version=2018-06-01"
            $t2 = (Get-AzAccessToken -ResourceUrl "https://management.azure.com/").Token
            $h2 = @{ Authorization = "Bearer $t2" }
            $dsList = Invoke-RestMethod -Method GET -Uri $listUri -Headers $h2 -ErrorAction Stop
            $names = @()
            if ($dsList.value) { $names = $dsList.value.name }
            Write-Host ("Datasets present: " + ($names -join ", ")) -ForegroundColor Gray
        } catch {}
    }

    # First try official cmdlet with a JSON file (ADF accepts name+properties shape)
    try {
        Set-AzDataFactoryV2Pipeline `
            -ResourceGroupName $ResourceGroupName `
            -DataFactoryName $TempAdfName `
            -Name $PipelineName `
            -DefinitionFile $tempJsonPath `
            -Force `
            -ErrorAction Stop | Out-Null
    }
    catch {
        Write-Host "❌ Set-AzDataFactoryV2Pipeline failed: $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "Attempting REST fallback to create pipeline..." -ForegroundColor Yellow
        $encodedName = [System.Uri]::EscapeDataString($PipelineName)
        $armUri = ("https://management.azure.com/subscriptions/{0}/resourceGroups/{1}/providers/Microsoft.DataFactory/factories/{2}/pipelines/{3}?api-version=2018-06-01" -f $SubscriptionId,$ResourceGroupName,$TempAdfName,$encodedName)
        $headers = Get-ArmHeaders
        Write-Host ("Pipeline REST PUT URI: " + $armUri) -ForegroundColor Gray
        $fullBody = @{ name = $PipelineName; properties = $properties } | ConvertTo-Json -Depth 100
        try {
            $null = Invoke-RestMethod -Method PUT -Uri $armUri -Headers $headers -Body $fullBody -ErrorAction Stop
        }
        catch {
            $msg = $_.Exception.Message
            if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $msg = $_.ErrorDetails.Message }
            Write-Host ("Pipeline REST PUT failed: {0}" -f $msg) -ForegroundColor Yellow
            throw
        }
        Write-Host "REST PUT completed." -ForegroundColor Gray
    }

    # Confirm pipeline now exists before proceeding (with retry for propagation)
    $deployed = $null
    for ($i = 0; $i -lt 6; $i++) {
        $deployed = Get-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $TempAdfName -Name $PipelineName -ErrorAction SilentlyContinue
        if ($deployed) { break }
        Start-Sleep -Seconds 5
    }
    if (-not $deployed) {
        # List pipelines for diagnostics via SDK
        $all = Get-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $TempAdfName -ErrorAction SilentlyContinue
        $count = if ($all) { ($all | Measure-Object).Count } else { 0 }
        Write-Host "Pipelines present in temp ADF (SDK): $count" -ForegroundColor Yellow
        # Also list via REST for parity
        try {
            $listArmId = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$TempAdfName/pipelines?api-version=2018-06-01"
            $listUri = "https://management.azure.com$listArmId"
            $ltoken = (Get-AzAccessToken -ResourceUrl "https://management.azure.com/").Token
            $lheaders = @{ Authorization = "Bearer $ltoken" }
            $listResp = Invoke-RestMethod -Method GET -Uri $listUri -Headers $lheaders -ErrorAction Stop
            Write-Host "Pipelines list (REST) content: $([string]$listResp)" -ForegroundColor DarkGray
        } catch {}
        throw "Pipeline '$PipelineName' was not found after deployment to ADF '$TempAdfName'."
    }

    # Call existing, proven ADF -> Fabric migration script
    & "$PSScriptRoot\adf_to_fabric_migration.ps1" `
        -FabricWorkspaceId $FabricWorkspaceId `
        -ResolutionsFile $ResolutionsFile `
        -Region $Region `
        -SubscriptionId $SubscriptionId `
        -ResourceGroupName $ResourceGroupName `
        -DataFactoryName $TempAdfName `
        -PipelineNames $PipelineName

    if ($CleanupTempAdf.IsPresent) {
        Write-Host "🧹 Deleting temporary ADF '$TempAdfName'..." -ForegroundColor Yellow
        try {
            Remove-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $TempAdfName -Force -ErrorAction Stop
            Write-Host "✔ Temporary ADF deleted: $TempAdfName" -ForegroundColor Green
        }
        catch {
            Write-Host "⚠️ Failed to delete temporary ADF '$TempAdfName': $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
}
catch {
    Write-Host "❌ Temporary ADF deployment or migration failed: $($_.Exception.Message)" -ForegroundColor Red
    throw
}

Write-Host "✅ Synapse → Fabric migration completed successfully." -ForegroundColor Green
