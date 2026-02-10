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
    [switch]$InjectWaitIfEmpty
)

Write-Host "🔐 Ensuring Azure login..." -ForegroundColor Yellow
if (-not (Get-AzContext)) {
    Connect-AzAccount | Out-Null
}
Select-AzSubscription -SubscriptionId $SubscriptionId | Out-Null

# Helper: ARM auth headers using active tenant
function Get-ArmHeaders {
    $ctx = Get-AzContext
    if (-not $ctx) { throw "No Az context. Please login." }
    $tenantId = $ctx.Tenant.Id
    $tok = (Get-AzAccessToken -ResourceUrl "https://management.azure.com/" -TenantId $tenantId).Token
    return @{ Authorization = "Bearer $tok"; "Content-Type" = "application/json" }
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
            ($spObj | ConvertTo-Json -Depth 100) | Out-File -FilePath $rawSynPath -Encoding utf8
            $synapsePipeline = $spObj
            Write-Host "✔ Retrieved Synapse pipeline via Az.Synapse." -ForegroundColor Green
        }
    }
    catch {
        Write-Host "ℹ️ Az.Synapse fetch did not succeed: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

# 2) Try ARM management-plane (works with standard ARM token and RBAC)
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
            foreach ($out in $a.Outputs) {
                $outputs += @{ referenceName = $out.ReferenceName; type = 'DatasetReference'; parameters = ($out.Parameters ? $out.Parameters : @{}) }
            }
        }
        $typeProps = @{}
        if ($a.Source) { $typeProps.source = $a.Source }
        if ($a.Sink)   { $typeProps.sink   = $a.Sink }
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

$properties = @{
    activities  = $activities
    parameters  = $parameters
    variables   = $variables
    annotations = $annotations
}
if ($description) { $properties.description = $description }

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

    function Ensure-AdfDataset($rg,$factory,$dsName,$lsName) {
        Write-Host "Creating/ensuring dataset: $dsName" -ForegroundColor Gray
        $dsObj = @{ name = $dsName; properties = @{ type = "DelimitedText"; linkedServiceName = @{ referenceName = $lsName; type = "LinkedServiceReference" }; typeProperties = @{ location = @{ type = "AzureBlobStorageLocation"; container = "placeholder"; fileName = "placeholder.txt" } } } }
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
        $tempLsName = "temp-synapse-migration-ls"
        Ensure-AdfLinkedService -rg $ResourceGroupName -factory $TempAdfName -lsName $tempLsName
        foreach ($ds in $referencedDatasets) {
            Ensure-AdfDataset -rg $ResourceGroupName -factory $TempAdfName -dsName $ds -lsName $tempLsName
            $lower = $ds.ToLower()
            if ($lower -ne $ds) { Ensure-AdfDataset -rg $ResourceGroupName -factory $TempAdfName -dsName $lower -lsName $tempLsName }
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
}
catch {
    Write-Host "❌ Temporary ADF deployment or migration failed: $($_.Exception.Message)" -ForegroundColor Red
    throw
}

Write-Host "✅ Synapse → Fabric migration completed successfully." -ForegroundColor Green
