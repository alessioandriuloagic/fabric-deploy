# =============================================================
# deploy.ps1 - Deploy modulare soluzione Fabric
# Compatibile con PowerShell 5.1 (Windows runner DevOps)
#
# TUTTI i secret codificati Base64. Pipeline JSON con ConvertTo-Json.
# Validazione anti-$(VAR) per variabili DevOps non risolte.
# -ForceRecreate: elimina e ricrea pipeline (fix cached definitions)
# =============================================================
param(
    [Parameter(Mandatory=$true)][string]$TenantId,
    [Parameter(Mandatory=$true)][string]$ClientId,
    [Parameter(Mandatory=$true)][string]$ClientSecret,
    [Parameter(Mandatory=$true)][string]$WorkspaceId,

    [string]$Connectors = "BC",
    [switch]$ForceRecreate,

    [string]$BcTenantId     = "",
    [string]$BcEnvironment  = "SandboxTest",
    [string]$BcCompanies    = 'CRONUS%20IT',
    [string]$BcEntities     = 'ItemLedgerEntries',
    [string]$BcPythonFile   = "bc_sync.py",

    [string]$CrmTenantId    = "",
    [string]$CrmClientId    = "",
    [string]$CrmClientSecret = "",
    [string]$CrmOrgUrl      = "",
    [string]$CrmApiVersion  = "v9.2",
    [string]$CrmEntities    = 'accounts',
    [string]$CrmPythonFile  = "crm_sync.py",

    [string]$LakehouseName      = "LH_Bronze",
    [string]$BcMirroringDbName  = "MirrorDB_BC",
    [string]$BcSparkJobName     = "SJD_BC_Sync",
    [string]$BcPipelineName     = "DP_BC_Sync",
    [string]$CrmMirroringDbName = "MirrorDB_CRM",
    [string]$CrmSparkJobName    = "SJD_CRM_Sync",
    [string]$CrmPipelineName    = "DP_CRM_Sync",

    [string]$PythonFileName = ""
)

$ErrorActionPreference = "Stop"

# ── Normalizza valori JSON-array ──────────────────────────────────────
# Le variabili DevOps NON devono contenere brackets [] perche' YAML li
# corrompe. Lo script accetta valori semplici ("val") o CSV ("a,b") e
# li converte in JSON array '["val"]' / '["a","b"]'.
# Se il valore contiene gia' '[' viene lasciato invariato (retrocompat.).
function Normalize-JsonArray([string]$Raw) {
    $v = $Raw.Trim()
    if ($v.StartsWith('[')) { return $v }                       # gia' JSON
    $items = $v.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }
    $inner = ($items | ForEach-Object { "`"$_`"" }) -join ','
    return "[$inner]"
}

$BcCompanies  = Normalize-JsonArray $BcCompanies
$BcEntities   = Normalize-JsonArray $BcEntities
$CrmEntities  = Normalize-JsonArray $CrmEntities

Write-Host "[NORM] BcCompanies  = $BcCompanies"
Write-Host "[NORM] BcEntities   = $BcEntities"
Write-Host "[NORM] CrmEntities  = $CrmEntities"

# ── Helper: controlla se un valore e' una variabile DevOps non risolta ──
function Assert-NotUnresolved([string]$Name, [string]$Value) {
    if ($Value -match '^\$\(') {
        Write-Host "##[error] '$Name' contiene variabile DevOps non risolta: $Value"
        Write-Host "##[error] Aggiungere '$($Value -replace '[\$\(\)]','')' al Variable Group in Azure DevOps."
        exit 1
    }
}

$connectorList = $Connectors.Split(",") | ForEach-Object { $_.Trim().ToUpper() }
Write-Host ""
Write-Host "============================================"
Write-Host "[CONFIG] Connettori: $($connectorList -join ', ')"
if ($ForceRecreate) { Write-Host "[CONFIG] ForceRecreate: ON" }
Write-Host "============================================"

# Valida variabili obbligatorie
Assert-NotUnresolved "TenantId"     $TenantId
Assert-NotUnresolved "ClientId"     $ClientId
Assert-NotUnresolved "ClientSecret" $ClientSecret
Assert-NotUnresolved "WorkspaceId"  $WorkspaceId

foreach ($conn in $connectorList) {
    switch ($conn) {
        "BC" {
            Assert-NotUnresolved "BcTenantId" $BcTenantId
            if ([string]::IsNullOrEmpty($BcTenantId)) { Write-Host "##[error] BcTenantId mancante."; exit 1 }
        }
        "CRM" {
            Assert-NotUnresolved "CrmOrgUrl"      $CrmOrgUrl
            Assert-NotUnresolved "CrmTenantId"    $CrmTenantId
            Assert-NotUnresolved "CrmClientId"    $CrmClientId
            Assert-NotUnresolved "CrmClientSecret" $CrmClientSecret
            Assert-NotUnresolved "CrmEntities"    $CrmEntities

            if ([string]::IsNullOrEmpty($CrmOrgUrl)) { Write-Host "##[error] CrmOrgUrl mancante."; exit 1 }

            # Default: usa credenziali Fabric se non specificate per CRM
            if ([string]::IsNullOrEmpty($CrmTenantId) -or $CrmTenantId -eq "")    { $CrmTenantId    = $TenantId }
            if ([string]::IsNullOrEmpty($CrmClientId) -or $CrmClientId -eq "")    { $CrmClientId    = $ClientId }
            if ([string]::IsNullOrEmpty($CrmClientSecret) -or $CrmClientSecret -eq ""){ $CrmClientSecret = $ClientSecret }
        }
        default { Write-Host "##[error] Connettore sconosciuto: $conn"; exit 1 }
    }
}

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────
function To-Base64([string]$str) {
    return [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($str))
}

function Invoke-FabricApi {
    param([string]$Method, [string]$Url, [hashtable]$Headers, [string]$Body = $null)
    $params = @{ Method = $Method; Uri = $Url; Headers = $Headers }
    if ($Body) { $params["Body"] = $Body }
    try {
        $response = Invoke-WebRequest @params -UseBasicParsing
    } catch {
        $ex = $_.Exception
        if ($ex.Response -ne $null) {
            $stream = $ex.Response.GetResponseStream()
            $reader = New-Object System.IO.StreamReader($stream)
            $reader.BaseStream.Position = 0
            Write-Host "##[error] API Response Body: $($reader.ReadToEnd())"
        } else { Write-Host "##[error] API Error: $($ex.Message)" }
        throw
    }
    if ($response.StatusCode -eq 202) {
        $locationUrl = $response.Headers["Location"]
        $retryAfterRaw = $response.Headers["Retry-After"]
        $retryAfter = if ($retryAfterRaw) { [int]$retryAfterRaw } else { 5 }
        Write-Host "    [ASYNC] Polling ogni ${retryAfter}s..."
        do { Start-Sleep -Seconds $retryAfter
             $pollResp = Invoke-WebRequest -Uri $locationUrl -Headers $Headers -Method GET -UseBasicParsing
        } while ($pollResp.StatusCode -eq 202)
        return $pollResp.Content | ConvertFrom-Json
    }
    return $response.Content | ConvertFrom-Json
}

function Get-OrCreate-Item {
    param([string]$DisplayName, [string]$Type, [string]$CreateUrl, [string]$ListUrl, [string]$BodyJson, [hashtable]$Headers)
    $existing = (Invoke-RestMethod -Uri $ListUrl -Headers $Headers -Method GET).value `
                | Where-Object { $_.displayName -eq $DisplayName -and $_.type -eq $Type }
    if ($existing) {
        Write-Host "  [OK] '$DisplayName' ($Type) gia esistente - ID: $($existing.id)"
        return $existing.id
    }
    Write-Host "  [NEW] Creo '$DisplayName' ($Type)..."
    $result = Invoke-FabricApi -Method POST -Url $CreateUrl -Headers $Headers -Body $BodyJson
    Write-Host "  [OK] Creato - ID: $($result.id)"
    return $result.id
}

function Get-OrCreate-MirroringDb {
    param([string]$DisplayName, [string]$Description, [hashtable]$Headers, [string]$BaseUrl)
    $mirroringPayload = @{
        properties = @{
            source = @{ type = "GenericMirror"; typeProperties = @{} }
            target = @{
                type = "MountedRelationalDatabase"
                typeProperties = @{ defaultSchema = "dbo"; format = "Delta" }
            }
        }
    } | ConvertTo-Json -Depth 10

    $mirroringBody = @{
        displayName = $DisplayName; description = $Description
        definition = @{
            parts = @(@{ path = "mirroring.json"; payload = (To-Base64 $mirroringPayload); payloadType = "InlineBase64" })
        }
    } | ConvertTo-Json -Depth 10

    $mirrorListUrl  = "$BaseUrl/mirroredDatabases"
    $existingMirror = (Invoke-RestMethod -Uri $mirrorListUrl -Headers $Headers -Method GET).value `
                      | Where-Object { $_.displayName -eq $DisplayName }

    if ($existingMirror) {
        Write-Host "  [OK] Mirroring DB '$DisplayName' gia esistente - ID: $($existingMirror.id)"
        return $existingMirror.id
    }
    Write-Host "  [NEW] Creo Mirroring Database '$DisplayName'..."
    $mirrorResult = Invoke-FabricApi -Method POST -Url $mirrorListUrl -Headers $Headers -Body $mirroringBody
    $mirroringId  = $mirrorResult.id
    if ([string]::IsNullOrWhiteSpace($mirroringId)) {
        Start-Sleep -Seconds 3
        $createdMirror = (Invoke-RestMethod -Uri $mirrorListUrl -Headers $Headers -Method GET).value `
                         | Where-Object { $_.displayName -eq $DisplayName }
        $mirroringId = $createdMirror.id
    }
    Write-Host "  [OK] Creato - ID: $mirroringId"
    return $mirroringId
}

function Start-Mirroring {
    param([string]$WorkspaceId, [string]$MirroringId, [hashtable]$Headers, [string]$BaseUrl)
    $url = "$BaseUrl/mirroredDatabases/$MirroringId/startMirroring"
    Write-Host "  Avvio mirroring (POST $url)..."
    try {
        Invoke-FabricApi -Method POST -Url $url -Headers $Headers | Out-Null
        Write-Host "  [OK] Mirroring avviato."
    } catch {
        Write-Host "  [WARN] startMirroring ha restituito un errore (potrebbe essere gia in esecuzione): $_"
    }
}

function Deploy-SparkJob {
    param([string]$SparkJobName, [string]$PythonFileName, [string]$LakehouseId,
          [string]$Description, [hashtable]$Headers, [string]$BaseUrl)

    $pythonLocalPath = Join-Path $PSScriptRoot "..\python\$PythonFileName"
    Write-Host "  Lettura Python: $pythonLocalPath"
    $pythonBytes  = [System.IO.File]::ReadAllBytes($pythonLocalPath)
    $pythonBase64 = [Convert]::ToBase64String($pythonBytes)

    $sparkDefPayload = @{
        executableFile = "$PythonFileName"; defaultLakehouseArtifactId = $LakehouseId
        mainClass = ""; additionalLakehouseIds = @(); retryPolicy = $null
        commandLineArguments = ""; additionalLibraryUris = @()
        language = "Python"; environmentArtifactId = $null
    } | ConvertTo-Json -Depth 10

    $sparkDefinition = @{
        format = "SparkJobDefinitionV2"
        parts = @(
            @{ path = "SparkJobDefinitionV1.json"; payload = (To-Base64 $sparkDefPayload); payloadType = "InlineBase64" },
            @{ path = "Main/$PythonFileName"; payload = $pythonBase64; payloadType = "InlineBase64" }
        )
    }

    $existingSJD = (Invoke-RestMethod -Uri "$BaseUrl/sparkJobDefinitions" -Headers $Headers -Method GET).value `
                   | Where-Object { $_.displayName -eq $SparkJobName }

    if ($existingSJD) {
        $sparkJobId = $existingSJD.id
        Write-Host "  [OK] Spark Job '$SparkJobName' gia esistente - ID: $sparkJobId"
        Write-Host "  [UPD] Aggiorno definizione..."
        $updateBody = @{ definition = $sparkDefinition } | ConvertTo-Json -Depth 10
        Invoke-FabricApi -Method POST -Url "$BaseUrl/sparkJobDefinitions/$sparkJobId/updateDefinition" -Headers $Headers -Body $updateBody
        Write-Host "  [OK] Definizione aggiornata"
    } else {
        Write-Host "  [NEW] Creo Spark Job '$SparkJobName'..."
        $sparkBody = @{
            displayName = $SparkJobName; description = $Description; definition = $sparkDefinition
        } | ConvertTo-Json -Depth 10
        $sjdResult = Invoke-FabricApi -Method POST -Url "$BaseUrl/sparkJobDefinitions" -Headers $Headers -Body $sparkBody
        $sparkJobId = $sjdResult.id
        if ([string]::IsNullOrWhiteSpace($sparkJobId)) {
            Start-Sleep -Seconds 3
            $createdSJD = (Invoke-RestMethod -Uri "$BaseUrl/sparkJobDefinitions" -Headers $Headers -Method GET).value `
                          | Where-Object { $_.displayName -eq $SparkJobName }
            $sparkJobId = $createdSJD.id
        }
        Write-Host "  [OK] Creato - ID: $sparkJobId"
    }
    return [string]$sparkJobId
}

# Costruisce JSON pipeline con ConvertTo-Json (NO here-string, NO $ o `)
function Build-PipelineDefinitionJson {
    param([string]$PipelineName, [string]$SparkJobName, [string]$SparkJobId,
          [string]$WorkspaceId, [string]$LakehouseId, [string]$CommandLineArgs)
    $def = @{
        name = $PipelineName; objectId = [guid]::NewGuid().ToString()
        properties = @{
            activities = @(@{
                name = $SparkJobName; type = "FabricSparkJobDefinition"; dependsOn = @()
                policy = @{
                    timeout = "0.12:00:00"; retry = 0
                    retryIntervalInSeconds = 30; secureOutput = $false; secureInput = $false
                }
                typeProperties = @{
                    sparkJobDefinitionId = $SparkJobId; workspaceId = $WorkspaceId
                    commandLineArguments = $CommandLineArgs
                    defaultLakehouse = @{ workspaceId = $WorkspaceId; artifactId = $LakehouseId }
                }
            })
        }
    }
    return ($def | ConvertTo-Json -Depth 15 -Compress)
}

# Crea o aggiorna (o forza ricreazione) una Data Pipeline
function Deploy-Pipeline {
    param([string]$PipelineName, [string]$PipelineDefJson, [string]$Description,
          [hashtable]$Headers, [string]$BaseUrl, [bool]$Force)

    $pipelineListUrl  = "$BaseUrl/dataPipelines"
    $existingPipeline = (Invoke-RestMethod -Uri $pipelineListUrl -Headers $Headers -Method GET).value `
                        | Where-Object { $_.displayName -eq $PipelineName }

    # ForceRecreate: elimina la pipeline esistente
    if ($existingPipeline -and $Force) {
        $oldId = $existingPipeline.id
        Write-Host "  [DEL] ForceRecreate: elimino pipeline '$PipelineName' ($oldId)..."
        Invoke-FabricApi -Method DELETE -Url "$BaseUrl/dataPipelines/$oldId" -Headers $Headers
        Write-Host "  [DEL] Eliminata. Ricreo..."
        Start-Sleep -Seconds 2
        $existingPipeline = $null
    }

    if ($existingPipeline) {
        $pipelineId = $existingPipeline.id
        Write-Host "  [OK] Pipeline gia esistente - ID: $pipelineId"
        Write-Host "  [UPD] Aggiorno la definizione..."
        $updateBody = @{
            definition = @{
                parts = @(@{ path = "pipeline-content.json"; payload = (To-Base64 $PipelineDefJson); payloadType = "InlineBase64" })
            }
        } | ConvertTo-Json -Depth 10
        Invoke-FabricApi -Method POST -Url "$BaseUrl/dataPipelines/$pipelineId/updateDefinition" -Headers $Headers -Body $updateBody
        Write-Host "  [OK] Definizione aggiornata"
    } else {
        Write-Host "  [NEW] Creo Data Pipeline '$PipelineName'..."
        $pipelineBody = @{
            displayName = $PipelineName; description = $Description
            definition = @{
                parts = @(@{ path = "pipeline-content.json"; payload = (To-Base64 $PipelineDefJson); payloadType = "InlineBase64" })
            }
        } | ConvertTo-Json -Depth 10
        $pipelineResult = Invoke-FabricApi -Method POST -Url $pipelineListUrl -Headers $Headers -Body $pipelineBody
        $pipelineId = $pipelineResult.id
        if ([string]::IsNullOrWhiteSpace($pipelineId)) {
            Start-Sleep -Seconds 3
            $createdPipeline = (Invoke-RestMethod -Uri $pipelineListUrl -Headers $Headers -Method GET).value `
                               | Where-Object { $_.displayName -eq $PipelineName }
            $pipelineId = $createdPipeline.id
        }
        Write-Host "  [OK] Creato - ID: $pipelineId"
    }
    return [string]$pipelineId
}

function Ensure-LakehouseFolder {
    param([string]$FolderPath, [string]$DfsToken, [string]$WsId, [string]$LhId)
    $dfsUrl = "https://onelake.dfs.fabric.microsoft.com/$WsId/$LhId/$FolderPath"
    try {
        Invoke-WebRequest -Uri "$($dfsUrl)?resource=directory" -Method PUT `
            -Headers @{ Authorization = "Bearer $DfsToken" } -UseBasicParsing | Out-Null
        Write-Host "  [OK] Cartella: $FolderPath"
    } catch {
        if ($_.Exception.Response.StatusCode.value__ -eq 409) {
            Write-Host "  [OK] Cartella gia esistente: $FolderPath"
        } else { Write-Host "  [WARN] Cartella $FolderPath : $($_.Exception.Message)" }
    }
}

# Valida che sparkArgs non contenga $ o `
function Assert-SparkArgsSafe([string]$SparkArgs, [string]$ConnectorName) {
    if ($SparkArgs.Contains('$')) {
        Write-Host "##[error] [$ConnectorName] commandLineArguments contengono il carattere DOLLARO!"
        Write-Host "##[error] Questo significa che una variabile DevOps non e' stata risolta."
        Write-Host "##[error] Valore: $SparkArgs"
        exit 1
    }
    if ($SparkArgs.Contains('``')) {
        Write-Host "##[error] [$ConnectorName] commandLineArguments contengono il carattere BACKTICK!"
        exit 1
    }
    Write-Host "  [SAFE] commandLineArguments validati (nessun $ o backtick)"
}

# ─────────────────────────────────────────
# 0. AUTENTICAZIONE
# ─────────────────────────────────────────
Write-Host ""
Write-Host "[AUTH] Autenticazione Service Principal..."
$tokenUrl  = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
$tokenBody = @{
    grant_type = "client_credentials"; client_id = $ClientId
    client_secret = $ClientSecret; scope = "https://api.fabric.microsoft.com/.default"
}
$token   = (Invoke-RestMethod -Uri $tokenUrl -Method POST -Body $tokenBody).access_token
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
$baseUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId"
Write-Host "[OK] Token Fabric ottenuto"

$onelakeTokenBody = @{
    grant_type = "client_credentials"; client_id = $ClientId
    client_secret = $ClientSecret; scope = "https://storage.azure.com/.default"
}
$onelakeToken = (Invoke-RestMethod -Uri $tokenUrl -Method POST -Body $onelakeTokenBody).access_token
Write-Host "[OK] Token OneLake ottenuto"

$deployedItems = @{}

# ─────────────────────────────────────────
# 1. LAKEHOUSE CONDIVISO
# ─────────────────────────────────────────
Write-Host ""
Write-Host "=== STEP 1: Lakehouse Condiviso ==="
$lakehouseBody = @{
    displayName = $LakehouseName; type = "Lakehouse"
    description = "Lakehouse Bronze - landing zone condivisa"
} | ConvertTo-Json -Depth 5

$lakehouseId = Get-OrCreate-Item -DisplayName $LakehouseName -Type "Lakehouse" `
    -CreateUrl "$baseUrl/lakehouses" -ListUrl "$baseUrl/lakehouses" `
    -BodyJson $lakehouseBody -Headers $headers
$lakehouseId = ([string]$lakehouseId).Trim()
Write-Host "  Lakehouse ID: $lakehouseId"

Write-Host ""
Write-Host "=== STEP 1b: Struttura Cartelle ==="
foreach ($f in @("Files/BC","Files/BC/Scripts","Files/CRM","Files/CRM/Scripts",
                 "Files/Orchestration","Files/MirroringState","Files/MirroringKeys")) {
    Ensure-LakehouseFolder -FolderPath $f -DfsToken $onelakeToken -WsId $WorkspaceId -LhId $lakehouseId
}

# =========================================================
# DEPLOY BC
# =========================================================
if ($connectorList -contains "BC") {
    Write-Host ""
    Write-Host "####################################################"
    Write-Host "# DEPLOY: BUSINESS CENTRAL"
    Write-Host "####################################################"

    Write-Host ""
    Write-Host "=== BC STEP 2: Mirroring Database ==="
    $mirroringId = ([string](Get-OrCreate-MirroringDb `
        -DisplayName $BcMirroringDbName -Description "Open Mirroring - BC" `
        -Headers $headers -BaseUrl $baseUrl)).Trim()
    Start-Mirroring -WorkspaceId $WorkspaceId -MirroringId $mirroringId -Headers $headers -BaseUrl $baseUrl

    Write-Host ""
    Write-Host "=== BC STEP 3: Spark Job ==="
    $sparkJobId = ([string](Deploy-SparkJob -SparkJobName $BcSparkJobName `
        -PythonFileName $BcPythonFile -LakehouseId $lakehouseId `
        -Description "Spark Job BC Sync" -Headers $headers -BaseUrl $baseUrl)).Trim()

    Write-Host ""
    Write-Host "=== BC STEP 4: Data Pipeline ==="
    Write-Host "  Spark Job ID: $sparkJobId"
    Write-Host "  Mirroring ID: $mirroringId"
    if ([string]::IsNullOrWhiteSpace($sparkJobId)) { Write-Host "##[error] sparkJobId BC vuoto!"; exit 1 }

    # Costruisci args - TUTTI i valori sensibili in Base64
    $sparkArgs = @(
        "--BC_TENANT_ID",            $BcTenantId,
        "--BC_CLIENT_ID",            $ClientId,
        "--BC_CLIENT_SECRET_B64",    (To-Base64 $ClientSecret),
        "--BC_ENVIRONMENT",          $BcEnvironment,
        "--BC_COMPANIES_B64",        (To-Base64 $BcCompanies),
        "--BC_ENTITIES_B64",         (To-Base64 $BcEntities),
        "--FABRIC_WORKSPACE_ID",     $WorkspaceId,
        "--FABRIC_LAKEHOUSE_ID",     $lakehouseId,
        "--FABRIC_MIRRORED_DB_ID",   $mirroringId,
        "--FABRIC_TENANT_ID",        $TenantId,
        "--FABRIC_CLIENT_ID",        $ClientId,
        "--FABRIC_CLIENT_SECRET_B64", (To-Base64 $ClientSecret)
    ) -join " "

    Assert-SparkArgsSafe $sparkArgs "BC"

    $pipelineDefJson = Build-PipelineDefinitionJson `
        -PipelineName $BcPipelineName -SparkJobName $BcSparkJobName `
        -SparkJobId $sparkJobId -WorkspaceId $WorkspaceId `
        -LakehouseId $lakehouseId -CommandLineArgs $sparkArgs

    $pipelineId = Deploy-Pipeline -PipelineName $BcPipelineName `
        -PipelineDefJson $pipelineDefJson -Description "Pipeline BC Sync" `
        -Headers $headers -BaseUrl $baseUrl -Force $ForceRecreate.IsPresent

    $deployedItems["BC"] = @{ MirroringDb=$mirroringId; SparkJob=$sparkJobId; Pipeline=$pipelineId }
    Write-Host "##vso[task.setvariable variable=BC_MIRRORING_ID]$mirroringId"
    Write-Host "##vso[task.setvariable variable=BC_SPARK_JOB_ID]$sparkJobId"
    Write-Host "##vso[task.setvariable variable=BC_PIPELINE_ID]$pipelineId"
}

# =========================================================
# DEPLOY CRM
# =========================================================
if ($connectorList -contains "CRM") {
    Write-Host ""
    Write-Host "####################################################"
    Write-Host "# DEPLOY: CRM / DATAVERSE"
    Write-Host "####################################################"

    Write-Host ""
    Write-Host "=== CRM STEP 2: Mirroring Database ==="
    $mirroringId = ([string](Get-OrCreate-MirroringDb `
        -DisplayName $CrmMirroringDbName -Description "Open Mirroring - CRM" `
        -Headers $headers -BaseUrl $baseUrl)).Trim()
    Start-Mirroring -WorkspaceId $WorkspaceId -MirroringId $mirroringId -Headers $headers -BaseUrl $baseUrl

    Write-Host ""
    Write-Host "=== CRM STEP 3: Spark Job ==="
    $sparkJobId = ([string](Deploy-SparkJob -SparkJobName $CrmSparkJobName `
        -PythonFileName $CrmPythonFile -LakehouseId $lakehouseId `
        -Description "Spark Job CRM Sync" -Headers $headers -BaseUrl $baseUrl)).Trim()

    Write-Host ""
    Write-Host "=== CRM STEP 4: Data Pipeline ==="
    Write-Host "  Spark Job ID: $sparkJobId"
    Write-Host "  Mirroring ID: $mirroringId"
    if ([string]::IsNullOrWhiteSpace($sparkJobId)) { Write-Host "##[error] sparkJobId CRM vuoto!"; exit 1 }

    $sparkArgs = @(
        "--CRM_TENANT_ID",             $CrmTenantId,
        "--CRM_CLIENT_ID",             $CrmClientId,
        "--CRM_CLIENT_SECRET_B64",     (To-Base64 $CrmClientSecret),
        "--CRM_ORG_URL",               $CrmOrgUrl,
        "--CRM_API_VERSION",           $CrmApiVersion,
        "--CRM_ENTITIES_B64",          (To-Base64 $CrmEntities),
        "--FABRIC_WORKSPACE_ID",       $WorkspaceId,
        "--FABRIC_LAKEHOUSE_ID",       $lakehouseId,
        "--FABRIC_MIRRORED_DB_ID",     $mirroringId,
        "--FABRIC_TENANT_ID",          $TenantId,
        "--FABRIC_CLIENT_ID",          $ClientId,
        "--FABRIC_CLIENT_SECRET_B64",  (To-Base64 $ClientSecret)
    ) -join " "

    Assert-SparkArgsSafe $sparkArgs "CRM"

    $pipelineDefJson = Build-PipelineDefinitionJson `
        -PipelineName $CrmPipelineName -SparkJobName $CrmSparkJobName `
        -SparkJobId $sparkJobId -WorkspaceId $WorkspaceId `
        -LakehouseId $lakehouseId -CommandLineArgs $sparkArgs

    $pipelineId = Deploy-Pipeline -PipelineName $CrmPipelineName `
        -PipelineDefJson $pipelineDefJson -Description "Pipeline CRM Sync" `
        -Headers $headers -BaseUrl $baseUrl -Force $ForceRecreate.IsPresent

    $deployedItems["CRM"] = @{ MirroringDb=$mirroringId; SparkJob=$sparkJobId; Pipeline=$pipelineId }
    Write-Host "##vso[task.setvariable variable=CRM_MIRRORING_ID]$mirroringId"
    Write-Host "##vso[task.setvariable variable=CRM_SPARK_JOB_ID]$sparkJobId"
    Write-Host "##vso[task.setvariable variable=CRM_PIPELINE_ID]$pipelineId"
}

# ─────────────────────────────────────────
# RIEPILOGO
# ─────────────────────────────────────────
Write-Host ""
Write-Host "============================================"
Write-Host "[OK] DEPLOY COMPLETATO"
Write-Host "============================================"
Write-Host "  Connettori: $($connectorList -join ', ')"
Write-Host "  Lakehouse : $lakehouseId ($LakehouseName)"
foreach ($conn in $deployedItems.Keys) {
    $items = $deployedItems[$conn]
    Write-Host "  --- $conn ---"
    Write-Host "    Mirroring DB: $($items.MirroringDb)"
    Write-Host "    Spark Job   : $($items.SparkJob)"
    Write-Host "    Pipeline    : $($items.Pipeline)"
}
Write-Host "============================================"
Write-Host "##vso[task.setvariable variable=LAKEHOUSE_ID]$lakehouseId"