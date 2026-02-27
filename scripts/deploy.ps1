# =============================================================
# deploy.ps1 - Deploy modulare soluzione Fabric
# Compatibile con PowerShell 5.1 (Windows runner DevOps)
#
# Supporta connettori multipli tramite flag:
#   -Connectors "BC"       -> solo Business Central
#   -Connectors "CRM"      -> solo Dataverse/CRM
#   -Connectors "BC,CRM"   -> entrambi
# =============================================================
param(
    [Parameter(Mandatory=$true)][string]$TenantId,
    [Parameter(Mandatory=$true)][string]$ClientId,
    [Parameter(Mandatory=$true)][string]$ClientSecret,
    [Parameter(Mandatory=$true)][string]$WorkspaceId,

    # ── Flag connettori (separati da virgola) ──
    [string]$Connectors = "BC",

    # ── Parametri BC ──
    [string]$BcTenantId     = "",
    [string]$BcEnvironment  = "SandboxTest",
    [string]$BcCompanies    = '["CRONUS%20IT"]',
    [string]$BcEntities     = '["ItemLedgerEntries"]',
    [string]$BcPythonFile   = "bc_sync.py",

    # ── Parametri CRM ──
    [string]$CrmTenantId    = "",
    [string]$CrmClientId    = "",
    [string]$CrmClientSecret = "",
    [string]$CrmOrgUrl      = "",
    [string]$CrmApiVersion  = "v9.2",
    [string]$CrmEntities    = '["accounts"]',
    [string]$CrmPythonFile  = "crm_sync.py",

    # ── Nomi item Fabric ──
    [string]$BcLakehouseName    = "LH_BC_Landing",
    [string]$BcMirroringDbName  = "MirrorDB_BC_Landing",
    [string]$BcSparkJobName     = "SJD_BC_To_Mirroring",
    [string]$BcPipelineName     = "DP_BC_To_Mirroring",

    [string]$CrmLakehouseName   = "LH_CRM_Landing",
    [string]$CrmMirroringDbName = "MirrorDB_CRM_Landing",
    [string]$CrmSparkJobName    = "SJD_CRM_To_Mirroring",
    [string]$CrmPipelineName    = "DP_CRM_To_Mirroring",

    [string]$PythonFileName = ""
)

$ErrorActionPreference = "Stop"

# ── Parse lista connettori ──
$connectorList = $Connectors.Split(",") | ForEach-Object { $_.Trim().ToUpper() }
Write-Host ""
Write-Host "============================================"
Write-Host "[CONFIG] Connettori richiesti: $($connectorList -join ', ')"
Write-Host "============================================"

# ── Validazione per connettore ──
foreach ($conn in $connectorList) {
    switch ($conn) {
        "BC" {
            if ([string]::IsNullOrEmpty($BcTenantId)) {
                Write-Host "##[error] Connettore BC richiesto ma BcTenantId mancante."
                exit 1
            }
        }
        "CRM" {
            if ([string]::IsNullOrEmpty($CrmOrgUrl)) {
                Write-Host "##[error] Connettore CRM richiesto ma CrmOrgUrl mancante."
                exit 1
            }
            if ([string]::IsNullOrEmpty($CrmTenantId))    { $CrmTenantId    = $TenantId }
            if ([string]::IsNullOrEmpty($CrmClientId))    { $CrmClientId    = $ClientId }
            if ([string]::IsNullOrEmpty($CrmClientSecret)){ $CrmClientSecret = $ClientSecret }
        }
        default {
            Write-Host "##[error] Connettore sconosciuto: $conn. Valori validi: BC, CRM"
            exit 1
        }
    }
}

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────
function To-Base64([string]$str) {
    return [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($str))
}

function Invoke-FabricApi {
    param(
        [string]$Method,
        [string]$Url,
        [hashtable]$Headers,
        [string]$Body = $null
    )
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
            $responseBody = $reader.ReadToEnd()
            Write-Host "##[error] API Response Body: $responseBody"
        } else {
            Write-Host "##[error] API Error: $($ex.Message)"
        }
        throw
    }

    if ($response.StatusCode -eq 202) {
        $locationUrl   = $response.Headers["Location"]
        $retryAfterRaw = $response.Headers["Retry-After"]
        if ($retryAfterRaw) { $retryAfter = [int]$retryAfterRaw } else { $retryAfter = 5 }

        Write-Host "    [ASYNC] Polling ogni ${retryAfter}s..."
        do {
            Start-Sleep -Seconds $retryAfter
            $pollResp = Invoke-WebRequest -Uri $locationUrl -Headers $Headers -Method GET -UseBasicParsing
        } while ($pollResp.StatusCode -eq 202)
        return $pollResp.Content | ConvertFrom-Json
    }
    return $response.Content | ConvertFrom-Json
}

function Get-OrCreate-Item {
    param(
        [string]$DisplayName,
        [string]$Type,
        [string]$CreateUrl,
        [string]$ListUrl,
        [string]$BodyJson,
        [hashtable]$Headers
    )
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
    param(
        [string]$DisplayName,
        [string]$Description,
        [hashtable]$Headers,
        [string]$BaseUrl
    )
    $mirroringPayload = @{
        properties = @{
            source = @{
                type           = "GenericMirror"
                typeProperties = @{}
            }
            target = @{
                type           = "MountedRelationalDatabase"
                typeProperties = @{
                    defaultSchema = "dbo"
                    format        = "Delta"
                }
            }
        }
    } | ConvertTo-Json -Depth 10

    $mirroringBody = @{
        displayName = $DisplayName
        description = $Description
        definition  = @{
            parts = @(@{
                path        = "mirroring.json"
                payload     = (To-Base64 $mirroringPayload)
                payloadType = "InlineBase64"
            })
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
        Write-Host "  [INFO] Risposta async senza ID, recupero dalla lista..."
        Start-Sleep -Seconds 3
        $createdMirror = (Invoke-RestMethod -Uri $mirrorListUrl -Headers $Headers -Method GET).value `
                         | Where-Object { $_.displayName -eq $DisplayName }
        $mirroringId = $createdMirror.id
    }
    Write-Host "  [OK] Creato - ID: $mirroringId"
    return $mirroringId
}

function Deploy-SparkJob {
    param(
        [string]$SparkJobName,
        [string]$PythonFileName,
        [string]$LakehouseId,
        [string]$Description,
        [hashtable]$Headers,
        [string]$BaseUrl
    )

    $pythonLocalPath = Join-Path $PSScriptRoot "..\python\$PythonFileName"
    Write-Host "  Lettura Python: $pythonLocalPath"
    $pythonBytes  = [System.IO.File]::ReadAllBytes($pythonLocalPath)
    $pythonBase64 = [Convert]::ToBase64String($pythonBytes)

    $sparkDefPayload = @{
        executableFile             = "$PythonFileName"
        defaultLakehouseArtifactId = $LakehouseId
        mainClass                  = ""
        additionalLakehouseIds     = @()
        retryPolicy                = $null
        commandLineArguments       = ""
        additionalLibraryUris      = @()
        language                   = "Python"
        environmentArtifactId      = $null
    } | ConvertTo-Json -Depth 10

    $sparkDefinition = @{
        format = "SparkJobDefinitionV2"
        parts  = @(
            @{
                path        = "SparkJobDefinitionV1.json"
                payload     = (To-Base64 $sparkDefPayload)
                payloadType = "InlineBase64"
            },
            @{
                path        = "Main/$PythonFileName"
                payload     = $pythonBase64
                payloadType = "InlineBase64"
            }
        )
    }

    $existingSJD = (Invoke-RestMethod -Uri "$BaseUrl/sparkJobDefinitions" -Headers $Headers -Method GET).value `
                   | Where-Object { $_.displayName -eq $SparkJobName }

    if ($existingSJD) {
        $sparkJobId = $existingSJD.id
        Write-Host "  [OK] Spark Job '$SparkJobName' gia esistente - ID: $sparkJobId"
        Write-Host "  [UPD] Aggiorno definizione (codice Python)..."
        $updateBody = @{ definition = $sparkDefinition } | ConvertTo-Json -Depth 10
        Invoke-FabricApi -Method POST `
            -Url "$BaseUrl/sparkJobDefinitions/$sparkJobId/updateDefinition" `
            -Headers $Headers `
            -Body $updateBody
        Write-Host "  [OK] Definizione aggiornata"
    } else {
        Write-Host "  [NEW] Creo Spark Job '$SparkJobName' (V2)..."
        $sparkBody = @{
            displayName = $SparkJobName
            description = $Description
            definition  = $sparkDefinition
        } | ConvertTo-Json -Depth 10

        $sjdResult  = Invoke-FabricApi -Method POST -Url "$BaseUrl/sparkJobDefinitions" -Headers $Headers -Body $sparkBody
        $sparkJobId = $sjdResult.id
        if ([string]::IsNullOrWhiteSpace($sparkJobId)) {
            Write-Host "  [INFO] Risposta async senza ID, recupero dalla lista..."
            Start-Sleep -Seconds 3
            $createdSJD = (Invoke-RestMethod -Uri "$BaseUrl/sparkJobDefinitions" -Headers $Headers -Method GET).value `
                          | Where-Object { $_.displayName -eq $SparkJobName }
            $sparkJobId = $createdSJD.id
        }
        Write-Host "  [OK] Creato - ID: $sparkJobId"
    }
    return [string]$sparkJobId
}

# ─────────────────────────────────────────
# 0. AUTENTICAZIONE FABRIC
# ─────────────────────────────────────────
Write-Host ""
Write-Host "[AUTH] Autenticazione Service Principal..."
$tokenUrl  = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
$tokenBody = @{
    grant_type    = "client_credentials"
    client_id     = $ClientId
    client_secret = $ClientSecret
    scope         = "https://api.fabric.microsoft.com/.default"
}
$token   = (Invoke-RestMethod -Uri $tokenUrl -Method POST -Body $tokenBody).access_token
$headers = @{
    Authorization  = "Bearer $token"
    "Content-Type" = "application/json"
}
$baseUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId"
Write-Host "[OK] Token Fabric ottenuto"

$deployedItems = @{}

# =========================================================
# DEPLOY BC CONNECTOR
# =========================================================
if ($connectorList -contains "BC") {
    Write-Host ""
    Write-Host "####################################################"
    Write-Host "# DEPLOY CONNETTORE: BUSINESS CENTRAL"
    Write-Host "####################################################"

    # -- STEP 1: Lakehouse BC --
    Write-Host ""
    Write-Host "=== BC STEP 1: Lakehouse ==="
    $lakehouseBody = @{
        displayName = $BcLakehouseName
        type        = "Lakehouse"
        description = "Lakehouse BC - contiene script Python e dati di staging"
    } | ConvertTo-Json -Depth 5

    $lakehouseId = Get-OrCreate-Item `
        -DisplayName $BcLakehouseName `
        -Type        "Lakehouse" `
        -CreateUrl   "$baseUrl/lakehouses" `
        -ListUrl     "$baseUrl/lakehouses" `
        -BodyJson    $lakehouseBody `
        -Headers     $headers
    Write-Host "  Lakehouse ID: $lakehouseId"

    # -- STEP 2: Mirroring DB BC --
    Write-Host ""
    Write-Host "=== BC STEP 2: Mirroring Database ==="
    $mirroringId = Get-OrCreate-MirroringDb `
        -DisplayName $BcMirroringDbName `
        -Description "Open Mirroring Landing Zone - Business Central" `
        -Headers     $headers `
        -BaseUrl     $baseUrl

    # -- STEP 3: Spark Job BC --
    Write-Host ""
    Write-Host "=== BC STEP 3: Spark Job Definition ==="
    $sparkJobId = Deploy-SparkJob `
        -SparkJobName  $BcSparkJobName `
        -PythonFileName $BcPythonFile `
        -LakehouseId   $lakehouseId `
        -Description   "Spark Job BC Sync - legge da BC e scrive su Mirroring Landing Zone" `
        -Headers       $headers `
        -BaseUrl       $baseUrl

    # ─────────────────────────────────────────
    # STEP 4: Data Pipeline BC
    # Identico allo script originale: here-string
    # inline con variabili nello scope corrente.
    # ─────────────────────────────────────────
    Write-Host ""
    Write-Host "=== BC STEP 4: Data Pipeline ==="

    # Sicurezza: forza ID a stringhe pulite (in caso di oggetti PS)
    $sparkJobId  = ([string]$sparkJobId).Trim()
    $WorkspaceId = ([string]$WorkspaceId).Trim()
    $lakehouseId = ([string]$lakehouseId).Trim()
    $mirroringId = ([string]$mirroringId).Trim()

    Write-Host "  Spark Job ID: $sparkJobId"
    Write-Host "  Workspace ID: $WorkspaceId"
    Write-Host "  Lakehouse ID: $lakehouseId"

    if ([string]::IsNullOrWhiteSpace($sparkJobId)) {
        Write-Host "##[error] sparkJobId e' vuoto! Impossibile creare la pipeline."
        exit 1
    }

    $companiesB64 = To-Base64 $BcCompanies
    $entitiesB64  = To-Base64 $BcEntities

    $sparkArgs = @(
        "--BC_TENANT_ID",         $BcTenantId,
        "--BC_CLIENT_ID",         $ClientId,
        "--BC_CLIENT_SECRET",     $ClientSecret,
        "--BC_ENVIRONMENT",       $BcEnvironment,
        "--BC_COMPANIES_B64",     $companiesB64,
        "--BC_ENTITIES_B64",      $entitiesB64,
        "--FABRIC_WORKSPACE_ID",  $WorkspaceId,
        "--FABRIC_LAKEHOUSE_ID",  $lakehouseId,
        "--FABRIC_MIRRORED_DB_ID", $mirroringId,
        "--FABRIC_TENANT_ID",     $TenantId,
        "--FABRIC_CLIENT_ID",     $ClientId,
        "--FABRIC_CLIENT_SECRET", $ClientSecret
    ) -join " "

    Write-Host "  commandLineArguments costruiti (companies/entities in Base64)"

    # Nomi locali per l'here-string (identici all'originale)
    $PipelineName = $BcPipelineName
    $SparkJobName = $BcSparkJobName

    $pipelineDefinition = @"
{
    "name": "$PipelineName",
    "objectId": "$(([guid]::NewGuid()).ToString())",
    "properties": {
        "activities": [
            {
                "name": "$SparkJobName",
                "type": "FabricSparkJobDefinition",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "sparkJobDefinitionId": "$sparkJobId",
                    "workspaceId": "$WorkspaceId",
                    "commandLineArguments": "$sparkArgs",
                    "defaultLakehouse": {
                        "workspaceId": "$WorkspaceId",
                        "artifactId": "$lakehouseId"
                    }
                }
            }
        ]
    }
}
"@

    Write-Host "  [DEBUG] Pipeline definition BC creata"

    # Controlla se esiste gia
    $pipelineListUrl  = "$baseUrl/dataPipelines"
    $existingPipeline = (Invoke-RestMethod -Uri $pipelineListUrl -Headers $headers -Method GET).value `
                        | Where-Object { $_.displayName -eq $PipelineName }

    if ($existingPipeline) {
        $pipelineId = $existingPipeline.id
        Write-Host "  [OK] Pipeline gia esistente - ID: $pipelineId"

        Write-Host "  [UPD] Aggiorno la definizione della pipeline..."
        $updateBody = @{
            definition = @{
                parts = @(@{
                    path        = "pipeline-content.json"
                    payload     = (To-Base64 $pipelineDefinition)
                    payloadType = "InlineBase64"
                })
            }
        } | ConvertTo-Json -Depth 10

        Invoke-FabricApi -Method POST `
            -Url "$baseUrl/dataPipelines/$pipelineId/updateDefinition" `
            -Headers $headers `
            -Body $updateBody
        Write-Host "  [OK] Definizione aggiornata"
    } else {
        Write-Host "  [NEW] Creo Data Pipeline '$PipelineName'..."

        $pipelineBody = @{
            displayName = $PipelineName
            description = "Pipeline BC - esegue Spark Job $SparkJobName"
            definition  = @{
                parts = @(@{
                    path        = "pipeline-content.json"
                    payload     = (To-Base64 $pipelineDefinition)
                    payloadType = "InlineBase64"
                })
            }
        } | ConvertTo-Json -Depth 10

        $pipelineResult = Invoke-FabricApi -Method POST -Url $pipelineListUrl -Headers $headers -Body $pipelineBody
        $pipelineId     = $pipelineResult.id
        if ([string]::IsNullOrWhiteSpace($pipelineId)) {
            Write-Host "  [INFO] Risposta async senza ID, recupero dalla lista..."
            Start-Sleep -Seconds 3
            $createdPipeline = (Invoke-RestMethod -Uri $pipelineListUrl -Headers $headers -Method GET).value `
                               | Where-Object { $_.displayName -eq $PipelineName }
            $pipelineId = $createdPipeline.id
        }
        Write-Host "  [OK] Creato - ID: $pipelineId"
    }

    # Salva risultati BC
    $bcLakehouseId_out = $lakehouseId
    $bcMirroringId_out = $mirroringId
    $bcSparkJobId_out  = $sparkJobId
    $bcPipelineId_out  = $pipelineId

    $deployedItems["BC"] = @{
        Lakehouse   = $bcLakehouseId_out
        MirroringDb = $bcMirroringId_out
        SparkJob    = $bcSparkJobId_out
        Pipeline    = $bcPipelineId_out
    }

    Write-Host "##vso[task.setvariable variable=BC_LAKEHOUSE_ID]$bcLakehouseId_out"
    Write-Host "##vso[task.setvariable variable=BC_MIRRORING_ID]$bcMirroringId_out"
    Write-Host "##vso[task.setvariable variable=BC_SPARK_JOB_ID]$bcSparkJobId_out"
    Write-Host "##vso[task.setvariable variable=BC_PIPELINE_ID]$bcPipelineId_out"
}

# =========================================================
# DEPLOY CRM CONNECTOR
# =========================================================
if ($connectorList -contains "CRM") {
    Write-Host ""
    Write-Host "####################################################"
    Write-Host "# DEPLOY CONNETTORE: CRM / DATAVERSE"
    Write-Host "####################################################"

    # -- STEP 1: Lakehouse CRM --
    Write-Host ""
    Write-Host "=== CRM STEP 1: Lakehouse ==="
    $lakehouseBody = @{
        displayName = $CrmLakehouseName
        type        = "Lakehouse"
        description = "Lakehouse CRM - contiene script Python e dati Dataverse"
    } | ConvertTo-Json -Depth 5

    $lakehouseId = Get-OrCreate-Item `
        -DisplayName $CrmLakehouseName `
        -Type        "Lakehouse" `
        -CreateUrl   "$baseUrl/lakehouses" `
        -ListUrl     "$baseUrl/lakehouses" `
        -BodyJson    $lakehouseBody `
        -Headers     $headers
    Write-Host "  Lakehouse ID: $lakehouseId"

    # -- STEP 2: Mirroring DB CRM --
    Write-Host ""
    Write-Host "=== CRM STEP 2: Mirroring Database ==="
    $mirroringId = Get-OrCreate-MirroringDb `
        -DisplayName $CrmMirroringDbName `
        -Description "Open Mirroring Landing Zone - CRM / Dataverse" `
        -Headers     $headers `
        -BaseUrl     $baseUrl

    # -- STEP 3: Spark Job CRM --
    Write-Host ""
    Write-Host "=== CRM STEP 3: Spark Job Definition ==="
    $sparkJobId = Deploy-SparkJob `
        -SparkJobName  $CrmSparkJobName `
        -PythonFileName $CrmPythonFile `
        -LakehouseId   $lakehouseId `
        -Description   "Spark Job CRM Sync - legge da Dataverse e scrive su Mirroring Landing Zone" `
        -Headers       $headers `
        -BaseUrl       $baseUrl

    # ─────────────────────────────────────────
    # STEP 4: Data Pipeline CRM
    # Identico pattern dell'originale.
    # ─────────────────────────────────────────
    Write-Host ""
    Write-Host "=== CRM STEP 4: Data Pipeline ==="

    # Sicurezza: forza ID a stringhe pulite
    $sparkJobId  = ([string]$sparkJobId).Trim()
    $WorkspaceId = ([string]$WorkspaceId).Trim()
    $lakehouseId = ([string]$lakehouseId).Trim()
    $mirroringId = ([string]$mirroringId).Trim()

    Write-Host "  Spark Job ID: $sparkJobId"
    Write-Host "  Workspace ID: $WorkspaceId"
    Write-Host "  Lakehouse ID: $lakehouseId"

    if ([string]::IsNullOrWhiteSpace($sparkJobId)) {
        Write-Host "##[error] sparkJobId e' vuoto! Impossibile creare la pipeline CRM."
        exit 1
    }

    $crmEntitiesB64 = To-Base64 $CrmEntities

    $sparkArgs = @(
        "--CRM_TENANT_ID",        $CrmTenantId,
        "--CRM_CLIENT_ID",        $CrmClientId,
        "--CRM_CLIENT_SECRET",    $CrmClientSecret,
        "--CRM_ORG_URL",          $CrmOrgUrl,
        "--CRM_API_VERSION",      $CrmApiVersion,
        "--CRM_ENTITIES_B64",     $crmEntitiesB64,
        "--FABRIC_WORKSPACE_ID",  $WorkspaceId,
        "--FABRIC_LAKEHOUSE_ID",  $lakehouseId,
        "--FABRIC_MIRRORED_DB_ID", $mirroringId,
        "--FABRIC_TENANT_ID",     $TenantId,
        "--FABRIC_CLIENT_ID",     $ClientId,
        "--FABRIC_CLIENT_SECRET", $ClientSecret
    ) -join " "

    Write-Host "  commandLineArguments costruiti (entities in Base64)"

    # Nomi locali per l'here-string
    $PipelineName = $CrmPipelineName
    $SparkJobName = $CrmSparkJobName

    $pipelineDefinition = @"
{
    "name": "$PipelineName",
    "objectId": "$(([guid]::NewGuid()).ToString())",
    "properties": {
        "activities": [
            {
                "name": "$SparkJobName",
                "type": "FabricSparkJobDefinition",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "sparkJobDefinitionId": "$sparkJobId",
                    "workspaceId": "$WorkspaceId",
                    "commandLineArguments": "$sparkArgs",
                    "defaultLakehouse": {
                        "workspaceId": "$WorkspaceId",
                        "artifactId": "$lakehouseId"
                    }
                }
            }
        ]
    }
}
"@

    Write-Host "  [DEBUG] Pipeline definition CRM creata"

    # Controlla se esiste gia
    $pipelineListUrl  = "$baseUrl/dataPipelines"
    $existingPipeline = (Invoke-RestMethod -Uri $pipelineListUrl -Headers $headers -Method GET).value `
                        | Where-Object { $_.displayName -eq $PipelineName }

    if ($existingPipeline) {
        $pipelineId = $existingPipeline.id
        Write-Host "  [OK] Pipeline gia esistente - ID: $pipelineId"

        Write-Host "  [UPD] Aggiorno la definizione della pipeline..."
        $updateBody = @{
            definition = @{
                parts = @(@{
                    path        = "pipeline-content.json"
                    payload     = (To-Base64 $pipelineDefinition)
                    payloadType = "InlineBase64"
                })
            }
        } | ConvertTo-Json -Depth 10

        Invoke-FabricApi -Method POST `
            -Url "$baseUrl/dataPipelines/$pipelineId/updateDefinition" `
            -Headers $headers `
            -Body $updateBody
        Write-Host "  [OK] Definizione aggiornata"
    } else {
        Write-Host "  [NEW] Creo Data Pipeline '$PipelineName'..."

        $pipelineBody = @{
            displayName = $PipelineName
            description = "Pipeline CRM - esegue Spark Job $SparkJobName"
            definition  = @{
                parts = @(@{
                    path        = "pipeline-content.json"
                    payload     = (To-Base64 $pipelineDefinition)
                    payloadType = "InlineBase64"
                })
            }
        } | ConvertTo-Json -Depth 10

        $pipelineResult = Invoke-FabricApi -Method POST -Url $pipelineListUrl -Headers $headers -Body $pipelineBody
        $pipelineId     = $pipelineResult.id
        if ([string]::IsNullOrWhiteSpace($pipelineId)) {
            Write-Host "  [INFO] Risposta async senza ID, recupero dalla lista..."
            Start-Sleep -Seconds 3
            $createdPipeline = (Invoke-RestMethod -Uri $pipelineListUrl -Headers $headers -Method GET).value `
                               | Where-Object { $_.displayName -eq $PipelineName }
            $pipelineId = $createdPipeline.id
        }
        Write-Host "  [OK] Creato - ID: $pipelineId"
    }

    # Salva risultati CRM
    $crmLakehouseId_out = $lakehouseId
    $crmMirroringId_out = $mirroringId
    $crmSparkJobId_out  = $sparkJobId
    $crmPipelineId_out  = $pipelineId

    $deployedItems["CRM"] = @{
        Lakehouse   = $crmLakehouseId_out
        MirroringDb = $crmMirroringId_out
        SparkJob    = $crmSparkJobId_out
        Pipeline    = $crmPipelineId_out
    }

    Write-Host "##vso[task.setvariable variable=CRM_LAKEHOUSE_ID]$crmLakehouseId_out"
    Write-Host "##vso[task.setvariable variable=CRM_MIRRORING_ID]$crmMirroringId_out"
    Write-Host "##vso[task.setvariable variable=CRM_SPARK_JOB_ID]$crmSparkJobId_out"
    Write-Host "##vso[task.setvariable variable=CRM_PIPELINE_ID]$crmPipelineId_out"
}

# ─────────────────────────────────────────
# RIEPILOGO FINALE
# ─────────────────────────────────────────
Write-Host ""
Write-Host "============================================"
Write-Host "[OK] DEPLOY COMPLETATO"
Write-Host "============================================"
Write-Host "  Connettori deployati: $($connectorList -join ', ')"
Write-Host ""

foreach ($conn in $deployedItems.Keys) {
    $items = $deployedItems[$conn]
    Write-Host "  --- $conn ---"
    Write-Host "    Lakehouse         : $($items.Lakehouse)"
    Write-Host "    Mirroring Database: $($items.MirroringDb)"
    Write-Host "    Spark Job         : $($items.SparkJob)"
    Write-Host "    Data Pipeline     : $($items.Pipeline)"
    Write-Host ""
}

Write-Host "============================================"