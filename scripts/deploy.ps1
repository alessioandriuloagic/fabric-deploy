# =============================================================
# deploy.ps1 - Deploy completo soluzione Fabric
# Compatibile con PowerShell 5.1 (Windows runner DevOps)
# Ordine: 1) Lakehouse -> 2) Mirroring DB -> 3) Spark Job -> 4) Pipeline
# Connettori supportati: BC, CRM (selezionabili via -Connectors)
# =============================================================
param(
    [Parameter(Mandatory=$true)][string]$TenantId,
    [Parameter(Mandatory=$true)][string]$ClientId,
    [Parameter(Mandatory=$true)][string]$ClientSecret,
    [Parameter(Mandatory=$true)][string]$WorkspaceId,
    [string]$LakehouseName   = "LH_BC_Landing",
    [string]$Connectors      = "BC",
    [switch]$ForceRecreate,

    # ── Parametri BC (da DevOps Library) ──
    [string]$BcTenantId      = "",
    [string]$BcEnvironment   = "SandboxTest",
    [string]$BcCompanies     = '["CRONUS%20IT"]',
    [string]$BcEntities      = '["ItemLedgerEntries"]',
    [string]$BcPythonFile    = "bc_sync.py",
    [string]$MirroringDbName = "MirrorDB_BC_Landing",
    [string]$SparkJobName    = "SJD_BC_To_Mirroring",
    [string]$PipelineName    = "DP_BC_To_Mirroring",

    # ── Parametri CRM (da DevOps Library) ──
    [string]$CrmTenantId     = "",
    [string]$CrmClientId     = "",
    [string]$CrmClientSecret = "",
    [string]$CrmOrgUrl       = "",
    [string]$CrmEntities     = '["accounts"]',
    [string]$CrmPythonFile   = "crm_sync.py"
)

$ErrorActionPreference = "Stop"

# ── Parse connectors ──
$connectorList = $Connectors.Split(",") | ForEach-Object { $_.Trim().ToUpper() }
Write-Host "[CONFIG] Connettori selezionati: $($connectorList -join ', ')"

# ── Validazione parametri per connettore ──
if ($connectorList -contains "BC" -and [string]::IsNullOrEmpty($BcTenantId)) {
    Write-Host "##[error] BcTenantId e' obbligatorio quando il connettore BC e' selezionato."
    exit 1
}
if ($connectorList -contains "CRM") {
    foreach ($p in @{CrmTenantId=$CrmTenantId; CrmClientId=$CrmClientId; CrmClientSecret=$CrmClientSecret; CrmOrgUrl=$CrmOrgUrl}.GetEnumerator()) {
        if ([string]::IsNullOrEmpty($p.Value)) {
            Write-Host "##[error] $($p.Key) e' obbligatorio quando il connettore CRM e' selezionato."
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
        # PS 5.1: leggi il body della risposta HTTP dallo stream
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

# ─────────────────────────────────────────
# 0. AUTENTICAZIONE
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

# ─────────────────────────────────────────
# 1. CREA LAKEHOUSE (condiviso)
# ─────────────────────────────────────────
Write-Host ""
Write-Host "=== STEP 1: Lakehouse ==="

$lakehouseBody = @{
    displayName = $LakehouseName
    type        = "Lakehouse"
    description = "Lakehouse condiviso - contiene script Python e dati di staging"
} | ConvertTo-Json -Depth 5

$lakehouseId = Get-OrCreate-Item `
    -DisplayName $LakehouseName `
    -Type        "Lakehouse" `
    -CreateUrl   "$baseUrl/lakehouses" `
    -ListUrl     "$baseUrl/lakehouses" `
    -BodyJson    $lakehouseBody `
    -Headers     $headers

Write-Host "  Lakehouse ID: $lakehouseId"

# ─────────────────────────────────────────
# BC CONNECTOR
# ─────────────────────────────────────────
$bcMirroringId  = $null
$bcSparkJobId   = $null
$bcPipelineId   = $null

if ($connectorList -contains "BC") {
    Write-Host ""
    Write-Host "=== [BC] STEP 2: Mirroring Database ==="

    $bcMirroringPayload = @{
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

    $bcMirroringBody = @{
        displayName = $MirroringDbName
        description = "Open Mirroring Landing Zone - Business Central"
        definition  = @{
            parts = @(@{
                path        = "mirroring.json"
                payload     = (To-Base64 $bcMirroringPayload)
                payloadType = "InlineBase64"
            })
        }
    } | ConvertTo-Json -Depth 10

    $mirrorListUrl  = "$baseUrl/mirroredDatabases"
    $existingMirror = (Invoke-RestMethod -Uri $mirrorListUrl -Headers $headers -Method GET).value `
                      | Where-Object { $_.displayName -eq $MirroringDbName }

    if ($existingMirror) {
        $bcMirroringId = $existingMirror.id
        Write-Host "  [OK] Mirroring DB gia esistente - ID: $bcMirroringId"
    } else {
        Write-Host "  [NEW] Creo Mirroring Database '$MirroringDbName'..."
        $mirrorResult = Invoke-FabricApi -Method POST -Url $mirrorListUrl -Headers $headers -Body $bcMirroringBody
        $bcMirroringId  = $mirrorResult.id
        if ([string]::IsNullOrWhiteSpace($bcMirroringId)) {
            Write-Host "  [INFO] Risposta async senza ID, recupero dalla lista..."
            Start-Sleep -Seconds 3
            $createdMirror = (Invoke-RestMethod -Uri $mirrorListUrl -Headers $headers -Method GET).value `
                             | Where-Object { $_.displayName -eq $MirroringDbName }
            $bcMirroringId = $createdMirror.id
        }
        Write-Host "  [OK] Creato - ID: $bcMirroringId"
    }

    # ── BC Spark Job Definition ──
    Write-Host ""
    Write-Host "=== [BC] STEP 3: Spark Job Definition ==="

    $bcPythonLocalPath = Join-Path $PSScriptRoot "..\python\$BcPythonFile"
    Write-Host "  Lettura Python: $bcPythonLocalPath"
    $bcPythonBytes  = [System.IO.File]::ReadAllBytes($bcPythonLocalPath)
    $bcPythonBase64 = [Convert]::ToBase64String($bcPythonBytes)

    $bcSparkDefPayload = @{
        executableFile             = "$BcPythonFile"
        defaultLakehouseArtifactId = $lakehouseId
        mainClass                  = ""
        additionalLakehouseIds     = @()
        retryPolicy                = $null
        commandLineArguments       = ""
        additionalLibraryUris      = @()
        language                   = "Python"
        environmentArtifactId      = $null
    } | ConvertTo-Json -Depth 10

    $bcSparkDefinition = @{
        format = "SparkJobDefinitionV2"
        parts  = @(
            @{
                path        = "SparkJobDefinitionV1.json"
                payload     = (To-Base64 $bcSparkDefPayload)
                payloadType = "InlineBase64"
            },
            @{
                path        = "Main/$BcPythonFile"
                payload     = $bcPythonBase64
                payloadType = "InlineBase64"
            }
        )
    }

    $existingSJD = (Invoke-RestMethod -Uri "$baseUrl/sparkJobDefinitions" -Headers $headers -Method GET).value `
                   | Where-Object { $_.displayName -eq $SparkJobName }

    if ($existingSJD) {
        $bcSparkJobId = $existingSJD.id
        Write-Host "  [OK] Spark Job gia esistente - ID: $bcSparkJobId"

        Write-Host "  [UPD] Aggiorno definizione (codice Python)..."
        $updateBody = @{ definition = $bcSparkDefinition } | ConvertTo-Json -Depth 10
        Invoke-FabricApi -Method POST `
            -Url "$baseUrl/sparkJobDefinitions/$bcSparkJobId/updateDefinition" `
            -Headers $headers `
            -Body $updateBody
        Write-Host "  [OK] Definizione aggiornata"
    } else {
        Write-Host "  [NEW] Creo Spark Job Definition '$SparkJobName' (V2 con Python incluso)..."

        $bcSparkBody = @{
            displayName = $SparkJobName
            description = "Spark Job BC Sync - legge da BC e scrive su Mirroring Landing Zone"
            definition  = $bcSparkDefinition
        } | ConvertTo-Json -Depth 10

        $sjdResult  = Invoke-FabricApi -Method POST -Url "$baseUrl/sparkJobDefinitions" -Headers $headers -Body $bcSparkBody

        $bcSparkJobId = $sjdResult.id
        if ([string]::IsNullOrWhiteSpace($bcSparkJobId)) {
            Write-Host "  [INFO] Risposta async senza ID, recupero dalla lista..."
            Start-Sleep -Seconds 3
            $createdSJD = (Invoke-RestMethod -Uri "$baseUrl/sparkJobDefinitions" -Headers $headers -Method GET).value `
                          | Where-Object { $_.displayName -eq $SparkJobName }
            $bcSparkJobId = $createdSJD.id
        }
        Write-Host "  [OK] Creato - ID: $bcSparkJobId"
    }

    # ── BC Data Pipeline ──
    Write-Host ""
    Write-Host "=== [BC] STEP 4: Data Pipeline ==="

    Write-Host "  Spark Job ID: $bcSparkJobId"
    Write-Host "  Workspace ID: $WorkspaceId"
    Write-Host "  Lakehouse ID: $lakehouseId"

    $bcSparkJobId  = [string]$bcSparkJobId
    $WorkspaceId   = [string]$WorkspaceId
    $lakehouseId   = [string]$lakehouseId
    $bcMirroringId = [string]$bcMirroringId

    if ([string]::IsNullOrWhiteSpace($bcSparkJobId)) {
        Write-Host "##[error] bcSparkJobId e' vuoto! Impossibile creare la pipeline BC."
        exit 1
    }

    $companiesB64 = To-Base64 $BcCompanies
    $entitiesB64  = To-Base64 $BcEntities

    $bcSparkArgs = @(
        "--BC_TENANT_ID",         $BcTenantId,
        "--BC_CLIENT_ID",         $ClientId,
        "--BC_CLIENT_SECRET",     $ClientSecret,
        "--BC_ENVIRONMENT",       $BcEnvironment,
        "--BC_COMPANIES_B64",     $companiesB64,
        "--BC_ENTITIES_B64",      $entitiesB64,
        "--FABRIC_WORKSPACE_ID",  $WorkspaceId,
        "--FABRIC_LAKEHOUSE_ID",  $lakehouseId,
        "--FABRIC_MIRRORED_DB_ID", $bcMirroringId,
        "--FABRIC_TENANT_ID",     $TenantId,
        "--FABRIC_CLIENT_ID",     $ClientId,
        "--FABRIC_CLIENT_SECRET", $ClientSecret
    ) -join " "

    Write-Host "  commandLineArguments costruiti (companies/entities in Base64)"

    $bcPipelineDefinition = @"
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
                    "sparkJobDefinitionId": "$bcSparkJobId",
                    "workspaceId": "$WorkspaceId",
                    "commandLineArguments": "$bcSparkArgs",
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

    $pipelineListUrl  = "$baseUrl/dataPipelines"
    $existingPipeline = (Invoke-RestMethod -Uri $pipelineListUrl -Headers $headers -Method GET).value `
                        | Where-Object { $_.displayName -eq $PipelineName }

    if ($existingPipeline -and $ForceRecreate) {
        Write-Host "  [DEL] ForceRecreate: elimino pipeline esistente '$PipelineName'..."
        Invoke-FabricApi -Method DELETE -Url "$pipelineListUrl/$($existingPipeline.id)" -Headers $headers
        Write-Host "  [OK] Pipeline eliminata"
        $existingPipeline = $null
    }

    if ($existingPipeline) {
        $bcPipelineId = $existingPipeline.id
        Write-Host "  [OK] Pipeline gia esistente - ID: $bcPipelineId"

        Write-Host "  [UPD] Aggiorno la definizione della pipeline..."
        $updateBody = @{
            definition = @{
                parts = @(@{
                    path        = "pipeline-content.json"
                    payload     = (To-Base64 $bcPipelineDefinition)
                    payloadType = "InlineBase64"
                })
            }
        } | ConvertTo-Json -Depth 10

        Invoke-FabricApi -Method POST `
            -Url "$pipelineListUrl/$bcPipelineId/updateDefinition" `
            -Headers $headers `
            -Body $updateBody
        Write-Host "  [OK] Definizione aggiornata"
    } else {
        Write-Host "  [NEW] Creo Data Pipeline '$PipelineName'..."

        $bcPipelineBody = @{
            displayName = $PipelineName
            description = "Pipeline BC - esegue Spark Job $SparkJobName"
            definition  = @{
                parts = @(@{
                    path        = "pipeline-content.json"
                    payload     = (To-Base64 $bcPipelineDefinition)
                    payloadType = "InlineBase64"
                })
            }
        } | ConvertTo-Json -Depth 10

        $pipelineResult = Invoke-FabricApi -Method POST -Url $pipelineListUrl -Headers $headers -Body $bcPipelineBody
        $bcPipelineId   = $pipelineResult.id
        if ([string]::IsNullOrWhiteSpace($bcPipelineId)) {
            Write-Host "  [INFO] Risposta async senza ID, recupero dalla lista..."
            Start-Sleep -Seconds 3
            $createdPipeline = (Invoke-RestMethod -Uri $pipelineListUrl -Headers $headers -Method GET).value `
                               | Where-Object { $_.displayName -eq $PipelineName }
            $bcPipelineId = $createdPipeline.id
        }
        Write-Host "  [OK] Creato - ID: $bcPipelineId"
    }
}

# ─────────────────────────────────────────
# CRM CONNECTOR
# ─────────────────────────────────────────
$crmMirroringDbName = "MirrorDB_CRM_Landing"
$crmSparkJobName    = "SJD_CRM_To_Mirroring"
$crmPipelineName    = "DP_CRM_To_Mirroring"

$crmMirroringId = $null
$crmSparkJobId  = $null
$crmPipelineId  = $null

if ($connectorList -contains "CRM") {
    Write-Host ""
    Write-Host "=== [CRM] STEP 2: Mirroring Database ==="

    $crmMirroringPayload = @{
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

    $crmMirroringBody = @{
        displayName = $crmMirroringDbName
        description = "Open Mirroring Landing Zone - Dynamics CRM"
        definition  = @{
            parts = @(@{
                path        = "mirroring.json"
                payload     = (To-Base64 $crmMirroringPayload)
                payloadType = "InlineBase64"
            })
        }
    } | ConvertTo-Json -Depth 10

    $crmMirrorListUrl  = "$baseUrl/mirroredDatabases"
    $existingCrmMirror = (Invoke-RestMethod -Uri $crmMirrorListUrl -Headers $headers -Method GET).value `
                         | Where-Object { $_.displayName -eq $crmMirroringDbName }

    if ($existingCrmMirror) {
        $crmMirroringId = $existingCrmMirror.id
        Write-Host "  [OK] Mirroring DB gia esistente - ID: $crmMirroringId"
    } else {
        Write-Host "  [NEW] Creo Mirroring Database '$crmMirroringDbName'..."
        $crmMirrorResult = Invoke-FabricApi -Method POST -Url $crmMirrorListUrl -Headers $headers -Body $crmMirroringBody
        $crmMirroringId  = $crmMirrorResult.id
        if ([string]::IsNullOrWhiteSpace($crmMirroringId)) {
            Write-Host "  [INFO] Risposta async senza ID, recupero dalla lista..."
            Start-Sleep -Seconds 3
            $createdCrmMirror = (Invoke-RestMethod -Uri $crmMirrorListUrl -Headers $headers -Method GET).value `
                                | Where-Object { $_.displayName -eq $crmMirroringDbName }
            $crmMirroringId = $createdCrmMirror.id
        }
        Write-Host "  [OK] Creato - ID: $crmMirroringId"
    }

    # ── CRM Spark Job Definition ──
    Write-Host ""
    Write-Host "=== [CRM] STEP 3: Spark Job Definition ==="

    $crmPythonLocalPath = Join-Path $PSScriptRoot "..\python\$CrmPythonFile"
    Write-Host "  Lettura Python: $crmPythonLocalPath"
    $crmPythonBytes  = [System.IO.File]::ReadAllBytes($crmPythonLocalPath)
    $crmPythonBase64 = [Convert]::ToBase64String($crmPythonBytes)

    $crmSparkDefPayload = @{
        executableFile             = "$CrmPythonFile"
        defaultLakehouseArtifactId = $lakehouseId
        mainClass                  = ""
        additionalLakehouseIds     = @()
        retryPolicy                = $null
        commandLineArguments       = ""
        additionalLibraryUris      = @()
        language                   = "Python"
        environmentArtifactId      = $null
    } | ConvertTo-Json -Depth 10

    $crmSparkDefinition = @{
        format = "SparkJobDefinitionV2"
        parts  = @(
            @{
                path        = "SparkJobDefinitionV1.json"
                payload     = (To-Base64 $crmSparkDefPayload)
                payloadType = "InlineBase64"
            },
            @{
                path        = "Main/$CrmPythonFile"
                payload     = $crmPythonBase64
                payloadType = "InlineBase64"
            }
        )
    }

    $existingCrmSJD = (Invoke-RestMethod -Uri "$baseUrl/sparkJobDefinitions" -Headers $headers -Method GET).value `
                      | Where-Object { $_.displayName -eq $crmSparkJobName }

    if ($existingCrmSJD) {
        $crmSparkJobId = $existingCrmSJD.id
        Write-Host "  [OK] Spark Job gia esistente - ID: $crmSparkJobId"

        Write-Host "  [UPD] Aggiorno definizione (codice Python)..."
        $updateBody = @{ definition = $crmSparkDefinition } | ConvertTo-Json -Depth 10
        Invoke-FabricApi -Method POST `
            -Url "$baseUrl/sparkJobDefinitions/$crmSparkJobId/updateDefinition" `
            -Headers $headers `
            -Body $updateBody
        Write-Host "  [OK] Definizione aggiornata"
    } else {
        Write-Host "  [NEW] Creo Spark Job Definition '$crmSparkJobName' (V2 con Python incluso)..."

        $crmSparkBody = @{
            displayName = $crmSparkJobName
            description = "Spark Job CRM Sync - legge da Dynamics CRM e scrive su Mirroring Landing Zone"
            definition  = $crmSparkDefinition
        } | ConvertTo-Json -Depth 10

        $crmSjdResult  = Invoke-FabricApi -Method POST -Url "$baseUrl/sparkJobDefinitions" -Headers $headers -Body $crmSparkBody

        $crmSparkJobId = $crmSjdResult.id
        if ([string]::IsNullOrWhiteSpace($crmSparkJobId)) {
            Write-Host "  [INFO] Risposta async senza ID, recupero dalla lista..."
            Start-Sleep -Seconds 3
            $createdCrmSJD = (Invoke-RestMethod -Uri "$baseUrl/sparkJobDefinitions" -Headers $headers -Method GET).value `
                             | Where-Object { $_.displayName -eq $crmSparkJobName }
            $crmSparkJobId = $createdCrmSJD.id
        }
        Write-Host "  [OK] Creato - ID: $crmSparkJobId"
    }

    # ── CRM Data Pipeline ──
    Write-Host ""
    Write-Host "=== [CRM] STEP 4: Data Pipeline ==="

    Write-Host "  Spark Job ID: $crmSparkJobId"
    Write-Host "  Workspace ID: $WorkspaceId"
    Write-Host "  Lakehouse ID: $lakehouseId"

    $crmSparkJobId  = [string]$crmSparkJobId
    $crmMirroringId = [string]$crmMirroringId

    if ([string]::IsNullOrWhiteSpace($crmSparkJobId)) {
        Write-Host "##[error] crmSparkJobId e' vuoto! Impossibile creare la pipeline CRM."
        exit 1
    }

    $crmEntitiesB64 = To-Base64 $CrmEntities

    $crmSparkArgs = @(
        "--CRM_TENANT_ID",        $CrmTenantId,
        "--CRM_CLIENT_ID",        $CrmClientId,
        "--CRM_CLIENT_SECRET",    $CrmClientSecret,
        "--CRM_ORG_URL",          $CrmOrgUrl,
        "--CRM_ENTITIES_B64",     $crmEntitiesB64,
        "--FABRIC_WORKSPACE_ID",  $WorkspaceId,
        "--FABRIC_LAKEHOUSE_ID",  $lakehouseId,
        "--FABRIC_MIRRORED_DB_ID", $crmMirroringId,
        "--FABRIC_TENANT_ID",     $TenantId,
        "--FABRIC_CLIENT_ID",     $ClientId,
        "--FABRIC_CLIENT_SECRET", $ClientSecret
    ) -join " "

    Write-Host "  commandLineArguments CRM costruiti (entities in Base64)"

    $crmPipelineDefinition = @"
{
    "name": "$crmPipelineName",
    "objectId": "$(([guid]::NewGuid()).ToString())",
    "properties": {
        "activities": [
            {
                "name": "$crmSparkJobName",
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
                    "sparkJobDefinitionId": "$crmSparkJobId",
                    "workspaceId": "$WorkspaceId",
                    "commandLineArguments": "$crmSparkArgs",
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

    $crmPipelineListUrl  = "$baseUrl/dataPipelines"
    $existingCrmPipeline = (Invoke-RestMethod -Uri $crmPipelineListUrl -Headers $headers -Method GET).value `
                           | Where-Object { $_.displayName -eq $crmPipelineName }

    if ($existingCrmPipeline -and $ForceRecreate) {
        Write-Host "  [DEL] ForceRecreate: elimino pipeline esistente '$crmPipelineName'..."
        Invoke-FabricApi -Method DELETE -Url "$crmPipelineListUrl/$($existingCrmPipeline.id)" -Headers $headers
        Write-Host "  [OK] Pipeline eliminata"
        $existingCrmPipeline = $null
    }

    if ($existingCrmPipeline) {
        $crmPipelineId = $existingCrmPipeline.id
        Write-Host "  [OK] Pipeline gia esistente - ID: $crmPipelineId"

        Write-Host "  [UPD] Aggiorno la definizione della pipeline CRM..."
        $updateBody = @{
            definition = @{
                parts = @(@{
                    path        = "pipeline-content.json"
                    payload     = (To-Base64 $crmPipelineDefinition)
                    payloadType = "InlineBase64"
                })
            }
        } | ConvertTo-Json -Depth 10

        Invoke-FabricApi -Method POST `
            -Url "$crmPipelineListUrl/$crmPipelineId/updateDefinition" `
            -Headers $headers `
            -Body $updateBody
        Write-Host "  [OK] Definizione aggiornata"
    } else {
        Write-Host "  [NEW] Creo Data Pipeline '$crmPipelineName'..."

        $crmPipelineBody = @{
            displayName = $crmPipelineName
            description = "Pipeline CRM - esegue Spark Job $crmSparkJobName"
            definition  = @{
                parts = @(@{
                    path        = "pipeline-content.json"
                    payload     = (To-Base64 $crmPipelineDefinition)
                    payloadType = "InlineBase64"
                })
            }
        } | ConvertTo-Json -Depth 10

        $crmPipelineResult = Invoke-FabricApi -Method POST -Url $crmPipelineListUrl -Headers $headers -Body $crmPipelineBody
        $crmPipelineId     = $crmPipelineResult.id
        if ([string]::IsNullOrWhiteSpace($crmPipelineId)) {
            Write-Host "  [INFO] Risposta async senza ID, recupero dalla lista..."
            Start-Sleep -Seconds 3
            $createdCrmPipeline = (Invoke-RestMethod -Uri $crmPipelineListUrl -Headers $headers -Method GET).value `
                                  | Where-Object { $_.displayName -eq $crmPipelineName }
            $crmPipelineId = $createdCrmPipeline.id
        }
        Write-Host "  [OK] Creato - ID: $crmPipelineId"
    }
}

# ─────────────────────────────────────────
# RIEPILOGO
# ─────────────────────────────────────────
Write-Host ""
Write-Host "============================================"
Write-Host "[OK] DEPLOY COMPLETATO"
Write-Host "============================================"
Write-Host "  Connettori       : $($connectorList -join ', ')"
Write-Host "  Lakehouse        : $lakehouseId"
if ($connectorList -contains "BC") {
    Write-Host "  [BC] Mirroring DB: $bcMirroringId"
    Write-Host "  [BC] Spark Job   : $bcSparkJobId"
    Write-Host "  [BC] Pipeline    : $bcPipelineId"
}
if ($connectorList -contains "CRM") {
    Write-Host "  [CRM] Mirroring DB: $crmMirroringId"
    Write-Host "  [CRM] Spark Job   : $crmSparkJobId"
    Write-Host "  [CRM] Pipeline    : $crmPipelineId"
}
Write-Host "============================================"
Write-Host ""

Write-Host "##vso[task.setvariable variable=LAKEHOUSE_ID]$lakehouseId"
if ($connectorList -contains "BC") {
    Write-Host "##vso[task.setvariable variable=BC_MIRRORING_ID]$bcMirroringId"
    Write-Host "##vso[task.setvariable variable=BC_SPARK_JOB_ID]$bcSparkJobId"
    Write-Host "##vso[task.setvariable variable=BC_PIPELINE_ID]$bcPipelineId"
}
if ($connectorList -contains "CRM") {
    Write-Host "##vso[task.setvariable variable=CRM_MIRRORING_ID]$crmMirroringId"
    Write-Host "##vso[task.setvariable variable=CRM_SPARK_JOB_ID]$crmSparkJobId"
    Write-Host "##vso[task.setvariable variable=CRM_PIPELINE_ID]$crmPipelineId"
}
