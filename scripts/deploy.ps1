# =============================================================
# deploy.ps1 - Deploy completo soluzione Fabric
# Compatibile con PowerShell 5.1 (Windows runner DevOps)
# Ordine: 1) Lakehouse -> 2) Mirroring DB -> 3) Spark Job -> 4) Pipeline
# =============================================================
param(
    [Parameter(Mandatory=$true)][string]$TenantId,
    [Parameter(Mandatory=$true)][string]$ClientId,
    [Parameter(Mandatory=$true)][string]$ClientSecret,
    [Parameter(Mandatory=$true)][string]$WorkspaceId,
    [string]$LakehouseName   = "LH_BC_Landing",
    [string]$MirroringDbName = "MirrorDB_BC_Landing",
    [string]$SparkJobName    = "SJD_BC_To_Mirroring",
    [string]$PipelineName    = "DP_BC_To_Mirroring",
    [string]$PythonFileName  = "bc_sync.py",

    # ── Parametri BC (da DevOps Library) ──
    [Parameter(Mandatory=$true)][string]$BcTenantId,
    [string]$BcEnvironment   = "SandboxTest",
    [string]$BcCompanies     = '["CRONUS%20IT"]',
    [string]$BcEntities      = '["ItemLedgerEntries"]'
)

$ErrorActionPreference = "Stop"

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
# 1. CREA LAKEHOUSE
# ─────────────────────────────────────────
Write-Host ""
Write-Host "=== STEP 1: Lakehouse ==="

$lakehouseBody = @{
    displayName = $LakehouseName
    type        = "Lakehouse"
    description = "Lakehouse BC - contiene script Python e dati di staging"
} | ConvertTo-Json -Depth 5

$lakehouseId = Get-OrCreate-Item `
    -DisplayName $LakehouseName `
    -Type        "Lakehouse" `
    -CreateUrl   "$baseUrl/lakehouses" `
    -ListUrl     "$baseUrl/lakehouses" `
    -BodyJson    $lakehouseBody `
    -Headers     $headers

Write-Host "  Lakehouse ID: $lakehouseId"
Write-Host "  NOTA: caricare manualmente bc_sync.py in Files/Scripts/ del Lakehouse"

# ─────────────────────────────────────────
# 2. CREA MIRRORING DATABASE
# ─────────────────────────────────────────
Write-Host ""
Write-Host "=== STEP 2: Mirroring Database (Open Mirroring) ==="

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
    displayName = $MirroringDbName
    description = "Open Mirroring Landing Zone - Business Central"
    definition  = @{
        parts = @(@{
            path        = "mirroring.json"
            payload     = (To-Base64 $mirroringPayload)
            payloadType = "InlineBase64"
        })
    }
} | ConvertTo-Json -Depth 10

$mirrorListUrl  = "$baseUrl/mirroredDatabases"
$existingMirror = (Invoke-RestMethod -Uri $mirrorListUrl -Headers $headers -Method GET).value `
                  | Where-Object { $_.displayName -eq $MirroringDbName }

if ($existingMirror) {
    $mirroringId = $existingMirror.id
    Write-Host "  [OK] Mirroring DB gia esistente - ID: $mirroringId"
} else {
    Write-Host "  [NEW] Creo Mirroring Database '$MirroringDbName'..."
    $mirrorResult = Invoke-FabricApi -Method POST -Url $mirrorListUrl -Headers $headers -Body $mirroringBody
    $mirroringId  = $mirrorResult.id
    if ([string]::IsNullOrWhiteSpace($mirroringId)) {
        Write-Host "  [INFO] Risposta async senza ID, recupero dalla lista..."
        Start-Sleep -Seconds 3
        $createdMirror = (Invoke-RestMethod -Uri $mirrorListUrl -Headers $headers -Method GET).value `
                         | Where-Object { $_.displayName -eq $MirroringDbName }
        $mirroringId = $createdMirror.id
    }
    Write-Host "  [OK] Creato - ID: $mirroringId"
}

# ─────────────────────────────────────────
# 3. CREA SPARK JOB DEFINITION (V2)
#    Include il file Python direttamente
#    nel payload - nessun upload separato.
#    I parametri vengono passati dalla Pipeline
#    (non dallo Spark Job) via commandLineArguments.
# ─────────────────────────────────────────
Write-Host ""
Write-Host "=== STEP 3: Spark Job Definition ==="

# Leggi il file Python dal repo e codificalo in base64
$pythonLocalPath = Join-Path $PSScriptRoot "..\python\$PythonFileName"
Write-Host "  Lettura Python: $pythonLocalPath"
$pythonBytes  = [System.IO.File]::ReadAllBytes($pythonLocalPath)
$pythonBase64 = [Convert]::ToBase64String($pythonBytes)

# SparkJobDefinitionV1.json — metadata del job (senza commandLineArguments, li passa la pipeline)
$sparkDefPayload = @{
    executableFile             = "$PythonFileName"
    defaultLakehouseArtifactId = $lakehouseId
    mainClass                  = ""
    additionalLakehouseIds     = @()
    retryPolicy                = $null
    commandLineArguments       = ""
    additionalLibraryUris      = @()
    language                   = "Python"
    environmentArtifactId      = $null
} | ConvertTo-Json -Depth 10

# Definizione completa V2 (riusata sia per create che update)
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

# Controlla se esiste gia
$existingSJD = (Invoke-RestMethod -Uri "$baseUrl/sparkJobDefinitions" -Headers $headers -Method GET).value `
               | Where-Object { $_.displayName -eq $SparkJobName }

if ($existingSJD) {
    $sparkJobId = $existingSJD.id
    Write-Host "  [OK] Spark Job gia esistente - ID: $sparkJobId"

    # Aggiorna definizione (Python) per sincronizzare con DevOps
    Write-Host "  [UPD] Aggiorno definizione (codice Python)..."
    $updateBody = @{ definition = $sparkDefinition } | ConvertTo-Json -Depth 10
    Invoke-FabricApi -Method POST `
        -Url "$baseUrl/sparkJobDefinitions/$sparkJobId/updateDefinition" `
        -Headers $headers `
        -Body $updateBody
    Write-Host "  [OK] Definizione aggiornata"
} else {
    Write-Host "  [NEW] Creo Spark Job Definition '$SparkJobName' (V2 con Python incluso)..."

    $sparkBody = @{
        displayName = $SparkJobName
        description = "Spark Job BC Sync - legge da BC e scrive su Mirroring Landing Zone"
        definition  = $sparkDefinition
    } | ConvertTo-Json -Depth 10

    $sjdResult  = Invoke-FabricApi -Method POST -Url "$baseUrl/sparkJobDefinitions" -Headers $headers -Body $sparkBody

    # L'API asincrona (202) non restituisce l'ID nell'operazione.
    # Recupero l'ID con un GET sulla lista dopo la creazione.
    $sparkJobId = $sjdResult.id
    if ([string]::IsNullOrWhiteSpace($sparkJobId)) {
        Write-Host "  [INFO] Risposta async senza ID, recupero dalla lista..."
        Start-Sleep -Seconds 3
        $createdSJD = (Invoke-RestMethod -Uri "$baseUrl/sparkJobDefinitions" -Headers $headers -Method GET).value `
                      | Where-Object { $_.displayName -eq $SparkJobName }
        $sparkJobId = $createdSJD.id
    }
    Write-Host "  [OK] Creato - ID: $sparkJobId"
}

# ─────────────────────────────────────────
# 4. CREA DATA PIPELINE
#    Contiene un'activity Spark Job Definition
#    che punta al job creato nello step 3
# ─────────────────────────────────────────
Write-Host ""
Write-Host "=== STEP 4: Data Pipeline ==="

# Definizione JSON della pipeline (here-string per controllo totale)
Write-Host "  Spark Job ID: $sparkJobId"
Write-Host "  Workspace ID: $WorkspaceId"
Write-Host "  Lakehouse ID: $lakehouseId"

# Sicurezza: forza ID a stringhe pulite (in caso di oggetti PS)
$sparkJobId  = [string]$sparkJobId
$WorkspaceId = [string]$WorkspaceId
$lakehouseId = [string]$lakehouseId
$mirroringId = [string]$mirroringId

if ([string]::IsNullOrWhiteSpace($sparkJobId)) {
    Write-Host "##[error] sparkJobId e' vuoto! Impossibile creare la pipeline."
    exit 1
}

# ── Costruisci commandLineArguments per la pipeline ──
# I valori JSON (companies, entities) vengono codificati in Base64
# per evitare problemi di parsing con spazi e caratteri speciali.
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

Write-Host "  [DEBUG] Pipeline definition creata"

# Controlla se esiste gia
$pipelineListUrl  = "$baseUrl/dataPipelines"
$existingPipeline = (Invoke-RestMethod -Uri $pipelineListUrl -Headers $headers -Method GET).value `
                    | Where-Object { $_.displayName -eq $PipelineName }

if ($existingPipeline) {
    $pipelineId = $existingPipeline.id
    Write-Host "  [OK] Pipeline gia esistente - ID: $pipelineId"

    # Aggiorna la definizione della pipeline esistente
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

# ─────────────────────────────────────────
# RIEPILOGO
# ─────────────────────────────────────────
Write-Host ""
Write-Host "============================================"
Write-Host "[OK] DEPLOY COMPLETATO"
Write-Host "============================================"
Write-Host "  Lakehouse         : $lakehouseId"
Write-Host "  Mirroring Database: $mirroringId"
Write-Host "  Spark Job         : $sparkJobId"
Write-Host "  Data Pipeline     : $pipelineId"
Write-Host "============================================"
Write-Host ""
Write-Host "PROSSIMO STEP:"
Write-Host "  Carica bc_sync.py nel Lakehouse:"
Write-Host "  $LakehouseName -> Files -> Scripts -> bc_sync.py"
Write-Host ""

Write-Host "##vso[task.setvariable variable=LAKEHOUSE_ID]$lakehouseId"
Write-Host "##vso[task.setvariable variable=MIRRORING_ID]$mirroringId"
Write-Host "##vso[task.setvariable variable=SPARK_JOB_ID]$sparkJobId"
Write-Host "##vso[task.setvariable variable=PIPELINE_ID]$pipelineId"