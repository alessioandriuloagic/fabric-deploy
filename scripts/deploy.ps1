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
    [string]$PythonFileName  = "bc_sync.py"
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
    Write-Host "  [OK] Creato - ID: $mirroringId"
}

# ─────────────────────────────────────────
# 3. CREA SPARK JOB DEFINITION (V2)
#    Include il file Python direttamente
#    nel payload - nessun upload separato
# ─────────────────────────────────────────
Write-Host ""
Write-Host "=== STEP 3: Spark Job Definition ==="

# Leggi il file Python dal repo e codificalo in base64
$pythonLocalPath = Join-Path $PSScriptRoot "..\python\$PythonFileName"
Write-Host "  Lettura Python: $pythonLocalPath"
$pythonBytes  = [System.IO.File]::ReadAllBytes($pythonLocalPath)
$pythonBase64 = [Convert]::ToBase64String($pythonBytes)

# SparkJobDefinitionV1.json — metadata del job
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

# Controlla se esiste gia
$existingSJD = (Invoke-RestMethod -Uri "$baseUrl/sparkJobDefinitions" -Headers $headers -Method GET).value `
               | Where-Object { $_.displayName -eq $SparkJobName }

if ($existingSJD) {
    $sparkJobId = $existingSJD.id
    Write-Host "  [OK] Spark Job gia esistente - ID: $sparkJobId"
} else {
    Write-Host "  [NEW] Creo Spark Job Definition '$SparkJobName' (V2 con Python incluso)..."

    # Formato V2: include il file Python direttamente nel payload
    $sparkBody = @{
        displayName = $SparkJobName
        description = "Spark Job BC Sync - legge da BC e scrive su Mirroring Landing Zone"
        definition  = @{
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
    } | ConvertTo-Json -Depth 10

    $sjdResult  = Invoke-FabricApi -Method POST -Url "$baseUrl/sparkJobDefinitions" -Headers $headers -Body $sparkBody
    $sparkJobId = $sjdResult.id
    Write-Host "  [OK] Creato - ID: $sparkJobId"
}

# ─────────────────────────────────────────
# 4. CREA DATA PIPELINE
# ─────────────────────────────────────────
Write-Host ""
Write-Host "=== STEP 4: Data Pipeline ==="

$pipelinePayload = @{
    name       = $PipelineName
    properties = @{
        activities = @(
            @{
                name      = "Run BC Sync"
                type      = "SparkJob"
                dependsOn = @()
                policy    = @{
                    timeout                = "0.12:00:00"
                    retry                  = 0
                    retryIntervalInSeconds = 30
                    secureOutput           = $false
                    secureInput            = $false
                }
                typeProperties = @{
                    sparkJobDefinitionId = $sparkJobId
                    workspaceId          = $WorkspaceId
                }
            }
        )
    }
} | ConvertTo-Json -Depth 10

$pipelineBody = @{
    displayName = $PipelineName
    type        = "DataPipeline"
    definition  = @{
        parts = @(@{
            path        = "pipeline-content.json"
            payload     = (To-Base64 $pipelinePayload)
            payloadType = "InlineBase64"
        })
    }
} | ConvertTo-Json -Depth 10

$pipelineId = Get-OrCreate-Item `
    -DisplayName $PipelineName `
    -Type        "DataPipeline" `
    -CreateUrl   "$baseUrl/items" `
    -ListUrl     "$baseUrl/items" `
    -BodyJson    $pipelineBody `
    -Headers     $headers

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