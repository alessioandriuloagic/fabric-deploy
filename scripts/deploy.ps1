# =============================================================
# deploy.ps1 - Deploy completo soluzione Fabric
# Compatibile con PowerShell 5.1 (Windows runner DevOps)
# Ordine: 1) Upload Python -> 2) Mirroring DB -> 3) Spark Job -> 4) Pipeline
# =============================================================
param(
    [Parameter(Mandatory=$true)][string]$TenantId,
    [Parameter(Mandatory=$true)][string]$ClientId,
    [Parameter(Mandatory=$true)][string]$ClientSecret,
    [Parameter(Mandatory=$true)][string]$WorkspaceId,
    [Parameter(Mandatory=$true)][string]$LakehouseId,
    [string]$PythonFileName  = "bc_sync.py",
    [string]$MirroringDbName = "MirrorDB_BC_Landing",
    [string]$SparkJobName    = "SJD_BC_To_Mirroring",
    [string]$PipelineName    = "DP_BC_To_Mirroring"
)

Set-StrictMode -Version Latest
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

    $response = Invoke-WebRequest @params -UseBasicParsing

    # Gestione Long Running Operation (202 Accepted)
    if ($response.StatusCode -eq 202) {
        $locationUrl = $response.Headers["Location"]

        # PS 5.1 non supporta ??, usiamo if/else
        $retryAfterRaw = $response.Headers["Retry-After"]
        if ($retryAfterRaw) {
            $retryAfter = [int]$retryAfterRaw
        } else {
            $retryAfter = 5
        }

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
$baseUrl    = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId"
$onelakeUrl = "https://onelake.dfs.fabric.microsoft.com"
Write-Host "[OK] Token Fabric ottenuto"

# ─────────────────────────────────────────
# 1. UPLOAD FILE PYTHON SU LAKEHOUSE
# ─────────────────────────────────────────
Write-Host ""
Write-Host "=== STEP 1: Upload Python sul Lakehouse ==="

$onelakeTokenBody = @{
    grant_type    = "client_credentials"
    client_id     = $ClientId
    client_secret = $ClientSecret
    scope         = "https://storage.azure.com/.default"
}
$onelakeToken   = (Invoke-RestMethod -Uri $tokenUrl -Method POST -Body $onelakeTokenBody).access_token
$onelakeHeaders = @{
    Authorization  = "Bearer $onelakeToken"
    "x-ms-version" = "2023-01-03"
}

$pythonLocalPath = Join-Path $PSScriptRoot "..\python\$PythonFileName"
$pythonUploadUrl = "$onelakeUrl/$WorkspaceId/$LakehouseId/Files/Scripts/$PythonFileName"

Write-Host "  Upload: $PythonFileName -> Files/Scripts/"

$pythonBytes = [System.IO.File]::ReadAllBytes($pythonLocalPath)

# Step 1a: crea file vuoto
Invoke-RestMethod -Uri "${pythonUploadUrl}?resource=file" `
    -Method PUT -Headers $onelakeHeaders | Out-Null

# Step 1b: append contenuto
$appendHeaders = $onelakeHeaders.Clone()
$appendHeaders["Content-Length"] = $pythonBytes.Length.ToString()
Invoke-RestMethod -Uri "${pythonUploadUrl}?action=append&position=0" `
    -Method PATCH -Headers $appendHeaders -Body $pythonBytes | Out-Null

# Step 1c: flush (commit)
Invoke-RestMethod -Uri "${pythonUploadUrl}?action=flush&position=$($pythonBytes.Length)" `
    -Method PATCH -Headers $onelakeHeaders | Out-Null

Write-Host "  [OK] Python caricato: Files/Scripts/$PythonFileName"

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
# 3. CREA SPARK JOB DEFINITION
# ─────────────────────────────────────────
Write-Host ""
Write-Host "=== STEP 3: Spark Job Definition ==="

$sparkDefPayload = @{
    executableFile              = "Files/Scripts/$PythonFileName"
    defaultLakehouseArtifactId  = $LakehouseId
    defaultLakehouseWorkspaceId = $WorkspaceId
    mainClass                   = ""
    additionalLakehouseIds      = @()
    retryPolicy                 = $null
    commandLineArguments        = ""
    environmentArtifactId       = $null
    environmentWorkspaceId      = $null
} | ConvertTo-Json -Depth 10

$sparkBody = @{
    displayName = $SparkJobName
    type        = "SparkJobDefinition"
    definition  = @{
        parts = @(@{
            path        = "SparkJobDefinitionV1.json"
            payload     = (To-Base64 $sparkDefPayload)
            payloadType = "InlineBase64"
        })
    }
} | ConvertTo-Json -Depth 10

$sparkJobId = Get-OrCreate-Item `
    -DisplayName $SparkJobName `
    -Type        "SparkJobDefinition" `
    -CreateUrl   "$baseUrl/items" `
    -ListUrl     "$baseUrl/items" `
    -BodyJson    $sparkBody `
    -Headers     $headers

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
Write-Host "  Mirroring Database : $mirroringId"
Write-Host "  Spark Job          : $sparkJobId"
Write-Host "  Data Pipeline      : $pipelineId"
Write-Host "  Python             : Files/Scripts/$PythonFileName"
Write-Host "============================================"

# Esporta ID come variabili DevOps per step successivi
Write-Host "##vso[task.setvariable variable=MIRRORING_ID]$mirroringId"
Write-Host "##vso[task.setvariable variable=SPARK_JOB_ID]$sparkJobId"
Write-Host "##vso[task.setvariable variable=PIPELINE_ID]$pipelineId"