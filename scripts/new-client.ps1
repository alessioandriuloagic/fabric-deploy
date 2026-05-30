# =============================================================
# new-client.ps1 - Onboarding di un nuovo cliente
#
# Crea un GitHub Environment (= cliente) e vi imposta secret e
# variabili necessari al deploy Fabric. Da eseguire UNA TANTUM
# per ogni nuovo cliente. Dopo, usare deploy-wizard.ps1 per i
# deploy.
#
# Prerequisiti:
#   - GitHub CLI: gh auth login (scope 'repo')
#   - Eseguire dalla root del repo (o passare -Repo owner/repo)
#
# Modi d'uso:
#   Interattivo (consigliato):
#     .\scripts\new-client.ps1 -Client cliente-x
#   Non interattivo (BC):
#     .\scripts\new-client.ps1 -Client cliente-x -Connectors BC `
#         -FabricTenantId ... -FabricClientId ... -FabricClientSecret ... `
#         -FabricWorkspaceId ... -BcTenantId ... -BcEnvironment Production `
#         -BcCompanies "CRONUS IT" -BcEntities "ItemLedgerEntries,Customers"
# =============================================================
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Client,
    [ValidateSet("BC", "CRM", "BC,CRM")]
    [string]$Connectors = "BC",
    [string]$Repo,

    # Fabric (sempre richiesti)
    [string]$FabricTenantId,
    [string]$FabricClientId,
    [string]$FabricClientSecret,
    [string]$FabricWorkspaceId,

    # BC
    [string]$BcTenantId,
    [string]$BcEnvironment,
    [string]$BcCompanies,
    [string]$BcEntities,

    # CRM / Dataverse
    [string]$CrmOrgUrl,
    [string]$CrmEnvironmentDomain,
    [string]$CrmEntities,
    [string]$CrmTenantId,
    [string]$CrmClientId,
    [string]$CrmClientSecret,
    [string]$CrmConnectionName
)

$ErrorActionPreference = "Stop"

function Assert-Gh {
    if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
        throw "GitHub CLI 'gh' non trovata. Installa da https://cli.github.com/ e poi: gh auth login"
    }
    gh auth status 1>$null 2>$null
    if ($LASTEXITCODE -ne 0) { throw "Non autenticato. Esegui: gh auth login" }
}

function Resolve-Repo {
    param([string]$Repo)
    if ($Repo) { return $Repo }
    $r = gh repo view --json nameWithOwner -q ".nameWithOwner" 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($r)) {
        throw "Impossibile rilevare il repo. Esegui dalla cartella del repo oppure passa -Repo owner/repo."
    }
    return $r.Trim()
}

function Read-IfEmpty {
    param([string]$Value, [string]$Prompt, [switch]$Secret)
    if ($Value) { return $Value }
    if ($Secret) {
        $sec = Read-Host -AsSecureString $Prompt
        $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec)
        try { return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
        finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
    }
    return Read-Host $Prompt
}

function Set-EnvSecret {
    param([string]$Repo, [string]$EnvName, [string]$Name, [string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) { return }
    $Value | gh secret set $Name --repo $Repo --env $EnvName --body - 1>$null
    if ($LASTEXITCODE -ne 0) { throw "Errore impostando secret $Name" }
    Write-Host "  [secret] $Name = ********"
}

function Set-EnvVar {
    param([string]$Repo, [string]$EnvName, [string]$Name, [string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) { return }
    gh variable set $Name --repo $Repo --env $EnvName --body $Value 1>$null
    if ($LASTEXITCODE -ne 0) { throw "Errore impostando variabile $Name" }
    Write-Host "  [var]    $Name = $Value"
}

# ── Avvio ────────────────────────────────────────────────────
Assert-Gh
$Repo = Resolve-Repo -Repo $Repo
$connList = $Connectors.Split(",") | ForEach-Object { $_.Trim().ToUpper() }

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host " Onboarding cliente : $Client"
Write-Host " Repo               : $Repo"
Write-Host " Connettori         : $($connList -join ', ')"
Write-Host "============================================" -ForegroundColor Cyan

# 1) Crea / aggiorna l'Environment
Write-Host ""
Write-Host "[1/3] Creo l'Environment '$Client'..."
gh api --method PUT "repos/$Repo/environments/$Client" 1>$null
if ($LASTEXITCODE -ne 0) { throw "Errore creando l'Environment '$Client'." }
Write-Host "  [OK] Environment pronto."

# 2) Raccogli i valori (interattivo se mancanti)
Write-Host ""
Write-Host "[2/3] Raccolgo i parametri..."

$FabricTenantId     = Read-IfEmpty $FabricTenantId     "FABRIC_TENANT_ID (tenant Azure cliente)"
$FabricClientId     = Read-IfEmpty $FabricClientId     "FABRIC_CLIENT_ID (Service Principal)"
$FabricClientSecret = Read-IfEmpty $FabricClientSecret "FABRIC_CLIENT_SECRET" -Secret
$FabricWorkspaceId  = Read-IfEmpty $FabricWorkspaceId  "FABRIC_WORKSPACE_ID (workspace Fabric target)"

if ($connList -contains "BC") {
    Write-Host "  -- Business Central --" -ForegroundColor DarkGray
    $BcTenantId    = Read-IfEmpty $BcTenantId    "BC_TENANT_ID"
    $BcEnvironment = Read-IfEmpty $BcEnvironment "BC_ENVIRONMENT (es. Production)"
    $BcCompanies   = Read-IfEmpty $BcCompanies   "BC_COMPANIES (es. 'CRONUS IT' o CSV)"
    $BcEntities    = Read-IfEmpty $BcEntities    "BC_ENTITIES (es. ItemLedgerEntries,Customers)"
}

if ($connList -contains "CRM") {
    Write-Host "  -- CRM / Dataverse --" -ForegroundColor DarkGray
    $CrmOrgUrl           = Read-IfEmpty $CrmOrgUrl           "CRM_ORG_URL (es. https://org.crm4.dynamics.com)"
    if (-not $CrmEnvironmentDomain) { $CrmEnvironmentDomain = $CrmOrgUrl }
    $CrmEntities         = Read-IfEmpty $CrmEntities         "CRM_ENTITIES (es. account,contact,opportunity)"
    # Opzionali: se vuoti, deploy.ps1 usa le credenziali Fabric
    $CrmTenantId         = Read-IfEmpty $CrmTenantId         "CRM_TENANT_ID (invio = usa Fabric)"
    $CrmClientId         = Read-IfEmpty $CrmClientId         "CRM_CLIENT_ID (invio = usa Fabric)"
    if ((-not [string]::IsNullOrWhiteSpace($CrmClientId)) -and (-not $CrmClientSecret)) {
        $CrmClientSecret = Read-IfEmpty $CrmClientSecret     "CRM_CLIENT_SECRET" -Secret
    }
}

# 3) Imposta secret e variabili nell'Environment
Write-Host ""
Write-Host "[3/3] Imposto secret e variabili su '$Client'..."

Set-EnvSecret $Repo $Client "FABRIC_TENANT_ID"     $FabricTenantId
Set-EnvSecret $Repo $Client "FABRIC_CLIENT_ID"     $FabricClientId
Set-EnvSecret $Repo $Client "FABRIC_CLIENT_SECRET" $FabricClientSecret
Set-EnvVar    $Repo $Client "FABRIC_WORKSPACE_ID"  $FabricWorkspaceId

if ($connList -contains "BC") {
    Set-EnvVar $Repo $Client "BC_TENANT_ID"  $BcTenantId
    Set-EnvVar $Repo $Client "BC_ENVIRONMENT" $BcEnvironment
    Set-EnvVar $Repo $Client "BC_COMPANIES"  $BcCompanies
    Set-EnvVar $Repo $Client "BC_ENTITIES"   $BcEntities
}

if ($connList -contains "CRM") {
    Set-EnvVar    $Repo $Client "CRM_ORG_URL"            $CrmOrgUrl
    Set-EnvVar    $Repo $Client "CRM_ENVIRONMENT_DOMAIN" $CrmEnvironmentDomain
    Set-EnvVar    $Repo $Client "CRM_ENTITIES"           $CrmEntities
    Set-EnvVar    $Repo $Client "CRM_CONNECTION_NAME"    $CrmConnectionName
    Set-EnvSecret $Repo $Client "CRM_TENANT_ID"          $CrmTenantId
    Set-EnvSecret $Repo $Client "CRM_CLIENT_ID"          $CrmClientId
    Set-EnvSecret $Repo $Client "CRM_CLIENT_SECRET"      $CrmClientSecret
}

Write-Host ""
Write-Host "[OK] Cliente '$Client' configurato." -ForegroundColor Green
Write-Host "Avvia il deploy con:  .\scripts\deploy-wizard.ps1 -Client $Client" -ForegroundColor DarkGray
