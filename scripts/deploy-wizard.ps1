# =============================================================
# deploy-wizard.ps1 - Wizard interattivo per avviare il deploy
#
# Legge i GitHub Environment esistenti (= clienti), chiede quale
# cliente + connettori + opzioni e lancia la GitHub Action
# "Fabric Deploy" (deploy.yml) via gh CLI.
#
# Prerequisiti:
#   - GitHub CLI installata:  https://cli.github.com/
#   - Autenticato:            gh auth login
#   - Eseguire dalla root del repo (o usare -Repo owner/repo)
#
# Esempi:
#   .\scripts\deploy-wizard.ps1
#   .\scripts\deploy-wizard.ps1 -Client cliente-x -Connectors BC -Watch
# =============================================================
[CmdletBinding()]
param(
    [string]$Client,
    [ValidateSet("BC", "CRM", "BC,CRM")]
    [string]$Connectors,
    [switch]$ForceRecreate,
    [string]$Repo,
    [string]$Ref = "main",
    [string]$Workflow = "deploy.yml",
    [switch]$Watch
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

function Get-Environments {
    param([string]$Repo)
    $json = gh api "repos/$Repo/environments" 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($json)) { return @() }
    return ($json | ConvertFrom-Json).environments.name
}

function Select-FromList {
    param([string]$Title, [string[]]$Items)
    Write-Host ""
    Write-Host $Title -ForegroundColor Cyan
    for ($i = 0; $i -lt $Items.Count; $i++) {
        Write-Host ("  [{0}] {1}" -f ($i + 1), $Items[$i])
    }
    do {
        $sel = Read-Host "Scelta (1-$($Items.Count))"
        $n = 0
    } until ([int]::TryParse($sel, [ref]$n) -and $n -ge 1 -and $n -le $Items.Count)
    return $Items[$n - 1]
}

# ── Avvio ────────────────────────────────────────────────────
Assert-Gh
$Repo = Resolve-Repo -Repo $Repo
Write-Host "[REPO] $Repo" -ForegroundColor DarkGray

# 1) Cliente (= GitHub Environment)
if (-not $Client) {
    $envs = Get-Environments -Repo $Repo
    if (-not $envs -or $envs.Count -eq 0) {
        Write-Host "Nessun Environment trovato nel repo." -ForegroundColor Yellow
        Write-Host "Crea prima un cliente con: .\scripts\new-client.ps1" -ForegroundColor Yellow
        $Client = Read-Host "Nome cliente/environment da usare comunque"
    }
    else {
        $Client = Select-FromList -Title "Seleziona il CLIENTE (GitHub Environment):" -Items $envs
    }
}

# 2) Connettori
if (-not $Connectors) {
    $Connectors = Select-FromList -Title "Seleziona i CONNETTORI:" -Items @("BC", "CRM", "BC,CRM")
}

# 3) Force recreate
if (-not $PSBoundParameters.ContainsKey('ForceRecreate')) {
    $ans = Read-Host "Forzare la ricreazione delle pipeline? (s/N)"
    $ForceRecreate = ($ans -match '^(s|si|y|yes)$')
}

# ── Riepilogo + conferma ─────────────────────────────────────
Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host " Cliente (environment) : $Client"
Write-Host " Connettori            : $Connectors"
Write-Host " ForceRecreate         : $ForceRecreate"
Write-Host " Workflow              : $Workflow  (ref: $Ref)"
Write-Host "============================================" -ForegroundColor Green
$go = Read-Host "Confermi l'avvio del deploy? (s/N)"
if ($go -notmatch '^(s|si|y|yes)$') { Write-Host "Annullato."; return }

# ── Lancio della Action ──────────────────────────────────────
$forceStr = $ForceRecreate.ToString().ToLower()
gh workflow run $Workflow `
    --repo $Repo `
    --ref $Ref `
    -f client=$Client `
    -f connectors=$Connectors `
    -f forceRecreate=$forceStr
if ($LASTEXITCODE -ne 0) { throw "Avvio workflow fallito." }

Write-Host ""
Write-Host "[OK] Workflow avviato." -ForegroundColor Green

if ($Watch) {
    Write-Host "Attendo che il run compaia..."
    Start-Sleep -Seconds 4
    $runId = gh run list --repo $Repo --workflow $Workflow --limit 1 --json databaseId -q ".[0].databaseId"
    if ($runId) {
        gh run watch $runId --repo $Repo --exit-status
    }
}
else {
    Write-Host "Segui l'esecuzione con:  gh run watch --repo $Repo" -ForegroundColor DarkGray
    Write-Host "Oppure apri:             gh run list --repo $Repo --workflow $Workflow" -ForegroundColor DarkGray
}
