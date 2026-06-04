# Lista della spesa — Deploy su Microsoft Fabric con GitHub Actions

Questa guida descrive tutto quello che serve per deployare item Fabric su un **cliente X qualsiasi**
usando GitHub Actions. È divisa in due parti:

- **Cosa devi chiedere al cliente** (prerequisiti manuali, una tantum)
- **Cosa fa il deploy automaticamente** (non serve toccare nulla)

---

## Indice

1. [Panoramica: i due ruoli di credenziali](#1-panoramica-i-due-ruoli-di-credenziali)
2. [Prerequisiti lato cliente — sempre richiesti](#2-prerequisiti-lato-cliente--sempre-richiesti)
3. [Prerequisiti aggiuntivi — connettore BC](#3-prerequisiti-aggiuntivi--connettore-bc)
4. [Prerequisiti aggiuntivi — connettore CRM / Dataverse](#4-prerequisiti-aggiuntivi--connettore-crm--dataverse)
5. [Cosa viene creato automaticamente dal deploy](#5-cosa-viene-creato-automaticamente-dal-deploy)
6. [Onboarding: come registrare un nuovo cliente](#6-onboarding-come-registrare-un-nuovo-cliente)
7. [Riepilogo variabili per GitHub Environment](#7-riepilogo-variabili-per-github-environment)
8. [FAQ rapide](#8-faq-rapide)

---

## 1. Panoramica: i due ruoli di credenziali

Il deploy usa due categorie di credenziali distinte:

| Ruolo | Scopo | Tenant di appartenenza |
|-------|-------|------------------------|
| **SP Fabric** | Crea e gestisce gli item Fabric (Lakehouse, Pipeline, Shortcut, ecc.) | Tenant Fabric del cliente |
| **Credenziali sorgente** | Autenticano la connessione al dato (BC o Dataverse) | Tenant BC / Dataverse del cliente |

Lo SP Fabric è **sempre obbligatorio**. Le credenziali sorgente dipendono dal connettore attivato.

> ⚠️ **Regola fondamentale**: lo SP Fabric deve essere creato sul **tenant del cliente**, non sul tuo.
> Fabric verifica che il Service Principal appartenga allo stesso tenant del workspace target.

---

## 2. Prerequisiti lato cliente — sempre richiesti

Questi tre passi vanno eseguiti **una volta sola** da un admin del tenant Fabric del cliente.

### Passo A — Creare il Service Principal Fabric

1. Accedi a [portal.azure.com](https://portal.azure.com) con un account admin del tenant cliente
2. Vai su **Microsoft Entra ID → App registrations → New registration**
3. Nome consigliato: `sp-fabric-deploy-<nome-cliente>`
4. Vai su **Certificates & secrets → New client secret** — copia il valore subito (non è recuperabile dopo)
5. Annota **Application (client) ID** e **Directory (tenant) ID** dalla pagina Overview

### Passo B — Aggiungere il SP al workspace Fabric

1. Apri il workspace su [app.fabric.microsoft.com](https://app.fabric.microsoft.com)
2. `Manage access → Add people or groups`
3. Cerca il nome dell'App Registration creata al passo A
4. Assegna ruolo **Member** (minimo) o **Admin**

### Passo C — Abilitare i Service Principal nel Fabric Admin Portal

> Impostazione globale di tenant — da fare **una sola volta**, vale per tutti i deploy futuri.

1. Vai su [app.fabric.microsoft.com](https://app.fabric.microsoft.com) → **Settings → Admin portal**
2. `Tenant settings → Developer settings`
3. Abilita **"Service principals can use Fabric APIs"**
4. Applica a tutti oppure a un security group che include il SP creato al passo A

### Valori da fornire per il GitHub Environment

Una volta completati i passi A–C, il cliente ti fornisce questi 4 valori:

| Nome variabile | Tipo GitHub | Dove si trova |
|----------------|-------------|---------------|
| `FABRIC_TENANT_ID` | 🔒 Secret | Entra ID → Overview → **Directory (tenant) ID** |
| `FABRIC_CLIENT_ID` | 🔒 Secret | App Registration → **Application (client) ID** |
| `FABRIC_CLIENT_SECRET` | 🔒 Secret | App Registration → Certificates & secrets |
| `FABRIC_WORKSPACE_ID` | 📝 Variable | URL del workspace Fabric: `.../groups/**<GUID>**/...` |

---

## 3. Prerequisiti aggiuntivi — connettore BC

Richiesti solo se il deploy include il connettore `BC`. Nessun SP aggiuntivo: sono solo parametri configurativi che l'admin BC del cliente conosce.

| Nome variabile | Tipo GitHub | Esempio / Note |
|----------------|-------------|----------------|
| `BC_TENANT_ID` | 📝 Variable | GUID del tenant Microsoft 365 / BC del cliente |
| `BC_ENVIRONMENT` | 📝 Variable | `Production` oppure `SandboxTest` |
| `BC_COMPANIES` | 📝 Variable | `CRONUS IT` oppure `CRONUS IT,MyCompany` |
| `BC_ENTITIES` | 📝 Variable | `ItemLedgerEntries,Customers` |

---

## 4. Prerequisiti aggiuntivi — connettore CRM / Dataverse

Richiesti solo se il deploy include il connettore `CRM`. Sono disponibili due opzioni.

---

### Opzione A — Connessione Dataverse pre-creata ✅ Consigliata

Un utente del cliente con accesso a Dataverse crea la Fabric Connection **una volta a mano** dal portale.
Il deploy automatico la riusa tramite ID: non serve nessuna App Registration sul tenant Dataverse.

**Chi esegue questi passi:** un utente con accesso all'ambiente Dataverse del cliente
(non serve essere admin Entra ID del tenant CRM).

1. Accedi a [app.fabric.microsoft.com](https://app.fabric.microsoft.com) con l'account del tenant Dataverse
2. Vai su **Settings → Manage connections and gateways → New connection**
3. Scegli **Dataverse**, inserisci l'URL dell'org e autenticati
4. Copia l'**ID della connessione** dall'URL o dal riquadro dettaglio
5. Vai sulla connessione → **Manage access** → aggiungi il SP Fabric (`FABRIC_CLIENT_ID`) con ruolo **User**

Valori da aggiungere al GitHub Environment:

| Nome variabile | Tipo GitHub | Esempio / Note |
|----------------|-------------|----------------|
| `CRM_ORG_URL` | 📝 Variable | `https://orgXXX.crm4.dynamics.com` |
| `CRM_ENVIRONMENT_DOMAIN` | 📝 Variable | Di solito uguale a `CRM_ORG_URL` |
| `CRM_ENTITIES` | 📝 Variable | `account,contact,opportunity` |
| `CRM_CONNECTION_ID` | 📝 Variable | ID della Fabric Connection creata al passo 4 |

---

### Opzione B — SP Dataverse (connessione creata dal deploy)

Il deploy crea la connessione Dataverse automaticamente usando un Service Principal sul tenant Dataverse.
Richiede che un admin Entra ID del tenant Dataverse del cliente crei un'App Registration e fornisca le credenziali.

Tutti i valori dell'Opzione A **tranne** `CRM_CONNECTION_ID`, **più**:

| Nome variabile | Tipo GitHub | Note |
|----------------|-------------|------|
| `CRM_TENANT_ID` | 🔒 Secret | Tenant ID del tenant Dataverse (default: uguale a `FABRIC_TENANT_ID`) |
| `CRM_CLIENT_ID` | 🔒 Secret | Client ID del SP Dataverse (default: uguale a `FABRIC_CLIENT_ID`) |
| `CRM_CLIENT_SECRET` | 🔒 Secret | Secret del SP Dataverse (default: uguale a `FABRIC_CLIENT_SECRET`) |

> Se il tenant Fabric e il tenant Dataverse del cliente coincidono, lo stesso SP Fabric può essere usato
> per entrambi — non serve creare un secondo SP.

---

## 5. Cosa viene creato automaticamente dal deploy

Il deploy è idempotente: verifica se l'item esiste già e lo riusa. Con `-ForceRecreate` elimina e ricrea le pipeline.

### Connettore BC

```
LH_Bronze               → Lakehouse condiviso (condiviso con CRM se entrambi attivi)
MirrorDB_BC             → Mirroring Database Business Central
SJD_BC_Sync             → Spark Job Definition (esegue bc_sync.py)
DP_BC_Sync              → Data Pipeline (orchestrazione)
```

### Connettore CRM

```
LH_Bronze                        → Lakehouse condiviso (condiviso con BC se entrambi attivi)
LH_Bronze/Tables/<entity_1>      → Shortcut Dataverse (dati live, senza Spark)
LH_Bronze/Tables/<entity_2>      → Shortcut Dataverse
...
Dataverse-<host>                 → Fabric Connection Dataverse (solo Opzione B)
```

> I dati Dataverse via shortcut sono accessibili immediatamente da SQL endpoint, Notebook e Power BI,
> senza nessun processo di sincronizzazione.

---

## 6. Onboarding: come registrare un nuovo cliente

### Passo 1 — Autenticati con GitHub CLI (una volta sola)

```powershell
gh auth login
# Seleziona: GitHub.com → HTTPS → autenticazione via browser
```

### Passo 2 — Crea il GitHub Environment e carica le credenziali

```powershell
# Interattivo (consigliato — guida passo per passo)
.\scripts\new-client.ps1 -Client nome-cliente
```

```powershell
# Non interattivo — esempio connettore BC
.\scripts\new-client.ps1 -Client nome-cliente -Connectors BC `
    -FabricTenantId   "<tenant-id>"     -FabricClientId     "<client-id>" `
    -FabricClientSecret "<secret>"      -FabricWorkspaceId  "<workspace-id>" `
    -BcTenantId       "<bc-tenant-id>"  -BcEnvironment      "Production" `
    -BcCompanies      "CRONUS IT"       -BcEntities         "ItemLedgerEntries"
```

### Passo 3 — Lancia il deploy

```powershell
# Wizard: elenca gli Environment disponibili e avvia la GitHub Action
.\scripts\deploy-wizard.ps1 -Client nome-cliente -Connectors BC
```

---

## 7. Riepilogo variabili per GitHub Environment

| Variabile | Tipo | Sempre | BC | CRM (A) | CRM (B) |
|-----------|------|--------|----|---------|---------|
| `FABRIC_TENANT_ID` | 🔒 Secret | ✅ | ✅ | ✅ | ✅ |
| `FABRIC_CLIENT_ID` | 🔒 Secret | ✅ | ✅ | ✅ | ✅ |
| `FABRIC_CLIENT_SECRET` | 🔒 Secret | ✅ | ✅ | ✅ | ✅ |
| `FABRIC_WORKSPACE_ID` | 📝 Variable | ✅ | ✅ | ✅ | ✅ |
| `BC_TENANT_ID` | 📝 Variable | — | ✅ | — | — |
| `BC_ENVIRONMENT` | 📝 Variable | — | ✅ | — | — |
| `BC_COMPANIES` | 📝 Variable | — | ✅ | — | — |
| `BC_ENTITIES` | 📝 Variable | — | ✅ | — | — |
| `CRM_ORG_URL` | 📝 Variable | — | — | ✅ | ✅ |
| `CRM_ENVIRONMENT_DOMAIN` | 📝 Variable | — | — | ✅ | ✅ |
| `CRM_ENTITIES` | 📝 Variable | — | — | ✅ | ✅ |
| `CRM_CONNECTION_ID` | 📝 Variable | — | — | ✅ | — |
| `CRM_TENANT_ID` | 🔒 Secret | — | — | — | ✅ |
| `CRM_CLIENT_ID` | 🔒 Secret | — | — | — | ✅ |
| `CRM_CLIENT_SECRET` | 🔒 Secret | — | — | — | ✅ |

> 🔒 **Secret** = valore nascosto nei log di Actions  
> 📝 **Variable** = valore visibile nei log di Actions

---

## 8. FAQ rapide

**Q: Lo SP Fabric deve essere sul mio tenant o su quello del cliente?**  
A: Sul **tenant del cliente**. Fabric verifica che il SP appartenga allo stesso tenant del workspace. Se stai testando su un tuo workspace personale, allora sei tu "il cliente" e usi il tuo tenant.

**Q: Posso riusare lo stesso SP per più clienti?**  
A: No. Ogni cliente ha il proprio tenant Fabric — lo SP esiste su quel tenant e può accedere solo ai workspace di quel tenant. Un SP per cliente.

**Q: Non ho i permessi per creare App Registration sul tenant Dataverse del cliente — sono bloccato?**  
A: No. Usa la **CRM Opzione A**: chiedi a un utente del tenant Dataverse di creare la Fabric Connection a mano e fornirti il `CRM_CONNECTION_ID`. Il tuo SP non deve essere sul tenant Dataverse.

**Q: Cosa succede se eseguo il deploy due volte?**  
A: Nulla di distruttivo. Lo script controlla se l'item esiste già e lo riusa. Usa `-ForceRecreate` solo se vuoi eliminare e ricreare le pipeline (utile in caso di definizioni corrotte).

**Q: Posso usare sia BC che CRM insieme?**  
A: Sì. Configura le variabili di entrambi i connettori nello stesso GitHub Environment e lancia il deploy con `-Connectors BC,CRM`.

**Q: Dove vedo i log del deploy?**  
A: GitHub → repository → tab **Actions** → seleziona il workflow run.
