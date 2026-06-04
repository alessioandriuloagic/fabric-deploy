# Lista della spesa — Deploy su Microsoft Fabric con GitHub Actions

Questa guida riassume tutto quello che serve per deployare item Fabric su un tenant target usando GitHub Actions.

---

## Indice

1. [Infrastruttura GitHub](#1-infrastruttura-github)
2. [Service Principal Entra ID — sempre obbligatorio](#2-service-principal-entra-id--sempre-obbligatorio)
3. [Connettore Business Central](#3-connettore-business-central-opzionale)
4. [Connettore CRM / Dataverse](#4-connettore-crm--dataverse-opzionale)
5. [Cosa viene creato automaticamente](#5-cosa-viene-creato-automaticamente)
6. [Permessi Fabric richiesti](#6-permessi-fabric-richiesti)
7. [Onboarding nuovo cliente](#7-onboarding-nuovo-cliente)
8. [FAQ rapide](#8-faq-rapide)

---

## 1. Infrastruttura GitHub

| Cosa | Dettaglio |
|------|-----------|
| Repository GitHub | Deve contenere `deploy.ps1`, `deploy.yml` e gli altri script |
| GitHub Actions abilitato | Abilitato di default; verificare in `Settings → Actions` |
| **GitHub Environment** per ogni cliente | `Settings → Environments → New environment` (es. `cliente-x`) |
| [GitHub CLI](https://cli.github.com/) installata in locale | Necessaria solo per l'onboarding via `new-client.ps1` |

> **Cosa è un GitHub Environment?**  
> È un contenitore isolato di secret e variabili associato a un cliente specifico.  
> Ogni cliente = un Environment. Il workflow riceve il nome del cliente come input e legge le credenziali dall'Environment corrispondente.

---

## 2. Service Principal Entra ID — sempre obbligatorio

Questi valori sono **richiesti per qualsiasi deploy**, indipendentemente dal connettore.

### Come crearlo

1. Vai su [portal.azure.com](https://portal.azure.com) → **Microsoft Entra ID → App registrations → New registration**
2. Dai un nome (es. `sp-fabric-deploy-clienteX`)
3. Vai su **Certificates & secrets → New client secret** e copia il valore
4. Annota **Application (client) ID** e **Directory (tenant) ID**

### Valori da configurare nel GitHub Environment

| Nome variabile | Tipo | Dove trovarlo |
|----------------|------|---------------|
| `FABRIC_TENANT_ID` | 🔒 Secret | Azure Entra ID → Overview → Directory (tenant) ID |
| `FABRIC_CLIENT_ID` | 🔒 Secret | App Registration → Application (client) ID |
| `FABRIC_CLIENT_SECRET` | 🔒 Secret | App Registration → Certificates & secrets |
| `FABRIC_WORKSPACE_ID` | 📝 Variable | Fabric Portal → URL del workspace (GUID) |

> **Dove trovo il Workspace ID?**  
> Apri il workspace su [app.fabric.microsoft.com](https://app.fabric.microsoft.com).  
> L'URL contiene il GUID: `.../groups/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/...`

---

## 3. Connettore Business Central (opzionale)

Richiesto solo se il deploy include il connettore `BC`.

Aggiungere queste variabili al GitHub Environment del cliente:

| Nome variabile | Tipo | Esempio |
|----------------|------|---------|
| `BC_TENANT_ID` | 📝 Variable | GUID del tenant Business Central |
| `BC_ENVIRONMENT` | 📝 Variable | `Production` oppure `SandboxTest` |
| `BC_COMPANIES` | 📝 Variable | `CRONUS%20IT` oppure `CRONUS IT,MyCompany` |
| `BC_ENTITIES` | 📝 Variable | `ItemLedgerEntries,Customers` |

### Item Fabric creati automaticamente

```
LH_Bronze         → Lakehouse condiviso (creato se non esiste)
MirrorDB_BC       → Mirroring Database Business Central
SJD_BC_Sync       → Spark Job Definition
DP_BC_Sync        → Data Pipeline
```

---

## 4. Connettore CRM / Dataverse (opzionale)

Richiesto solo se il deploy include il connettore `CRM`.

Sono disponibili due modalità:

> ⚠️ **Blocco frequente — tenant CRM esterno (es. tenant agic o cliente)**  
> Se non hai i permessi per creare App Registration sul tenant Dataverse (es. il tenant CRM appartiene al cliente o a un partner), **non puoi usare l'Opzione B**.  
> La soluzione è **Opzione A**: un admin del tenant CRM crea la Fabric Connection una volta sola dal portale → ti fornisce il `CRM_CONNECTION_ID`. Nessuna App Registration necessaria da parte tua.

---

### Opzione A — Riuso connessione Dataverse esistente ✅ Consigliata (e unica percorribile su tenant esterni)

La connessione Dataverse viene creata **una volta a mano** da un utente con accesso al tenant CRM, direttamente dal portale Fabric. Il deploy automatico la riusa tramite ID.

**Chi deve farlo:** un utente con accesso all'ambiente Dataverse target (non servono diritti di admin Entra ID, basta avere un account sul tenant CRM).

**Come creare la connessione manualmente:**

1. Apri [app.fabric.microsoft.com](https://app.fabric.microsoft.com) con l'account del tenant CRM
2. Vai su **Settings → Manage connections and gateways → New connection**
3. Scegli **Dataverse**, inserisci l'URL dell'org e autenticati
4. Copia l'**ID della connessione** dall'URL o dal dettaglio connessione
5. Nel workspace Fabric, aggiungi il Service Principal di deploy come **User** su quella connessione (`Manage access` della connessione)

| Nome variabile | Tipo | Esempio / Note |
|----------------|------|----------------|
| `CRM_ORG_URL` | 📝 Variable | `https://orgXXX.crm4.dynamics.com` |
| `CRM_ENVIRONMENT_DOMAIN` | 📝 Variable | Di solito uguale a `CRM_ORG_URL` |
| `CRM_ENTITIES` | 📝 Variable | `account,contact,opportunity` |
| `CRM_CONNECTION_ID` | 📝 Variable | ID della Fabric Connection creata al passo precedente |

> Il Service Principal Fabric (`FABRIC_CLIENT_ID`) deve avere il ruolo **User** sulla connessione Dataverse.  
> Non servono App Registration o credenziali CRM aggiuntive nel deploy automatico.

---

### Opzione B — Creazione connessione al volo (Service Principal Dataverse)

Tutti i valori dell'Opzione A, **più**:

| Nome variabile | Tipo | Note |
|----------------|------|------|
| `CRM_TENANT_ID` | 🔒 Secret | Default: uguale a `FABRIC_TENANT_ID` |
| `CRM_CLIENT_ID` | 🔒 Secret | Default: uguale a `FABRIC_CLIENT_ID` |
| `CRM_CLIENT_SECRET` | 🔒 Secret | Default: uguale a `FABRIC_CLIENT_SECRET` |

### Item Fabric creati automaticamente

```
LH_Bronze/Tables/<entity>   → Shortcut Dataverse per ogni entità configurata
Dataverse-<host>            → Fabric Connection Dataverse (solo Opzione B)
```

> I dati Dataverse sono accessibili **live** (senza Spark Job) da SQL endpoint, Notebook e Power BI.

---

## 5. Cosa viene creato automaticamente

Il deploy non richiede la creazione manuale di alcun item Fabric. Tutto viene creato o riutilizzato dallo script:

```
LH_Bronze
├── Tables/
│   ├── <entity_CRM_1>     ← shortcut Dataverse (connettore CRM)
│   └── <entity_CRM_2>
└── (Files/)

MirrorDB_BC                 ← solo connettore BC
SJD_BC_Sync                 ← solo connettore BC
DP_BC_Sync                  ← solo connettore BC
```

---

## 6. Permessi Fabric richiesti

### Sul workspace Fabric target

Il Service Principal (`FABRIC_CLIENT_ID`) deve essere aggiunto come **Member** o **Admin** del workspace:

1. Apri il workspace su Fabric Portal
2. `Manage access → Add people or groups`
3. Cerca il nome dell'App Registration
4. Assegna ruolo **Member** (minimo) o **Admin**

### Nel Fabric Admin Portal (impostazione di tenant — da fare una volta sola)

1. Vai su [app.fabric.microsoft.com](https://app.fabric.microsoft.com) → **Settings → Admin portal**
2. `Tenant settings → Developer settings`
3. Abilita **"Service principals can use Fabric APIs"**
4. Applica a tutti o a un security group specifico

---

## 7. Onboarding nuovo cliente

### Passo 1 — Autenticati con GitHub CLI (una volta sola)

```powershell
gh auth login
# Seleziona GitHub.com → HTTPS → autenticazione via browser
```

### Passo 2 — Crea l'Environment e configura le credenziali

```powershell
# Interattivo (consigliato per il primo utilizzo)
.\scripts\new-client.ps1 -Client nome-cliente

# Oppure non interattivo, tutto in un comando
.\scripts\new-client.ps1 -Client nome-cliente -Connectors BC `
    -FabricTenantId "..."   -FabricClientId "..."   -FabricClientSecret "..." `
    -FabricWorkspaceId "..." -BcTenantId "..."       -BcEnvironment "Production" `
    -BcCompanies "CRONUS IT" -BcEntities "ItemLedgerEntries"
```

### Passo 3 — Lancia il deploy

```powershell
# Wizard interattivo: mostra gli Environment disponibili e avvia l'Action
.\scripts\deploy-wizard.ps1 -Client nome-cliente -Connectors BC
```

Il wizard lancia automaticamente la GitHub Action. Nessun deploy manuale dal portale.

---

## 8. FAQ rapide

**Q: Posso usare lo stesso Service Principal per più clienti?**  
A: Tecnicamente sì, ma è sconsigliato. Ogni cliente dovrebbe avere un SP dedicato con accesso solo al proprio workspace.

**Q: Cosa succede se eseguo il deploy due volte?**  
A: Lo script è idempotente: controlla se l'item esiste già e lo riusa. Usa `-ForceRecreate` solo se vuoi eliminare e ricreare le pipeline.

**Q: Dove vedo i log del deploy?**  
A: Su GitHub → repository → tab **Actions** → seleziona il run.

**Q: Non ho i permessi per creare App Registration sul tenant CRM — sono bloccato?**  
A: No. Usa l'**Opzione A**: chiedi a un admin o utente del tenant CRM di creare la Fabric Connection manualmente dal portale, poi fornisciti il `CRM_CONNECTION_ID`. Il tuo Service Principal di deploy non deve essere sul tenant CRM, deve solo avere il ruolo **User** su quella connessione.

**Q: Il connettore CRM richiede licenze Dataverse aggiuntive?**  
A: No, usa shortcut nativi Fabric verso Dataverse. Serve solo che il Service Principal abbia accesso all'ambiente Dataverse target.

**Q: Posso usare sia BC che CRM insieme?**  
A: Sì. Configura tutte le variabili di entrambi i connettori e lancia il deploy con `-Connectors BC,CRM`.

---

## Riepilogo variabili per Environment

| Variabile | Tipo | BC | CRM (A) | CRM (B) |
|-----------|------|----|---------|---------|
| `FABRIC_TENANT_ID` | 🔒 Secret | ✅ | ✅ | ✅ |
| `FABRIC_CLIENT_ID` | 🔒 Secret | ✅ | ✅ | ✅ |
| `FABRIC_CLIENT_SECRET` | 🔒 Secret | ✅ | ✅ | ✅ |
| `FABRIC_WORKSPACE_ID` | 📝 Variable | ✅ | ✅ | ✅ |
| `BC_TENANT_ID` | 📝 Variable | ✅ | — | — |
| `BC_ENVIRONMENT` | 📝 Variable | ✅ | — | — |
| `BC_COMPANIES` | 📝 Variable | ✅ | — | — |
| `BC_ENTITIES` | 📝 Variable | ✅ | — | — |
| `CRM_ORG_URL` | 📝 Variable | — | ✅ | ✅ |
| `CRM_ENVIRONMENT_DOMAIN` | 📝 Variable | — | ✅ | ✅ |
| `CRM_ENTITIES` | 📝 Variable | — | ✅ | ✅ |
| `CRM_CONNECTION_ID` | 📝 Variable | — | ✅ | — |
| `CRM_TENANT_ID` | 🔒 Secret | — | — | ✅ |
| `CRM_CLIENT_ID` | 🔒 Secret | — | — | ✅ |
| `CRM_CLIENT_SECRET` | 🔒 Secret | — | — | ✅ |

> 🔒 = **Secret** GitHub (valore nascosto nei log)  
> 📝 = **Variable** GitHub (valore visibile nei log)
