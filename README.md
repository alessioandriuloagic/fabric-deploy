# Fabric Data Integration - Modular Connector Architecture

## Architettura

Soluzione modulare per pubblicare item Fabric per cliente, tramite **GitHub Actions**.

```
Repository
├── .github/workflows/
│   └── deploy.yml               # Pipeline GitHub Actions (workflow_dispatch)
├── scripts/
│   └── deploy.ps1               # Deploy modulare (flag -Connectors "BC,CRM")
└── python/
    └── bc_sync.py               # Connettore Business Central
```

Ogni connettore crea item Fabric distinti:

| Item | BC | CRM (Dataverse) |
|------|------|------|
| Lakehouse (condiviso) | `LH_Bronze` | `LH_Bronze` |
| Mirroring DB | `MirrorDB_BC` | *(N/A - uso shortcut)* |
| Spark Job | `SJD_BC_Sync` | *(N/A)* |
| Pipeline | `DP_BC_Sync` | *(N/A)* |
| Connection Dataverse | *(N/A)* | `Dataverse-<host>` |
| OneLake Shortcut | *(N/A)* | `LH_Bronze/Tables/<entity>` |

> **CRM**: il deploy crea una **Connection Dataverse** nel tenant Fabric e poi
> uno **shortcut Dataverse** sotto `Tables/<entityName>` del Lakehouse per
> ogni entità configurata. Non serve alcun Spark Job: i dati vengono letti
> direttamente da Dataverse (live, via shortcut) e sono interrogabili
> immediatamente da SQL endpoint, Notebook, Power BI.

---

## Flag `connectors`

Workflow input `connectors`:

- `BC` → solo Business Central (Spark Job + Mirroring + Pipeline)
- `CRM` → solo CRM/Dataverse (Connection + Shortcuts)
- `BC,CRM` → entrambi

---

## Setup GitHub Actions

### 1. Crea GitHub Environment (`dev`, `prod`)

`Settings → Environments → New environment`

Per ognuno, aggiungi le variabili/secret:

**Sempre obbligatorie (Fabric):**
| Nome | Tipo | Descrizione |
|------|------|-------------|
| `FABRIC_TENANT_ID` | secret | Tenant ID Azure del cliente |
| `FABRIC_CLIENT_ID` | secret | Client ID Service Principal Fabric |
| `FABRIC_CLIENT_SECRET` | secret | Client Secret SP Fabric |
| `FABRIC_WORKSPACE_ID` | variable | ID Workspace Fabric target |

**Se connettore BC:**
| Nome | Tipo | Descrizione |
|------|------|-------------|
| `BC_TENANT_ID` | variable | Tenant ID per Business Central |
| `BC_ENVIRONMENT` | variable | Es: `Production` |
| `BC_COMPANIES` | variable | Es: `CRONUS%20IT` (o CSV) |
| `BC_ENTITIES` | variable | Es: `ItemLedgerEntries,Customers` |

**Se connettore CRM (Dataverse shortcut):**
| Nome | Tipo | Descrizione |
|------|------|-------------|
| `CRM_ORG_URL` | variable | URL Dataverse (es: `https://contoso.crm4.dynamics.com`) |
| `CRM_ENVIRONMENT_DOMAIN` | variable | Dominio richiesto dallo shortcut (di solito = `CRM_ORG_URL`) |
| `CRM_ENTITIES` | variable | CSV nomi tabella logici. **Default attuale:** `msdynmkt_email,msdynmkt_journey,contact` |
| `CRM_TENANT_ID` | secret *(opz.)* | Default = `FABRIC_TENANT_ID` |
| `CRM_CLIENT_ID` | secret *(opz.)* | Default = `FABRIC_CLIENT_ID` |
| `CRM_CLIENT_SECRET` | secret *(opz.)* | Default = `FABRIC_CLIENT_SECRET` |
| `CRM_CONNECTION_NAME` | variable *(opz.)* | Default = `Dataverse-<host>` |

### 2. Service Principal: permessi richiesti

**Per Fabric:**
- Workspace Admin/Member sul workspace target
- Permesso di creare Connections (Tenant setting in Fabric Admin Portal)

**Per Dataverse (shortcut):**
- App registrata in Azure AD con API Permission `Dynamics CRM → user_impersonation`
- In *Power Platform Admin Center → Environment → Settings → Users*: aggiungere
  l'app come **Application User** con Security Role di lettura sulle entità

### 3. Prerequisito Dataverse: "Link to Microsoft Fabric"

Lo shortcut Dataverse richiede che l'environment Dataverse abbia abilitato il
feature **Link to Microsoft Fabric** (Power Apps Maker → Tables → Link to
Microsoft Fabric). Questo espone le tabelle Dataverse come Delta in OneLake,
rendendole indirizzabili come shortcut.

> Operazione *una tantum* da fare manualmente lato Dataverse.

### 4. Trigger workflow

`Actions → Fabric Deploy → Run workflow` con i parametri:
- `targetEnvironment`: `dev` | `prod`
- `connectors`: `BC` | `CRM` | `BC,CRM`
- `forceRecreate`: ricreazione pipeline (utile in caso di definition cache)

---

## Differenze BC vs CRM

| Aspetto | BC | CRM (Dataverse) |
|---------|----|----|
| **Tecnica** | Spark Job + Open Mirroring (CSV) | Shortcut OneLake (Delta live) |
| **Latenza dati** | Polling pianificato | Quasi real-time (managed link) |
| **Item creati** | Mirroring DB, Spark Job, Pipeline | Connection + Shortcut |
| **Codice Python** | `bc_sync.py` | nessuno |
| **Aggiunta entità** | Aggiorna `BC_ENTITIES` + run | Aggiorna `CRM_ENTITIES` + run |

---

## Esecuzione locale (debug)

```powershell
.\scripts\deploy.ps1 `
  -TenantId       "<tenant>" `
  -ClientId       "<client>" `
  -ClientSecret   "<secret>" `
  -WorkspaceId    "<workspaceId>" `
  -Connectors     "CRM" `
  -CrmTenantId    "<tenant>" `
  -CrmClientId    "<client>" `
  -CrmClientSecret "<secret>" `
  -CrmOrgUrl      "https://contoso.crm4.dynamics.com" `
  -CrmEnvironmentDomain "https://contoso.crm4.dynamics.com" `
  -CrmEntities    "msdynmkt_email,msdynmkt_journey,contact"
```
