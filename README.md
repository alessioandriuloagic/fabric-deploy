# Fabric Data Integration - Modular Connector Architecture

## Architettura

La soluzione supporta **connettori multipli**, ognuno indipendente con i propri item Fabric:

```
Repository
├── azure-pipelines.yml          # Pipeline DevOps con parametro Connectors
├── scripts/
│   └── deploy.ps1               # Deploy modulare (flag -Connectors "BC,CRM")
└── python/
    ├── bc_sync.py               # Connettore Business Central (esistente)
    └── crm_sync.py              # Connettore CRM / Dataverse (nuovo)
```

Ogni connettore crea un set isolato di item Fabric:

| Item | BC | CRM |
|------|------|------|
| Lakehouse | `LH_BC_Landing` | `LH_CRM_Landing` |
| Mirroring DB | `MirrorDB_BC_Landing` | `MirrorDB_CRM_Landing` |
| Spark Job | `SJD_BC_To_Mirroring` | `SJD_CRM_To_Mirroring` |
| Pipeline | `DP_BC_To_Mirroring` | `DP_CRM_To_Mirroring` |

---

## Come funziona il flag Connectors

Nella pipeline DevOps si seleziona quale connettore installare:

- **`BC`** → installa solo Business Central
- **`CRM`** → installa solo CRM/Dataverse
- **`BC,CRM`** → installa entrambi

Il deploy crea solo gli item necessari per i connettori selezionati.

---

## Setup per nuovo cliente

### 1. Variable Group in Azure DevOps

Creare il Variable Group `fabric-deploy-<env>` (es: `fabric-deploy-prod`) con:

**Sempre obbligatorie (Fabric):**
| Variabile | Descrizione |
|-----------|-------------|
| `FABRIC_TENANT_ID` | Tenant ID Azure del cliente |
| `FABRIC_CLIENT_ID` | Client ID Service Principal Fabric |
| `FABRIC_CLIENT_SECRET` | Client Secret (secret!) |
| `FABRIC_WORKSPACE_ID` | ID Workspace Fabric target |

**Se connettore BC:**
| Variabile | Descrizione |
|-----------|-------------|
| `BC_TENANT_ID` | Tenant ID per Business Central |
| `BC_ENVIRONMENT` | Nome ambiente BC (es: `Production`) |
| `BC_COMPANIES` | JSON array aziende (es: `["CRONUS%20IT"]`) |
| `BC_ENTITIES` | JSON array entità (es: `["ItemLedgerEntries","Customers"]`) |

**Se connettore CRM:**
| Variabile | Descrizione |
|-----------|-------------|
| `CRM_ORG_URL` | URL organizzazione Dataverse (es: `https://contoso.crm4.dynamics.com`) |
| `CRM_ENTITIES` | JSON array entity set (es: `["accounts","contacts","opportunities"]`) |
| `CRM_TENANT_ID` | *(opzionale)* Tenant CRM, default = FABRIC_TENANT_ID |
| `CRM_CLIENT_ID` | *(opzionale)* Client ID CRM, default = FABRIC_CLIENT_ID |
| `CRM_CLIENT_SECRET` | *(opzionale)* Secret CRM, default = FABRIC_CLIENT_SECRET |
| `CRM_API_VERSION` | *(opzionale)* Versione Web API, default = `v9.2` |

### 2. Azure AD App Registration per CRM

Il Service Principal deve avere permessi su Dataverse:

1. In **Azure Portal → App Registrations** → la tua app
2. **API Permissions → Add → Dynamics CRM → user_impersonation** (o Application permission)
3. In **Power Platform Admin Center → Environment → Settings → Users**:
   - Aggiungere l'app come Application User
   - Assegnare un Security Role con permessi di lettura sulle entità desiderate

### 3. Trovare l'URL dell'organizzazione CRM

L'URL ha il formato: `https://<orgname>.crm<N>.dynamics.com`

Per trovarlo:
- **Power Platform Admin Center** → Environments → seleziona ambiente → copia l'URL
- Oppure: Dynamics 365 → Settings → Customizations → Developer Resources → Web API URL

**Nota:** il suffisso `crmN` dipende dalla region:
- `crm.dynamics.com` = Nord America
- `crm4.dynamics.com` = EMEA
- `crm5.dynamics.com` = Asia Pacific
- ecc.

### 4. Entity Set Names per CRM

I nomi EntitySet in Dataverse sono tipicamente il **nome logico plurale** della tabella:

| Tabella | EntitySet |
|---------|-----------|
| Account | `accounts` |
| Contact | `contacts` |
| Opportunity | `opportunities` |
| Lead | `leads` |
| Case/Incident | `incidents` |
| Quote | `quotes` |
| Order | `salesorders` |
| Invoice | `invoices` |
| Custom: `new_myentity` | `new_myentities` |

Per verificare: `GET https://<orgurl>/api/data/v9.2/` elenca tutti gli EntitySet disponibili.

---

## Esecuzione Pipeline DevOps

```
# Solo BC
az pipelines run --name "fabric-deploy" --parameters connectors=BC targetEnvironment=prod

# Solo CRM
az pipelines run --name "fabric-deploy" --parameters connectors=CRM targetEnvironment=prod

# Entrambi
az pipelines run --name "fabric-deploy" --parameters connectors=BC,CRM targetEnvironment=prod
```

Oppure tramite UI DevOps selezionando i parametri.

---

## Differenze chiave BC vs CRM

| Aspetto | BC | CRM / Dataverse |
|---------|----|----|
| **API** | ODataV4 via BC API | Web API v9.2 (OData v4) |
| **Auth scope** | `https://api.businesscentral.dynamics.com/.default` | `https://<orgurl>/.default` |
| **URL base** | `https://api.businesscentral.dynamics.com/v2.0/{tenant}/{env}/ODataV4/Company('{co}')/{entity}` | `https://<orgurl>/api/data/v9.2/{entityset}` |
| **Filtro incrementale** | `SystemModifiedAt gt datetime'...'` | `modifiedon gt ...` |
| **Paginazione** | `@odata.nextLink` | `@odata.nextLink` (max 5000/page) |
| **Rate limiting** | Standard HTTP retry | 429 con `Retry-After` header |
| **Chiave primaria** | Tipicamente `id` | Tipicamente `<entity>id` (es: `accountid`) |
| **Multi-company** | Sì (loop su companies) | No (1 org = 1 ambiente) |