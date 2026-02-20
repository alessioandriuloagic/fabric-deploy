# fabric-deploy

Deploy automatico di una soluzione Microsoft Fabric via Azure DevOps.

## Struttura repo

```
fabric-deploy/
├── python/
│   └── bc_sync.py                  ← Script Python (Spark Job)
├── scripts/
│   └── deploy.ps1                  ← Script di deploy PowerShell
├── azure-pipelines.yml             ← Pipeline Azure DevOps
└── README.md
```

## Cosa viene deployato

Il deploy crea nell'ordine:

1. **File Python** → caricato su `Files/Scripts/bc_sync.py` nel Lakehouse
2. **Mirrored Database** (Open Mirroring) → Landing Zone per i CSV da BC
3. **Spark Job Definition** → punta al file Python nel Lakehouse
4. **Data Pipeline** → esegue lo Spark Job

---

## Setup iniziale (una tantum)

### 1. Variable Group in Azure DevOps Library

Vai su **Pipelines → Library → + Variable group** e crea un gruppo chiamato `fabric-deploy-dev`.

Aggiungi queste variabili (quelle con 🔒 impostale come Secret):

| Variabile               | Esempio                                | Secret |
|-------------------------|----------------------------------------|--------|
| `FABRIC_TENANT_ID`      | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |        |
| `FABRIC_CLIENT_ID`      | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |        |
| `FABRIC_CLIENT_SECRET`  | `your-secret-here`                     | 🔒     |
| `FABRIC_WORKSPACE_ID`   | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |        |
| `FABRIC_LAKEHOUSE_ID`   | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |        |
| `PYTHON_FILE_NAME`      | `bc_sync.py`                           |        |

Per ogni cliente/ambiente aggiuntivo crea un gruppo separato: `fabric-deploy-prod`, `fabric-deploy-clienteA`, ecc.

### 2. Permessi Service Principal su Fabric

Nel Fabric Portal:
- Vai nel workspace → **Manage access**
- Aggiungi il Service Principal come **Member** o **Admin**

In Fabric Admin Portal (solo se non già fatto):
- Vai su **Admin Portal → Tenant settings**
- Abilita **"Service principals can use Fabric APIs"**

### 3. Collega la pipeline DevOps

- Vai su **Pipelines → New pipeline**
- Seleziona il repo e punta al file `azure-pipelines.yml`
- Autorizza l'accesso al Variable Group quando richiesto

---

## Come fare il deploy

1. Vai su **Pipelines** in Azure DevOps
2. Seleziona la pipeline `fabric-deploy`
3. Clicca **Run pipeline**
4. Seleziona il parametro `targetEnvironment` (es. `dev`)
5. Clicca **Run**

Il deploy dura circa 1-2 minuti. Al termine trovi nel log i 3 ID creati.

---

## Idempotenza

Lo script è idempotente: se un item esiste già (stesso `displayName`) non lo ricrea,
lo lascia invariato. Puoi rilanciarla pipeline più volte senza problemi.

---

## Aggiungere un nuovo cliente

1. Crea un nuovo Variable Group: `fabric-deploy-clienteX`
2. Inserisci i valori specifici del workspace del cliente
3. Aggiungi `clienteX` alla lista `values` in `azure-pipelines.yml`
4. Lancia la pipeline selezionando `clienteX`
