import requests
import pandas as pd
import os
import re
import json
import numpy as np
import base64
from datetime import datetime, timedelta
from azure.identity import ClientSecretCredential   # <-- niente più browser
from azure.storage.filedatalake import DataLakeServiceClient
from time import sleep
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import traceback
import csv
import sys

# ================= CONFIG =================
# I valori vengono letti dai parametri passati allo Spark Job
# oppure dalle variabili d'ambiente (impostate dal deploy DevOps).
# In locale per test puoi ancora hardcodarli qui sotto.

def _get_param(name, default=None):
    """
    Legge un parametro dallo Spark Job commandLineArguments oppure da env var.
    Lo Spark Job in Fabric passa i parametri come: --KEY value (in sys.argv).
    Nota: mssparkutils.notebook/runtime NON è disponibile per Spark Job Definitions.
    """
    # 1. Cerca in sys.argv nel formato --KEY value
    key = f"--{name}"
    for i, arg in enumerate(sys.argv):
        if arg == key and i + 1 < len(sys.argv):
            return sys.argv[i + 1]

    # 2. Fallback: variabile d'ambiente (utile nei test locali / DevOps pipeline)
    return os.environ.get(name, default)

# Business Central
BC_TENANT_ID     = _get_param("BC_TENANT_ID")
BC_CLIENT_ID     = _get_param("BC_CLIENT_ID")
BC_CLIENT_SECRET = _get_param("BC_CLIENT_SECRET")

ENVIRONMENT = _get_param("BC_ENVIRONMENT", "SandboxTest")

# Multi-Company — passato come Base64 di una stringa JSON es: '["CRONUS%20IT","CRONUS%20USA"]'
def _decode_b64_param(name, default_json='[]'):
    """Legge un parametro Base64-encoded e lo decodifica a stringa."""
    raw = _get_param(name)
    if raw:
        return base64.b64decode(raw).decode('utf-8')
    return default_json

_companies_raw = _decode_b64_param("BC_COMPANIES_B64", '["CRONUS%20IT"]')
COMPANIES = json.loads(_companies_raw)

# Multi-Entity — passato come Base64 di una stringa JSON es: '["ItemLedgerEntries"]'
_entities_raw = _decode_b64_param("BC_ENTITIES_B64", '["ItemLedgerEntries"]')
ENTITIES = json.loads(_entities_raw)

# OneLake
WORKSPACE_ID      = _get_param("FABRIC_WORKSPACE_ID")
LAKEHOUSE_ID      = _get_param("FABRIC_LAKEHOUSE_ID")
MIRRORED_DB_ID    = _get_param("FABRIC_MIRRORED_DB_ID")

# Target: la Landing Zone è sul Mirrored Database, NON sul Lakehouse
TARGET_FOLDER      = "Files/LandingZone"
KEYS_TARGET_FOLDER = "Files/MirroringKeys"

# OneLake Service Principal (stessa App Registration usata per Fabric e BC)
ONELAKE_TENANT_ID     = _get_param("FABRIC_TENANT_ID",     BC_TENANT_ID)
ONELAKE_CLIENT_ID     = _get_param("FABRIC_CLIENT_ID",     BC_CLIENT_ID)
ONELAKE_CLIENT_SECRET = _get_param("FABRIC_CLIENT_SECRET", BC_CLIENT_SECRET)

# Validazione: verifica che i parametri obbligatori siano presenti
_required = {
    "BC_TENANT_ID": BC_TENANT_ID,
    "BC_CLIENT_ID": BC_CLIENT_ID,
    "BC_CLIENT_SECRET": BC_CLIENT_SECRET,
    "FABRIC_WORKSPACE_ID": WORKSPACE_ID,
    "FABRIC_LAKEHOUSE_ID": LAKEHOUSE_ID,
    "FABRIC_MIRRORED_DB_ID": MIRRORED_DB_ID,
}
_missing = [k for k, v in _required.items() if not v]
if _missing:
    raise RuntimeError(
        f"Parametri obbligatori mancanti: {', '.join(_missing)}. "
        f"Passarli come --KEY value nello Spark Job o come variabili d'ambiente."
    )

# State file — salvato su OneLake invece che in locale
STATE_FILE = f"{LAKEHOUSE_ID}/Files/MirroringState/.mirroring_state.json"

# Retry config
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 1

# ========================================

def _get_onelake_credential():
    """
    Ritorna una ClientSecretCredential per OneLake.
    Funziona sia su Fabric Spark che in locale, senza browser.
    """
    return ClientSecretCredential(
        tenant_id=ONELAKE_TENANT_ID,
        client_id=ONELAKE_CLIENT_ID,
        client_secret=ONELAKE_CLIENT_SECRET
    )

def _get_onelake_client():
    """
    Ritorna un DataLakeServiceClient autenticato via Service Principal.
    """
    account_url = "https://onelake.dfs.fabric.microsoft.com"
    credential  = _get_onelake_credential()
    return DataLakeServiceClient(account_url=account_url, credential=credential)

def load_state():
    """
    Carica lo stato dell'ultimo carico da OneLake (persiste tra run Spark).
    """
    try:
        service_client     = _get_onelake_client()
        file_system_client = service_client.get_file_system_client(WORKSPACE_ID)
        file_client        = file_system_client.get_file_client(STATE_FILE)
        data = file_client.download_file().readall()
        return json.loads(data.decode('utf-8'))
    except Exception:
        return {}

def save_state(state):
    """
    Salva lo stato su OneLake in modo che persista tra run Spark.
    """
    try:
        service_client     = _get_onelake_client()
        file_system_client = service_client.get_file_system_client(WORKSPACE_ID)
        file_client        = file_system_client.get_file_client(STATE_FILE)
        state_bytes = json.dumps(state, indent=2, default=str).encode('utf-8')
        file_client.upload_data(state_bytes, overwrite=True)
        print(f"💾 Stato salvato su OneLake: {STATE_FILE}")
    except Exception as e:
        print(f"⚠️  Impossibile salvare stato su OneLake: {e}")

def get_last_load_timestamp(company, entity):
    """
    Recupera il timestamp dell'ultimo carico per company/entity
    """
    state = load_state()
    key = f"{company}::{entity}"
    return state.get(key, {}).get("last_load", None)

def update_load_timestamp(company, entity, timestamp):
    """
    Aggiorna il timestamp dell'ultimo carico
    """
    state = load_state()
    key = f"{company}::{entity}"
    if key not in state:
        state[key] = {}
    state[key]["last_load"] = timestamp
    save_state(state)

def get_next_file_sequence(company, entity):
    """
    Recupera il numero del prossimo file sequenziale
    """
    state = load_state()
    key = f"{company}::{entity}"
    return state.get(key, {}).get("next_sequence", 1)

def update_file_sequence(company, entity, sequence):
    """
    Aggiorna il numero sequenziale del file
    """
    state = load_state()
    key = f"{company}::{entity}"
    if key not in state:
        state[key] = {}
    state[key]["next_sequence"] = sequence + 1
    save_state(state)

def add_row_marker_column(df, mode=None, is_incremental=None, key_columns=None, keep_data_for_delete=False):
    """
    Aggiunge la colonna __rowMarker__ per Open Mirroring
    - 0 = Insert (default per initial load)
    - 1 = Update (usato per incremental)
    """
    """
    Aggiunge la colonna __rowMarker__ per Open Mirroring.

    Parametri:
    - mode: 'insert'|'update'|'delete'|'upsert' o intero {0,1,2,4}. Se fornito prende priorità.
    - is_incremental: compatibilità retrocompatibile con chiamate esistenti (True -> Update, False -> Insert).
    - key_columns: lista di colonne chiave usata per i Delete quando si vuole mantenere solo le key.
    - keep_data_for_delete: se True mantiene tutte le colonne anche per i Delete; altrimenti ritorna solo le key + __rowMarker__.

    Valori __rowMarker__:
    - 0 = Insert
    - 1 = Update
    - 2 = Delete
    - 4 = Upsert
    """
    mapper = {'insert': 0, 'update': 1, 'delete': 2, 'upsert': 4}

    # Determina il marker: se 'mode' è fornito lo usa, altrimenti usa is_incremental per retrocompatibilità
    if mode is None:
        marker = 1 if is_incremental else 0
    else:
        if isinstance(mode, int):
            marker = int(mode)
            if marker not in (0, 1, 2, 4):
                raise ValueError(f"mode numerico non valido: {mode}. Usare 0,1,2,4")
        else:
            mode_l = str(mode).lower()
            if mode_l not in mapper:
                raise ValueError(f"mode non valido: {mode}. Usare insert/update/delete/upsert o 0/1/2/4.")
            marker = mapper[mode_l]

    # Caso Delete: se vogliamo solo le colonne chiave, restituisci DF minimale
    if marker == 2 and key_columns and not keep_data_for_delete:
        missing = [c for c in key_columns if c not in df.columns]
        if missing:
            raise KeyError(f"Colonne chiave mancanti nel DataFrame: {missing}")
        out = df.loc[:, key_columns].copy()
        out['__rowMarker__'] = int(marker)
        cols = [c for c in out.columns if c != '__rowMarker__']
        out = out[cols + ['__rowMarker__']]
        out['__rowMarker__'] = out['__rowMarker__'].astype(int)
        return out

    # Per tutti gli altri casi aggiungi/aggiorna la colonna su tutto il df
    out = df.copy()
    out['__rowMarker__'] = int(marker)
    cols = [c for c in out.columns if c != '__rowMarker__']
    out = out[cols + ['__rowMarker__']]
    out['__rowMarker__'] = out['__rowMarker__'].astype(int)
    return out

def get_session_with_retries():
    """
    Crea una sessione requests con retry automico per BC OData
    """
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def sanitize_column_name(col_name):
    """
    Pulisce i nomi delle colonne da caratteri problematici
    """
    # Rimuovi caratteri speciali, mantieni solo alfanumerici, underscore, spazi
    sanitized = re.sub(r'[^a-zA-Z0-9_\s]', '', str(col_name))
    # Rimuovi spazi multipli
    sanitized = re.sub(r'\s+', '_', sanitized.strip())
    # Limita lunghezza
    return sanitized[:100] if sanitized else "Column"

def sanitize_dataframe(df):
    """
    Pulisce l'intero dataframe da valori e colonne problematiche
    """
    # Renomina colonne
    df.columns = [sanitize_column_name(col) for col in df.columns]
    # Rimuovi colonne duplicate
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Pulizia valori: Converti problematic values
    for col in df.columns:
        if df[col].dtype == 'object':
            # Converti esplicitamente a string per gestire NaN/None
            df[col] = df[col].astype(str).replace('nan', '').replace('None', '')
            # Rimuovi valori nan/null/None
            df[col] = df[col].fillna('')
            # Rimuovi caratteri di controllo (null bytes, etc)
            df[col] = df[col].str.replace(r'[\x00-\x1f]', '', regex=True)
            # Rimuovi quote/escape problematiche
            df[col] = df[col].str.replace('""', '"', regex=False)
        elif df[col].dtype in ['float64', 'float32']:
            # Converti infinity e NaN in null string (Open Mirroring preferisce empty)
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            df[col] = df[col].fillna('')
        elif df[col].dtype == 'bool':
            # Converti bool a string (true/false)
            df[col] = df[col].astype(str).str.lower()
        else:
            # Converti other NaN a empty string
            df[col] = df[col].fillna('')
    
    return df


def _keys_file_path(company, entity):
    # Path su OneLake per le chiavi (sul Lakehouse, non sul Mirrored DB)
    safe_company = re.sub(r'[^a-zA-Z0-9_-]', '_', company)
    safe_entity = re.sub(r'[^a-zA-Z0-9_-]', '_', entity)
    return f"{LAKEHOUSE_ID}/{KEYS_TARGET_FOLDER}/{safe_company}/{safe_entity}/keys.csv"


def load_previous_keys(company, entity):
    """
    Carica le chiavi salvate dal run precedente da OneLake.
    """
    try:
        service_client     = _get_onelake_client()
        file_system_client = service_client.get_file_system_client(WORKSPACE_ID)
        keys_path          = _keys_file_path(company, entity)
        file_client        = file_system_client.get_file_client(keys_path)
        data = file_client.download_file().readall()
        df   = pd.read_csv(pd.io.common.BytesIO(data), dtype=str)
        return df
    except Exception:
        return None


def save_current_keys(df_keys, company, entity):
    """
    Salva le chiavi correnti su OneLake per il prossimo run.
    """
    try:
        service_client     = _get_onelake_client()
        file_system_client = service_client.get_file_system_client(WORKSPACE_ID)
        keys_bytes = df_keys.to_csv(index=False).encode('utf-8')
        keys_path  = _keys_file_path(company, entity)
        file_client = file_system_client.get_file_client(keys_path)
        file_client.upload_data(keys_bytes, overwrite=True)
        return keys_path
    except Exception as e:
        raise Exception(f"Impossibile salvare chiavi su OneLake: {e}")


def validate_csv_file(file_name):
    """
    Valida il CSV generato prima dell'upload
    """
    print(f"🔍 Validazione CSV: {file_name}")
    try:
        with open(file_name, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            rows = list(reader)
            if not rows:
                raise ValueError("CSV vuoto")
            header = rows[0]
            print(f"  ✓ Header: {len(header)} colonne")
            print(f"  ✓ Colonne: {', '.join(header[:5])}{'...' if len(header) > 5 else ''}")
            print(f"  ✓ Righe dati: {len(rows) - 1}")
            
            # Validazioni
            for i, row in enumerate(rows[1:5]):
                if len(row) != len(header):
                    print(f"  ⚠️  Riga {i+1}: colonne non allineate ({len(row)} vs {len(header)})")
            
            print("✅ CSV valido")
            return True
    except Exception as e:
        print(f"❌ Errore validazione CSV: {str(e)}")
        return False

def get_openmirroring_schema(df):
    """
    Genera SchemaDefinition per Open Mirroring dal DataFrame.
    Assicura compatibilità con formati Open Mirroring Fabric.
    """
    schema_columns = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        
        # Mapping pandas dtype a Open Mirroring types
        if 'int' in dtype:
            if 'int64' in dtype:
                mirroring_type = 'Int64'
            else:
                mirroring_type = 'Int32'
        elif 'float' in dtype:
            mirroring_type = 'Double'
        elif 'bool' in dtype:
            mirroring_type = 'Boolean'
        elif 'datetime' in dtype:
            mirroring_type = 'DateTime'
        else:
            # Default String per object columns
            mirroring_type = 'String'
        
        schema_columns.append({
            "Name": col,
            "DataType": mirroring_type,
            "IsNullable": True
        })
    
    # Log schema per debug
    print(f"    DEBUG Schema: {len(schema_columns)} colonne")
    for sc in schema_columns[:3]:
        print(f"      - {sc['Name']}: {sc['DataType']}")
    
    return {"Columns": schema_columns}

def create_openmirroring_metadata(df, key_columns=None):
    """
    Crea il file _metadata.json per Open Mirroring.
    Allineato al formato generato da Business Central.
    """
    # Identifica colone chiave: preferibilmente "id", altrimenti la prima
    if key_columns is None:
        if "id" in [col.lower() for col in df.columns]:
            key_columns = ["id"]
        else:
            key_columns = [df.columns[0]]
    
    # Genera schema con mapping corretto
    schema_columns = []
    for col in df.columns:
        # Determina il DataType correto
        dtype = str(df[col].dtype)
        if 'int' in dtype:
            mirroring_type = 'Int64' if 'int64' in dtype else 'Int32'
        elif 'float' in dtype:
            mirroring_type = 'Double'
        elif 'bool' in dtype:
            mirroring_type = 'Boolean'
        elif 'datetime' in dtype:
            mirroring_type = 'DateTime'
        else:
            mirroring_type = 'String'
        
        col_def = {
            "Name": col,
            "DataType": mirroring_type,
        }
        
        # Per le colonne chiave, non aggiungere IsNullable (come fa BC)
        if col not in key_columns:
            col_def["IsNullable"] = True
        
        schema_columns.append(col_def)
    
    # Metadata nel formato BC/Open Mirroring
    metadata = {
        "keyColumns": key_columns,  # lowercase come BC
        "fileDetectionStrategy": "LastUpdateTimeFileDetection",
        "SchemaDefinition": {
            "Columns": schema_columns
        },
        "fileFormat": "csv"  # CSV, non DelimitedText
    }
    return metadata

def create_partner_events(company, entity):
    """
    Crea il file _partnerEvents.json per Open Mirroring
    """
    partner_events = {
        "partnerName": "BusinessCentralMirroring",
        "sourceInfo": {
            "sourceType": "DynamicsBC",
            "sourceVersion": "21.0",
            "additionalInformation": {
                "environment": ENVIRONMENT,
                "company": company,
                "entity": entity,
                "createdAt": datetime.now().isoformat()
            }
        }
    }
    return partner_events

def get_bc_token():
    """
    Ottieni token per Business Central via Client Credentials (Service Principal).
    Nessun browser — compatibile con Spark Job non interattivo su Fabric.
    """
    credential = ClientSecretCredential(
        tenant_id=BC_TENANT_ID,
        client_id=BC_CLIENT_ID,
        client_secret=BC_CLIENT_SECRET
    )
    token = credential.get_token("https://api.businesscentral.dynamics.com/.default")
    return token.token

def pull_full_data(token, company, entity, last_load_timestamp=None, mode=None, delete_keys=None, key_columns=None, keep_data_for_delete=False):
    """
    Pull load da BC OData per una specifica company e entity.

    Se last_load_timestamp è fornito, fa un incremental load.

    Nuovi parametri per supportare flussi Delete:
    - mode: se 'delete' restituisce un DataFrame contenente solo le key rows contrassegnate come Delete.
    - delete_keys: lista o DataFrame con le chiavi da marcare come Delete (obbligatorio se mode=='delete').
    - key_columns: lista di nomi colonna usata per i delete quando delete_keys è una lista semplice.
    - keep_data_for_delete: se True mantiene tutte le colonne anche per Delete; altrimenti ritorna solo le key + __rowMarker__.
    """
    base_url = (
        f"https://api.businesscentral.dynamics.com/"
        f"v2.0/{BC_TENANT_ID}/{ENVIRONMENT}/ODataV4/"
        f"Company('{company}')/{entity}"
    )
    headers = {"Authorization": f"Bearer {token}"}
    rows = []
    session = get_session_with_retries()
    
    is_incremental = last_load_timestamp is not None

    # Supporto per flussi Delete: short-circuit senza effettuare chiamate HTTP
    if mode == 'delete':
        if delete_keys is None:
            raise ValueError("Per mode='delete' è richiesto il parametro 'delete_keys' (lista o DataFrame delle chiavi).")

        # Costruisci DataFrame dalle delete_keys
        if isinstance(delete_keys, pd.DataFrame):
            df = delete_keys.copy()
        else:
            # delete_keys può essere lista di dicts o lista di scalar (quando key_columns fornito)
            if all(isinstance(x, dict) for x in delete_keys):
                df = pd.DataFrame(delete_keys)
            else:
                if not key_columns or len(key_columns) != 1:
                    # Se fornita lista di scalars, serve esattamente una key column
                    raise ValueError("Quando 'delete_keys' è una lista di valori scalari, fornire 'key_columns' con una sola colonna.")
                df = pd.DataFrame({key_columns[0]: delete_keys})

        # Sanitizza e mantieni solo le key colonne se richiesto
        df = sanitize_dataframe(df)

        # Aggiungi __rowMarker__ per Delete (2)
        df = add_row_marker_column(df, mode='delete', key_columns=key_columns, keep_data_for_delete=keep_data_for_delete)

        return df

    try:
        # Se è incremental, aggiungi filtro Data Modifica
        if is_incremental:
            # Formato ISO per OData: 2025-01-01T00:00:00Z
            if isinstance(last_load_timestamp, datetime):
                filter_date = last_load_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                filter_date = str(last_load_timestamp)
                if not filter_date.endswith('Z'):
                    filter_date = filter_date.rstrip() + "Z"
            # Usa SystemModifiedAt (campo nativo di BC per tracking incrementale)
            filter_query = f"?$filter=SystemModifiedAt gt datetime'{filter_date}'"
            url_with_filter = base_url + filter_query
            print(f"  📋 Filtro incrementale: SystemModifiedAt > {filter_date}")
        else:
            url_with_filter = base_url

        pagination_url = url_with_filter
        retry_without_filter = False
        while pagination_url:
            try:
                response = session.get(pagination_url, headers=headers, timeout=30)
                if response.status_code == 400 and is_incremental and not retry_without_filter:
                    # Fallback: riprova senza filtro se il campo non esiste
                    print(f"  ⚠️  Filtro fallito (400), retry senza filtro...")
                    retry_without_filter = True
                    pagination_url = base_url
                    continue
                response.raise_for_status()
                data = response.json()
                rows.extend(data.get("value", []))
                pagination_url = data.get("@odata.nextLink")
            except Exception as e:
                if "400" in str(e) and is_incremental and not retry_without_filter:
                    print(f"  ⚠️  Filtro fallito, retry senza filtro...")
                    retry_without_filter = True
                    pagination_url = base_url
                    continue
                raise
            
    finally:
        session.close()

    if not rows:
        print(f"  ⚠️  Nessun dato trovato per {company}::{entity}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    print(f"  📊 DataFrame originale: {len(df)} righe, {len(df.columns)} colonne")
    
    # Rimuovi colonne OData di sistema
    odata_cols = [col for col in df.columns if col.startswith('@')]
    if odata_cols:
        df = df.drop(columns=odata_cols)
        print(f"  ⚠️  Colonne OData rimosse: {len(odata_cols)}")
    
    # Pulizia colonne totalmente vuote
    df = df.dropna(axis=1, how='all')
    
    # Applica sanitizzazione aggressiva
    df = sanitize_dataframe(df)
    
    # Aggiungi __rowMarker__ per Open Mirroring
    df = add_row_marker_column(df, is_incremental=is_incremental)
    
    print(f"  ✅ DataFrame pulito: {len(df)} righe, {len(df.columns)} colonne")
    print(f"  🏷️  __rowMarker__ aggiunto: {'Update' if is_incremental else 'Insert'}")
    
    return df

def validate_dataframe_for_mirroring(df):
    """
    Valida il DataFrame prima dell'upload per Open Mirroring.
    Ritorna (valid: bool, warnings: list)
    """
    warnings = []
    
    # Check colonne
    if len(df.columns) == 0:
        return False, ["DataFrame vuoto"]
    
    if '__rowMarker__' not in df.columns:
        warnings.append("⚠️  __rowMarker__ mancante")
    
    # Check __rowMarker__ valori
    if '__rowMarker__' in df.columns:
        valid_markers = {0, 1, 2, 4}
        invalid = df[~df['__rowMarker__'].isin(valid_markers)]
        if not invalid.empty:
            warnings.append(f"⚠️  {len(invalid)} righe con __rowMarker__ invalido")
    
    # Check NULL/NaN
    null_counts = df.isnull().sum()
    null_cols = null_counts[null_counts > 0]
    if not null_cols.empty:
        warnings.append(f"⚠️  Colonne con NULL: {list(null_cols.index)}")
    
    # Check tipi strani
    for col in df.columns:
        if df[col].dtype == 'object':
            # Verifica se contiene valori molto lunghi (potrebbe causare problemi)
            max_len = df[col].astype(str).str.len().max()
            if max_len > 10000:
                warnings.append(f"⚠️  Colonna {col}: valori <br> lunghi ({max_len} chars max)")
    
    return len(warnings) == 0 or True, warnings


def upload_to_onelake(df, company, entity, metadata_override=None):
    """
    Upload DataFrame direttamente su OneLake con struttura Open Mirroring.
    Crea: LandingZone/{TableName}/
           - 00000000000000000001.csv (file dati sequenziale)
           - _metadata.json (schema)
           - _partnerEvents.json (info sorgente)
    Usa ClientSecretCredential — nessun browser, compatibile con Spark Job su Fabric.
    """
    table_name    = entity
    is_valid, validation_warnings = validate_dataframe_for_mirroring(df)
    for warning in validation_warnings:
        print(f"  {warning}")

    next_sequence  = get_next_file_sequence(company, entity)
    data_file_name = f"{next_sequence:020d}.csv"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"\n📤 Tentativo {attempt} upload OpenMirroring...")
            print(f"   Company: {company} | Entity: {entity} | File: {data_file_name}")
            print(f"   Righe: {len(df)} | Colonne: {len(df.columns)}")

            service_client     = _get_onelake_client()
            file_system_client = service_client.get_file_system_client(WORKSPACE_ID)
            table_folder       = f"{MIRRORED_DB_ID}/{TARGET_FOLDER}/{table_name}"

            # 1. Upload CSV
            csv_bytes      = df.to_csv(index=False, encoding='utf-8', quoting=1, lineterminator='\r\n').encode('utf-8')
            data_file_path = f"{table_folder}/{data_file_name}"
            print(f"  📝 Upload CSV: {data_file_name} ({len(csv_bytes)} bytes)")
            file_client = file_system_client.get_file_client(data_file_path)
            file_client.upload_data(csv_bytes, overwrite=True)
            print(f"    ✓ Righe: {len(df)}")

            # 2. Upload _metadata.json
            metadata       = metadata_override if metadata_override else create_openmirroring_metadata(df)
            metadata_bytes = json.dumps(metadata, indent=2).encode('utf-8')
            metadata_path  = f"{table_folder}/_metadata.json"
            print(f"  📋 Upload _metadata.json | keyColumns: {metadata.get('keyColumns', [])}")
            file_client = file_system_client.get_file_client(metadata_path)
            file_client.upload_data(metadata_bytes, overwrite=True)

            # 3. Upload _partnerEvents.json (livello database)
            partner_events       = create_partner_events(company, entity)
            partner_events_bytes = json.dumps(partner_events, indent=2).encode('utf-8')
            partner_events_path  = f"{MIRRORED_DB_ID}/{TARGET_FOLDER}/_partnerEvents.json"
            print(f"  ℹ️  Upload _partnerEvents.json")
            file_client = file_system_client.get_file_client(partner_events_path)
            file_client.upload_data(partner_events_bytes, overwrite=True)

            # 4. Aggiorna stato
            update_file_sequence(company, entity, next_sequence)
            update_load_timestamp(company, entity, datetime.now())

            print(f"\n✅ Upload completato! File: {data_file_name} | Righe: {len(df)} | Colonne: {len(df.columns)}")
            return True

        except Exception as e:
            print(f"  ❌ Tentativo {attempt}: {str(e)}")
            if attempt < MAX_RETRIES:
                wait_time = RETRY_BACKOFF_FACTOR * (2 ** (attempt - 1))
                print(f"  ⏳ Retry tra {wait_time}s...")
                sleep(wait_time)
            else:
                print(f"  🔴 Upload fallito dopo {MAX_RETRIES} tentativi")
                raise

    return False


# ================= RUN =================

if __name__ == "__main__":
    try:
        print("🚀 Avvio POC Multi-Entity Multi-Company BC → OneLake LandingZone")
        print(f"📊 Companies: {COMPANIES}")
        print(f"📋 Entities: {ENTITIES}\n")

        # BC token (una volta sola all'inizio)
        print("🔐 Autenticazione con Business Central...")
        token = get_bc_token()
        print("✅ Token ottenuto\n")

        # Ciclo su tutte le combinazioni company/entity
        total_uploaded = 0
        failed_combinations = []

        for company in COMPANIES:
            for entity in ENTITIES:
                print(f"\n{'='*70}")
                print(f"Processing: {company} :: {entity}")
                print(f"{'='*70}")
                
                try:
                    # Determina se è incremental (se esiste last_load)
                    last_load = get_last_load_timestamp(company, entity)
                    is_incremental = last_load is not None
                    
                    print(f"{'📈' if is_incremental else '📥'} Tipo carico: {'INCREMENTAL' if is_incremental else 'INITIAL'}")
                    if is_incremental:
                        print(f"   Ultimo carico: {last_load}")

                    # Pull dati
                    print(f"📥 Estrazione dati da OData...")
                    df = pull_full_data(token, company, entity, last_load)
                    
                    if df.empty:
                        print("⚠️  Nessun dato trovato. Skip.")
                        continue
                    
                    print(f"✅ Record estratti: {len(df)}")

                    # Determina le key columns dalla metadata (formato BC: keyColumns lowercase)
                    metadata = create_openmirroring_metadata(df)
                    key_columns = metadata.get('keyColumns', None)
                    
                    if key_columns is None:
                        print(f"  ⚠️  Nessuna key column trovata nel metadata. Usa automaticamente prima colonna.")
                        key_columns = [df.columns[0]]
                    
                    current_keys_df = df.loc[:, key_columns].drop_duplicates().astype(str).reset_index(drop=True)

                    # --- Rileva e invia delete confrontando chiavi precedenti vs correnti ---
                    prev_keys_df = load_previous_keys(company, entity)
                    if prev_keys_df is not None:
                        prev = prev_keys_df.astype(str)
                        merged = prev.merge(current_keys_df, on=key_columns, how='left', indicator=True)
                        deleted_keys_df = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
                        if not deleted_keys_df.empty:
                            print(f"  🗑️  Trovate {len(deleted_keys_df)} delete da inviare")
                            delete_df = add_row_marker_column(deleted_keys_df, mode='delete', key_columns=key_columns, keep_data_for_delete=False)
                            metadata_delete = create_openmirroring_metadata(deleted_keys_df, key_columns=key_columns)
                            upload_to_onelake(delete_df, company, entity, metadata_override=metadata_delete)
                        else:
                            print("  ℹ️  Nessuna delete rilevata")
                    else:
                        print("  ℹ️  Nessuna chiave precedente: file first run")

                    # Upload file dati (Insert o Update) - direttamente da DataFrame
                    print(f"\n📤 Preparazione upload Open Mirroring (dati)...")
                    upload_to_onelake(df, company, entity)
                    
                    total_uploaded += 1
                    print(f"✅ Success: {company} :: {entity}")

                    # Salva le chiavi correnti su OneLake per il prossimo run
                    try:
                        save_current_keys(current_keys_df, company, entity)
                        print(f"💾 Chiavi salvate su OneLake per next-run")
                    except Exception as e:
                        print(f"⚠️  Impossibile salvare chiavi: {e}")

                except Exception as e:
                    print(f"❌ Errore: {company} :: {entity}")
                    print(f"   {str(e)}")
                    failed_combinations.append(f"{company}::{entity}")
                    traceback.print_exc()

        print(f"\n\n{'='*70}")
        print(f"🎉 PROCESSAMENTO COMPLETATO")
        print(f"{'='*70}")
        print(f"✅ Upload riusciti: {total_uploaded}")
        if failed_combinations:
            print(f"❌ Combinazioni fallite: {len(failed_combinations)}")
            for combo in failed_combinations:
                print(f"   - {combo}")
        else:
            print(f"✨ Nessun errore!")
        
        print(f"\n📊 Stato caricamenti salvato in: {STATE_FILE}")
        
    except Exception as e:
        print(f"\n❌ ERRORE FATALE: {str(e)}")
        print(f"\n🔴 Stack trace:")
        traceback.print_exc()
        exit(1)