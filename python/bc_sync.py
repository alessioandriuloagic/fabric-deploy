import requests
import pandas as pd
import os
import re
import json
import numpy as np
import base64
from datetime import datetime, timedelta
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from time import sleep
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import traceback
import csv
import sys

# ================= CONFIG =================

def _get_param(name, default=None):
    """
    Legge un parametro dallo Spark Job commandLineArguments oppure da env var.
    Lo Spark Job in Fabric passa i parametri come: --KEY value (in sys.argv).
    """
    key = f"--{name}"
    for i, arg in enumerate(sys.argv):
        if arg == key and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return os.environ.get(name, default)

def _get_param_b64(name, default=None):
    """Legge un parametro codificato in Base64 e lo decodifica."""
    raw = _get_param(name)
    if raw:
        try:
            return base64.b64decode(raw).decode('utf-8')
        except Exception:
            return raw  # fallback: valore non codificato
    return default

# Business Central
BC_TENANT_ID     = _get_param("BC_TENANT_ID")
BC_CLIENT_ID     = _get_param("BC_CLIENT_ID")
BC_CLIENT_SECRET = _get_param_b64("BC_CLIENT_SECRET_B64") or _get_param("BC_CLIENT_SECRET")

ENVIRONMENT = _get_param("BC_ENVIRONMENT", "SandboxTest")

def _decode_b64_param(name, default_json='[]'):
    raw = _get_param(name)
    if raw:
        return base64.b64decode(raw).decode('utf-8')
    return default_json

_companies_raw = _decode_b64_param("BC_COMPANIES_B64", '["CRONUS%20IT"]')
COMPANIES = json.loads(_companies_raw)

_entities_raw = _decode_b64_param("BC_ENTITIES_B64", '["ItemLedgerEntries"]')
ENTITIES = json.loads(_entities_raw)

# OneLake
WORKSPACE_ID      = _get_param("FABRIC_WORKSPACE_ID")
LAKEHOUSE_ID      = _get_param("FABRIC_LAKEHOUSE_ID")
MIRRORED_DB_ID    = _get_param("FABRIC_MIRRORED_DB_ID")

TARGET_FOLDER      = "Files/LandingZone"
KEYS_TARGET_FOLDER = "Files/MirroringKeys"

# OneLake Service Principal
ONELAKE_TENANT_ID     = _get_param("FABRIC_TENANT_ID",     BC_TENANT_ID)
ONELAKE_CLIENT_ID     = _get_param("FABRIC_CLIENT_ID",     BC_CLIENT_ID)
ONELAKE_CLIENT_SECRET = _get_param_b64("FABRIC_CLIENT_SECRET_B64") or _get_param("FABRIC_CLIENT_SECRET") or BC_CLIENT_SECRET

# Validazione parametri obbligatori
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

STATE_FILE = f"{LAKEHOUSE_ID}/Files/MirroringState/.mirroring_state.json"

MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 1

# ================= ONELAKE HELPERS =================

def _get_onelake_credential():
    return ClientSecretCredential(
        tenant_id=ONELAKE_TENANT_ID,
        client_id=ONELAKE_CLIENT_ID,
        client_secret=ONELAKE_CLIENT_SECRET
    )

def _get_onelake_client():
    return DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        credential=_get_onelake_credential()
    )

# ================= STATE MANAGEMENT =================

def load_state():
    try:
        client = _get_onelake_client().get_file_system_client(WORKSPACE_ID)
        data = client.get_file_client(STATE_FILE).download_file().readall()
        return json.loads(data.decode('utf-8'))
    except Exception:
        return {}

def save_state(state):
    try:
        client = _get_onelake_client().get_file_system_client(WORKSPACE_ID)
        state_bytes = json.dumps(state, indent=2, default=str).encode('utf-8')
        client.get_file_client(STATE_FILE).upload_data(state_bytes, overwrite=True)
    except Exception as e:
        print(f"[WARN] Impossibile salvare stato: {e}")

def get_last_load_timestamp(company, entity):
    state = load_state()
    key = f"{company}::{entity}"
    return state.get(key, {}).get("last_load", None)

def update_load_timestamp(company, entity, timestamp):
    state = load_state()
    key = f"{company}::{entity}"
    if key not in state:
        state[key] = {}
    state[key]["last_load"] = timestamp
    save_state(state)

def get_next_file_sequence(company, entity):
    state = load_state()
    key = f"{company}::{entity}"
    return state.get(key, {}).get("next_sequence", 1)

def update_file_sequence(company, entity, sequence):
    state = load_state()
    key = f"{company}::{entity}"
    if key not in state:
        state[key] = {}
    state[key]["next_sequence"] = sequence + 1
    save_state(state)

# ================= DATA HELPERS =================

def add_row_marker_column(df, mode=None, is_incremental=None, key_columns=None, keep_data_for_delete=False):
    """
    Aggiunge la colonna __rowMarker__ per Open Mirroring.
    0=Insert, 1=Update, 2=Delete, 4=Upsert
    """
    mapper = {'insert': 0, 'update': 1, 'delete': 2, 'upsert': 4}

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

    out = df.copy()
    out['__rowMarker__'] = int(marker)
    cols = [c for c in out.columns if c != '__rowMarker__']
    out = out[cols + ['__rowMarker__']]
    out['__rowMarker__'] = out['__rowMarker__'].astype(int)
    return out

def get_session_with_retries():
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
    sanitized = re.sub(r'[^a-zA-Z0-9_\s]', '', str(col_name))
    sanitized = re.sub(r'\s+', '_', sanitized.strip())
    return sanitized[:100] if sanitized else "Column"

def sanitize_dataframe(df):
    df.columns = [sanitize_column_name(col) for col in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]

    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).replace('nan', '').replace('None', '')
            df[col] = df[col].fillna('')
            df[col] = df[col].str.replace(r'[\x00-\x1f]', '', regex=True)
            df[col] = df[col].str.replace('""', '"', regex=False)
        elif df[col].dtype in ['float64', 'float32']:
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            df[col] = df[col].fillna('')
        elif df[col].dtype == 'bool':
            df[col] = df[col].astype(str).str.lower()
        else:
            df[col] = df[col].fillna('')

    return df

# ================= KEYS MANAGEMENT =================

def _keys_file_path(company, entity):
    safe_company = re.sub(r'[^a-zA-Z0-9_-]', '_', company)
    safe_entity = re.sub(r'[^a-zA-Z0-9_-]', '_', entity)
    return f"{LAKEHOUSE_ID}/{KEYS_TARGET_FOLDER}/{safe_company}/{safe_entity}/keys.csv"

def load_previous_keys(company, entity):
    try:
        client = _get_onelake_client().get_file_system_client(WORKSPACE_ID)
        data = client.get_file_client(_keys_file_path(company, entity)).download_file().readall()
        return pd.read_csv(pd.io.common.BytesIO(data), dtype=str)
    except Exception:
        return None

def save_current_keys(df_keys, company, entity):
    try:
        client = _get_onelake_client().get_file_system_client(WORKSPACE_ID)
        keys_bytes = df_keys.to_csv(index=False).encode('utf-8')
        client.get_file_client(_keys_file_path(company, entity)).upload_data(keys_bytes, overwrite=True)
    except Exception as e:
        raise Exception(f"Impossibile salvare chiavi su OneLake: {e}")

# ================= OPEN MIRRORING METADATA =================

def create_openmirroring_metadata(df, key_columns=None):
    if key_columns is None:
        if "id" in [col.lower() for col in df.columns]:
            key_columns = ["id"]
        else:
            key_columns = [df.columns[0]]

    schema_columns = []
    for col in df.columns:
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

        col_def = {"Name": col, "DataType": mirroring_type}
        if col not in key_columns:
            col_def["IsNullable"] = True
        schema_columns.append(col_def)

    return {
        "keyColumns": key_columns,
        "fileDetectionStrategy": "LastUpdateTimeFileDetection",
        "SchemaDefinition": {"Columns": schema_columns},
        "fileFormat": "csv"
    }

def create_partner_events(company, entity):
    return {
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

# ================= BC API =================

def get_bc_token():
    credential = ClientSecretCredential(
        tenant_id=BC_TENANT_ID,
        client_id=BC_CLIENT_ID,
        client_secret=BC_CLIENT_SECRET
    )
    token = credential.get_token("https://api.businesscentral.dynamics.com/.default")
    return token.token

def pull_full_data(token, company, entity, last_load_timestamp=None, mode=None, delete_keys=None, key_columns=None, keep_data_for_delete=False):
    base_url = (
        f"https://api.businesscentral.dynamics.com/"
        f"v2.0/{BC_TENANT_ID}/{ENVIRONMENT}/ODataV4/"
        f"Company('{company}')/{entity}"
    )
    headers = {"Authorization": f"Bearer {token}"}
    rows = []
    session = get_session_with_retries()

    is_incremental = last_load_timestamp is not None

    if mode == 'delete':
        if delete_keys is None:
            raise ValueError("Per mode='delete' serve 'delete_keys'.")
        if isinstance(delete_keys, pd.DataFrame):
            df = delete_keys.copy()
        else:
            if all(isinstance(x, dict) for x in delete_keys):
                df = pd.DataFrame(delete_keys)
            else:
                if not key_columns or len(key_columns) != 1:
                    raise ValueError("Quando 'delete_keys' e' una lista di scalari, fornire 'key_columns' con una sola colonna.")
                df = pd.DataFrame({key_columns[0]: delete_keys})
        df = sanitize_dataframe(df)
        df = add_row_marker_column(df, mode='delete', key_columns=key_columns, keep_data_for_delete=keep_data_for_delete)
        return df

    try:
        if is_incremental:
            if isinstance(last_load_timestamp, datetime):
                filter_date = last_load_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                filter_date = str(last_load_timestamp)
                if not filter_date.endswith('Z'):
                    filter_date = filter_date.rstrip() + "Z"
            filter_query = f"?$filter=SystemModifiedAt gt datetime'{filter_date}'"
            url_with_filter = base_url + filter_query
            print(f"  [INCREMENTAL] SystemModifiedAt > {filter_date}")
        else:
            url_with_filter = base_url

        pagination_url = url_with_filter
        retry_without_filter = False
        while pagination_url:
            try:
                response = session.get(pagination_url, headers=headers, timeout=30)
                if response.status_code == 400 and is_incremental and not retry_without_filter:
                    print("  [WARN] Filtro fallito (400), retry senza filtro...")
                    retry_without_filter = True
                    pagination_url = base_url
                    continue
                response.raise_for_status()
                data = response.json()
                rows.extend(data.get("value", []))
                pagination_url = data.get("@odata.nextLink")
            except Exception as e:
                if "400" in str(e) and is_incremental and not retry_without_filter:
                    print("  [WARN] Filtro fallito, retry senza filtro...")
                    retry_without_filter = True
                    pagination_url = base_url
                    continue
                raise

    finally:
        session.close()

    if not rows:
        print(f"  [WARN] Nessun dato trovato per {company}::{entity}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    odata_cols = [col for col in df.columns if col.startswith('@')]
    if odata_cols:
        df = df.drop(columns=odata_cols)

    df = df.dropna(axis=1, how='all')
    df = sanitize_dataframe(df)
    df = add_row_marker_column(df, is_incremental=is_incremental)

    print(f"  [OK] {len(df)} righe, {len(df.columns)} colonne (__rowMarker__={'Update' if is_incremental else 'Insert'})")
    return df

# ================= VALIDATION =================

def validate_dataframe_for_mirroring(df):
    warnings = []
    if len(df.columns) == 0:
        return False, ["DataFrame vuoto"]
    if '__rowMarker__' not in df.columns:
        warnings.append("[WARN] __rowMarker__ mancante")
    if '__rowMarker__' in df.columns:
        valid_markers = {0, 1, 2, 4}
        invalid = df[~df['__rowMarker__'].isin(valid_markers)]
        if not invalid.empty:
            warnings.append(f"[WARN] {len(invalid)} righe con __rowMarker__ invalido")
    null_counts = df.isnull().sum()
    null_cols = null_counts[null_counts > 0]
    if not null_cols.empty:
        warnings.append(f"[WARN] Colonne con NULL: {list(null_cols.index)}")
    for col in df.columns:
        if df[col].dtype == 'object':
            max_len = df[col].astype(str).str.len().max()
            if max_len > 10000:
                warnings.append(f"[WARN] Colonna {col}: valori molto lunghi ({max_len} chars)")
    return len(warnings) == 0 or True, warnings

# ================= UPLOAD =================

def upload_to_onelake(df, company, entity, metadata_override=None):
    table_name = entity
    is_valid, validation_warnings = validate_dataframe_for_mirroring(df)
    for w in validation_warnings:
        print(f"  {w}")

    next_sequence = get_next_file_sequence(company, entity)
    data_file_name = f"{next_sequence:020d}.csv"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"  [UPLOAD] Tentativo {attempt} | {company}::{entity} | File: {data_file_name} | Righe: {len(df)}")

            client = _get_onelake_client().get_file_system_client(WORKSPACE_ID)
            table_folder = f"{MIRRORED_DB_ID}/{TARGET_FOLDER}/{table_name}"

            # 1. CSV
            csv_bytes = df.to_csv(index=False, encoding='utf-8', quoting=1, lineterminator='\r\n').encode('utf-8')
            client.get_file_client(f"{table_folder}/{data_file_name}").upload_data(csv_bytes, overwrite=True)

            # 2. _metadata.json
            metadata = metadata_override if metadata_override else create_openmirroring_metadata(df)
            metadata_bytes = json.dumps(metadata, indent=2).encode('utf-8')
            client.get_file_client(f"{table_folder}/_metadata.json").upload_data(metadata_bytes, overwrite=True)

            # 3. _partnerEvents.json
            pe_bytes = json.dumps(create_partner_events(company, entity), indent=2).encode('utf-8')
            client.get_file_client(f"{MIRRORED_DB_ID}/{TARGET_FOLDER}/_partnerEvents.json").upload_data(pe_bytes, overwrite=True)

            # 4. Aggiorna stato
            update_file_sequence(company, entity, next_sequence)
            update_load_timestamp(company, entity, datetime.now())

            print(f"  [OK] Upload completato: {data_file_name} ({len(csv_bytes)} bytes, {len(df)} righe)")
            return True

        except Exception as e:
            print(f"  [ERROR] Tentativo {attempt}: {str(e)}")
            if attempt < MAX_RETRIES:
                wait_time = RETRY_BACKOFF_FACTOR * (2 ** (attempt - 1))
                print(f"  [RETRY] Attesa {wait_time}s...")
                sleep(wait_time)
            else:
                print(f"  [FAIL] Upload fallito dopo {MAX_RETRIES} tentativi")
                raise

    return False


# ================= RUN =================

if __name__ == "__main__":
    spark = None
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    except Exception:
        pass

    try:
        print(f"[START] BC -> OneLake | Companies: {COMPANIES} | Entities: {ENTITIES}")

        print("[AUTH] Autenticazione BC...")
        token = get_bc_token()
        print("[AUTH] Token ottenuto")

        total_uploaded = 0
        failed_combinations = []

        for company in COMPANIES:
            for entity in ENTITIES:
                print(f"\n[PROCESS] {company} :: {entity}")

                try:
                    last_load = get_last_load_timestamp(company, entity)
                    is_incremental = last_load is not None

                    if is_incremental:
                        print(f"  [MODE] INCREMENTAL (ultimo carico: {last_load})")
                    else:
                        print(f"  [MODE] INITIAL LOAD")

                    df = pull_full_data(token, company, entity, last_load)

                    if df.empty:
                        print("  [SKIP] Nessun dato trovato")
                        continue

                    metadata = create_openmirroring_metadata(df)
                    key_columns = metadata.get('keyColumns', [df.columns[0]])

                    current_keys_df = df.loc[:, key_columns].drop_duplicates().astype(str).reset_index(drop=True)

                    # Rileva delete
                    prev_keys_df = load_previous_keys(company, entity)
                    if prev_keys_df is not None:
                        prev = prev_keys_df.astype(str)
                        merged = prev.merge(current_keys_df, on=key_columns, how='left', indicator=True)
                        deleted_keys_df = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
                        if not deleted_keys_df.empty:
                            print(f"  [DELETE] {len(deleted_keys_df)} righe da eliminare")
                            delete_df = add_row_marker_column(deleted_keys_df, mode='delete', key_columns=key_columns, keep_data_for_delete=False)
                            metadata_delete = create_openmirroring_metadata(deleted_keys_df, key_columns=key_columns)
                            upload_to_onelake(delete_df, company, entity, metadata_override=metadata_delete)

                    # Upload dati
                    upload_to_onelake(df, company, entity)

                    total_uploaded += 1

                    # Salva chiavi per prossimo run
                    try:
                        save_current_keys(current_keys_df, company, entity)
                    except Exception as e:
                        print(f"  [WARN] Impossibile salvare chiavi: {e}")

                except Exception as e:
                    print(f"  [ERROR] {company}::{entity} - {str(e)}")
                    failed_combinations.append(f"{company}::{entity}")
                    traceback.print_exc()

        print(f"\n[DONE] Upload riusciti: {total_uploaded}")
        if failed_combinations:
            print(f"[DONE] Falliti: {len(failed_combinations)} -> {failed_combinations}")

    except Exception as e:
        print(f"[FATAL] {str(e)}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()