"""
bc_connector.py
===============
Configurazione, autenticazione e lettura dati da Business Central OData V4 API.

Questo modulo gestisce tutto ciò che è specifico di Business Central:
- Lettura e validazione parametri BC_*
- Autenticazione OAuth2 (Client Credentials) verso Azure AD
- Chiamate all'API OData di BC con paginazione, retry e filtro incrementale
- Generazione del file ``_partnerEvents.json`` specifico per BC

Autenticazione
--------------
Business Central usa OAuth2 Client Credentials con scope:
``https://api.businesscentral.dynamics.com/.default``

Il tenant Azure AD deve avere un'App Registration con permessi BC:
- API: Dynamics 365 Business Central
- Permission: Financials.ReadWrite.All (o specifici per le entità)

URL OData
---------
::

    https://api.businesscentral.dynamics.com/v2.0/<tenant>/<env>/ODataV4/
    Company('<company>')/<entity>

Filtro incrementale
-------------------
Se è presente un timestamp dell'ultimo carico, viene aggiunto il filtro OData:
``?$filter=SystemModifiedAt gt datetime'<timestamp>'``

Se il filtro restituisce HTTP 400 (entità che non supporta SystemModifiedAt),
la funzione esegue automaticamente un retry senza filtro (full load).

Funzioni esportate
------------------
- load_bc_config()              → dict configurazione BC
- get_bc_token(cfg)             → Bearer token string
- pull_bc_data(...)             → pd.DataFrame con __rowMarker__
- create_bc_partner_events(...) → dict per _partnerEvents.json
"""

import json
from datetime import datetime
from azure.identity import ClientSecretCredential

from .config import get_param, get_param_b64, decode_b64_param, validate_required
from .dataframe_utils import get_session_with_retries, sanitize_dataframe, add_row_marker_column


def load_bc_config():
    """
    Legge e valida tutti i parametri Business Central da sys.argv / variabili d'ambiente.

    Parametri obbligatori
    ---------------------
    - ``BC_TENANT_ID``       — Azure AD Tenant ID
    - ``BC_CLIENT_ID``       — Application (client) ID del Service Principal
    - ``BC_CLIENT_SECRET``   — Client secret (plain-text)
      oppure ``BC_CLIENT_SECRET_B64`` — Client secret codificato Base64

    Parametri opzionali
    -------------------
    - ``BC_ENVIRONMENT``    — Nome dell'ambiente BC (default: ``"SandboxTest"``)
    - ``BC_COMPANIES_B64``  — Lista JSON di companies codificata Base64
                              (default: ``["CRONUS%20IT"]``)
    - ``BC_ENTITIES_B64``   — Lista JSON di entità OData codificata Base64
                              (default: ``["ItemLedgerEntries"]``)

    Restituisce
    -----------
    dict
        Chiavi: ``tenant_id, client_id, client_secret, environment,
        companies (list[str]), entities (list[str])``

    Eccezioni
    ---------
    RuntimeError
        Se BC_TENANT_ID, BC_CLIENT_ID o BC_CLIENT_SECRET sono mancanti.

    Esempio
    -------
    La lista companies va passata come Base64 di JSON:
    ``base64('["CRONUS IT", "DEMO COMPANY"]')`` → ``BC_COMPANIES_B64``
    """
    tenant_id = get_param("BC_TENANT_ID")
    client_id = get_param("BC_CLIENT_ID")
    # Il secret può arrivare in Base64 (per caratteri speciali) o plain-text
    client_secret = get_param_b64("BC_CLIENT_SECRET_B64") or get_param("BC_CLIENT_SECRET")
    environment = get_param("BC_ENVIRONMENT", "SandboxTest")

    companies_raw = decode_b64_param("BC_COMPANIES_B64", '["CRONUS%20IT"]')
    companies = json.loads(companies_raw)

    entities_raw = decode_b64_param("BC_ENTITIES_B64", '["ItemLedgerEntries"]')
    entities = json.loads(entities_raw)

    validate_required({
        "BC_TENANT_ID": tenant_id,
        "BC_CLIENT_ID": client_id,
        "BC_CLIENT_SECRET": client_secret,
    })

    return {
        "tenant_id": tenant_id,
        "client_id": client_id,
        "client_secret": client_secret,
        "environment": environment,
        "companies": companies,
        "entities": entities,
    }


def get_bc_token(cfg):
    """
    Ottiene un Bearer token OAuth2 per Business Central tramite Client Credentials flow.

    Il token ha una durata di ~60 minuti. In esecuzioni lunghe con molte
    entità, potrebbe scadere: in tal caso questo modulo andrebbe esteso
    con una logica di refresh (o la funzione va richiamata periodicamente).

    Parametri
    ---------
    cfg : dict
        Dizionario di configurazione BC (output di :func:`load_bc_config`).

    Restituisce
    -----------
    str
        Bearer token da usare nell'header ``Authorization: Bearer <token>``.
    """
    credential = ClientSecretCredential(
        tenant_id=cfg["tenant_id"],
        client_id=cfg["client_id"],
        client_secret=cfg["client_secret"],
    )
    token = credential.get_token("https://api.businesscentral.dynamics.com/.default")
    return token.token


def pull_bc_data(
    token,
    cfg,
    company,
    entity,
    last_load_timestamp=None,
    mode=None,
    delete_keys=None,
    key_columns=None,
    keep_data_for_delete=False,
    max_retries=3,
    backoff_factor=1,
):
    """
    Recupera i dati da Business Central tramite OData V4 API con paginazione automatica.

    Modalità di esecuzione
    ----------------------
    1. **Initial load** (``last_load_timestamp=None``):
       Scarica tutti i record dell'entità senza filtri.
       → ``__rowMarker__ = 0`` (Insert)

    2. **Incremental load** (``last_load_timestamp`` valorizzato):
       Applica ``$filter=SystemModifiedAt gt datetime'<timestamp>'``.
       Se BC risponde 400 (entità non filtrabile), ritenta senza filtro.
       → ``__rowMarker__ = 1`` (Update)

    3. **Delete** (``mode='delete'``):
       Non chiama l'API — costruisce un DataFrame dalle chiavi da cancellare.
       → ``__rowMarker__ = 2`` (Delete)

    Paginazione
    -----------
    BC restituisce un massimo di 20.000 record per pagina (configurabile lato BC).
    Il link alla pagina successiva si trova in ``@odata.nextLink`` nella risposta.
    La funzione segue automaticamente tutti i nextLink fino all'esaurimento.

    Parametri
    ---------
    token : str
        Bearer token ottenuto con :func:`get_bc_token`.
    cfg : dict
        Configurazione BC (output di :func:`load_bc_config`).
    company : str
        Nome company BC (es. ``"CRONUS IT"``). Viene URL-encoded nell'endpoint.
    entity : str
        Nome EntitySet OData (es. ``"ItemLedgerEntries"``).
    last_load_timestamp : str | datetime | None
        Se None → initial load. Se valorizzato → incremental load.
    mode : str | None
        ``'delete'`` per modalità cancellazione esplicita. None per auto-detect.
    delete_keys : pd.DataFrame | list | None
        Chiavi da cancellare. Richiesto se ``mode='delete'``.
        Può essere un DataFrame, una lista di dict, o una lista di scalari
        (in quest'ultimo caso ``key_columns`` deve avere una sola colonna).
    key_columns : list[str] | None
        Colonne chiave primaria. Richiesto quando ``delete_keys`` è lista di scalari.
    keep_data_for_delete : bool
        Se True, mantiene tutte le colonne nel DataFrame di delete (non solo le chiavi).
    max_retries : int
        Numero massimo di retry per errori HTTP transitori (default 3).
    backoff_factor : int | float
        Fattore backoff esponenziale tra retry (default 1).

    Restituisce
    -----------
    pd.DataFrame
        DataFrame con i dati e colonna ``__rowMarker__``.
        DataFrame vuoto se non ci sono record.

    Eccezioni
    ---------
    ValueError
        Se i parametri per la modalità delete sono incongruenti.
    requests.HTTPError
        Se la chiamata API fallisce e i retry sono esauriti.
    """
    import pandas as pd

    base_url = (
        f"https://api.businesscentral.dynamics.com/"
        f"v2.0/{cfg['tenant_id']}/{cfg['environment']}/ODataV4/"
        f"Company('{company}')/{entity}"
    )
    headers = {"Authorization": f"Bearer {token}"}
    rows = []
    session = get_session_with_retries(max_retries, backoff_factor)
    is_incremental = last_load_timestamp is not None

    # ── Modalità delete: nessuna chiamata API ─────────────────────────────────
    if mode == "delete":
        if delete_keys is None:
            raise ValueError("Per mode='delete' serve 'delete_keys'.")

        if isinstance(delete_keys, pd.DataFrame):
            df = delete_keys.copy()
        elif all(isinstance(x, dict) for x in delete_keys):
            # Lista di dizionari → DataFrame diretto
            df = pd.DataFrame(delete_keys)
        else:
            # Lista di scalari → richiede una singola key_column
            if not key_columns or len(key_columns) != 1:
                raise ValueError(
                    "Quando 'delete_keys' è una lista di scalari, "
                    "fornire 'key_columns' con una sola colonna."
                )
            df = pd.DataFrame({key_columns[0]: delete_keys})

        df = sanitize_dataframe(df)
        return add_row_marker_column(
            df,
            mode="delete",
            key_columns=key_columns,
            keep_data_for_delete=keep_data_for_delete,
        )

    # ── Chiamata API con paginazione ──────────────────────────────────────────
    try:
        if is_incremental:
            # Formatta il timestamp per il filtro OData
            if isinstance(last_load_timestamp, datetime):
                filter_date = last_load_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                filter_date = str(last_load_timestamp).rstrip()
                if not filter_date.endswith("Z"):
                    filter_date += "Z"
            filter_query = f"?$filter=SystemModifiedAt gt datetime'{filter_date}'"
            url_with_filter = base_url + filter_query
            print(f"  [INCREMENTAL] SystemModifiedAt > {filter_date}")
        else:
            url_with_filter = base_url

        pagination_url = url_with_filter
        retry_without_filter = False  # flag per non ciclare in loop su retry senza filtro

        while pagination_url:
            try:
                response = session.get(pagination_url, headers=headers, timeout=30)

                # HTTP 400 in incremental: l'entità non supporta SystemModifiedAt
                # → retry automatico senza filtro (full load)
                if response.status_code == 400 and is_incremental and not retry_without_filter:
                    print("  [WARN] Filtro SystemModifiedAt non supportato (400), retry senza filtro...")
                    retry_without_filter = True
                    pagination_url = base_url
                    continue

                response.raise_for_status()
                data = response.json()
                rows.extend(data.get("value", []))
                # Segui il nextLink per la paginazione (None se ultima pagina)
                pagination_url = data.get("@odata.nextLink")

            except Exception as e:
                # Gestione alternativa del 400 quando viene sollevato come eccezione
                if "400" in str(e) and is_incremental and not retry_without_filter:
                    print("  [WARN] Filtro fallito (eccezione), retry senza filtro...")
                    retry_without_filter = True
                    pagination_url = base_url
                    continue
                raise

    finally:
        session.close()

    # ── Post-processing ───────────────────────────────────────────────────────
    if not rows:
        print(f"  [WARN] Nessun dato trovato per {company}::{entity}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Rimuove le colonne di metadati OData (es. @odata.etag)
    odata_cols = [col for col in df.columns if col.startswith("@")]
    if odata_cols:
        df = df.drop(columns=odata_cols)

    # Rimuove colonne completamente vuote (ottimizzazione)
    df = df.dropna(axis=1, how="all")

    df = sanitize_dataframe(df)
    df = add_row_marker_column(df, is_incremental=is_incremental)

    print(
        f"  [OK] {len(df):,} righe, {len(df.columns)} colonne "
        f"(__rowMarker__={'Update' if is_incremental else 'Insert'})"
    )
    return df


def create_bc_partner_events(cfg, company, entity):
    """
    Genera il dizionario ``_partnerEvents.json`` specifico per Business Central.

    Il file ``_partnerEvents.json`` è scritto nella root della LandingZone
    (non nella cartella dell'entità) e descrive la sorgente dati a Fabric.

    Parametri
    ---------
    cfg : dict
        Configurazione BC (output di :func:`load_bc_config`).
    company : str
        Nome della company BC.
    entity : str
        Nome dell'entità sincronizzata.

    Restituisce
    -----------
    dict
        Struttura del file ``_partnerEvents.json``.
    """
    return {
        "partnerName": "BusinessCentralMirroring",
        "sourceInfo": {
            "sourceType": "DynamicsBC",
            "sourceVersion": "21.0",
            "additionalInformation": {
                "environment": cfg["environment"],
                "company": company,
                "entity": entity,
                # Timestamp ISO per tracciabilità dell'evento
                "createdAt": datetime.now().isoformat(),
            },
        },
    }
