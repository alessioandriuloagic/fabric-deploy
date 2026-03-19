"""
crm_connector.py
================
Configurazione, autenticazione e lettura dati da Microsoft Dataverse (CRM) Web API.

Questo modulo gestisce tutto ciò che è specifico di Dataverse/CRM:
- Lettura e validazione parametri CRM_*
- Autenticazione OAuth2 (Client Credentials) verso Azure AD con scope Dataverse
- Chiamate alla Web API OData V4 con paginazione, throttle e filtro incrementale
- Generazione del file ``_partnerEvents.json`` specifico per CRM

Autenticazione
--------------
Dataverse usa OAuth2 Client Credentials con scope dinamico basato sull'org URL:
``https://<org>.crm4.dynamics.com/.default``

Il tenant Azure AD deve avere un'App Registration con:
- API: Dynamics CRM
- Permission: user_impersonation (per client credentials, configurabile come application permission)
- L'utente applicazione deve essere creato nell'environment Dataverse

URL Web API
-----------
::

    https://<org>.crm4.dynamics.com/api/data/<version>/<entity>

Filtro incrementale
-------------------
Se è presente un timestamp dell'ultimo carico, viene aggiunto il filtro:
``?$filter=modifiedon gt <timestamp>``

A differenza di BC, Dataverse usa il campo ``modifiedon`` (senza ``datetime''`` wrapper).

Throttle handling
-----------------
Dataverse implementa throttling aggressivo (HTTP 429).
La funzione legge l'header ``Retry-After`` e attende prima di riprovare.

Funzioni esportate
------------------
- load_crm_config()              → dict configurazione CRM
- get_crm_token(cfg)             → Bearer token string
- pull_crm_data(...)             → pd.DataFrame con __rowMarker__
- create_crm_partner_events(...) → dict per _partnerEvents.json
"""

import json
from datetime import datetime
from time import sleep
from azure.identity import ClientSecretCredential

from .config import get_param, get_param_b64, decode_b64_param, validate_required
from .dataframe_utils import get_session_with_retries, sanitize_dataframe, add_row_marker_column

# Page size massimo per Dataverse Web API
# Dataverse supporta al massimo 5.000 record per pagina
CRM_PAGE_SIZE = 5000


def load_crm_config():
    """
    Legge e valida tutti i parametri CRM/Dataverse da sys.argv / variabili d'ambiente.

    Parametri obbligatori
    ---------------------
    - ``CRM_TENANT_ID``       — Azure AD Tenant ID
    - ``CRM_CLIENT_ID``       — Application (client) ID del Service Principal
    - ``CRM_CLIENT_SECRET``   — Client secret (plain-text)
      oppure ``CRM_CLIENT_SECRET_B64`` — Client secret codificato Base64
    - ``CRM_ORG_URL``         — URL dell'organizzazione Dataverse
                               (es. ``https://myorg.crm4.dynamics.com``)

    Parametri opzionali
    -------------------
    - ``CRM_API_VERSION``  — Versione Web API (default: ``"v9.2"``)
    - ``CRM_ENTITIES_B64`` — Lista JSON di EntitySet codificata Base64
                             (default: ``["accounts"]``)
                             I nomi EntitySet sono plurali lowercase:
                             accounts, contacts, opportunities, leads, ecc.

    Restituisce
    -----------
    dict
        Chiavi: ``tenant_id, client_id, client_secret, org_url,
        api_version, entities (list[str])``

    Eccezioni
    ---------
    RuntimeError
        Se uno dei parametri obbligatori è mancante.
    """
    tenant_id = get_param("CRM_TENANT_ID")
    client_id = get_param("CRM_CLIENT_ID")
    # Il secret può arrivare in Base64 (per caratteri speciali) o plain-text
    client_secret = get_param_b64("CRM_CLIENT_SECRET_B64") or get_param("CRM_CLIENT_SECRET")
    # Rimuove trailing slash per evitare URL malformati
    org_url = get_param("CRM_ORG_URL", "").rstrip("/")
    api_version = get_param("CRM_API_VERSION", "v9.2")

    entities_raw = decode_b64_param("CRM_ENTITIES_B64", '["accounts"]')
    entities = json.loads(entities_raw)

    validate_required({
        "CRM_TENANT_ID": tenant_id,
        "CRM_CLIENT_ID": client_id,
        "CRM_CLIENT_SECRET": client_secret,
        "CRM_ORG_URL": org_url,
    })

    return {
        "tenant_id": tenant_id,
        "client_id": client_id,
        "client_secret": client_secret,
        "org_url": org_url,
        "api_version": api_version,
        "entities": entities,
    }


def get_crm_token(cfg):
    """
    Ottiene un Bearer token OAuth2 per Dataverse tramite Client Credentials flow.

    Lo scope è costruito dinamicamente dall'URL dell'organizzazione:
    ``<org_url>/.default``

    Se l'URL non include il protocollo, viene aggiunto automaticamente ``https://``.

    Parametri
    ---------
    cfg : dict
        Configurazione CRM (output di :func:`load_crm_config`).

    Restituisce
    -----------
    str
        Bearer token.

    Note
    ----
    Il token scade dopo ~60 minuti. Per sincronizzazioni molto lunghe con
    molte entità, valutare il refresh del token a intervalli regolari.
    """
    credential = ClientSecretCredential(
        tenant_id=cfg["tenant_id"],
        client_id=cfg["client_id"],
        client_secret=cfg["client_secret"],
    )
    org_url = cfg["org_url"].strip()
    # Garantisce che l'URL abbia il protocollo (richiesto per lo scope OAuth2)
    if not org_url.startswith("https://") and not org_url.startswith("http://"):
        org_url = f"https://{org_url}"
    scope = f"{org_url}/.default"
    print(f"[AUTH] Scope usato: {scope}")
    token = credential.get_token(scope)
    return token.token


def pull_crm_data(
    token,
    cfg,
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
    Recupera i dati da Dataverse Web API con paginazione, throttle e retry automatici.

    Modalità di esecuzione
    ----------------------
    1. **Initial load** (``last_load_timestamp=None``):
       Scarica tutti i record dell'entità.
       → ``__rowMarker__ = 0`` (Insert)

    2. **Incremental load** (``last_load_timestamp`` valorizzato):
       Applica ``$filter=modifiedon gt <timestamp>``.
       Se risponde 400, ritenta senza filtro (full load).
       → ``__rowMarker__ = 1`` (Update)

    3. **Delete** (``mode='delete'``):
       Non chiama l'API — costruisce il DataFrame dalle chiavi da cancellare.
       → ``__rowMarker__ = 2`` (Delete)

    Paginazione
    -----------
    Dataverse usa un page size massimo di 5.000 record (``CRM_PAGE_SIZE``),
    configurabile con l'header ``Prefer: odata.maxpagesize=<n>``.
    Il link alla pagina successiva si trova in ``@odata.nextLink``.

    Throttle
    --------
    HTTP 429 viene gestito leggendo l'header ``Retry-After`` e attendendo
    il numero di secondi indicato prima di riprovare (senza consumare un retry).

    Pulizia response
    ----------------
    Dataverse arricchisce la response con colonne OData di navigazione
    (es. ``_ownerid_value@OData.Community.Display.V1.FormattedValue``).
    Queste vengono rimosse perché non utili al mirroring e potrebbero causare
    problemi di naming dopo la sanitizzazione.

    Parametri
    ---------
    token : str
        Bearer token ottenuto con :func:`get_crm_token`.
    cfg : dict
        Configurazione CRM (output di :func:`load_crm_config`).
    entity : str
        Nome EntitySet Dataverse in formato plural lowercase
        (es. ``"accounts"``, ``"contacts"``, ``"opportunities"``).
    last_load_timestamp : str | datetime | None
        Timestamp dell'ultimo carico. None = initial load.
    mode : str | None
        ``'delete'`` per modalità cancellazione. None = auto-detect.
    delete_keys : pd.DataFrame | list | None
        Chiavi da cancellare. Richiesto se ``mode='delete'``.
    key_columns : list[str] | None
        Colonne chiave primaria. Richiesto quando ``delete_keys`` è lista di scalari.
    keep_data_for_delete : bool
        Se True, mantiene tutte le colonne nel DataFrame di delete.
    max_retries : int
        Retry HTTP massimi (default 3).
    backoff_factor : int | float
        Fattore backoff esponenziale (default 1).

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

    base_url = f"{cfg['org_url']}/api/data/{cfg['api_version']}/{entity}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        # Header OData richiesti da Dataverse Web API
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        # Imposta il page size massimo
        "Prefer": f"odata.maxpagesize={CRM_PAGE_SIZE}",
    }
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
            df = pd.DataFrame(delete_keys)
        else:
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
            # Formatta timestamp per filtro OData Dataverse
            # Nota: Dataverse usa 'modifiedon', non 'SystemModifiedAt' come BC
            if isinstance(last_load_timestamp, datetime):
                filter_date = last_load_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                filter_date = str(last_load_timestamp).rstrip()
                if not filter_date.endswith("Z"):
                    filter_date += "Z"
            filter_query = f"?$filter=modifiedon gt {filter_date}"
            url_with_filter = base_url + filter_query
            print(f"  [INCREMENTAL] modifiedon > {filter_date}")
        else:
            url_with_filter = base_url

        pagination_url = url_with_filter
        retry_without_filter = False  # evita loop infinito sul retry senza filtro
        page_count = 0

        while pagination_url:
            try:
                response = session.get(pagination_url, headers=headers, timeout=60)

                # HTTP 400: filtro non supportato → retry senza filtro
                if response.status_code == 400 and is_incremental and not retry_without_filter:
                    print("  [WARN] Filtro modifiedon non supportato (400), retry senza filtro...")
                    retry_without_filter = True
                    pagination_url = url_with_filter = base_url
                    continue

                # HTTP 429: throttle da Dataverse → attendi Retry-After
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 30))
                    print(f"  [THROTTLE] Rate limited da Dataverse. Attesa {retry_after}s...")
                    sleep(retry_after)
                    continue  # riprova la stessa URL senza consumare un retry

                response.raise_for_status()
                data = response.json()
                batch = data.get("value", [])
                rows.extend(batch)
                page_count += 1

                # Progress log ogni 10 pagine (utile per entità grandi)
                if page_count % 10 == 0:
                    print(f"  [PAGING] Pagina {page_count}, {len(rows):,} righe totali scaricate...")

                # Segui nextLink per la pagina successiva
                pagination_url = data.get("@odata.nextLink")

            except Exception as e:
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
        print(f"  [WARN] Nessun dato trovato per CRM::{entity}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Rimuove colonne OData standard (es. @odata.etag, @odata.context)
    odata_cols = [col for col in df.columns if col.startswith("@")]
    if odata_cols:
        df = df.drop(columns=odata_cols)

    # Rimuove annotation di navigazione Dataverse
    # (es. "_ownerid_value@OData.Community.Display.V1.FormattedValue")
    nav_cols = [col for col in df.columns if "@" in col]
    if nav_cols:
        df = df.drop(columns=nav_cols)

    # Rimuove colonne completamente vuote
    df = df.dropna(axis=1, how="all")

    df = sanitize_dataframe(df)
    df = add_row_marker_column(df, is_incremental=is_incremental)

    print(
        f"  [OK] {len(df):,} righe, {len(df.columns)} colonne "
        f"(__rowMarker__={'Update' if is_incremental else 'Insert'})"
    )
    return df


def create_crm_partner_events(cfg, entity):
    """
    Genera il dizionario ``_partnerEvents.json`` specifico per Dataverse/CRM.

    Il file viene scritto nella root della LandingZone e descrive la sorgente
    dati a Microsoft Fabric Open Mirroring.

    Parametri
    ---------
    cfg : dict
        Configurazione CRM (output di :func:`load_crm_config`).
    entity : str
        Nome EntitySet dell'entità sincronizzata.

    Restituisce
    -----------
    dict
        Struttura del file ``_partnerEvents.json``.
    """
    return {
        "partnerName": "DataverseMirroring",
        "sourceInfo": {
            "sourceType": "Dataverse",
            "sourceVersion": cfg["api_version"],
            "additionalInformation": {
                "orgUrl": cfg["org_url"],
                "entity": entity,
                # Timestamp ISO per tracciabilità
                "createdAt": datetime.now().isoformat(),
            },
        },
    }
