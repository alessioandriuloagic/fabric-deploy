"""
dataframe_utils.py
==================
Utility condivise per la manipolazione di DataFrame e per le sessioni HTTP.

Tutte le funzioni di questo modulo sono source-agnostic: vengono usate
sia da bc_connector che da crm_connector senza alcuna modifica.

Funzioni esportate
------------------
- get_session_with_retries(max_retries, backoff_factor)
    Crea una requests.Session con retry automatici su errori transitori.
- sanitize_column_name(col_name)
    Normalizza un singolo nome di colonna per compatibilità con Parquet/Delta.
- sanitize_dataframe(df)
    Applica sanitize_column_name a tutte le colonne e normalizza i valori.
- add_row_marker_column(df, mode, is_incremental, key_columns, keep_data_for_delete)
    Aggiunge la colonna ``__rowMarker__`` richiesta da Open Mirroring Fabric.
"""

import re
import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------------------------------------------------------------------------
# HTTP Session
# ---------------------------------------------------------------------------

def get_session_with_retries(max_retries=3, backoff_factor=1):
    """
    Crea una ``requests.Session`` con retry automatici configurati.

    La sessione ritenta automaticamente su questi status HTTP:
    - 429  Too Many Requests (throttle)
    - 500  Internal Server Error
    - 502  Bad Gateway
    - 503  Service Unavailable
    - 504  Gateway Timeout

    Il backoff esponenziale applica un'attesa crescente tra i tentativi:
    ``attesa = backoff_factor * (2 ** (retry_number - 1))``

    Parametri
    ---------
    max_retries : int
        Numero massimo di tentativi (default 3).
    backoff_factor : int | float
        Fattore moltiplicatore per il backoff esponenziale (default 1).

    Restituisce
    -----------
    requests.Session
        Sessione configurata con retry su http:// e https://.

    Esempio
    -------
    >>> session = get_session_with_retries(max_retries=5, backoff_factor=2)
    >>> response = session.get("https://api.example.com/data", timeout=30)
    """
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ---------------------------------------------------------------------------
# Sanitizzazione colonne e DataFrame
# ---------------------------------------------------------------------------

def sanitize_column_name(col_name):
    """
    Normalizza un nome di colonna per renderlo compatibile con Parquet, Delta e Open Mirroring.

    Operazioni applicate:
    1. Rimuove tutti i caratteri non alfanumerici (eccetto underscore e spazi).
    2. Sostituisce sequenze di spazi con ``_``.
    3. Tronca a 100 caratteri.
    4. Se il risultato è vuoto, restituisce ``"Column"`` come fallback.

    Parametri
    ---------
    col_name : str
        Nome di colonna originale (può contenere spazi, @, ., /, ecc.).

    Restituisce
    -----------
    str
        Nome di colonna normalizzato.

    Esempi
    ------
    >>> sanitize_column_name("@odata.etag")
    'odataetag'
    >>> sanitize_column_name("First Name")
    'First_Name'
    >>> sanitize_column_name("   ")
    'Column'
    """
    sanitized = re.sub(r"[^a-zA-Z0-9_\s]", "", str(col_name))
    sanitized = re.sub(r"\s+", "_", sanitized.strip())
    return sanitized[:100] if sanitized else "Column"


def sanitize_dataframe(df):
    """
    Applica una pulizia completa al DataFrame prima dell'upload su OneLake.

    Operazioni eseguite:
    1. **Nomi colonne** — normalizzati con :func:`sanitize_column_name`.
    2. **Colonne duplicate** — la prima occorrenza viene mantenuta, le altre rimosse.
    3. **Colonne object (string)**:
       - Converte tutto in str, poi sostituisce 'nan' e 'None' con stringa vuota.
       - Rimuove caratteri di controllo (``\\x00``–``\\x1f``).
       - Normalizza le virgolette doppie consecutive (``""`` → ``"``).
    4. **Colonne float** — sostituisce ``±inf`` con NaN, poi NaN con stringa vuota.
    5. **Colonne bool** — converte in 'true'/'false' lowercase.
    6. **Altre colonne** — riempie NaN con stringa vuota.

    Parametri
    ---------
    df : pd.DataFrame
        DataFrame da sanitizzare (viene modificato in place per le colonne,
        ma il DataFrame originale non viene alterato grazie alle operazioni pandas).

    Restituisce
    -----------
    pd.DataFrame
        DataFrame sanitizzato.

    Note
    ----
    Questa funzione va applicata **prima** di :func:`add_row_marker_column`
    per garantire che ``__rowMarker__`` non venga sovrascritto dal loop.
    """
    # 1. Normalizza nomi colonne
    df.columns = [sanitize_column_name(col) for col in df.columns]

    # 2. Rimuove colonne duplicate (mantieni la prima)
    df = df.loc[:, ~df.columns.duplicated()]

    for col in df.columns:
        if df[col].dtype == "object":
            # Converte in stringa e pulisce valori speciali
            df[col] = df[col].astype(str).replace("nan", "").replace("None", "")
            df[col] = df[col].fillna("")
            # Rimuove caratteri di controllo ASCII (tabs, newline non gestiti, ecc.)
            df[col] = df[col].str.replace(r"[\x00-\x1f]", "", regex=True)
            # Normalizza virgolette doppie consecutive prodotte da pandas csv
            df[col] = df[col].str.replace('""', '"', regex=False)
        elif df[col].dtype in ["float64", "float32"]:
            # Inf non è serializzabile in CSV/Parquet
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            df[col] = df[col].fillna("")
        elif df[col].dtype == "bool":
            # Open Mirroring si aspetta stringhe lowercase per i booleani
            df[col] = df[col].astype(str).str.lower()
        else:
            df[col] = df[col].fillna("")

    return df


# ---------------------------------------------------------------------------
# Open Mirroring row marker
# ---------------------------------------------------------------------------

def add_row_marker_column(
    df,
    mode=None,
    is_incremental=None,
    key_columns=None,
    keep_data_for_delete=False,
):
    """
    Aggiunge la colonna ``__rowMarker__`` al DataFrame per Open Mirroring Fabric.

    Open Mirroring richiede questa colonna per capire come trattare ogni riga:

    +---------+---+--------------------------------------------------------------------+
    | Stringa | N | Comportamento                                                      |
    +=========+===+====================================================================+
    | insert  | 0 | Inserisce la riga (initial load o nuovi record)                    |
    +---------+---+--------------------------------------------------------------------+
    | update  | 1 | Aggiorna la riga (incremental load)                                |
    +---------+---+--------------------------------------------------------------------+
    | delete  | 2 | Cancella la riga (rilevamento delete tra run successivi)           |
    +---------+---+--------------------------------------------------------------------+
    | upsert  | 4 | Insert se non esiste, update se esiste                             |
    +---------+---+--------------------------------------------------------------------+

    Comportamento speciale per ``mode='delete'``:
    - Se ``keep_data_for_delete=False`` (default), il DataFrame restituito contiene
      **solo** le ``key_columns`` + ``__rowMarker__``. Open Mirroring richiede solo
      le chiavi per processare le cancellazioni.
    - Se ``keep_data_for_delete=True``, tutte le colonne vengono mantenute.

    Parametri
    ---------
    df : pd.DataFrame
        DataFrame di input. Non viene modificato in place.
    mode : str | int | None
        Modalità esplicita. Se None, viene derivata da ``is_incremental``:
        True → Update (1), False → Insert (0).
    is_incremental : bool | None
        Usato solo quando ``mode`` è None.
    key_columns : list[str] | None
        Colonne chiave primaria. Obbligatorio per ``mode='delete'``.
    keep_data_for_delete : bool
        Se True, mantiene tutte le colonne anche in modalità delete.

    Restituisce
    -----------
    pd.DataFrame
        Nuovo DataFrame con ``__rowMarker__`` come ultima colonna, dtype int.

    Eccezioni
    ---------
    ValueError
        Se ``mode`` non è uno dei valori validi.
    KeyError
        Se alcune ``key_columns`` non esistono nel DataFrame (in delete mode).

    Esempi
    ------
    >>> # Initial load → Insert (0)
    >>> df_out = add_row_marker_column(df, is_incremental=False)

    >>> # Incremental load → Update (1)
    >>> df_out = add_row_marker_column(df, is_incremental=True)

    >>> # Delete esplicito con solo chiavi nel risultato
    >>> df_out = add_row_marker_column(df, mode='delete', key_columns=['id'])
    """
    mapper = {"insert": 0, "update": 1, "delete": 2, "upsert": 4}

    # Determina il marker numerico
    if mode is None:
        # Derivo automaticamente da is_incremental
        marker = 1 if is_incremental else 0
    elif isinstance(mode, int):
        marker = int(mode)
        if marker not in (0, 1, 2, 4):
            raise ValueError(f"mode numerico non valido: {mode}. Usare 0,1,2,4")
    else:
        mode_l = str(mode).lower()
        if mode_l not in mapper:
            raise ValueError(
                f"mode non valido: '{mode}'. Usare insert/update/delete/upsert o 0/1/2/4."
            )
        marker = mapper[mode_l]

    # Modalità delete con riduzione alle sole colonne chiave
    if marker == 2 and key_columns and not keep_data_for_delete:
        missing = [c for c in key_columns if c not in df.columns]
        if missing:
            raise KeyError(f"Colonne chiave mancanti nel DataFrame: {missing}")
        out = df.loc[:, key_columns].copy()
        out["__rowMarker__"] = int(marker)
        cols = [c for c in out.columns if c != "__rowMarker__"]
        out = out[cols + ["__rowMarker__"]]
        out["__rowMarker__"] = out["__rowMarker__"].astype(int)
        return out

    # Tutti gli altri casi: copia completa + aggiunta colonna
    out = df.copy()
    out["__rowMarker__"] = int(marker)
    # Sposta __rowMarker__ in ultima posizione (convenzione Open Mirroring)
    cols = [c for c in out.columns if c != "__rowMarker__"]
    out = out[cols + ["__rowMarker__"]]
    out["__rowMarker__"] = out["__rowMarker__"].astype(int)
    return out
