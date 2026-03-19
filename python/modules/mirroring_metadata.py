"""
mirroring_metadata.py
=====================
Generazione dei file di metadata richiesti da Microsoft Fabric Open Mirroring.

Open Mirroring si aspetta, per ogni tabella nella LandingZone, due file:
  - ``_metadata.json``     — schema della tabella e colonne chiave
  - ``_partnerEvents.json``— informazioni sulla sorgente dati

Questo modulo si occupa di ``_metadata.json`` e della validazione del DataFrame
prima dell'upload. La generazione di ``_partnerEvents.json`` è delegata ai
moduli specifici della sorgente (bc_connector, crm_connector) perché contiene
informazioni dipendenti dalla sorgente.

Funzioni esportate
------------------
- create_openmirroring_metadata(df, key_columns, smart_id_detection)
    Genera il dict ``_metadata.json`` inferendo lo schema dal DataFrame.
- validate_dataframe_for_mirroring(df)
    Valida il DataFrame e restituisce avvisi prima dell'upload.

Formato _metadata.json
-----------------------
.. code-block:: json

    {
      "keyColumns": ["id"],
      "fileDetectionStrategy": "LastUpdateTimeFileDetection",
      "fileFormat": "csv",
      "SchemaDefinition": {
        "Columns": [
          {"Name": "id",   "DataType": "String"},
          {"Name": "name", "DataType": "String", "IsNullable": true},
          ...
        ]
      }
    }

Mapping tipi pandas → Open Mirroring
--------------------------------------
+------------------+------------------+
| dtype pandas     | DataType Fabric  |
+==================+==================+
| int64            | Int64            |
+------------------+------------------+
| int32/int16/int8 | Int32            |
+------------------+------------------+
| float64/float32  | Double           |
+------------------+------------------+
| bool             | Boolean          |
+------------------+------------------+
| datetime*        | DateTime         |
+------------------+------------------+
| object (string)  | String           |
+------------------+------------------+
"""


def create_openmirroring_metadata(df, key_columns=None, smart_id_detection=False):
    """
    Genera il dizionario ``_metadata.json`` per Open Mirroring a partire dal DataFrame.

    Rilevamento automatico della chiave primaria
    --------------------------------------------
    Se ``key_columns`` è None, la funzione cerca automaticamente le chiavi:

    - **Modalità standard** (``smart_id_detection=False``, usata da BC):
      Cerca una colonna il cui nome (lowercase) sia esattamente ``'id'``.
      Se non trovata, usa la prima colonna del DataFrame.

    - **Modalità smart** (``smart_id_detection=True``, usata da CRM/Dataverse):
      Preferisce colonne il cui nome termina con ``'id'`` e non contiene ``'odata'``
      (es. ``accountid``, ``contactid``). Se non ne trova, ricade sulla logica
      standard. Questo riflette la convenzione di naming di Dataverse.

    Parametri
    ---------
    df : pd.DataFrame
        DataFrame di riferimento per inferire i tipi. Non viene modificato.
    key_columns : list[str] | None
        Colonne chiave esplicite. Se None, vengono rilevate automaticamente.
    smart_id_detection : bool
        Se True, usa la logica di rilevamento avanzata (per Dataverse/CRM).

    Restituisce
    -----------
    dict
        Dizionario pronto per essere serializzato in ``_metadata.json``.

    Nota
    ----
    Le colonne chiave non hanno il flag ``"IsNullable"`` nel metadata
    (per convenzione Open Mirroring le chiavi non possono essere NULL).
    """
    if key_columns is None:
        if smart_id_detection:
            # Logica CRM: preferisce colonne che terminano con 'id' (es. accountid)
            # ed esclude colonne OData come @odata.etag
            id_cols = [
                c for c in df.columns
                if c.lower().endswith("id") and "odata" not in c.lower()
            ]
            if id_cols:
                key_columns = [id_cols[0]]
            elif "id" in [col.lower() for col in df.columns]:
                key_columns = ["id"]
            else:
                key_columns = [df.columns[0]]
        else:
            # Logica BC: cerca semplicemente 'id'
            if "id" in [col.lower() for col in df.columns]:
                key_columns = ["id"]
            else:
                key_columns = [df.columns[0]]

    # Costruisce la lista di colonne per SchemaDefinition
    schema_columns = []
    for col in df.columns:
        dtype = str(df[col].dtype)

        # Mappa dtype pandas → tipo Open Mirroring
        if "int" in dtype:
            mirroring_type = "Int64" if "int64" in dtype else "Int32"
        elif "float" in dtype:
            mirroring_type = "Double"
        elif "bool" in dtype:
            mirroring_type = "Boolean"
        elif "datetime" in dtype:
            mirroring_type = "DateTime"
        else:
            # object, string, category, ecc. → String
            mirroring_type = "String"

        col_def = {"Name": col, "DataType": mirroring_type}

        # Le colonne non-chiave sono nullable per definizione
        if col not in key_columns:
            col_def["IsNullable"] = True

        schema_columns.append(col_def)

    return {
        "keyColumns": key_columns,
        # Fabric rileva i nuovi file CSV tramite timestamp di modifica
        "fileDetectionStrategy": "LastUpdateTimeFileDetection",
        "SchemaDefinition": {"Columns": schema_columns},
        "fileFormat": "csv",
    }


def validate_dataframe_for_mirroring(df):
    """
    Valida il DataFrame prima dell'upload su Open Mirroring.

    Controlla:
    1. DataFrame non vuoto (almeno una colonna).
    2. Presenza della colonna ``__rowMarker__``.
    3. Valori di ``__rowMarker__`` appartenenti al set valido {0, 1, 2, 4}.
    4. Colonne con valori NULL (solo avviso, non bloccante).
    5. Colonne stringa con valori molto lunghi > 10.000 caratteri (solo avviso).

    La funzione non blocca mai l'esecuzione: restituisce sempre ``True``
    come primo elemento della tupla (la validazione è advisory, non bloccante),
    e una lista di avvisi stringa.

    Parametri
    ---------
    df : pd.DataFrame
        DataFrame da validare.

    Restituisce
    -----------
    tuple[bool, list[str]]
        ``(True, [avvisi])`` — il bool è sempre True per non bloccare il flusso.
        ``(False, ["DataFrame vuoto"])`` — unico caso bloccante.

    Esempio
    -------
    >>> is_valid, warnings = validate_dataframe_for_mirroring(df)
    >>> for w in warnings:
    ...     print(w)
    """
    warnings_list = []

    # Caso bloccante: nessuna colonna
    if len(df.columns) == 0:
        return False, ["DataFrame vuoto"]

    # Verifica presenza __rowMarker__
    if "__rowMarker__" not in df.columns:
        warnings_list.append("[WARN] __rowMarker__ mancante — aggiungere prima di fare upload")
    else:
        # Verifica valori validi di __rowMarker__
        valid_markers = {0, 1, 2, 4}
        invalid = df[~df["__rowMarker__"].isin(valid_markers)]
        if not invalid.empty:
            warnings_list.append(
                f"[WARN] {len(invalid)} righe con __rowMarker__ invalido "
                f"(valori trovati: {df['__rowMarker__'].unique().tolist()})"
            )

    # Verifica colonne con valori NULL
    null_counts = df.isnull().sum()
    null_cols = null_counts[null_counts > 0]
    if not null_cols.empty:
        warnings_list.append(f"[WARN] Colonne con NULL: {list(null_cols.index)}")

    # Verifica valori stringa molto lunghi (possibile problema di performance o parsing)
    for col in df.columns:
        if df[col].dtype == "object":
            max_len = df[col].astype(str).str.len().max()
            if max_len > 10_000:
                warnings_list.append(
                    f"[WARN] Colonna '{col}' contiene valori molto lunghi "
                    f"(max {max_len} caratteri) — potrebbe causare problemi di parsing"
                )

    return True, warnings_list
