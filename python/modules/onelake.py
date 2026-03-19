"""
onelake.py
==========
Classe ``OneLakeManager``: gestione centralizzata di tutte le operazioni su OneLake.

Responsabilità
--------------
1. **State management** — Legge e scrive un file JSON su OneLake che tiene traccia
   dell'ultimo timestamp caricato e della sequenza del prossimo file CSV per ogni
   combinazione namespace::entity. Questo permette di distinguere initial load da
   incremental load e di generare nomi file ordinati correttamente.

2. **Keys management** — Persiste le chiavi primarie dell'ultimo run per ogni entità.
   Queste chiavi vengono confrontate con quelle del run corrente per rilevare i record
   cancellati nella sorgente (che altrimenti non verrebbero mai propagati al mirroring).

3. **Upload** — Carica su OneLake i tre file richiesti da Open Mirroring per ogni
   batch di dati: il CSV dei dati, ``_metadata.json`` e ``_partnerEvents.json``.
   L'upload include retry esponenziale e aggiornamento dello stato al completamento.

Struttura su OneLake
--------------------
::

    <workspace_id>/
    └── <lakehouse_id>/
        ├── Files/MirroringState/
        │   └── .mirroring_state.json        ← stato sequenze e timestamp
        └── Files/MirroringKeys/
            └── <namespace>/
                └── <entity>/
                    └── keys.csv             ← chiavi dell'ultimo run

    <workspace_id>/
    └── <mirrored_db_id>/
        └── Files/LandingZone/
            ├── _partnerEvents.json
            └── <entity>/
                ├── _metadata.json
                ├── 00000000000000000001.csv ← sequenza file (20 cifre, zero-padded)
                ├── 00000000000000000002.csv
                └── ...

Formato stato JSON
------------------
.. code-block:: json

    {
      "CRONUS IT::ItemLedgerEntries": {
        "last_load": "2024-01-15T10:30:00",
        "next_sequence": 5
      },
      "CRM::accounts": {
        "last_load": "2024-01-15T09:00:00",
        "next_sequence": 3
      }
    }
"""

import re
import json
import pandas as pd
from datetime import datetime
from time import sleep
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

from .mirroring_metadata import create_openmirroring_metadata, validate_dataframe_for_mirroring


class OneLakeManager:
    """
    Gestore centralizzato per le operazioni su Microsoft Fabric OneLake.

    Tutte le interazioni con OneLake (lettura/scrittura stato, chiavi, upload CSV)
    passano attraverso questa classe. Ogni istanza è configurata per una specifica
    combinazione di workspace/lakehouse/mirrored-db.

    Parametri
    ---------
    tenant_id : str
        Azure AD Tenant ID del Service Principal per OneLake.
    client_id : str
        Application (client) ID del Service Principal.
    client_secret : str
        Client secret del Service Principal.
    workspace_id : str
        ID del workspace Fabric (GUID).
    lakehouse_id : str
        ID del Lakehouse Fabric (GUID). Usato come root per stato e chiavi.
    mirrored_db_id : str
        ID del Mirrored Database Fabric (GUID). Usato come root per la LandingZone.
    state_file : str
        Path del file JSON di stato, relativo al filesystem del workspace.
        Es. ``"<lakehouse_id>/Files/MirroringState/.mirroring_state.json"``.
    target_folder : str
        Sottocartella della LandingZone (default ``"Files/LandingZone"``).
    keys_target_folder : str
        Sottocartella per le chiavi (default ``"Files/MirroringKeys"``).
    max_retries : int
        Numero massimo di tentativi per ogni operazione di upload (default 3).
    retry_backoff_factor : int | float
        Fattore per il backoff esponenziale tra retry (default 1).
        Attesa = ``retry_backoff_factor * (2 ** (tentativo - 1))``.

    Note
    ----
    Il Service Principal deve avere il ruolo **Storage Blob Data Contributor**
    (o equivalente) sul workspace OneLake.
    """

    def __init__(
        self,
        tenant_id,
        client_id,
        client_secret,
        workspace_id,
        lakehouse_id,
        mirrored_db_id,
        state_file,
        target_folder="Files/LandingZone",
        keys_target_folder="Files/MirroringKeys",
        max_retries=3,
        retry_backoff_factor=1,
    ):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.mirrored_db_id = mirrored_db_id
        self.state_file = state_file
        self.target_folder = target_folder
        self.keys_target_folder = keys_target_folder
        self.max_retries = max_retries
        self.retry_backoff_factor = retry_backoff_factor

    # =========================================================================
    # Metodi privati — Azure client
    # =========================================================================

    def _get_client(self):
        """
        Crea e restituisce un ``DataLakeServiceClient`` autenticato con il Service Principal.

        Il client viene ricreato ad ogni chiamata per evitare problemi di token scaduto
        in esecuzioni molto lunghe.
        """
        credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return DataLakeServiceClient(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            credential=credential,
        )

    def _get_fs_client(self):
        """
        Restituisce il ``FileSystemClient`` per il workspace corrente.
        In OneLake, il "filesystem" corrisponde al workspace ID.
        """
        return self._get_client().get_file_system_client(self.workspace_id)

    # =========================================================================
    # State management
    # =========================================================================

    def load_state(self):
        """
        Legge il file di stato JSON da OneLake.

        Il file di stato tiene traccia, per ogni coppia ``namespace::entity``, di:
        - ``last_load``: timestamp dell'ultimo carico completato con successo.
        - ``next_sequence``: numero progressivo per il prossimo file CSV.

        Restituisce
        -----------
        dict
            Stato corrente, oppure ``{}`` se il file non esiste o si verifica un errore
            (la prima esecuzione non avrà mai un file di stato).
        """
        try:
            data = (
                self._get_fs_client()
                .get_file_client(self.state_file)
                .download_file()
                .readall()
            )
            return json.loads(data.decode("utf-8"))
        except Exception:
            # File non esistente (primo run) o errore di lettura: partiamo da zero
            return {}

    def save_state(self, state):
        """
        Serializza e salva il dizionario di stato come JSON su OneLake.

        Parametri
        ---------
        state : dict
            Dizionario di stato da persistere.

        Note
        ----
        Il fallimento del salvataggio viene loggato ma non rilancia l'eccezione
        per non interrompere il flusso principale. Al prossimo run verrà eseguito
        un initial load completo per l'entità interessata.
        """
        try:
            state_bytes = json.dumps(state, indent=2, default=str).encode("utf-8")
            self._get_fs_client().get_file_client(self.state_file).upload_data(
                state_bytes, overwrite=True
            )
        except Exception as e:
            print(f"[WARN] Impossibile salvare stato: {e}")

    def get_last_load_timestamp(self, namespace, entity):
        """
        Restituisce il timestamp dell'ultimo carico completato per ``namespace::entity``.

        Parametri
        ---------
        namespace : str
            Identificatore della sorgente. Per BC è il nome company
            (es. ``"CRONUS IT"``); per CRM è la stringa fissa ``"CRM"``.
        entity : str
            Nome dell'entità (es. ``"ItemLedgerEntries"`` o ``"accounts"``).

        Restituisce
        -----------
        str | None
            Timestamp come stringa ISO (es. ``"2024-01-15T10:30:00"``),
            oppure ``None`` se è il primo run (→ initial load).
        """
        state = self.load_state()
        return state.get(f"{namespace}::{entity}", {}).get("last_load", None)

    def update_load_timestamp(self, namespace, entity, timestamp):
        """
        Aggiorna il timestamp dell'ultimo carico per ``namespace::entity``.

        Parametri
        ---------
        namespace : str
        entity : str
        timestamp : datetime | str
            Timestamp del completamento del carico corrente.
        """
        state = self.load_state()
        key = f"{namespace}::{entity}"
        state.setdefault(key, {})["last_load"] = timestamp
        self.save_state(state)

    def get_next_file_sequence(self, namespace, entity):
        """
        Restituisce il numero progressivo per il prossimo file CSV da caricare.

        I file vengono nominati con 20 cifre zero-padded
        (es. ``00000000000000000001.csv``) per garantire ordinamento corretto
        nel filesystem e in Open Mirroring.

        Parametri
        ---------
        namespace : str
        entity : str

        Restituisce
        -----------
        int
            Numero progressivo (parte da 1 al primo run).
        """
        state = self.load_state()
        return state.get(f"{namespace}::{entity}", {}).get("next_sequence", 1)

    def update_file_sequence(self, namespace, entity, sequence):
        """
        Incrementa e persiste il numero progressivo del file per il prossimo run.

        Parametri
        ---------
        namespace : str
        entity : str
        sequence : int
            Sequenza **appena usata**. Verrà salvato ``sequence + 1``.
        """
        state = self.load_state()
        key = f"{namespace}::{entity}"
        state.setdefault(key, {})["next_sequence"] = sequence + 1
        self.save_state(state)

    # =========================================================================
    # Keys management
    # =========================================================================

    def _keys_file_path(self, namespace, entity):
        """
        Calcola il path del file CSV delle chiavi per ``namespace::entity``.

        I caratteri non sicuri per i path vengono sostituiti con ``_``
        (es. spazi nel nome company, caratteri speciali).

        Parametri
        ---------
        namespace : str
        entity : str

        Restituisce
        -----------
        str
            Path relativo al filesystem del workspace OneLake.
        """
        safe_ns = re.sub(r"[^a-zA-Z0-9_-]", "_", namespace)
        safe_entity = re.sub(r"[^a-zA-Z0-9_-]", "_", entity)
        return f"{self.lakehouse_id}/{self.keys_target_folder}/{safe_ns}/{safe_entity}/keys.csv"

    def load_previous_keys(self, namespace, entity):
        """
        Carica il DataFrame delle chiavi primarie salvate nel run precedente.

        Queste chiavi vengono confrontate con quelle del run corrente per
        rilevare i record cancellati nella sorgente:
        ``chiavi_precedenti - chiavi_correnti = record da cancellare in Fabric``.

        Parametri
        ---------
        namespace : str
        entity : str

        Restituisce
        -----------
        pd.DataFrame | None
            DataFrame con le chiavi (dtype=str per evitare problemi di tipo),
            oppure ``None`` se il file non esiste (primo run → nessun delete da propagare).
        """
        try:
            data = (
                self._get_fs_client()
                .get_file_client(self._keys_file_path(namespace, entity))
                .download_file()
                .readall()
            )
            return pd.read_csv(pd.io.common.BytesIO(data), dtype=str)
        except Exception:
            # File non esistente (primo run): nessun rilevamento delete possibile
            return None

    def save_current_keys(self, df_keys, namespace, entity):
        """
        Persiste il DataFrame delle chiavi correnti su OneLake per il prossimo run.

        Parametri
        ---------
        df_keys : pd.DataFrame
            DataFrame contenente solo le colonne chiave del run corrente.
        namespace : str
        entity : str

        Eccezioni
        ---------
        Exception
            Rilancia l'errore con un messaggio descrittivo (il chiamante decide
            se bloccante o solo warning).
        """
        try:
            keys_bytes = df_keys.to_csv(index=False).encode("utf-8")
            self._get_fs_client().get_file_client(
                self._keys_file_path(namespace, entity)
            ).upload_data(keys_bytes, overwrite=True)
        except Exception as e:
            raise Exception(f"Impossibile salvare chiavi su OneLake: {e}")

    # =========================================================================
    # Upload
    # =========================================================================

    def upload(
        self,
        df,
        namespace,
        entity,
        metadata_override=None,
        partner_events=None,
        smart_id_detection=False,
    ):
        """
        Carica un DataFrame su Open Mirroring OneLake con retry esponenziale.

        Per ogni upload vengono scritti fino a tre file nella LandingZone:

        1. **``<sequence>.csv``** — dati del batch in formato CSV (RFC 4180,
           quoting=ALL per sicurezza, terminatore ``\\r\\n``).
        2. **``_metadata.json``** — schema della tabella e colonne chiave.
           Viene rigenerato ad ogni upload per riflettere eventuali variazioni
           di schema (nuove colonne nella sorgente).
        3. **``_partnerEvents.json``** (opzionale) — informazioni sulla sorgente.
           Se ``partner_events`` è None, il file viene omesso.

        Al completamento, vengono aggiornati atomicamente sequenza e timestamp nel
        file di stato.

        Parametri
        ---------
        df : pd.DataFrame
            DataFrame da caricare. Deve includere ``__rowMarker__``.
        namespace : str
            Identificatore sorgente (company BC o ``"CRM"``).
            Usato per la chiave di stato e per i log.
        entity : str
            Nome entità. Usato come nome cartella nella LandingZone.
        metadata_override : dict | None
            Se valorizzato, sovrascrive il metadata generato automaticamente.
            Utile per passare un metadata pre-calcolato (es. per i delete).
        partner_events : dict | None
            Contenuto del file ``_partnerEvents.json``. Se None, il file non
            viene scritto (Open Mirroring è tollerante all'assenza di questo file).
        smart_id_detection : bool
            Passato a ``create_openmirroring_metadata`` per la rilevazione
            automatica delle chiavi primarie in modalità CRM.

        Restituisce
        -----------
        bool
            ``True`` se l'upload è completato con successo.

        Eccezioni
        ---------
        Exception
            Rilancia l'ultima eccezione dopo aver esaurito tutti i tentativi.

        Note sul retry
        --------------
        Il retry viene gestito manualmente (non dalla sessione HTTP) perché
        l'upload su Azure Data Lake Gen2 è un'operazione atomica e non ci
        sono problemi di idempotenza: ogni file viene sovrascritto (overwrite=True).
        """
        # Validazione pre-upload (solo avvisi, non blocca)
        _, validation_warnings = validate_dataframe_for_mirroring(df)
        for w in validation_warnings:
            print(f"  {w}")

        next_sequence = self.get_next_file_sequence(namespace, entity)
        data_file_name = f"{next_sequence:020d}.csv"  # es. 00000000000000000001.csv

        for attempt in range(1, self.max_retries + 1):
            try:
                print(
                    f"  [UPLOAD] Tentativo {attempt}/{self.max_retries} "
                    f"| {namespace}::{entity} "
                    f"| File: {data_file_name} "
                    f"| Righe: {len(df)}"
                )

                fs = self._get_fs_client()
                # Path della cartella della tabella nella LandingZone
                table_folder = f"{self.mirrored_db_id}/{self.target_folder}/{entity}"

                # ── 1. Carica CSV ──────────────────────────────────────────────
                # quoting=1 (csv.QUOTE_ALL) racchiude ogni campo in virgolette
                # per evitare ambiguità con virgole nei valori
                csv_bytes = df.to_csv(
                    index=False,
                    encoding="utf-8",
                    quoting=1,
                    lineterminator="\r\n",
                ).encode("utf-8")
                fs.get_file_client(f"{table_folder}/{data_file_name}").upload_data(
                    csv_bytes, overwrite=True
                )

                # ── 2. Carica _metadata.json ───────────────────────────────────
                metadata = metadata_override or create_openmirroring_metadata(
                    df, smart_id_detection=smart_id_detection
                )
                metadata_bytes = json.dumps(metadata, indent=2).encode("utf-8")
                fs.get_file_client(f"{table_folder}/_metadata.json").upload_data(
                    metadata_bytes, overwrite=True
                )

                # ── 3. Carica _partnerEvents.json (opzionale) ─────────────────
                if partner_events:
                    pe_bytes = json.dumps(partner_events, indent=2).encode("utf-8")
                    fs.get_file_client(
                        f"{self.mirrored_db_id}/{self.target_folder}/_partnerEvents.json"
                    ).upload_data(pe_bytes, overwrite=True)

                # ── 4. Aggiorna stato (sequenza + timestamp) ──────────────────
                self.update_file_sequence(namespace, entity, next_sequence)
                self.update_load_timestamp(namespace, entity, datetime.now())

                print(
                    f"  [OK] Upload completato: {data_file_name} "
                    f"({len(csv_bytes):,} bytes, {len(df):,} righe)"
                )
                return True

            except Exception as e:
                print(f"  [ERROR] Tentativo {attempt}: {str(e)}")
                if attempt < self.max_retries:
                    # Backoff esponenziale: 1s, 2s, 4s, ...
                    wait_time = self.retry_backoff_factor * (2 ** (attempt - 1))
                    print(f"  [RETRY] Attesa {wait_time}s prima del prossimo tentativo...")
                    sleep(wait_time)
                else:
                    print(f"  [FAIL] Upload fallito dopo {self.max_retries} tentativi")
                    raise

        return False
