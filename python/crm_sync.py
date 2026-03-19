"""
crm_sync.py
===========
Entry point per la sincronizzazione CRM/Dataverse → Microsoft Fabric OneLake.

Questo script può essere:
1. **Eseguito direttamente** come Spark Job Definition in Fabric.
2. **Importato** da ``sync_all.py`` tramite la funzione ``run()``.

Flusso di esecuzione
--------------------
Per ogni entità Dataverse:

1. Legge il timestamp dell'ultimo carico dallo stato OneLake.
2. Scarica i dati da Dataverse (initial o incremental in base al timestamp).
3. Confronta le chiavi correnti con quelle del run precedente per rilevare delete.
4. Se ci sono delete, le carica su OneLake (marker=2).
5. Carica i dati principali su OneLake (marker=0 insert o 1 update).
6. Persiste le chiavi correnti per il rilevamento delete del prossimo run.

Nota sulla chiave primaria in Dataverse
---------------------------------------
Dataverse usa ``<entityname>id`` come chiave primaria (es. ``accountid``).
La funzione ``create_openmirroring_metadata`` con ``smart_id_detection=True``
rileva automaticamente questa colonna senza configurazione manuale.

Il namespace usato per lo stato OneLake è la stringa fissa ``"CRM"``
(indipendentemente dall'entità), per distinguerlo dal namespace BC (company name).

Parametri Spark Job
-------------------
Obbligatori:
  --CRM_TENANT_ID          Azure AD Tenant ID
  --CRM_CLIENT_ID          Application ID Service Principal CRM
  --CRM_CLIENT_SECRET_B64  Client secret CRM in Base64 (oppure --CRM_CLIENT_SECRET)
  --CRM_ORG_URL            URL organizzazione Dataverse (es. https://org.crm4.dynamics.com)
  --FABRIC_WORKSPACE_ID    ID workspace Fabric
  --FABRIC_LAKEHOUSE_ID    ID Lakehouse Fabric
  --FABRIC_MIRRORED_DB_ID  ID Mirrored Database Fabric

Opzionali:
  --CRM_API_VERSION        Versione Web API (default: v9.2)
  --CRM_ENTITIES_B64       Lista JSON EntitySet in Base64 (default: ["accounts"])
  --FABRIC_TENANT_ID       Tenant ID SP OneLake (default: CRM_TENANT_ID)
  --FABRIC_CLIENT_ID       Client ID SP OneLake (default: CRM_CLIENT_ID)
  --FABRIC_CLIENT_SECRET_B64  Secret SP OneLake in Base64 (default: CRM_CLIENT_SECRET)
"""

import sys
import traceback

from modules.crm_connector import load_crm_config, get_crm_token, pull_crm_data, create_crm_partner_events
from modules.onelake_config import load_onelake_config, build_onelake_manager
from modules.mirroring_metadata import create_openmirroring_metadata
from modules.dataframe_utils import add_row_marker_column

# Namespace fisso usato per le chiavi di stato OneLake (es. "CRM::accounts")
CRM_NAMESPACE = "CRM"


def run():
    """
    Esegue la sincronizzazione completa CRM/Dataverse → OneLake.

    Restituisce
    -----------
    tuple[int, list[str]]
        ``(total_uploaded, failed_entities)``
        dove ``failed_entities`` è la lista dei nomi EntitySet che hanno fallito.

    Eccezioni
    ---------
    RuntimeError
        Se i parametri obbligatori mancano (sollevata da load_crm_config o load_onelake_config).
    """
    # ── Carica configurazione ─────────────────────────────────────────────────
    crm_cfg = load_crm_config()

    # OneLake: usa le credenziali CRM come fallback se non sono specificate quelle Fabric
    onelake_cfg = load_onelake_config(
        fallback_tenant_id=crm_cfg["tenant_id"],
        fallback_client_id=crm_cfg["client_id"],
        fallback_client_secret=crm_cfg["client_secret"],
        # File di stato separato da BC per evitare conflitti se si usa lo stesso Lakehouse
        state_file_suffix=".crm_mirroring_state.json",
    )
    manager = build_onelake_manager(onelake_cfg)

    print(
        f"[START] CRM/Dataverse -> OneLake "
        f"| Org: {crm_cfg['org_url']} "
        f"| Entities: {crm_cfg['entities']}"
    )

    # ── Autenticazione Dataverse ──────────────────────────────────────────────
    print("[AUTH] Autenticazione Dataverse...")
    token = get_crm_token(crm_cfg)
    print("[AUTH] Token ottenuto")

    total_uploaded = 0
    failed_entities = []  # tiene traccia delle entità fallite

    # ── Loop su tutte le entità Dataverse ────────────────────────────────────
    for entity in crm_cfg["entities"]:
        print(f"\n[PROCESS] CRM :: {entity}")
        try:
            # Determina se è un initial load o incremental
            # Il namespace "CRM" è fisso: lo stato viene memorizzato come "CRM::accounts", ecc.
            last_load = manager.get_last_load_timestamp(CRM_NAMESPACE, entity)
            is_incremental = last_load is not None

            print(
                f"  [MODE] "
                + (f"INCREMENTAL (ultimo carico: {last_load})" if is_incremental else "INITIAL LOAD")
            )

            # Scarica dati da Dataverse (con filtro su modifiedon se incrementale)
            df = pull_crm_data(token, crm_cfg, entity, last_load)

            if df.empty:
                print("  [SKIP] Nessun dato trovato, salto")
                continue

            # Rileva chiavi primarie con la logica smart per Dataverse
            # (es. accountid, contactid — colonne che terminano con 'id')
            metadata = create_openmirroring_metadata(df, smart_id_detection=True)
            key_columns = metadata.get("keyColumns", [df.columns[0]])
            current_keys_df = (
                df.loc[:, key_columns]
                .drop_duplicates()
                .astype(str)
                .reset_index(drop=True)
            )

            # ── Rilevamento e propagazione delete ─────────────────────────────
            # Record presenti nel run precedente ma assenti ora → cancellati in Dataverse
            prev_keys_df = manager.load_previous_keys(CRM_NAMESPACE, entity)
            if prev_keys_df is not None:
                prev = prev_keys_df.astype(str)
                # Left join: 'left_only' = record precedenti non più presenti = delete
                merged = prev.merge(
                    current_keys_df, on=key_columns, how="left", indicator=True
                )
                deleted_keys_df = merged[merged["_merge"] == "left_only"].drop(
                    columns=["_merge"]
                )

                if not deleted_keys_df.empty:
                    print(f"  [DELETE] {len(deleted_keys_df)} righe cancellate in Dataverse da propagare")
                    # Crea DataFrame con solo chiavi + __rowMarker__=2 (Delete)
                    delete_df = add_row_marker_column(
                        deleted_keys_df,
                        mode="delete",
                        key_columns=key_columns,
                        keep_data_for_delete=False,
                    )
                    metadata_delete = create_openmirroring_metadata(
                        deleted_keys_df, key_columns=key_columns
                    )
                    manager.upload(
                        delete_df,
                        CRM_NAMESPACE,
                        entity,
                        metadata_override=metadata_delete,
                        partner_events=create_crm_partner_events(crm_cfg, entity),
                        smart_id_detection=True,
                    )

            # ── Upload dati principali ────────────────────────────────────────
            manager.upload(
                df,
                CRM_NAMESPACE,
                entity,
                metadata_override=metadata,
                partner_events=create_crm_partner_events(crm_cfg, entity),
                smart_id_detection=True,
            )
            total_uploaded += 1

            # ── Persiste chiavi per il prossimo run ───────────────────────────
            try:
                manager.save_current_keys(current_keys_df, CRM_NAMESPACE, entity)
            except Exception as e:
                # Non bloccante: al prossimo run non rileveremo delete per questa entità
                print(f"  [WARN] Impossibile salvare chiavi: {e}")

        except Exception as e:
            print(f"  [ERROR] CRM::{entity} - {str(e)}")
            failed_entities.append(entity)
            traceback.print_exc()

    # ── Riepilogo finale ──────────────────────────────────────────────────────
    print(f"\n[DONE] Upload riusciti: {total_uploaded}")
    if failed_entities:
        print(f"[DONE] Falliti: {len(failed_entities)} → {failed_entities}")

    return total_uploaded, failed_entities


# ─── Entry point standalone (Spark Job Definition) ───────────────────────────

if __name__ == "__main__":
    # Inizializzazione SparkSession (opzionale: serve solo se usi API Spark nel codice)
    spark = None
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    except Exception:
        pass  # Ambiente non-Spark (es. test locale): procedi senza Spark

    try:
        run()
    except Exception as e:
        print(f"[FATAL] {str(e)}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
