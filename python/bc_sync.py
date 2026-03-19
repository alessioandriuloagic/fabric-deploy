"""
bc_sync.py
==========
Entry point per la sincronizzazione Business Central → Microsoft Fabric OneLake.

Questo script può essere:
1. **Eseguito direttamente** come Spark Job Definition in Fabric.
2. **Importato** da ``sync_all.py`` tramite la funzione ``run()``.

Flusso di esecuzione
--------------------
Per ogni combinazione ``company × entity``:

1. Legge il timestamp dell'ultimo carico dallo stato OneLake.
2. Scarica i dati da BC (initial o incremental in base al timestamp).
3. Confronta le chiavi correnti con quelle del run precedente per rilevare delete.
4. Se ci sono delete, le carica su OneLake (marker=2).
5. Carica i dati principali su OneLake (marker=0 insert o 1 update).
6. Persiste le chiavi correnti per il rilevamento delete del prossimo run.

Parametri Spark Job
-------------------
Obbligatori:
  --BC_TENANT_ID           Azure AD Tenant ID
  --BC_CLIENT_ID           Application ID Service Principal BC
  --BC_CLIENT_SECRET_B64   Client secret BC in Base64 (oppure --BC_CLIENT_SECRET)
  --FABRIC_WORKSPACE_ID    ID workspace Fabric
  --FABRIC_LAKEHOUSE_ID    ID Lakehouse Fabric
  --FABRIC_MIRRORED_DB_ID  ID Mirrored Database Fabric

Opzionali:
  --BC_ENVIRONMENT         Ambiente BC (default: SandboxTest)
  --BC_COMPANIES_B64       Lista JSON companies in Base64 (default: ["CRONUS%20IT"])
  --BC_ENTITIES_B64        Lista JSON entità in Base64 (default: ["ItemLedgerEntries"])
  --FABRIC_TENANT_ID       Tenant ID SP OneLake (default: BC_TENANT_ID)
  --FABRIC_CLIENT_ID       Client ID SP OneLake (default: BC_CLIENT_ID)
  --FABRIC_CLIENT_SECRET_B64  Secret SP OneLake in Base64 (default: BC_CLIENT_SECRET)
"""

import sys
import traceback

from modules.bc_connector import load_bc_config, get_bc_token, pull_bc_data, create_bc_partner_events
from modules.onelake_config import load_onelake_config, build_onelake_manager
from modules.mirroring_metadata import create_openmirroring_metadata
from modules.dataframe_utils import add_row_marker_column


def run():
    """
    Esegue la sincronizzazione completa Business Central → OneLake.

    Restituisce
    -----------
    tuple[int, list[str]]
        ``(total_uploaded, failed_combinations)``
        dove ``failed_combinations`` è una lista di stringhe ``"company::entity"``
        per le combinazioni che hanno fallito.

    Eccezioni
    ---------
    RuntimeError
        Se i parametri obbligatori mancano (sollevata da load_bc_config o load_onelake_config).
    """
    # ── Carica configurazione ─────────────────────────────────────────────────
    bc_cfg = load_bc_config()

    # OneLake: usa le credenziali BC come fallback se non sono specificate quelle Fabric
    onelake_cfg = load_onelake_config(
        fallback_tenant_id=bc_cfg["tenant_id"],
        fallback_client_id=bc_cfg["client_id"],
        fallback_client_secret=bc_cfg["client_secret"],
        state_file_suffix=".mirroring_state.json",  # file di stato dedicato BC
    )
    manager = build_onelake_manager(onelake_cfg)

    print(
        f"[START] BC -> OneLake "
        f"| Companies: {bc_cfg['companies']} "
        f"| Entities: {bc_cfg['entities']}"
    )

    # ── Autenticazione BC ─────────────────────────────────────────────────────
    print("[AUTH] Autenticazione Business Central...")
    token = get_bc_token(bc_cfg)
    print("[AUTH] Token ottenuto")

    total_uploaded = 0
    failed_combinations = []  # tiene traccia delle combinazioni fallite

    # ── Loop su tutte le combinazioni company × entity ────────────────────────
    for company in bc_cfg["companies"]:
        for entity in bc_cfg["entities"]:
            print(f"\n[PROCESS] {company} :: {entity}")
            try:
                # Determina se è un initial load o incremental
                last_load = manager.get_last_load_timestamp(company, entity)
                is_incremental = last_load is not None

                print(
                    f"  [MODE] "
                    + (f"INCREMENTAL (ultimo carico: {last_load})" if is_incremental else "INITIAL LOAD")
                )

                # Scarica dati da BC (con filtro incrementale se applicabile)
                df = pull_bc_data(token, bc_cfg, company, entity, last_load)

                if df.empty:
                    print("  [SKIP] Nessun dato trovato, salto")
                    continue

                # Calcola metadata e chiavi primarie dai dati correnti
                metadata = create_openmirroring_metadata(df)
                key_columns = metadata.get("keyColumns", [df.columns[0]])
                current_keys_df = (
                    df.loc[:, key_columns]
                    .drop_duplicates()
                    .astype(str)
                    .reset_index(drop=True)
                )

                # ── Rilevamento e propagazione delete ─────────────────────────
                # Confronta le chiavi del run corrente con quelle del run precedente.
                # I record presenti prima ma assenti ora sono stati cancellati in BC.
                prev_keys_df = manager.load_previous_keys(company, entity)
                if prev_keys_df is not None:
                    prev = prev_keys_df.astype(str)
                    # Left join: le righe con indicator='left_only' sono i delete
                    merged = prev.merge(
                        current_keys_df, on=key_columns, how="left", indicator=True
                    )
                    deleted_keys_df = merged[merged["_merge"] == "left_only"].drop(
                        columns=["_merge"]
                    )

                    if not deleted_keys_df.empty:
                        print(f"  [DELETE] {len(deleted_keys_df)} righe cancellate in BC da propagare")
                        # Crea DataFrame di delete con solo le chiavi + __rowMarker__=2
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
                            company,
                            entity,
                            metadata_override=metadata_delete,
                            partner_events=create_bc_partner_events(bc_cfg, company, entity),
                        )

                # ── Upload dati principali ────────────────────────────────────
                manager.upload(
                    df,
                    company,
                    entity,
                    metadata_override=metadata,
                    partner_events=create_bc_partner_events(bc_cfg, company, entity),
                )
                total_uploaded += 1

                # ── Persiste chiavi per il prossimo run ───────────────────────
                try:
                    manager.save_current_keys(current_keys_df, company, entity)
                except Exception as e:
                    # Non bloccante: al prossimo run non rileveremo delete per questa entità
                    print(f"  [WARN] Impossibile salvare chiavi: {e}")

            except Exception as e:
                print(f"  [ERROR] {company}::{entity} - {str(e)}")
                failed_combinations.append(f"{company}::{entity}")
                traceback.print_exc()

    # ── Riepilogo finale ──────────────────────────────────────────────────────
    print(f"\n[DONE] Upload riusciti: {total_uploaded}")
    if failed_combinations:
        print(f"[DONE] Falliti: {len(failed_combinations)} → {failed_combinations}")

    return total_uploaded, failed_combinations


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
