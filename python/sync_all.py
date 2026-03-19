"""
sync_all.py
===========
Orchestratore principale: esegue BC e/o CRM sync in un unico Spark Job.

Questo è il file da configurare come **Main definition file** nel Spark Job
Definition di Fabric quando si vuole sincronizzare più sorgenti in un solo job.

Flusso di esecuzione
--------------------
1. Legge il parametro ``--SOURCE`` (BC | CRM | ALL, default: ALL).
2. Se SOURCE include BC: chiama ``bc_sync.run()`` → sincronizza Business Central.
3. Se SOURCE include CRM: chiama ``crm_sync.run()`` → sincronizza Dataverse.
4. Stampa un riepilogo finale con contatori e lista fallimenti.
5. Esce con ``sys.exit(1)`` se almeno una sorgente ha avuto un errore fatale.

Parametri Spark Job
-------------------
Generali:
  --SOURCE   BC | CRM | ALL (default: ALL)
             Determina quale sorgente sincronizzare.
             - ``BC``  → solo Business Central
             - ``CRM`` → solo Dataverse/CRM
             - ``ALL`` → entrambe in sequenza (BC prima, poi CRM)

Business Central (richiesti se SOURCE=BC o SOURCE=ALL):
  --BC_TENANT_ID, --BC_CLIENT_ID, --BC_CLIENT_SECRET_B64 (o --BC_CLIENT_SECRET)
  --BC_ENVIRONMENT, --BC_COMPANIES_B64, --BC_ENTITIES_B64

CRM/Dataverse (richiesti se SOURCE=CRM o SOURCE=ALL):
  --CRM_TENANT_ID, --CRM_CLIENT_ID, --CRM_CLIENT_SECRET_B64 (o --CRM_CLIENT_SECRET)
  --CRM_ORG_URL, --CRM_API_VERSION, --CRM_ENTITIES_B64

OneLake/Fabric (sempre richiesti):
  --FABRIC_WORKSPACE_ID, --FABRIC_LAKEHOUSE_ID, --FABRIC_MIRRORED_DB_ID
  --FABRIC_TENANT_ID, --FABRIC_CLIENT_ID, --FABRIC_CLIENT_SECRET_B64 (opzionali: fallback su SP sorgente)

Esempi
------
Solo Business Central::

    spark-submit sync_all.py \\
      --SOURCE BC \\
      --BC_TENANT_ID <tenant> \\
      --BC_CLIENT_ID <client_id> \\
      --BC_CLIENT_SECRET_B64 <base64_secret> \\
      --BC_COMPANIES_B64 <base64_json_lista> \\
      --BC_ENTITIES_B64 <base64_json_lista> \\
      --FABRIC_WORKSPACE_ID <workspace_id> \\
      --FABRIC_LAKEHOUSE_ID <lakehouse_id> \\
      --FABRIC_MIRRORED_DB_ID <mirrored_db_id>

BC + CRM in sequenza::

    spark-submit sync_all.py --SOURCE ALL ...tutti i parametri BC e CRM...

Struttura output riepilogo::

    ============================================================
      RIEPILOGO SYNC_ALL
    ============================================================
      [BC]  Upload: 3 | Status: OK
      [CRM] Upload: 2 | Status: PARZIALE (1 falliti)
             Falliti: ['leads']
"""

import sys
import traceback

from modules.config import get_param


def _run_bc():
    """
    Avvia la sincronizzazione Business Central invocando ``bc_sync.run()``.

    Restituisce
    -----------
    tuple[int, list[str]]
        ``(total_uploaded, failed_combinations)`` da ``bc_sync.run()``.
    """
    print("\n" + "=" * 60)
    print("  SORGENTE: Business Central")
    print("=" * 60)
    import bc_sync
    return bc_sync.run()


def _run_crm():
    """
    Avvia la sincronizzazione CRM/Dataverse invocando ``crm_sync.run()``.

    Restituisce
    -----------
    tuple[int, list[str]]
        ``(total_uploaded, failed_entities)`` da ``crm_sync.run()``.
    """
    print("\n" + "=" * 60)
    print("  SORGENTE: CRM / Dataverse")
    print("=" * 60)
    import crm_sync
    return crm_sync.run()


def run_all():
    """
    Punto di ingresso principale dell'orchestratore.

    Legge il parametro ``--SOURCE``, esegue le sincronizzazioni richieste
    in sequenza e stampa un riepilogo finale.

    Sequenza di esecuzione quando SOURCE=ALL: BC prima, CRM dopo.
    Un fallimento su una sorgente non blocca l'esecuzione delle successive.

    Eccezioni
    ---------
    ValueError
        Se ``--SOURCE`` ha un valore non riconosciuto.
    SystemExit(1)
        Se almeno una sorgente ha avuto un errore fatale (non recuperabile).
    """
    source = (get_param("SOURCE", "ALL") or "ALL").upper().strip()
    valid_sources = {"BC", "CRM", "ALL"}

    if source not in valid_sources:
        raise ValueError(
            f"Valore --SOURCE non valido: '{source}'. "
            f"Usare uno tra: {', '.join(sorted(valid_sources))}."
        )

    print(f"[SYNC_ALL] Avvio orchestratore | SOURCE={source}")

    results = {}   # {sorgente: {"uploaded": int, "failed": list}}
    errors = []    # sorgenti con errore fatale (eccezione non recuperata)

    # ── Business Central ──────────────────────────────────────────────────────
    if source in ("BC", "ALL"):
        try:
            uploaded, failed = _run_bc()
            results["BC"] = {"uploaded": uploaded, "failed": failed}
        except Exception as e:
            # Errore fatale BC (es. parametri mancanti, autenticazione fallita)
            print(f"[FATAL][BC] {e}")
            traceback.print_exc()
            errors.append("BC")

    # ── CRM / Dataverse ───────────────────────────────────────────────────────
    if source in ("CRM", "ALL"):
        try:
            uploaded, failed = _run_crm()
            results["CRM"] = {"uploaded": uploaded, "failed": failed}
        except Exception as e:
            # Errore fatale CRM (es. parametri mancanti, autenticazione fallita)
            print(f"[FATAL][CRM] {e}")
            traceback.print_exc()
            errors.append("CRM")

    # ── Riepilogo finale ──────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  RIEPILOGO SYNC_ALL")
    print("=" * 60)

    for src, res in results.items():
        if not res["failed"]:
            status = "OK"
        else:
            status = f"PARZIALE ({len(res['failed'])} falliti)"
        print(f"  [{src}] Upload: {res['uploaded']} | Status: {status}")
        if res["failed"]:
            print(f"         Falliti: {res['failed']}")

    if errors:
        print(f"  [ERRORE FATALE] Sorgenti con errore critico: {errors}")
        # Esce con codice non-zero per segnalare il fallimento al Spark Job
        sys.exit(1)

    print("=" * 60)


# ─── Entry point standalone (Spark Job Definition) ───────────────────────────

if __name__ == "__main__":
    # Inizializzazione SparkSession (opzionale)
    spark = None
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    except Exception:
        pass

    try:
        run_all()
    except SystemExit:
        raise  # propaga sys.exit() senza stampare di nuovo lo stack
    except Exception as e:
        print(f"[FATAL] {str(e)}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
