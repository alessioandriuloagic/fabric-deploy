"""
onelake_config.py
=================
Lettura parametri Fabric/OneLake e costruzione dell'istanza ``OneLakeManager``.

Questo modulo funge da "factory" per il ``OneLakeManager``: legge i parametri
dalla configurazione (sys.argv / env), li valida e restituisce un'istanza
pronta all'uso.

Separazione delle responsabilità
---------------------------------
I parametri OneLake (FABRIC_*) sono separati dai parametri della sorgente
(BC_*, CRM_*) per permettere di usare Service Principal diversi:

- **Sorgente SP** — usato per autenticarsi su Business Central o Dataverse.
- **OneLake SP** — usato per scrivere su Fabric OneLake.

Se i parametri FABRIC_TENANT_ID / FABRIC_CLIENT_ID / FABRIC_CLIENT_SECRET
non sono specificati, vengono usate le credenziali della sorgente come fallback
(caso tipico: stesso SP per sorgente e destinazione).

Funzioni esportate
------------------
- load_onelake_config(fallback_tenant_id, fallback_client_id, fallback_client_secret, state_file_suffix)
    Legge e valida i parametri OneLake.
- build_onelake_manager(onelake_cfg, max_retries, retry_backoff_factor)
    Istanzia un ``OneLakeManager`` dal dizionario di configurazione.
"""

from .config import get_param, get_param_b64, validate_required
from .onelake import OneLakeManager


def load_onelake_config(
    fallback_tenant_id=None,
    fallback_client_id=None,
    fallback_client_secret=None,
    state_file_suffix=".mirroring_state.json",
):
    """
    Legge e valida i parametri Fabric / OneLake da sys.argv o variabili d'ambiente.

    Parametri obbligatori (da sys.argv o env)
    ------------------------------------------
    - ``FABRIC_WORKSPACE_ID``   — ID workspace Fabric (GUID)
    - ``FABRIC_LAKEHOUSE_ID``   — ID Lakehouse (GUID)
    - ``FABRIC_MIRRORED_DB_ID`` — ID Mirrored Database (GUID)

    Parametri opzionali (con fallback alle credenziali della sorgente)
    ------------------------------------------------------------------
    - ``FABRIC_TENANT_ID``          → fallback su ``fallback_tenant_id``
    - ``FABRIC_CLIENT_ID``          → fallback su ``fallback_client_id``
    - ``FABRIC_CLIENT_SECRET_B64``  → fallback su ``fallback_client_secret``
    - ``FABRIC_CLIENT_SECRET``      → fallback su ``fallback_client_secret``

    Il path del file di stato viene costruito automaticamente come:
    ``<lakehouse_id>/Files/MirroringState/<state_file_suffix>``

    Parametri
    ---------
    fallback_tenant_id : str | None
        Tenant ID della sorgente (BC o CRM). Usato se FABRIC_TENANT_ID è assente.
    fallback_client_id : str | None
        Client ID della sorgente. Usato se FABRIC_CLIENT_ID è assente.
    fallback_client_secret : str | None
        Client secret della sorgente. Usato se FABRIC_CLIENT_SECRET* è assente.
    state_file_suffix : str
        Nome del file di stato. Usa suffissi diversi per BC e CRM per evitare
        conflitti se si usa lo stesso Lakehouse:
        - BC  → ``".mirroring_state.json"``
        - CRM → ``".crm_mirroring_state.json"``

    Restituisce
    -----------
    dict
        Dizionario con chiavi:
        ``tenant_id, client_id, client_secret,
        workspace_id, lakehouse_id, mirrored_db_id, state_file``

    Eccezioni
    ---------
    RuntimeError
        Se uno dei parametri obbligatori (FABRIC_WORKSPACE_ID, LAKEHOUSE_ID,
        MIRRORED_DB_ID) non è valorizzato.
    """
    workspace_id = get_param("FABRIC_WORKSPACE_ID")
    lakehouse_id = get_param("FABRIC_LAKEHOUSE_ID")
    mirrored_db_id = get_param("FABRIC_MIRRORED_DB_ID")

    # Credenziali OneLake: usa FABRIC_* se presenti, altrimenti quelle della sorgente
    tenant_id = get_param("FABRIC_TENANT_ID", fallback_tenant_id) or fallback_tenant_id
    client_id = get_param("FABRIC_CLIENT_ID", fallback_client_id) or fallback_client_id
    client_secret = (
        get_param_b64("FABRIC_CLIENT_SECRET_B64")     # Base64 ha priorità
        or get_param("FABRIC_CLIENT_SECRET")           # Plain-text
        or fallback_client_secret                      # Credenziali sorgente
    )

    # Solo workspace/lakehouse/mirrored_db sono obbligatori come parametri Fabric
    validate_required({
        "FABRIC_WORKSPACE_ID": workspace_id,
        "FABRIC_LAKEHOUSE_ID": lakehouse_id,
        "FABRIC_MIRRORED_DB_ID": mirrored_db_id,
    })

    # Path del file di stato costruito dentro il Lakehouse
    state_file = f"{lakehouse_id}/Files/MirroringState/{state_file_suffix}"

    return {
        "tenant_id": tenant_id,
        "client_id": client_id,
        "client_secret": client_secret,
        "workspace_id": workspace_id,
        "lakehouse_id": lakehouse_id,
        "mirrored_db_id": mirrored_db_id,
        "state_file": state_file,
    }


def build_onelake_manager(onelake_cfg, max_retries=3, retry_backoff_factor=1):
    """
    Costruisce e restituisce un'istanza di ``OneLakeManager``.

    Funziona come factory: prende il dizionario restituito da
    :func:`load_onelake_config` e istanzia il manager con tutti i parametri.

    Parametri
    ---------
    onelake_cfg : dict
        Dizionario di configurazione OneLake (output di ``load_onelake_config``).
    max_retries : int
        Numero massimo di retry per ogni upload (default 3).
    retry_backoff_factor : int | float
        Fattore backoff esponenziale (default 1).

    Restituisce
    -----------
    OneLakeManager
        Istanza pronta all'uso.

    Esempio
    -------
    >>> onelake_cfg = load_onelake_config(fallback_tenant_id=bc_cfg["tenant_id"], ...)
    >>> manager = build_onelake_manager(onelake_cfg)
    >>> manager.upload(df, "CRONUS IT", "ItemLedgerEntries", ...)
    """
    return OneLakeManager(
        tenant_id=onelake_cfg["tenant_id"],
        client_id=onelake_cfg["client_id"],
        client_secret=onelake_cfg["client_secret"],
        workspace_id=onelake_cfg["workspace_id"],
        lakehouse_id=onelake_cfg["lakehouse_id"],
        mirrored_db_id=onelake_cfg["mirrored_db_id"],
        state_file=onelake_cfg["state_file"],
        max_retries=max_retries,
        retry_backoff_factor=retry_backoff_factor,
    )
