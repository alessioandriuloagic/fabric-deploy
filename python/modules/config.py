"""
config.py
=========
Modulo di lettura e validazione parametri di configurazione.

I parametri vengono letti da due sorgenti, in ordine di priorità:
  1. sys.argv  — argomenti passati allo Spark Job Definition in Fabric
                 nella forma: --NOME_PARAM valore
  2. os.environ — variabili d'ambiente del processo (utile per test locali)

Funzioni esportate
------------------
- get_param(name, default)      → lettura parametro plain-text
- get_param_b64(name, default)  → lettura parametro codificato Base64
- decode_b64_param(name, ...)   → lettura parametro Base64 contenente JSON
- validate_required(params)     → verifica presenza parametri obbligatori
"""

import os
import sys
import base64


def get_param(name, default=None):
    """
    Legge un parametro di configurazione plain-text.

    Cerca prima in sys.argv nella forma ``--NAME value`` (come li passa
    Fabric Spark Job Definition), poi come variabile d'ambiente ``NAME``.

    Parametri
    ---------
    name : str
        Nome del parametro (senza il prefisso ``--``).
    default : any, opzionale
        Valore restituito se il parametro non è trovato in nessuna sorgente.

    Restituisce
    -----------
    str | None
        Il valore trovato, oppure ``default``.

    Esempio
    -------
    >>> # Spark Job passa: --BC_TENANT_ID abc-123
    >>> get_param("BC_TENANT_ID")
    'abc-123'
    """
    key = f"--{name}"
    for i, arg in enumerate(sys.argv):
        if arg == key and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return os.environ.get(name, default)


def get_param_b64(name, default=None):
    """
    Legge un parametro codificato in Base64 e lo decodifica in plain-text.

    Utile per parametri sensibili come client secret che potrebbero
    contenere caratteri speciali incompatibili con sys.argv.
    Se la decodifica fallisce (valore non è Base64 valido), restituisce
    il valore grezzo come fallback.

    Parametri
    ---------
    name : str
        Nome del parametro (es. ``BC_CLIENT_SECRET_B64``).
    default : any, opzionale
        Valore restituito se il parametro è assente.

    Restituisce
    -----------
    str | None
        Stringa decodificata, valore grezzo (fallback), oppure ``default``.

    Esempio
    -------
    >>> # base64("myS3cr3t!") → "bXlTM2NyM3Qh"
    >>> get_param_b64("BC_CLIENT_SECRET_B64")
    'myS3cr3t!'
    """
    raw = get_param(name)
    if raw:
        try:
            return base64.b64decode(raw).decode("utf-8")
        except Exception:
            # Il valore non è Base64 valido: lo restituiamo così com'è
            return raw
    return default


def decode_b64_param(name, default_json="[]"):
    """
    Legge un parametro Base64 il cui contenuto decodificato è una stringa JSON.

    Usato tipicamente per passare liste (companies, entities) come singolo
    argomento allo Spark Job senza problemi di escaping.

    Parametri
    ---------
    name : str
        Nome del parametro (es. ``BC_COMPANIES_B64``).
    default_json : str
        Stringa JSON restituita se il parametro è assente (default ``"[]"``).

    Restituisce
    -----------
    str
        Stringa JSON pronta per essere passata a ``json.loads()``.

    Esempio
    -------
    >>> # base64('["CRONUS IT", "DEMO"]') → parametro passato al job
    >>> decode_b64_param("BC_COMPANIES_B64", '["CRONUS IT"]')
    '["CRONUS IT", "DEMO"]'
    """
    raw = get_param(name)
    if raw:
        return base64.b64decode(raw).decode("utf-8")
    return default_json


def validate_required(params_dict):
    """
    Verifica che tutti i parametri obbligatori siano valorizzati (non None, non stringa vuota).

    Lancia ``RuntimeError`` con l'elenco dei parametri mancanti se almeno
    uno non è valorizzato, in modo da bloccare l'esecuzione subito con un
    messaggio chiaro anziché fallire più avanti con un errore generico.

    Parametri
    ---------
    params_dict : dict[str, any]
        Dizionario ``{NOME_PARAMETRO: valore_letto}``.

    Eccezioni
    ---------
    RuntimeError
        Se uno o più valori sono falsy (None, stringa vuota, 0, ...).

    Esempio
    -------
    >>> validate_required({
    ...     "BC_TENANT_ID": "abc",
    ...     "BC_CLIENT_ID": None,      # mancante
    ...     "BC_CLIENT_SECRET": "",    # mancante
    ... })
    RuntimeError: Parametri obbligatori mancanti: BC_CLIENT_ID, BC_CLIENT_SECRET. ...
    """
    missing = [k for k, v in params_dict.items() if not v]
    if missing:
        raise RuntimeError(
            f"Parametri obbligatori mancanti: {', '.join(missing)}. "
            f"Passarli come --KEY value nello Spark Job o come variabili d'ambiente."
        )
