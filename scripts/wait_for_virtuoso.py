#!/usr/bin/env python3
import logging
import socket
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class VirtuosoConfig:
    host: str = 'localhost'
    port: int = 8805
    timeout: int = 5

def setup_logging() -> logging.Logger:
    """Configura il logging per lo script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

def check_virtuoso_sparql(config: VirtuosoConfig) -> Tuple[bool, Optional[str]]:
    """
    Verifica se Virtuoso risponde a una semplice query SPARQL
    """
    query = "SELECT * { ?s ?p ?o } LIMIT 1"
    encoded_query = urllib.parse.quote(query)
    url = f"http://{config.host}:{config.port}/sparql?query={encoded_query}"

    try:
        request = urllib.request.Request(
            url,
            headers={
                'Accept': 'application/sparql-results+json',
                'User-Agent': 'VirtuosoHealthCheck/1.0'
            }
        )
        
        with urllib.request.urlopen(request, timeout=config.timeout) as response:
            return response.status == 200, None
            
    except urllib.error.URLError as e:
        return False, f"Errore SPARQL endpoint: {str(e)}"
    except Exception as e:
        return False, f"Errore inatteso: {str(e)}"

def wait_for_virtuoso(config: VirtuosoConfig, max_wait: int = 180) -> Tuple[bool, int, Optional[str]]:
    """
    Attende che Virtuoso sia completamente avviato e pronto
    
    Args:
        config: Configurazione di Virtuoso
        max_wait: Tempo massimo di attesa in secondi
    
    Returns:
        Tuple[bool, int, Optional[str]]: (successo, tempo_impiegato, messaggio_errore)
    """
    logger = logging.getLogger(__name__)
    logger.info("Attendo l'avvio di Virtuoso...")
    start_time = time.time()
    
    while time.time() - start_time < max_wait:        
        sparql_ok, sparql_error = check_virtuoso_sparql(config)
        
        if sparql_ok:
            elapsed = int(time.time() - start_time)
            logger.info(f"Virtuoso è pronto! (tempo impiegato: {elapsed}s)")
            return True, elapsed, None
        elif sparql_error:
            logger.warning(f"SPARQL check fallito: {sparql_error}")
        
        time.sleep(2)
    
    timeout_msg = f"Timeout dopo {max_wait}s - Virtuoso non è pronto"
    logger.error(timeout_msg)
    return False, max_wait, timeout_msg

def main():
    logger = setup_logging()
    
    # Configura le impostazioni di connessione
    config = VirtuosoConfig(
        host="localhost",
        port=8805,
        timeout=5
    )
    
    try:
        success, elapsed, error = wait_for_virtuoso(config)
        if not success and error:
            logger.error(f"Impossibile connettersi a Virtuoso: {error}")
            sys.exit(1)
        sys.exit(0)
    except Exception as e:
        logger.error(f"Errore inatteso: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()