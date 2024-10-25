#!/usr/bin/env python3
import socket
import time
import sys
import subprocess
from typing import Tuple

def check_virtuoso_port(host: str = 'localhost', port: int = 1105, timeout: int = 1) -> bool:
    """
    Verifica se Virtuoso è in ascolto sulla porta specificata
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            return result == 0
    except socket.error:
        return False

def check_virtuoso_isql(max_attempts: int = 5) -> bool:
    """
    Verifica se Virtuoso risponde ai comandi ISQL
    """
    for _ in range(max_attempts):
        try:
            result = subprocess.run(
                ['isql', '-S', '1105', '-U', 'dba', '-P', 'dba', '-Q', 'status();'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0 and 'Server is ready' in result.stdout:
                return True
        except (subprocess.SubprocessError, subprocess.TimeoutExpired):
            pass
        time.sleep(2)
    return False

def wait_for_virtuoso(max_wait: int = 180) -> Tuple[bool, int]:
    """
    Attende che Virtuoso sia completamente avviato e pronto
    
    Args:
        max_wait: Tempo massimo di attesa in secondi
    
    Returns:
        Tuple[bool, int]: (successo, tempo_impiegato)
    """
    print("Attendo l'avvio di Virtuoso...")
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        if check_virtuoso_port():
            print("Porta Virtuoso rilevata, verifico lo stato del servizio...")
            if check_virtuoso_isql():
                elapsed = int(time.time() - start_time)
                print(f"Virtuoso è pronto! (tempo impiegato: {elapsed}s)")
                return True, elapsed
        time.sleep(2)
        
    print(f"Timeout dopo {max_wait}s - Virtuoso non è pronto")
    return False, max_wait

if __name__ == '__main__':
    success, _ = wait_for_virtuoso()
    sys.exit(0 if success else 1)