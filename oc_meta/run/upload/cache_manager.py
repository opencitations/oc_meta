import json
import os
import signal
import atexit
from typing import Set
import redis
from redis.exceptions import ConnectionError as RedisConnectionError


class CacheManager:
    REDIS_KEY = "processed_files"  # Chiave per il set Redis

    def __init__(
        self,
        json_cache_file: str,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 4,
    ):
        self.json_cache_file = json_cache_file
        self._redis = None
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.processed_files: Set[str] = set()

        # Inizializza il cache
        self._init_cache()

        # Registra handlers per graceful shutdown
        self._register_shutdown_handlers()

    def _init_redis(self) -> None:
        """Inizializza la connessione Redis"""
        try:
            self._redis = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                decode_responses=True,  # Assicura che le stringhe siano decodificate
            )
            self._redis.ping()  # Verifica la connessione
        except RedisConnectionError:
            print("Warning: Redis non disponibile. Using only JSON cache.")
            self._redis = None

    def _init_cache(self) -> None:
        """Inizializza il cache da file JSON e Redis"""
        self._init_redis()

        # Carica dal file JSON
        if os.path.exists(self.json_cache_file):
            with open(self.json_cache_file, "r", encoding="utf8") as f:
                self.processed_files.update(json.load(f))

        # Se Redis è disponibile, sincronizza
        if self._redis:
            # Carica i dati esistenti da Redis
            existing_redis_files = self._redis.smembers(self.REDIS_KEY)
            # Aggiunge i file dal JSON a Redis
            if self.processed_files:
                self._redis.sadd(self.REDIS_KEY, *self.processed_files)
            # Aggiorna il set locale con i dati da Redis
            self.processed_files.update(existing_redis_files)

    def _save_to_json(self) -> None:
        """Save the cache to a JSON file."""
        cache_dir = os.path.dirname(self.json_cache_file)
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)

        with open(self.json_cache_file, "w", encoding="utf8") as f:
            json.dump(list(self.processed_files), f, indent=4, ensure_ascii=False)
        print(f"Cache saved to {self.json_cache_file}")

    def _register_shutdown_handlers(self) -> None:
        """Registra gli handler per gestire l'interruzione del processo"""
        atexit.register(self._cleanup)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame) -> None:
        """Gestisce i segnali di interruzione"""
        print(f"\nRicevuto segnale di interruzione {signum}")
        self._cleanup()
        exit(0)

    def _cleanup(self) -> None:
        """Esegue le operazioni di cleanup"""
        print("\nSalvataggio cache su file...")
        if self._redis:
            # Aggiorna il set locale con i dati più recenti da Redis
            self.processed_files.update(self._redis.smembers(self.REDIS_KEY))
        self._save_to_json()
        print("Cache salvato.")

    def add(self, filename: str) -> None:
        """
        Aggiunge un file al cache

        Args:
            filename (str): Nome del file da aggiungere
        """
        self.processed_files.add(filename)
        if self._redis:
            self._redis.sadd(self.REDIS_KEY, filename)

    def __contains__(self, filename: str) -> bool:
        """
        Verifica se un file è nel cache

        Args:
            filename (str): Nome del file da verificare

        Returns:
            bool: True se il file è nel cache, False altrimenti
        """
        return filename in self.processed_files

    def get_all(self) -> Set[str]:
        """
        Restituisce tutti i file nel cache

        Returns:
            Set[str]: Set di nomi dei file processati
        """
        if self._redis:
            self.processed_files.update(self._redis.smembers(self.REDIS_KEY))
        return self.processed_files
