from typing import Set

import redis
from redis.exceptions import ConnectionError as RedisConnectionError


class CacheManager:
    REDIS_KEY = "processed_files"

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 4,
    ):
        self._redis = None
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.processed_files: Set[str] = set()

        self._init_cache()

    def _init_redis(self) -> None:
        """Initialize Redis connection."""
        try:
            self._redis = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                decode_responses=True,
            )
            self._redis.ping()
        except RedisConnectionError:
            raise RuntimeError("Redis is not available. Cache requires Redis.")

    def _init_cache(self) -> None:
        """Initialize cache from Redis."""
        self._init_redis()
        existing_redis_files = self._redis.smembers(self.REDIS_KEY)
        self.processed_files.update(existing_redis_files)

    def add(self, filename: str) -> None:
        """Add a file to the cache."""
        self.processed_files.add(filename)
        self._redis.sadd(self.REDIS_KEY, filename)

    def __contains__(self, filename: str) -> bool:
        """Check if a file is in the cache."""
        return filename in self.processed_files

    def get_all(self) -> Set[str]:
        """Return all files in the cache."""
        self.processed_files.update(self._redis.smembers(self.REDIS_KEY))
        return self.processed_files
