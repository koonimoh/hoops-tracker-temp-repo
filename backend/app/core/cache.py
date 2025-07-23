import redis
import json
import pickle
from functools import wraps
from typing import Any, Optional, Union, Callable
from app.core.config import settings, REDIS_CACHE_CONFIG
from app.core.logging import logger
import hashlib

class CacheManager:
    def __init__(self):
        self.redis_client = redis.Redis(**REDIS_CACHE_CONFIG)
        self.default_timeout = settings.cache_default_timeout
        
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            value = self.redis_client.get(f"{settings.cache_key_prefix}{key}")
            return pickle.loads(value) if value else None
        except Exception as e:
            logger.error(f"Cache get error for key {key}: {e}")
            return None
    
    def set(self, key: str, value: Any, timeout: Optional[int] = None) -> bool:
        """Set value in cache"""
        try:
            timeout = timeout or self.default_timeout
            serialized = pickle.dumps(value)
            return self.redis_client.setex(
                f"{settings.cache_key_prefix}{key}",
                timeout,
                serialized
            )
        except Exception as e:
            logger.error(f"Cache set error for key {key}: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete key from cache"""
        try:
            return bool(self.redis_client.delete(f"{settings.cache_key_prefix}{key}"))
        except Exception as e:
            logger.error(f"Cache delete error for key {key}: {e}")
            return False
    
    def clear_pattern(self, pattern: str) -> int:
        """Clear all keys matching pattern"""
        try:
            keys = self.redis_client.keys(f"{settings.cache_key_prefix}{pattern}")
            return self.redis_client.delete(*keys) if keys else 0
        except Exception as e:
            logger.error(f"Cache clear pattern error for {pattern}: {e}")
            return 0

cache = CacheManager()

def cached(timeout: Optional[int] = None, key_func: Optional[Callable] = None):
    """Decorator for caching function results"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                key_data = f"{func.__name__}:{str(args)}:{str(sorted(kwargs.items()))}"
                cache_key = hashlib.md5(key_data.encode()).hexdigest()
            
            # Try to get from cache
            result = cache.get(cache_key)
            if result is not None:
                logger.debug(f"Cache hit for {func.__name__}")
                return result
            
            # Execute function and cache result
            logger.debug(f"Cache miss for {func.__name__}")
            result = func(*args, **kwargs)
            cache.set(cache_key, result, timeout)
            return result
        return wrapper
    return decorator

def cache_key_for_player_stats(player_id: str, stat_key: str, season_year: int):
    """Generate cache key for player stats"""
    return f"player_stats:{player_id}:{stat_key}:{season_year}"

def cache_key_for_search(query: str, search_type: str):
    """Generate cache key for search results"""
    query_hash = hashlib.md5(query.encode()).hexdigest()
    return f"search:{search_type}:{query_hash}"