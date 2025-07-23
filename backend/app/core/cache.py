"""
Simple in-memory cache manager (no Redis dependency).
"""

import time
import hashlib
from functools import wraps
from typing import Any, Optional, Callable, Dict
from app.core.logging import logger

class SimpleCacheManager:
    """Simple in-memory cache with TTL support."""
    
    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self.default_timeout = 300  # 5 minutes
    
    def init_app(self, app):
        """No-op init_app for Flask compatibility."""
        return None
    
    def _is_expired(self, entry: Dict[str, Any]) -> bool:
        """Check if cache entry is expired."""
        if 'expires_at' not in entry:
            return False
        return time.time() > entry['expires_at']
    
    def _cleanup_expired(self):
        """Remove expired entries from cache."""
        current_time = time.time()
        expired_keys = []
        
        for key, entry in self._cache.items():
            if 'expires_at' in entry and current_time > entry['expires_at']:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self._cache[key]
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        try:
            if key not in self._cache:
                return None
            
            entry = self._cache[key]
            if self._is_expired(entry):
                del self._cache[key]
                return None
            
            return entry['value']
        except Exception as e:
            logger.error(f"Cache get error for key {key}: {e}")
            return None
    
    def set(self, key: str, value: Any, timeout: Optional[int] = None) -> bool:
        """Set value in cache."""
        try:
            timeout = timeout or self.default_timeout
            expires_at = time.time() + timeout if timeout > 0 else None
            
            self._cache[key] = {
                'value': value,
                'expires_at': expires_at,
                'created_at': time.time()
            }
            
            # Periodic cleanup
            if len(self._cache) % 100 == 0:
                self._cleanup_expired()
            
            return True
        except Exception as e:
            logger.error(f"Cache set error for key {key}: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        try:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
        except Exception as e:
            logger.error(f"Cache delete error for key {key}: {e}")
            return False
    
    def clear(self) -> bool:
        """Clear all cache entries."""
        try:
            self._cache.clear()
            return True
        except Exception as e:
            logger.error(f"Cache clear error: {e}")
            return False
    
    def clear_pattern(self, pattern: str) -> int:
        """Clear all keys matching pattern (simple string matching)."""
        try:
            keys_to_delete = []
            for key in self._cache.keys():
                if pattern in key:
                    keys_to_delete.append(key)
            
            for key in keys_to_delete:
                del self._cache[key]
            
            return len(keys_to_delete)
        except Exception as e:
            logger.error(f"Cache clear pattern error for {pattern}: {e}")
            return 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        self._cleanup_expired()
        return {
            'total_keys': len(self._cache),
            'memory_usage_estimate': len(str(self._cache)),
            'cache_type': 'in_memory'
        }

# Create cache manager instance
cache = SimpleCacheManager()

def cached(timeout: Optional[int] = None, key_func: Optional[Callable] = None):
    """Decorator for caching function results."""
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
    """Generate cache key for player stats."""
    return f"player_stats:{player_id}:{stat_key}:{season_year}"

def cache_key_for_search(query: str, search_type: str):
    """Generate cache key for search results."""
    query_hash = hashlib.md5(query.encode()).hexdigest()
    return f"search:{search_type}:{query_hash}"