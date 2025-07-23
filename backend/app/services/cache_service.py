"""
Advanced caching service with intelligent cache management.
"""

import time
import pickle
from typing import Any, Optional, Dict, List, Callable
from datetime import datetime, timedelta
from app.core.cache import cache, CacheManager
from app.core.logging import logger
from app.core.config import settings
import hashlib
import json

class AdvancedCacheService:
    """Advanced caching service with hierarchical cache management."""
    
    def __init__(self):
        self.cache_manager = CacheManager()
        self.default_ttl = settings.cache_default_timeout
        self.cache_stats = {
            'hits': 0,
            'misses': 0,
            'sets': 0,
            'deletes': 0
        }
        
    def get_with_fallback(self, key: str, fallback_func: Callable, 
                         ttl: Optional[int] = None) -> Any:
        """Get from cache or execute fallback function."""
        try:
            # Try to get from cache
            cached_value = self.cache_manager.get(key)
            
            if cached_value is not None:
                self.cache_stats['hits'] += 1
                logger.debug(f"Cache hit for key: {key}")
                return cached_value
            
            # Cache miss - execute fallback
            self.cache_stats['misses'] += 1
            logger.debug(f"Cache miss for key: {key}")
            
            value = fallback_func()
            
            # Store in cache
            if value is not None:
                self.set(key, value, ttl)
            
            return value
            
        except Exception as e:
            logger.error(f"Cache get_with_fallback error for key {key}: {e}")
            # Return fallback result even if caching fails
            return fallback_func()
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache with optional TTL."""
        try:
            ttl = ttl or self.default_ttl
            success = self.cache_manager.set(key, value, ttl)
            
            if success:
                self.cache_stats['sets'] += 1
                logger.debug(f"Cache set for key: {key}")
            
            return success
            
        except Exception as e:
            logger.error(f"Cache set error for key {key}: {e}")
            return False
    
    def get(self, key: str) -> Any:
        """Get value from cache."""
        try:
            value = self.cache_manager.get(key)
            
            if value is not None:
                self.cache_stats['hits'] += 1
            else:
                self.cache_stats['misses'] += 1
            
            return value
            
        except Exception as e:
            logger.error(f"Cache get error for key {key}: {e}")
            return None
    
    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        try:
            success = self.cache_manager.delete(key)
            
            if success:
                self.cache_stats['deletes'] += 1
                logger.debug(f"Cache delete for key: {key}")
            
            return success
            
        except Exception as e:
            logger.error(f"Cache delete error for key {key}: {e}")
            return False
    
    def clear_pattern(self, pattern: str) -> int:
        """Clear all keys matching pattern."""
        try:
            count = self.cache_manager.clear_pattern(pattern)
            self.cache_stats['deletes'] += count
            logger.info(f"Cleared {count} cache keys matching pattern: {pattern}")
            return count
            
        except Exception as e:
            logger.error(f"Cache clear pattern error for {pattern}: {e}")
            return 0
    
    def generate_cache_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate consistent cache key from arguments."""
        try:
            # Create a consistent string from arguments
            key_parts = [prefix]
            
            # Add positional arguments
            for arg in args:
                if isinstance(arg, (dict, list)):
                    key_parts.append(json.dumps(arg, sort_keys=True))
                else:
                    key_parts.append(str(arg))
            
            # Add keyword arguments
            for k, v in sorted(kwargs.items()):
                if isinstance(v, (dict, list)):
                    key_parts.append(f"{k}:{json.dumps(v, sort_keys=True)}")
                else:
                    key_parts.append(f"{k}:{v}")
            
            # Create hash of the key parts
            key_string = ":".join(key_parts)
            key_hash = hashlib.md5(key_string.encode()).hexdigest()
            
            return f"{prefix}:{key_hash}"
            
        except Exception as e:
            logger.error(f"Error generating cache key: {e}")
            return f"{prefix}:default"
    
    def cache_player_data(self, player_id: str, data: Dict[str, Any], 
                         ttl: int = 1800) -> bool:
        """Cache player data with hierarchical keys."""
        try:
            # Cache basic player info
            player_key = f"player:{player_id}"
            self.set(player_key, data.get('basic_info'), ttl)
            
            # Cache season stats
            if 'season_stats' in data:
                stats_key = f"player_stats:{player_id}:season"
                self.set(stats_key, data['season_stats'], ttl)
            
            # Cache recent games
            if 'recent_games' in data:
                games_key = f"player_games:{player_id}:recent"
                self.set(games_key, data['recent_games'], ttl)
            
            return True
            
        except Exception as e:
            logger.error(f"Error caching player data: {e}")
            return False
    
    def get_cached_player_data(self, player_id: str) -> Dict[str, Any]:
        """Get cached player data from hierarchical cache."""
        try:
            player_data = {}
            
            # Get basic player info
            player_key = f"player:{player_id}"
            basic_info = self.get(player_key)
            if basic_info:
                player_data['basic_info'] = basic_info
            
            # Get season stats
            stats_key = f"player_stats:{player_id}:season"
            season_stats = self.get(stats_key)
            if season_stats:
                player_data['season_stats'] = season_stats
            
            # Get recent games
            games_key = f"player_games:{player_id}:recent"
            recent_games = self.get(games_key)
            if recent_games:
                player_data['recent_games'] = recent_games
            
            return player_data
            
        except Exception as e:
            logger.error(f"Error getting cached player data: {e}")
            return {}
    
    def invalidate_player_cache(self, player_id: str):
        """Invalidate all cache entries for a player."""
        try:
            patterns = [
                f"player:{player_id}*",
                f"player_stats:{player_id}*",
                f"player_games:{player_id}*",
                f"search:*{player_id}*"
            ]
            
            total_cleared = 0
            for pattern in patterns:
                cleared = self.clear_pattern(pattern)
                total_cleared += cleared
            
            logger.info(f"Invalidated {total_cleared} cache entries for player {player_id}")
            
        except Exception as e:
            logger.error(f"Error invalidating player cache: {e}")
    
    def warm_cache(self, cache_targets: List[Dict[str, Any]]):
        """Warm up cache with frequently accessed data."""
        try:
            logger.info(f"Starting cache warm-up for {len(cache_targets)} targets")
            
            for target in cache_targets:
                try:
                    cache_type = target.get('type')
                    data = target.get('data')
                    ttl = target.get('ttl', self.default_ttl)
                    
                    if cache_type == 'league_leaders':
                        self._warm_league_leaders_cache(data, ttl)
                    elif cache_type == 'team_standings':
                        self._warm_team_standings_cache(data, ttl)
                    elif cache_type == 'popular_players':
                        self._warm_popular_players_cache(data, ttl)
                    
                except Exception as e:
                    logger.error(f"Error warming cache target {target}: {e}")
            
            logger.info("Cache warm-up completed")
            
        except Exception as e:
            logger.error(f"Cache warm-up failed: {e}")
    
    def _warm_league_leaders_cache(self, data: Dict, ttl: int):
        """Warm league leaders cache."""
        stat_keys = data.get('stat_keys', ['pts', 'reb', 'ast', 'stl', 'blk'])
        
        for stat_key in stat_keys:
            cache_key = f"league_leaders:{stat_key}:2025"
            # This would normally fetch from database/API
            placeholder_data = {'stat_key': stat_key, 'leaders': []}
            self.set(cache_key, placeholder_data, ttl)
    
    def _warm_team_standings_cache(self, data: Dict, ttl: int):
        """Warm team standings cache."""
        cache_key = "team_standings:2025"
        placeholder_data = {'standings': [], 'last_updated': datetime.utcnow().isoformat()}
        self.set(cache_key, placeholder_data, ttl)
    
    def _warm_popular_players_cache(self, data: Dict, ttl: int):
        """Warm popular players cache."""
        cache_key = "popular_players:top_50"
        placeholder_data = {'players': [], 'last_updated': datetime.utcnow().isoformat()}
        self.set(cache_key, placeholder_data, ttl)
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        try:
            total_operations = (
                self.cache_stats['hits'] + 
                self.cache_stats['misses'] + 
                self.cache_stats['sets'] + 
                self.cache_stats['deletes']
            )
            
            hit_rate = 0
            if self.cache_stats['hits'] + self.cache_stats['misses'] > 0:
                hit_rate = self.cache_stats['hits'] / (
                    self.cache_stats['hits'] + self.cache_stats['misses']
                ) * 100
            
            return {
                'operations': self.cache_stats.copy(),
                'total_operations': total_operations,
                'hit_rate_percentage': round(hit_rate, 2),
                'cache_health': 'healthy' if hit_rate > 50 else 'needs_optimization'
            }
            
        except Exception as e:
            logger.error(f"Error getting cache statistics: {e}")
            return {}
    
    def cache_cleanup(self, max_age_hours: int = 24):
        """Clean up old cache entries."""
        try:
            # This is a simplified cleanup - in practice, you'd need to track
            # cache entry timestamps and clean based on that
            logger.info(f"Starting cache cleanup for entries older than {max_age_hours} hours")
            
            # Clear old search results
            self.clear_pattern("search:*")
            
            # Clear old temporary data
            self.clear_pattern("temp:*")
            
            logger.info("Cache cleanup completed")
            
        except Exception as e:
            logger.error(f"Cache cleanup failed: {e}")
    
    def get_cache_size_estimate(self) -> Dict[str, Any]:
        """Get estimate of cache size and usage."""
        try:
            # This is a placeholder - actual implementation would depend on Redis
            return {
                'estimated_entries': 'unknown',
                'estimated_size_mb': 'unknown',
                'note': 'Detailed cache size requires Redis INFO command'
            }
            
        except Exception as e:
            logger.error(f"Error getting cache size estimate: {e}")
            return {}

# Create global cache service instance
cache_service = AdvancedCacheService()

# Convenience functions
def get_with_fallback(key: str, fallback_func: Callable, ttl: Optional[int] = None) -> Any:
    """Get from cache or execute fallback."""
    return cache_service.get_with_fallback(key, fallback_func, ttl)

def cache_player_data(player_id: str, data: Dict[str, Any], ttl: int = 1800) -> bool:
    """Cache comprehensive player data."""
    return cache_service.cache_player_data(player_id, data, ttl)

def get_cached_player_data(player_id: str) -> Dict[str, Any]:
    """Get cached player data."""
    return cache_service.get_cached_player_data(player_id)

def invalidate_player_cache(player_id: str):
    """Invalidate player cache."""
    cache_service.invalidate_player_cache(player_id)

def get_cache_stats() -> Dict[str, Any]:
    """Get cache performance statistics."""
    return cache_service.get_cache_statistics()