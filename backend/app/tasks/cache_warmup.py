"""
Cache warming and management tasks.
"""

from app.tasks.celery_app import celery_app
from app.services.cache_service import cache_service
from app.services.stats_service import stats_service
from app.services.search_service import search_service
from app.core.logging import logger
from app.core.cache import cache
from typing import List, Dict, Any
import time

@celery_app.task(bind=True, name='warm_cache')
def warm_cache_task(self, cache_types: List[str] = None):
    """
    Celery task to warm up application caches.
    
    Args:
        cache_types: List of cache types to warm. If None, warms all types.
    """
    try:
        logger.info("Starting cache warm-up task")
        
        cache_types = cache_types or [
            'league_leaders',
            'popular_players', 
            'team_standings',
            'search_suggestions'
        ]
        
        self.update_state(
            state='PROGRESS',
            meta={'message': 'Starting cache warm-up...', 'progress': 0}
        )
        
        results = {}
        total_types = len(cache_types)
        
        for i, cache_type in enumerate(cache_types):
            try:
                start_time = time.time()
                
                if cache_type == 'league_leaders':
                    result = self._warm_league_leaders_cache()
                elif cache_type == 'popular_players':
                    result = self._warm_popular_players_cache()
                elif cache_type == 'team_standings':
                    result = self._warm_team_standings_cache()
                elif cache_type == 'search_suggestions':
                    result = self._warm_search_cache()
                else:
                    result = {'success': False, 'error': f'Unknown cache type: {cache_type}'}
                
                duration = time.time() - start_time
                result['duration'] = round(duration, 2)
                results[cache_type] = result
                
                # Update progress
                progress = ((i + 1) / total_types) * 100
                self.update_state(
                    state='PROGRESS',
                    meta={'message': f'Warmed {cache_type} cache', 'progress': progress}
                )
                
                logger.info(f"Warmed {cache_type} cache in {duration:.2f}s")
                
            except Exception as e:
                logger.error(f"Failed to warm {cache_type} cache: {e}")
                results[cache_type] = {
                    'success': False,
                    'error': str(e)
                }
        
        successful_warmups = sum(1 for r in results.values() if r.get('success', False))
        
        logger.info(f"Cache warm-up completed: {successful_warmups}/{total_types} successful")
        
        return {
            'success': successful_warmups > 0,
            'message': f'Warmed {successful_warmups}/{total_types} cache types',
            'details': results
        }
        
    except Exception as e:
        logger.error(f"Cache warm-up task failed: {e}")
        
        self.update_state(
            state='FAILURE',
            meta={'message': f'Cache warm-up failed: {str(e)}', 'error': str(e)}
        )
        
        return {
            'success': False,
            'message': f'Cache warm-up failed: {str(e)}',
            'error': str(e)
        }

def _warm_league_leaders_cache(self) -> Dict[str, Any]:
    """Warm league leaders cache."""
    try:
        stat_keys = ['pts', 'reb', 'ast', 'stl', 'blk', 'fg_pct', 'ft_pct']
        cached_stats = 0
        
        for stat_key in stat_keys:
            leaders = stats_service.get_league_leaders(stat_key, limit=20)
            if leaders:
                cache_key = f"league_leaders_{stat_key}_2025"
                cache.set(cache_key, leaders, 3600)  # 1 hour cache
                cached_stats += 1
        
        return {
            'success': cached_stats > 0,
            'cached_stats': cached_stats,
            'total_stats': len(stat_keys)
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}

def _warm_popular_players_cache(self) -> Dict[str, Any]:
    """Warm popular players cache."""
    try:
        # Cache popular players for search suggestions
        popular_players = [
            'LeBron James', 'Stephen Curry', 'Kevin Durant', 'Giannis Antetokounmpo',
            'Luka Doncic', 'Jayson Tatum', 'Joel Embiid', 'Nikola Jokic'
        ]
        
        cached_players = 0
        
        for player_name in popular_players:
            # Search for the player to cache their data
            search_results = search_service.unified_player_search(player_name, limit=1)
            if search_results:
                player_id = search_results[0].get('id')
                if player_id:
                    # Get and cache player stats
                    player_stats = stats_service.get_player_season_stats(player_id)
                    if player_stats:
                        cache_service.cache_player_data(player_id, {
                            'basic_info': search_results[0],
                            'season_stats': player_stats
                        })
                        cached_players += 1
        
        return {
            'success': cached_players > 0,
            'cached_players': cached_players,
            'total_players': len(popular_players)
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}

def _warm_team_standings_cache(self) -> Dict[str, Any]:
    """Warm team standings cache."""
    try:
        # This would normally fetch current standings
        cache_key = "team_standings_2025"
        standings_data = {
            'standings': [],  # Would be populated with real data
            'last_updated': time.time(),
            'cached_by': 'warm_cache_task'
        }
        
        cache.set(cache_key, standings_data, 3600)  # 1 hour cache
        
        return {
            'success': True,
            'cached_item': 'team_standings'
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}

def _warm_search_cache(self) -> Dict[str, Any]:
    """Warm search-related caches."""
    try:
        # Cache common search terms
        common_searches = [
            'lebron', 'curry', 'durant', 'giannis', 'luka',
            'tatum', 'embiid', 'jokic', 'butler', 'lillard'
        ]
        
        cached_searches = 0
        
        for search_term in common_searches:
            results = search_service.unified_player_search(search_term, limit=5)
            if results:
                cache_key = f"search_players_{search_term}"
                cache.set(cache_key, results, 1800)  # 30 minute cache
                cached_searches += 1
        
        return {
            'success': cached_searches > 0,
            'cached_searches': cached_searches,
            'total_searches': len(common_searches)
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}

@celery_app.task(bind=True, name='cleanup_cache')
def cleanup_cache_task(self, max_age_hours: int = 24):
    """
    Celery task to clean up old cache entries.
    
    Args:
        max_age_hours: Maximum age of cache entries to keep.
    """
    try:
        logger.info(f"Starting cache cleanup task (max_age={max_age_hours}h)")
        
        self.update_state(
            state='PROGRESS',
            meta={'message': 'Starting cache cleanup...', 'progress': 0}
        )
        
        # Clean up different types of cache entries
        cleanup_patterns = [
            'search_*',      # Search results
            'temp_*',        # Temporary data
            'session_*',     # Old sessions (if using cache for sessions)
            'odds_*',        # Betting odds cache
        ]
        
        total_cleared = 0
        
        for i, pattern in enumerate(cleanup_patterns):
            try:
                cleared = cache.clear_pattern(pattern)
                total_cleared += cleared
                
                progress = ((i + 1) / len(cleanup_patterns)) * 100
                self.update_state(
                    state='PROGRESS',
                    meta={'message': f'Cleared {pattern} entries', 'progress': progress}
                )
                
                logger.info(f"Cleared {cleared} entries matching pattern: {pattern}")
                
            except Exception as e:
                logger.error(f"Failed to clear pattern {pattern}: {e}")
        
        # Run cache service cleanup
        cache_service.cache_cleanup(max_age_hours)
        
        logger.info(f"Cache cleanup completed: {total_cleared} entries cleared")
        
        return {
            'success': True,
            'message': f'Cache cleanup completed',
            'entries_cleared': total_cleared,
            'patterns_processed': len(cleanup_patterns)
        }
        
    except Exception as e:
        logger.error(f"Cache cleanup task failed: {e}")
        
        self.update_state(
            state='FAILURE',
            meta={'message': f'Cache cleanup failed: {str(e)}', 'error': str(e)}
        )
        
        return {
            'success': False,
            'message': f'Cache cleanup failed: {str(e)}',
            'error': str(e)
        }

@celery_app.task(name='cache_statistics')
def cache_statistics_task():
    """Task to collect and log cache statistics."""
    try:
        logger.info("Collecting cache statistics")
        
        stats = cache_service.get_cache_statistics()
        cache_size = cache_service.get_cache_size_estimate()
        
        logger.info(f"Cache statistics: {stats}")
        logger.info(f"Cache size estimate: {cache_size}")
        
        return {
            'success': True,
            'statistics': stats,
            'size_estimate': cache_size
        }
        
    except Exception as e:
        logger.error(f"Cache statistics task failed: {e}")
        return {
            'success': False,
            'error': str(e)
        }

# Convenience functions
def trigger_cache_warmup(cache_types: List[str] = None):
    """Trigger cache warm-up task."""
    return warm_cache_task.delay(cache_types)

def trigger_cache_cleanup(max_age_hours: int = 24):
    """Trigger cache cleanup task."""
    return cleanup_cache_task.delay(max_age_hours)

def trigger_cache_stats():
    """Trigger cache statistics collection."""
    return cache_statistics_task.delay()