"""
Threading utilities for safe concurrent operations.
"""

import threading
import time
import queue
import statistics
import numpy as np
from typing import Callable, Any, List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from datetime import datetime, timedelta
from dataclasses import dataclass
import asyncio

# Import logging (adjust the import based on your project structure)
try:
    from app.core.logging import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

# Import supabase client (adjust based on your project structure)
try:
    from app.core.database import supabase
except ImportError:
    # Mock supabase for now - replace with actual import
    class MockSupabase:
        def table(self, name):
            return self
        def select(self, *args):
            return self
        def eq(self, *args):
            return self
        def single(self):
            return self
        def execute(self):
            return type('obj', (object,), {'error': None, 'data': []})()
        def rpc(self, *args, **kwargs):
            return self
    supabase = MockSupabase()

# Mock decorators if not available
def cached(timeout=3600):
    def decorator(func):
        return func
    return decorator

def performance_monitor(func):
    return func

class ThreadSafeCounter:
    """Thread-safe counter implementation."""
    
    def __init__(self, initial_value: int = 0):
        self._value = initial_value
        self._lock = threading.Lock()
    
    def increment(self, step: int = 1) -> int:
        """Increment counter and return new value."""
        with self._lock:
            self._value += step
            return self._value
    
    def decrement(self, step: int = 1) -> int:
        """Decrement counter and return new value.""" 
        with self._lock:
            self._value -= step
            return self._value
    
    def get_value(self) -> int:
        """Get current counter value."""
        with self._lock:
            return self._value
    
    def set_value(self, value: int) -> int:
        """Set counter value and return new value."""
        with self._lock:
            self._value = value
            return self._value
    
    def reset(self) -> int:
        """Reset counter to zero and return previous value."""
        with self._lock:
            old_value = self._value
            self._value = 0
            return old_value

class ThreadSafeDict:
    """Thread-safe dictionary implementation."""
    
    def __init__(self, initial_data: Dict = None):
        self._data = initial_data or {}
        self._lock = threading.RLock()  # Re-entrant lock
    
    def get(self, key: Any, default: Any = None) -> Any:
        """Get value for key."""
        with self._lock:
            return self._data.get(key, default)
    
    def set(self, key: Any, value: Any) -> None:
        """Set value for key."""
        with self._lock:
            self._data[key] = value
    
    def delete(self, key: Any) -> bool:
        """Delete key if exists. Returns True if key was present."""
        with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False
    
    def update(self, other_dict: Dict) -> None:
        """Update with another dictionary."""
        with self._lock:
            self._data.update(other_dict)
    
    def keys(self) -> List[Any]:
        """Get list of keys."""
        with self._lock:
            return list(self._data.keys())
    
    def values(self) -> List[Any]:
        """Get list of values."""
        with self._lock:
            return list(self._data.values())
    
    def items(self) -> List[tuple]:
        """Get list of key-value pairs."""
        with self._lock:
            return list(self._data.items())
    
    def size(self) -> int:
        """Get dictionary size."""
        with self._lock:
            return len(self._data)

@dataclass
class TaskResult:
    """Result of a threaded task execution."""
    task_id: str
    success: bool
    result: Any = None
    error: Optional[str] = None
    duration: float = 0.0
    thread_id: Optional[int] = None

class ThreadPoolManager:
    """Advanced thread pool manager with monitoring and error handling."""
    
    def __init__(self, max_workers: int = 10, name: str = "ThreadPool"):
        self.max_workers = max_workers
        self.name = name
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=name)
        self.active_tasks = ThreadSafeCounter()
        self.completed_tasks = ThreadSafeCounter()
        self.failed_tasks = ThreadSafeCounter()
        self.task_results = ThreadSafeDict()
        self._shutdown = False
    
    def submit_task(self, task_id: str, func: Callable[..., Any], *args, **kwargs) -> Future:
        """Submit a task to the thread pool."""
        if self._shutdown:
            raise RuntimeError("ThreadPoolManager has been shut down")
        
        def wrapper():
            start_time = time.time()
            thread_id = threading.get_ident()
            
            try:
                self.active_tasks.increment()
                logger.info(f"Starting task {task_id} on thread {thread_id}")
                
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                
                task_result = TaskResult(
                    task_id=task_id,
                    success=True,
                    result=result,
                    duration=duration,
                    thread_id=thread_id
                )
                
                self.task_results.set(task_id, task_result)
                self.completed_tasks.increment()
                logger.info(f"Task {task_id} completed successfully in {duration:.2f}s")
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                error_msg = str(e)
                
                task_result = TaskResult(
                    task_id=task_id,
                    success=False,
                    error=error_msg,
                    duration=duration,
                    thread_id=thread_id
                )
                
                self.task_results.set(task_id, task_result)
                self.failed_tasks.increment()
                logger.error(f"Task {task_id} failed after {duration:.2f}s: {error_msg}")
                
                raise
                
            finally:
                self.active_tasks.decrement()
        
        return self.executor.submit(wrapper)
    
    def get_task_result(self, task_id: str) -> Optional[TaskResult]:
        """Get result of a completed task."""
        return self.task_results.get(task_id)
    
    def get_stats(self) -> Dict[str, int]:
        """Get thread pool statistics."""
        return {
            'active_tasks': self.active_tasks.get_value(),
            'completed_tasks': self.completed_tasks.get_value(),
            'failed_tasks': self.failed_tasks.get_value(),
            'max_workers': self.max_workers
        }
    
    def shutdown(self, wait: bool = True):
        """Shutdown the thread pool."""
        self._shutdown = True
        self.executor.shutdown(wait=wait)
        logger.info(f"ThreadPoolManager '{self.name}' shut down")

class AsyncTaskManager:
    """Manage asynchronous tasks with coordination."""
    
    def __init__(self):
        self.tasks = {}
        self.results = ThreadSafeDict()
        self._lock = threading.Lock()
    
    async def run_task(self, task_id: str, coro):
        """Run an async task."""
        try:
            result = await coro
            self.results.set(task_id, {'success': True, 'result': result})
            return result
        except Exception as e:
            self.results.set(task_id, {'success': False, 'error': str(e)})
            raise
    
    def get_result(self, task_id: str):
        """Get result of an async task."""
        return self.results.get(task_id)

class StatsService:
    """Basketball statistics service with threading support."""
    
    def __init__(self):
        self.current_season_year = datetime.now().year
        self.thread_pool = ThreadPoolManager(max_workers=5, name="StatsService")
    
    def _aggregate_season_stats(self, stats_data: List[Dict]) -> Dict[str, Any]:
        """Aggregate statistics for a season."""
        try:
            if not stats_data:
                return {}
            
            # Group stats by key
            stats_by_key = {}
            for stat in stats_data:
                key = stat['stat_key']
                value = float(stat['stat_value'])
                
                if key not in stats_by_key:
                    stats_by_key[key] = []
                stats_by_key[key].append(value)
            
            # Calculate aggregations
            aggregated = {
                'totals': {},
                'averages': {},
                'games_played': len(set(stat['game_date'] for stat in stats_data)),
                'shooting': {},
                'per_36': {},
                'advanced': {}
            }
            
            # Calculate totals and averages
            for stat_key, values in stats_by_key.items():
                aggregated['totals'][stat_key] = sum(values)
                aggregated['averages'][stat_key] = round(sum(values) / len(values), 1)
            
            # Calculate additional metrics
            self._calculate_shooting_percentages(aggregated, stats_by_key)
            self._calculate_per36_stats(aggregated, stats_by_key)
            self._calculate_advanced_stats(aggregated, stats_by_key)
            
            return aggregated
            
        except Exception as e:
            logger.error(f"Error aggregating season stats: {e}")
            return {}
    
    def _calculate_shooting_percentages(self, aggregated: Dict, stats_by_key: Dict):
        """Calculate shooting percentages."""
        try:
            # Field Goal Percentage
            if 'fg_made' in stats_by_key and 'fg_att' in stats_by_key:
                total_made = sum(stats_by_key['fg_made'])
                total_att = sum(stats_by_key['fg_att'])
                
                if total_att > 0:
                    fg_pct = (total_made / total_att) * 100
                    aggregated['shooting']['fg_pct'] = round(fg_pct, 1)
            
            # Free Throw Percentage
            if 'ft_made' in stats_by_key and 'ft_att' in stats_by_key:
                total_made = sum(stats_by_key['ft_made'])
                total_att = sum(stats_by_key['ft_att'])
                
                if total_att > 0:
                    ft_pct = (total_made / total_att) * 100
                    aggregated['shooting']['ft_pct'] = round(ft_pct, 1)
            
            # Three Point Percentage
            if 'fg3_made' in stats_by_key and 'fg3_att' in stats_by_key:
                total_made = sum(stats_by_key['fg3_made'])
                total_att = sum(stats_by_key['fg3_att'])
                
                if total_att > 0:
                    fg3_pct = (total_made / total_att) * 100
                    aggregated['shooting']['fg3_pct'] = round(fg3_pct, 1)
                    
        except Exception as e:
            logger.error(f"Error calculating shooting percentages: {e}")
    
    def _calculate_per36_stats(self, aggregated: Dict, stats_by_key: Dict):
        """Calculate per-36 minute statistics."""
        try:
            total_minutes = sum(stats_by_key.get('min', [0]))
            
            if total_minutes == 0:
                return
            
            per36_stats = ['pts', 'reb', 'ast', 'stl', 'blk', 'tov']
            
            for stat_key in per36_stats:
                if stat_key in stats_by_key:
                    total_stat = sum(stats_by_key[stat_key])
                    per36_value = (total_stat / total_minutes) * 36
                    aggregated['per_36'][stat_key] = round(per36_value, 1)
            
        except Exception as e:
            logger.error(f"Error calculating per-36 stats: {e}")
    
    def _calculate_advanced_stats(self, aggregated: Dict, stats_by_key: Dict):
        """Calculate advanced statistics."""
        try:
            # Player Efficiency Rating (simplified)
            required_stats = ['pts', 'reb', 'ast', 'stl', 'blk', 'tov', 'pf', 'min']
            if all(key in stats_by_key for key in required_stats):
                total_minutes = sum(stats_by_key['min'])
                
                if total_minutes > 0:
                    per = (
                        sum(stats_by_key['pts']) +
                        sum(stats_by_key['reb']) +
                        sum(stats_by_key['ast']) +
                        sum(stats_by_key['stl']) +
                        sum(stats_by_key['blk']) -
                        sum(stats_by_key['tov']) -
                        sum(stats_by_key['pf'])
                    ) / total_minutes * 36
                    
                    aggregated['advanced'] = {
                        'per': round(max(0, per), 1)
                    }
            
            # True Shooting Percentage
            if all(key in stats_by_key for key in ['pts', 'fg_att', 'ft_att']):
                total_points = sum(stats_by_key['pts'])
                total_fga = sum(stats_by_key['fg_att'])
                total_fta = sum(stats_by_key['ft_att'])
                
                true_shot_attempts = total_fga + (0.44 * total_fta)
                
                if true_shot_attempts > 0:
                    ts_pct = (total_points / (2 * true_shot_attempts)) * 100
                    aggregated['advanced'] = aggregated.get('advanced', {})
                    aggregated['advanced']['ts_pct'] = round(ts_pct, 1)
            
        except Exception as e:
            logger.error(f"Error calculating advanced stats: {e}")
    
    @cached(timeout=3600)
    def get_player_season_stats(self, player_id: str, season_year: int = None) -> Dict[str, Any]:
        """Get comprehensive player statistics for a season."""
        try:
            season_year = season_year or self.current_season_year
            logger.info(f"Getting season stats for player {player_id} in {season_year}")
            
            # Get season ID
            season_result = supabase.table('seasons').select('id').eq('year', season_year).single().execute()
            
            if season_result.error:
                logger.error(f"Failed to get season: {season_result.error}")
                return {}
            
            season_id = season_result.data['id']
            
            # Get player stats for the season
            stats_result = supabase.table('player_stats').select(
                'stat_key, stat_value, game_date'
            ).eq('player_id', player_id).eq('season_id', season_id).execute()
            
            if stats_result.error:
                logger.error(f"Failed to get player stats: {stats_result.error}")
                return {}
            
            return self._aggregate_season_stats(stats_result.data or [])
            
        except Exception as e:
            logger.error(f"Error getting player season stats: {e}")
            return {}
    
    @cached(timeout=1800)
    def get_league_leaders(self, stat_key: str, season_year: int = None, limit: int = 20) -> List[Dict[str, Any]]:
        """Get league leaders for a specific statistic."""
        try:
            season_year = season_year or self.current_season_year
            logger.info(f"Getting league leaders for {stat_key} in {season_year}")
            
            # Use RPC function if available
            result = supabase.rpc('get_league_leaders', {
                'stat_key_param': stat_key,
                'season_year_param': season_year,
                'result_limit': limit,
                'min_games': 10
            }).execute()
            
            if result.error:
                logger.error(f"Failed to get league leaders: {result.error}")
                return []
            
            return result.data or []
            
        except Exception as e:
            logger.error(f"Error getting league leaders: {e}")
            return []
    
    @cached(timeout=3600)
    def get_team_stats(self, team_id: str, season_year: int = None) -> Dict[str, Any]:
        """Get comprehensive team statistics."""
        try:
            season_year = season_year or self.current_season_year
            logger.info(f"Getting team stats for {team_id} in {season_year}")
            
            # Get team's players
            players_result = supabase.table('players').select('id').eq('team_id', team_id).execute()
            
            if players_result.error:
                logger.error(f"Failed to get team players: {players_result.error}")
                return {}
            
            player_ids = [p['id'] for p in players_result.data or []]
            
            if not player_ids:
                return {}
            
            # Get aggregated team stats
            team_stats = {}
            
            for player_id in player_ids:
                player_stats = self.get_player_season_stats(player_id, season_year)
                
                # Aggregate to team level
                for category in ['totals', 'averages']:
                    if category not in team_stats:
                        team_stats[category] = {}
                    
                    for stat_key, value in player_stats.get(category, {}).items():
                        if stat_key not in team_stats[category]:
                            team_stats[category][stat_key] = 0
                        
                        team_stats[category][stat_key] += value
            
            # Calculate team averages
            num_players = len(player_ids)
            if num_players > 0:
                for stat_key in team_stats.get('averages', {}):
                    team_stats['averages'][stat_key] = round(
                        team_stats['averages'][stat_key] / num_players, 1
                    )
            
            return team_stats
            
        except Exception as e:
            logger.error(f"Error getting team stats: {e}")
            return {}
    
    @performance_monitor
    def get_trends(self, player_id: str, stat_key: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get trending statistics for a player over time."""
        try:
            logger.info(f"Getting {days}-day trends for player {player_id}, stat {stat_key}")
            
            cutoff_date = datetime.now() - timedelta(days=days)
            
            # Get recent stats
            result = supabase.table('player_stats').select(
                'stat_value, game_date'
            ).eq('player_id', player_id).eq('stat_key', stat_key).gte(
                'game_date', cutoff_date.date().isoformat()
            ).order('game_date').execute()
            
            if result.error:
                logger.error(f"Failed to get trend data: {result.error}")
                return []
            
            stats_data = result.data or []
            
            if not stats_data:
                return []
            
            # Calculate rolling averages
            return self._calculate_rolling_trends(stats_data)
            
        except Exception as e:
            logger.error(f"Error getting trends: {e}")
            return []
    
    def _calculate_rolling_trends(self, stats_data: List[Dict], window: int = 5) -> List[Dict[str, Any]]:
        """Calculate rolling averages for trend analysis."""
        try:
            trends = []
            values = [float(stat['stat_value']) for stat in stats_data]
            
            for i, stat in enumerate(stats_data):
                # Calculate rolling average
                start_idx = max(0, i - window + 1)
                window_values = values[start_idx:i + 1]
                rolling_avg = statistics.mean(window_values)
                
                trends.append({
                    'date': stat['game_date'],
                    'value': stat['stat_value'],
                    'rolling_avg': round(rolling_avg, 1),
                    'games_in_window': len(window_values)
                })
            
            return trends
            
        except Exception as e:
            logger.error(f"Error calculating rolling trends: {e}")
            return []
    
    def compute_per36(self, stats: List[Dict]) -> List[Dict]:
        """Calculate per-36 minute statistics."""
        try:
            per36_stats = []
            
            for stat in stats:
                minutes = stat.get('minutes_played', 0)
                
                if minutes and minutes > 0:
                    per36_value = (stat['stat_value'] / minutes) * 36
                else:
                    per36_value = 0
                
                per36_stat = stat.copy()
                per36_stat['per36'] = round(per36_value, 1)
                per36_stats.append(per36_stat)
            
            return per36_stats
            
        except Exception as e:
            logger.error(f"Error computing per-36 stats: {e}")
            return stats
    
    def rolling_average(self, values: List[float], window: int = 5) -> List[float]:
        """Calculate rolling averages."""
        try:
            if not values:
                return []
            
            rolling_avgs = []
            
            for i in range(len(values)):
                start_idx = max(0, i - window + 1)
                window_values = values[start_idx:i + 1]
                avg = statistics.mean(window_values)
                rolling_avgs.append(round(avg, 1))
            
            return rolling_avgs
            
        except Exception as e:
            logger.error(f"Error calculating rolling average: {e}")
            return values
    
    def calculate_efficiency_rating(self, stats: Dict[str, float]) -> float:
        """Calculate player efficiency rating."""
        try:
            # Basic PER calculation
            per = (
                stats.get('pts', 0) +
                stats.get('reb', 0) +
                stats.get('ast', 0) +
                stats.get('stl', 0) +
                stats.get('blk', 0) -
                stats.get('tov', 0) -
                stats.get('pf', 0)
            )
            
            minutes = stats.get('min', 1)
            
            if minutes > 0:
                per_36 = (per / minutes) * 36
                return round(max(0, per_36), 1)
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error calculating efficiency rating: {e}")
            return 0.0
    
    @cached(timeout=3600)
    def get_stat_distribution(self, stat_key: str, season_year: int = None) -> Dict[str, Any]:
        """Get statistical distribution for a stat across all players."""
        try:
            season_year = season_year or self.current_season_year
            
            # Get all player averages for this stat
            result = supabase.rpc('get_league_leaders', {
                'stat_key_param': stat_key,
                'season_year_param': season_year,
                'result_limit': 500,
                'min_games': 1
            }).execute()
            
            if result.error or not result.data:
                return {}
            
            values = [float(player['avg_value']) for player in result.data]
            
            if not values:
                return {}
            
            # Calculate distribution statistics
            distribution = {
                'count': len(values),
                'mean': round(statistics.mean(values), 2),
                'median': round(statistics.median(values), 2),
                'std_dev': round(statistics.stdev(values) if len(values) > 1 else 0, 2),
                'min': round(min(values), 2),
                'max': round(max(values), 2),
                'percentiles': {
                    '25th': round(np.percentile(values, 25), 2),
                    '50th': round(np.percentile(values, 50), 2),
                    '75th': round(np.percentile(values, 75), 2),
                    '90th': round(np.percentile(values, 90), 2),
                    '95th': round(np.percentile(values, 95), 2)
                }
            }
            
            return distribution
            
        except Exception as e:
            logger.error(f"Error getting stat distribution: {e}")
            return {}
    
    def compare_players(self, player_ids: List[str], stats: List[str], 
                       season_year: int = None) -> Dict[str, Any]:
        """Compare multiple players across specified statistics."""
        try:
            season_year = season_year or self.current_season_year
            comparison = {
                'players': {},
                'stat_comparison': {},
                'rankings': {}
            }
            
            # Get stats for each player
            for player_id in player_ids:
                player_stats = self.get_player_season_stats(player_id, season_year)
                
                if player_stats:
                    # Get player info
                    player_result = supabase.table('players').select(
                        'name, position, teams(name, abbreviation)'
                    ).eq('id', player_id).single().execute()
                    
                    if not player_result.error and player_result.data:
                        comparison['players'][player_id] = {
                            'info': player_result.data,
                            'stats': player_stats
                        }
            
            # Compare specific stats
            for stat in stats:
                comparison['stat_comparison'][stat] = {}
                stat_values = []
                
                for player_id, data in comparison['players'].items():
                    value = data['stats'].get('averages', {}).get(stat, 0)
                    comparison['stat_comparison'][stat][player_id] = value
                    stat_values.append((player_id, value))
                
                # Rank players for this stat
                stat_values.sort(key=lambda x: x[1], reverse=True)
                comparison['rankings'][stat] = stat_values
            
            return comparison
            
        except Exception as e:
            logger.error(f"Error comparing players: {e}")
            return {}
    
    def get_stat_leaders_by_position(self, stat_key: str, position: str, 
                                   limit: int = 10) -> List[Dict[str, Any]]:
        """Get stat leaders filtered by position."""
        try:
            # Get all league leaders first
            all_leaders = self.get_league_leaders(stat_key, limit=100)
            
            # Filter by position
            position_leaders = []
            
            for leader in all_leaders:
                # Get player position
                player_result = supabase.table('players').select('position').eq(
                    'id', leader['player_id']
                ).single().execute()
                
                if (not player_result.error and 
                    player_result.data and 
                    player_result.data.get('position') == position):
                    position_leaders.append(leader)
                
                if len(position_leaders) >= limit:
                    break
            
            return position_leaders
            
        except Exception as e:
            logger.error(f"Error getting position leaders: {e}")
            return []

# Create global stats service instance
stats_service = StatsService()

# Convenience functions
def get_league_leaders(stat_key: str, season_year: int = None, limit: int = 20) -> List[Dict[str, Any]]:
    """Get league leaders for a statistic."""
    return stats_service.get_league_leaders(stat_key, season_year, limit)

def get_player_stats(player_id: str, season_year: int = None) -> Dict[str, Any]:
    """Get comprehensive player statistics."""
    return stats_service.get_player_season_stats(player_id, season_year)

def get_player_trends(player_id: str, stat_key: str, days: int = 30) -> List[Dict[str, Any]]:
    """Get player performance trends."""
    return stats_service.get_trends(player_id, stat_key, days)

def compare_players(player_ids: List[str], stats: List[str]) -> Dict[str, Any]:
    """Compare multiple players."""
    return stats_service.compare_players(player_ids, stats)