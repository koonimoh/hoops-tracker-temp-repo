"""
Statistics processing service for NBA data analysis.
"""

import statistics
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from app.db.supabase import supabase
from app.core.logging import logger
from app.core.cache import cached, cache
from app.utils.decorators import performance_monitor
import numpy as np

class StatsService:
    """Advanced statistics processing and analysis service."""
    
    def __init__(self):
        self.current_season_year = 2025
        
    @cached(timeout=1800)
    @performance_monitor
    def get_league_leaders(self, stat_key: str, season_year: int = None, 
                         limit: int = 20, min_games: int = 10) -> List[Dict[str, Any]]:
        """Get league leaders for a specific statistic."""
        try:
            season_year = season_year or self.current_season_year
            logger.info(f"Getting league leaders for {stat_key} in {season_year}")
            
            # Use the database function
            result = supabase.rpc('get_league_leaders', {
                'stat_key_param': stat_key,
                'season_year_param': season_year,
                'result_limit': limit,
                'min_games': min_games
            }).execute()
            
            if result.error:
                logger.error(f"Failed to get league leaders: {result.error}")
                return []
            
            return result.data or []
            
        except Exception as e:
            logger.error(f"Error getting league leaders: {e}")
            return []
    
    @cached(timeout=1800)
    @performance_monitor
    def get_player_season_stats(self, player_id: str, season_year: int = None) -> Dict[str, Any]:
        """Get comprehensive season statistics for a player."""
        try:
            season_year = season_year or self.current_season_year
            logger.info(f"Getting season stats for player {player_id} in {season_year}")
            
            # Get season ID
            season_result = supabase.table('seasons').select('id').eq('year', season_year).eq('is_current', True).single().execute()
            
            if season_result.error or not season_result.data:
                logger.error("Current season not found")
                return {}
            
            season_id = season_result.data['id']
            
            # Get all stats for the player in this season
            stats_result = supabase.table('player_stats').select(
                'stat_key, stat_value, game_date, minutes_played'
            ).eq('player_id', player_id).eq('season_id', season_id).execute()
            
            if stats_result.error:
                logger.error(f"Failed to get player stats: {stats_result.error}")
                return {}
            
            raw_stats = stats_result.data or []
            
            if not raw_stats:
                return {}
            
            # Process and aggregate stats
            return self._aggregate_player_stats(raw_stats)
            
        except Exception as e:
            logger.error(f"Error getting player season stats: {e}")
            return {}
    
    def _aggregate_player_stats(self, raw_stats: List[Dict]) -> Dict[str, Any]:
        """Aggregate raw stats into season averages and totals."""
        try:
            stats_by_key = {}
            games_played = set()
            
            # Group stats by key and collect game dates
            for stat in raw_stats:
                key = stat['stat_key']
                value = float(stat['stat_value'])
                game_date = stat['game_date']
                
                if key not in stats_by_key:
                    stats_by_key[key] = []
                
                stats_by_key[key].append(value)
                
                if game_date:
                    games_played.add(game_date)
            
            total_games = len(games_played)
            
            if total_games == 0:
                return {}
            
            # Calculate averages and totals
            aggregated = {
                'games_played': total_games,
                'totals': {},
                'averages': {},
                'per_36': {},
                'shooting_percentages': {}
            }
            
            for key, values in stats_by_key.items():
                total = sum(values)
                average = total / total_games
                
                aggregated['totals'][key] = round(total, 1)
                aggregated['averages'][key] = round(average, 1)
            
            # Calculate shooting percentages
            self._calculate_shooting_percentages(aggregated, stats_by_key)
            
            # Calculate per-36 minute stats
            if 'min' in stats_by_key:
                self._calculate_per36_stats(aggregated, stats_by_key)
            
            # Calculate advanced stats
            self._calculate_advanced_stats(aggregated, stats_by_key)
            
            return aggregated
            
        except Exception as e:
            logger.error(f"Error aggregating player stats: {e}")
            return {}
    
    def _calculate_shooting_percentages(self, aggregated: Dict, stats_by_key: Dict):
        """Calculate shooting percentages."""
        try:
            shooting_stats = {
                'fg_pct': ('fg_made', 'fg_att'),
                'fg3_pct': ('fg3_made', 'fg3_att'),
                'ft_pct': ('ft_made', 'ft_att')
            }
            
            for pct_key, (made_key, att_key) in shooting_stats.items():
                if made_key in stats_by_key and att_key in stats_by_key:
                    total_made = sum(stats_by_key[made_key])
                    total_attempted = sum(stats_by_key[att_key])
                    
                    if total_attempted > 0:
                        percentage = (total_made / total_attempted) * 100
                        aggregated['shooting_percentages'][pct_key] = round(percentage, 1)
                    else:
                        aggregated['shooting_percentages'][pct_key] = 0.0
            
        except Exception as e# Remaining Skeleton Files for Hoops Tracker
