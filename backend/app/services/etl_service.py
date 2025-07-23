"""
ETL (Extract, Transform, Load) service for NBA data processing.
"""

import asyncio
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from app.services.nba_api_service import nba_api
from app.db.supabase import supabase, bulk_insert_data
from app.core.logging import logger
from app.core.cache import cache
from app.utils.parallel_executor import parallel_executor
from app.utils.decorators import performance_monitor, retry_on_failure

class ETLService:
    """Enhanced ETL service with parallel processing and error handling."""
    
    def __init__(self):
        self.batch_size = 100
        self.max_workers = 8
        self.retry_attempts = 3
        self.rate_limit_delay = 1.0
        
    @performance_monitor
    @retry_on_failure(max_attempts=3, delay=2)
    def extract_all_players(self) -> List[Dict[str, Any]]:
        """Extract all NBA players from the API."""
        try:
            logger.info("Starting player extraction from NBA API")
            
            # Get all players from NBA API
            players = nba_api.get_all_players()
            
            if not players:
                logger.warning("No players returned from NBA API")
                return []
            
            logger.info(f"Extracted {len(players)} players from NBA API")
            return players
            
        except Exception as e:
            logger.error(f"Failed to extract players: {e}")
            raise
    
    @performance_monitor
    def extract_all_teams(self) -> List[Dict[str, Any]]:
        """Extract all NBA teams from the API."""
        try:
            logger.info("Starting team extraction from NBA API")
            
            teams = nba_api.get_all_teams()
            
            if not teams:
                logger.warning("No teams returned from NBA API")
                return []
            
            logger.info(f"Extracted {len(teams)} teams from NBA API")
            return teams
            
        except Exception as e:
            logger.error(f"Failed to extract teams: {e}")
            raise
    
    def extract_player_stats_parallel(self, player_ids: List[int], 
                                    season: str = "2024-25") -> List[Dict[str, Any]]:
        """Extract player statistics in parallel."""
        try:
            logger.info(f"Starting parallel extraction of stats for {len(player_ids)} players")
            
            def extract_single_player_stats(player_id: int) -> Dict[str, Any]:
                """Extract stats for a single player."""
                try:
                    time.sleep(self.rate_limit_delay)  # Rate limiting
                    stats = nba_api.get_player_game_log(player_id, season)
                    return {
                        'player_id': player_id,
                        'stats': stats,
                        'success': True,
                        'error': None
                    }
                except Exception as e:
                    logger.error(f"Failed to extract stats for player {player_id}: {e}")
                    return {
                        'player_id': player_id,
                        'stats': [],
                        'success': False,
                        'error': str(e)
                    }
            
            # Use parallel executor for I/O bound operations
            results = parallel_executor.execute_in_threads(
                extract_single_player_stats, 
                player_ids
            )
            
            successful_results = [r for r in results if r['result']['success']]
            failed_results = [r for r in results if not r['result']['success']]
            
            logger.info(f"Successfully extracted stats for {len(successful_results)} players")
            if failed_results:
                logger.warning(f"Failed to extract stats for {len(failed_results)} players")
            
            # Flatten the stats data
            all_stats = []
            for result in successful_results:
                player_stats = result['result']['stats']
                all_stats.extend(player_stats)
            
            return all_stats
            
        except Exception as e:
            logger.error(f"Failed to extract player stats in parallel: {e}")
            return []
    
    def transform_players(self, raw_players: List[Dict]) -> List[Dict[str, Any]]:
        """Transform raw player data into database format."""
        try:
            logger.info(f"Transforming {len(raw_players)} players")
            
            transformed = []
            for player in raw_players:
                try:
                    transformed_player = {
                        'nba_id': player.get('id'),
                        'name': player.get('full_name', '').strip(),
                        'first_name': player.get('first_name', '').strip(),
                        'last_name': player.get('last_name', '').strip(),
                        'is_active': player.get('is_active', True),
                        'created_at': datetime.utcnow().isoformat(),
                        'updated_at': datetime.utcnow().isoformat()
                    }
                    
                    # Only add if we have required fields
                    if transformed_player['nba_id'] and transformed_player['name']:
                        transformed.append(transformed_player)
                    else:
                        logger.warning(f"Skipping player with missing data: {player}")
                        
                except Exception as e:
                    logger.error(f"Error transforming player {player}: {e}")
                    continue
            
            logger.info(f"Successfully transformed {len(transformed)} players")
            return transformed
            
        except Exception as e:
            logger.error(f"Failed to transform players: {e}")
            return []
    
    def transform_teams(self, raw_teams: List[Dict]) -> List[Dict[str, Any]]:
        """Transform raw team data into database format."""
        try:
            logger.info(f"Transforming {len(raw_teams)} teams")
            
            transformed = []
            for team in raw_teams:
                try:
                    transformed_team = {
                        'nba_id': team.get('id'),
                        'name': team.get('full_name', '').strip(),
                        'abbreviation': team.get('abbreviation', '').strip(),
                        'nickname': team.get('nickname', '').strip(),
                        'city': team.get('city', '').strip(),
                        'state': team.get('state', '').strip(),
                        'year_founded': team.get('year_founded'),
                        'created_at': datetime.utcnow().isoformat(),
                        'updated_at': datetime.utcnow().isoformat()
                    }
                    
                    # Only add if we have required fields
                    if transformed_team['nba_id'] and transformed_team['name']:
                        transformed.append(transformed_team)
                    else:
                        logger.warning(f"Skipping team with missing data: {team}")
                        
                except Exception as e:
                    logger.error(f"Error transforming team {team}: {e}")
                    continue
            
            logger.info(f"Successfully transformed {len(transformed)} teams")
            return transformed
            
        except Exception as e:
            logger.error(f"Failed to transform teams: {e}")
            return []
    
    def transform_player_stats(self, raw_stats: List[Dict], 
                             player_id_mapping: Dict[int, str]) -> List[Dict[str, Any]]:
        """Transform raw player stats into database format."""
        try:
            logger.info(f"Transforming {len(raw_stats)} player stats")
            
            transformed = []
            current_season_id = self._get_current_season_id()
            
            for stat in raw_stats:
                try:
                    nba_player_id = stat.get('PLAYER_ID')
                    player_uuid = player_id_mapping.get(nba_player_id)
                    
                    if not player_uuid:
                        continue
                    
                    # Extract individual stats
                    stat_entries = [
                        {'key': 'pts', 'value': stat.get('PTS', 0)},
                        {'key': 'reb', 'value': stat.get('REB', 0)},
                        {'key': 'ast', 'value': stat.get('AST', 0)},
                        {'key': 'stl', 'value': stat.get('STL', 0)},
                        {'key': 'blk', 'value': stat.get('BLK', 0)},
                        {'key': 'tov', 'value': stat.get('TOV', 0)},
                        {'key': 'pf', 'value': stat.get('PF', 0)},
                        {'key': 'min', 'value': stat.get('MIN', 0)},
                        {'key': 'fg_made', 'value': stat.get('FGM', 0)},
                        {'key': 'fg_att', 'value': stat.get('FGA', 0)},
                        {'key': 'fg3_made', 'value': stat.get('FG3M', 0)},
                        {'key': 'fg3_att', 'value': stat.get('FG3A', 0)},
                        {'key': 'ft_made', 'value': stat.get('FTM', 0)},
                        {'key': 'ft_att', 'value': stat.get('FTA', 0)},
                    ]
                    
                    game_date = stat.get('GAME_DATE')
                    if isinstance(game_date, str):
                        try:
                            game_date = datetime.strptime(game_date, '%Y-%m-%d').date()
                        except:
                            game_date = None
                    
                    for entry in stat_entries:
                        if entry['value'] is not None:
                            transformed_stat = {
                                'player_id': player_uuid,
                                'season_id': current_season_id,
                                'game_id': stat.get('GAME_ID'),
                                'game_date': game_date.isoformat() if game_date else None,
                                'stat_key': entry['key'],
                                'stat_value': float(entry['value']),
                                'minutes_played': stat.get('MIN'),
                                'plus_minus': stat.get('PLUS_MINUS'),
                                'created_at': datetime.utcnow().isoformat(),
                                'updated_at': datetime.utcnow().isoformat()
                            }
                            transformed.append(transformed_stat)
                            
                except Exception as e:
                    logger.error(f"Error transforming stat {stat}: {e}")
                    continue
            
            logger.info(f"Successfully transformed {len(transformed)} player stats")
            return transformed
            
        except Exception as e:
            logger.error(f"Failed to transform player stats: {e}")
            return []
    
    def load_players(self, players: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Load players into database with batch processing."""
        try:
            logger.info(f"Loading {len(players)} players into database")
            
            if not players:
                return {'success': True, 'loaded': 0, 'errors': []}
            
            # Process in batches
            results = []
            errors = []
            total_loaded = 0
            
            for i in range(0, len(players), self.batch_size):
                batch = players[i:i + self.batch_size]
                
                try:
                    result = bulk_insert_data(
                        'players', 
                        batch, 
                        on_conflict='nba_id'
                    )
                    
                    if result['error']:
                        errors.append(f"Batch {i//self.batch_size + 1}: {result['error']}")
                    else:
                        total_loaded += len(batch)
                        
                except Exception as e:
                    error_msg = f"Batch {i//self.batch_size + 1}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
            
            logger.info(f"Successfully loaded {total_loaded} players")
            
            return {
                'success': len(errors)