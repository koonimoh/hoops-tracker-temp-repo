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
                'success': len(errors) == 0,
                'loaded': total_loaded,
                'errors': errors
            }
            
        except Exception as e:
            logger.error(f"Failed to load players: {e}")
            return {
                'success': False,
                'loaded': 0,
                'errors': [str(e)]
            }
    
    def load_teams(self, teams: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Load teams into database with batch processing."""
        try:
            logger.info(f"Loading {len(teams)} teams into database")
            
            if not teams:
                return {'success': True, 'loaded': 0, 'errors': []}
            
            result = bulk_insert_data(
                'teams', 
                teams, 
                on_conflict='nba_id'
            )
            
            if result['error']:
                logger.error(f"Failed to load teams: {result['error']}")
                return {
                    'success': False,
                    'loaded': 0,
                    'errors': [result['error']]
                }
            
            logger.info(f"Successfully loaded {len(teams)} teams")
            return {
                'success': True,
                'loaded': len(teams),
                'errors': []
            }
            
        except Exception as e:
            logger.error(f"Failed to load teams: {e}")
            return {
                'success': False,
                'loaded': 0,
                'errors': [str(e)]
            }
    
    def load_player_stats(self, stats: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Load player stats into database with batch processing."""
        try:
            logger.info(f"Loading {len(stats)} player stats into database")
            
            if not stats:
                return {'success': True, 'loaded': 0, 'errors': []}
            
            # Process in batches
            errors = []
            total_loaded = 0
            
            for i in range(0, len(stats), self.batch_size):
                batch = stats[i:i + self.batch_size]
                
                try:
                    result = bulk_insert_data(
                        'player_stats', 
                        batch, 
                        on_conflict='player_id,season_id,stat_key,game_date,game_id'
                    )
                    
                    if result['error']:
                        errors.append(f"Batch {i//self.batch_size + 1}: {result['error']}")
                    else:
                        total_loaded += len(batch)
                        
                except Exception as e:
                    error_msg = f"Batch {i//self.batch_size + 1}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
            
            logger.info(f"Successfully loaded {total_loaded} player stats")
            
            return {
                'success': len(errors) == 0,
                'loaded': total_loaded,
                'errors': errors
            }
            
        except Exception as e:
            logger.error(f"Failed to load player stats: {e}")
            return {
                'success': False,
                'loaded': 0,
                'errors': [str(e)]
            }
    
    @performance_monitor
    def full_etl_pipeline(self, include_stats: bool = True) -> Dict[str, Any]:
        """Execute complete ETL pipeline."""
        try:
            logger.info("Starting full ETL pipeline")
            start_time = time.time()
            
            results = {
                'teams': {'success': False, 'loaded': 0, 'errors': []},
                'players': {'success': False, 'loaded': 0, 'errors': []},
                'stats': {'success': False, 'loaded': 0, 'errors': []},
                'total_time': 0,
                'pipeline_success': False
            }
            
            # Step 1: Extract and load teams
            logger.info("Step 1: Processing teams")
            raw_teams = self.extract_all_teams()
            if raw_teams:
                transformed_teams = self.transform_teams(raw_teams)
                results['teams'] = self.load_teams(transformed_teams)
            
            # Step 2: Extract and load players
            logger.info("Step 2: Processing players")
            raw_players = self.extract_all_players()
            if raw_players:
                transformed_players = self.transform_players(raw_players)
                results['players'] = self.load_players(transformed_players)
            
            # Step 3: Extract and load stats (if requested)
            if include_stats and raw_players:
                logger.info("Step 3: Processing player stats")
                
                # Get player ID mapping
                player_id_mapping = self._create_player_id_mapping()
                
                # Extract stats for a subset of players (to avoid rate limits)
                active_player_ids = [p['id'] for p in raw_players[:50] if p.get('is_active')]
                
                if active_player_ids:
                    raw_stats = self.extract_player_stats_parallel(active_player_ids)
                    if raw_stats:
                        transformed_stats = self.transform_player_stats(raw_stats, player_id_mapping)
                        results['stats'] = self.load_player_stats(transformed_stats)
            
            # Calculate total time
            results['total_time'] = time.time() - start_time
            
            # Determine overall success
            results['pipeline_success'] = (
                results['teams']['success'] and 
                results['players']['success'] and
                (not include_stats or results['stats']['success'])
            )
            
            logger.info(f"ETL pipeline completed in {results['total_time']:.2f} seconds")
            logger.info(f"Pipeline success: {results['pipeline_success']}")
            
            # Clear relevant caches
            self._clear_etl_caches()
            
            return results
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            return {
                'teams': {'success': False, 'loaded': 0, 'errors': [str(e)]},
                'players': {'success': False, 'loaded': 0, 'errors': [str(e)]},
                'stats': {'success': False, 'loaded': 0, 'errors': [str(e)]},
                'total_time': 0,
                'pipeline_success': False
            }
    
    def incremental_stats_update(self, days_back: int = 7) -> Dict[str, Any]:
        """Perform incremental update of recent player stats."""
        try:
            logger.info(f"Starting incremental stats update for last {days_back} days")
            
            # Get active players
            players_result = supabase.table('players').select('id, nba_id').eq('is_active', True).execute()
            
            if players_result.error:
                raise Exception(f"Failed to get active players: {players_result.error}")
            
            active_players = players_result.data
            player_ids = [p['nba_id'] for p in active_players]
            
            # Create player ID mapping
            player_id_mapping = {p['nba_id']: p['id'] for p in active_players}
            
            # Extract recent stats
            raw_stats = self.extract_player_stats_parallel(player_ids[:20])  # Limit for testing
            
            if not raw_stats:
                return {'success': True, 'loaded': 0, 'message': 'No new stats to update'}
            
            # Transform and load
            transformed_stats = self.transform_player_stats(raw_stats, player_id_mapping)
            result = self.load_player_stats(transformed_stats)
            
            logger.info(f"Incremental update completed: {result['loaded']} stats loaded")
            
            return result
            
        except Exception as e:
            logger.error(f"Incremental stats update failed: {e}")
            return {
                'success': False,
                'loaded': 0,
                'errors': [str(e)]
            }
    
    def _get_current_season_id(self) -> Optional[str]:
        """Get the current season ID from database."""
        try:
            result = supabase.table('seasons').select('id').eq('is_current', True).single().execute()
            
            if result.error or not result.data:
                logger.warning("No current season found, creating default season")
                return self._create_default_season()
            
            return result.data['id']
            
        except Exception as e:
            logger.error(f"Failed to get current season ID: {e}")
            return None
    
    def _create_default_season(self) -> Optional[str]:
        """Create default current season."""
        try:
            current_year = datetime.now().year
            season_data = {
                'year': current_year,
                'season_type': 'Regular Season',
                'is_current': True,
                'start_date': f'{current_year}-10-01',
                'end_date': f'{current_year + 1}-04-30'
            }
            
            result = supabase.table('seasons').insert(season_data).execute()
            
            if result.error:
                logger.error(f"Failed to create default season: {result.error}")
                return None
            
            return result.data[0]['id']
            
        except Exception as e:
            logger.error(f"Failed to create default season: {e}")
            return None
    
    def _create_player_id_mapping(self) -> Dict[int, str]:
        """Create mapping from NBA player IDs to database UUIDs."""
        try:
            result = supabase.table('players').select('id, nba_id').execute()
            
            if result.error:
                logger.error(f"Failed to get player ID mapping: {result.error}")
                return {}
            
            return {p['nba_id']: p['id'] for p in result.data if p.get('nba_id')}
            
        except Exception as e:
            logger.error(f"Failed to create player ID mapping: {e}")
            return {}
    
    def _clear_etl_caches(self):
        """Clear caches related to ETL data."""
        try:
            cache_keys = [
                'all_players',
                'all_teams',
                'league_leaders_*',
                'team_standings_*',
                'player_stats_*'
            ]
            
            for key in cache_keys:
                cache.clear_pattern(key)
            
            logger.info("ETL-related caches cleared")
            
        except Exception as e:
            logger.error(f"Failed to clear ETL caches: {e}")
    
    def get_etl_status(self) -> Dict[str, Any]:
        """Get current ETL process status."""
        try:
            # Get table counts
            from app.db.supabase import get_db_stats
            stats = get_db_stats()
            
            # Get last update times
            last_updates = {}
            tables = ['players', 'teams', 'player_stats']
            
            for table in tables:
                try:
                    result = supabase.table(table).select('updated_at').order('updated_at', desc=True).limit(1).execute()
                    if result.data:
                        last_updates[table] = result.data[0]['updated_at']
                    else:
                        last_updates[table] = None
                except:
                    last_updates[table] = None
            
            return {
                'table_counts': stats,
                'last_updates': last_updates,
                'etl_health': 'healthy' if stats.get('players', 0) > 0 and stats.get('teams', 0) > 0 else 'needs_data'
            }
            
        except Exception as e:
            logger.error(f"Failed to get ETL status: {e}")
            return {
                'table_counts': {},
                'last_updates': {},
                'etl_health': 'error',
                'error': str(e)
            }

# Create global ETL service instance
etl_service = ETLService()

# Convenience functions
def run_full_etl(include_stats: bool = True) -> Dict[str, Any]:
    """Run full ETL pipeline."""
    return etl_service.full_etl_pipeline(include_stats)

def update_recent_stats(days_back: int = 7) -> Dict[str, Any]:
    """Update recent player statistics."""
    return etl_service.incremental_stats_update(days_back)

def get_etl_health() -> Dict[str, Any]:
    """Get ETL system health status."""
    return etl_service.get_etl_status()