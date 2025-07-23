"""
Celery application configuration for background tasks.
"""

from celery import Celery
from celery.schedules import crontab
from app.core.config import settings
from app.core.logging import logger
import os

# Create Celery app
celery_app = Celery(
    "hoops_tracker",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=[
        'app.tasks.fetch_data',
        'app.tasks.cache_warmup'
    ]
)

# Celery configuration
celery_app.conf.update(
    # Task serialization
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    
    # Timezone
    timezone='UTC',
    enable_utc=True,
    
    # Task routing
    task_routes={
        'app.tasks.fetch_data.*': {'queue': 'data_processing'},
        'app.tasks.cache_warmup.*': {'queue': 'cache_management'},
    },
    
    # Worker configuration
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    
    # Task execution
    task_always_eager=False,
    task_eager_propagates=True,
    
    # Result backend settings
    result_expires=3600,  # 1 hour
    
    # Error handling
    task_reject_on_worker_lost=True,
    task_ignore_result=False,
)

# Periodic tasks schedule
# Periodic tasks schedule
celery_app.conf.beat_schedule = {
    # Fetch NBA stats every hour during season
    'fetch-nba-stats': {
        'task': 'app.tasks.fetch_data.fetch_stats_task',
        'schedule': crontab(minute=0),  # Every hour
        'args': (True,)  # Incremental update
    },
    
    # Daily ETL at 3 AM
    'daily-etl': {
        'task': 'app.tasks.fetch_data.daily_etl_task',
        'schedule': crontab(hour=3, minute=0),
    },
    
    # Cache warmup every 4 hours
    'warm-cache': {
        'task': 'app.tasks.cache_warmup.warm_cache_task',
        'schedule': crontab(minute=0, hour='*/4'),
    },
    
    # Evaluate bets daily at 10 PM
    'evaluate-bets': {
        'task': 'app.tasks.fetch_data.evaluate_bets_task', 
        'schedule': crontab(hour=22, minute=0),
    },
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