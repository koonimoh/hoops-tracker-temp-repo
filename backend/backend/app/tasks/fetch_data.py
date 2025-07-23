"""
Data fetching and processing tasks using Celery.
"""

from celery import current_task
from app.tasks.celery_app import celery_app
from app.services.etl_service import etl_service
from app.services.bets_service import betting_service
from app.services.nba_api_service import nba_api
from app.core.logging import logger
from app.core.cache import cache
from typing import Dict, Any
import time

@celery_app.task(bind=True, name='fetch_stats')
def fetch_stats_task(self, incremental: bool = True):
    """
    Celery task to fetch NBA statistics.
    
    Args:
        incremental: If True, only fetch recent data. If False, full refresh.
    """
    try:
        logger.info(f"Starting fetch stats task (incremental={incremental})")
        
        # Update task state
        self.update_state(
            state='PROGRESS',
            meta={'message': 'Starting data fetch...', 'progress': 0}
        )
        
        if incremental:
            # Incremental update (last 7 days)
            result = etl_service.incremental_stats_update(days_back=7)
        else:
            # Full ETL pipeline
            result = etl_service.full_etl_pipeline(include_stats=True)
        
        # Update task state
        self.update_state(
            state='PROGRESS', 
            meta={'message': 'Data fetch completed', 'progress': 100}
        )
        
        logger.info(f"Fetch stats task completed: {result}")
        
        # Clear related caches
        cache.clear_pattern('league_leaders_*')
        cache.clear_pattern('player_stats_*')
        
        return {
            'success': result.get('pipeline_success', result.get('success', False)),
            'message': 'Data fetch completed successfully',
            'details': result
        }
        
    except Exception as e:
        logger.error(f"Fetch stats task failed: {e}")
        
        self.update_state(
            state='FAILURE',
            meta={'message': f'Task failed: {str(e)}', 'error': str(e)}
        )
        
        return {
            'success': False,
            'message': f'Data fetch failed: {str(e)}',
            'error': str(e)
        }

@celery_app.task(bind=True, name='evaluate_bets')
def evaluate_bets_task(self):
    """Celery task to evaluate pending bets."""
    try:
        logger.info("Starting evaluate bets task")
        
        self.update_state(
            state='PROGRESS',
            meta={'message': 'Evaluating pending bets...', 'progress': 0}
        )
        
        # Evaluate all pending bets
        result = betting_service.auto_resolve_pending_bets()
        
        self.update_state(
            state='PROGRESS',
            meta={'message': 'Bet evaluation completed', 'progress': 100}
        )
        
        logger.info(f"Evaluate bets task completed: {result}")
        
        return {
            'success': True,
            'message': 'Bet evaluation completed',
            'resolved_count': result.get('resolved', 0),
            'details': result
        }
        
    except Exception as e:
        logger.error(f"Evaluate bets task failed: {e}")
        
        self.update_state(
            state='FAILURE',
            meta={'message': f'Bet evaluation failed: {str(e)}', 'error': str(e)}
        )
        
        return {
            'success': False,
            'message': f'Bet evaluation failed: {str(e)}',
            'error': str(e)
        }

@celery_app.task(bind=True, name='daily_etl')
def daily_etl_task(self):
    """Daily ETL task for comprehensive data update."""
    try:
        logger.info("Starting daily ETL task")
        
        self.update_state(
            state='PROGRESS',
            meta={'message': 'Starting daily ETL process...', 'progress': 0}
        )
        
        # Run full ETL pipeline
        result = etl_service.full_etl_pipeline(include_stats=True)
        
        self.update_state(
            state='PROGRESS',
            meta={'message': 'ETL process completed', 'progress': 100}
        )
        
        logger.info(f"Daily ETL task completed: {result}")
        
        # Clear all relevant caches after ETL
        cache_patterns = [
            'league_leaders_*',
            'player_stats_*', 
            'team_standings_*',
            'search_*',
            'popular_players_*'
        ]
        
        for pattern in cache_patterns:
            cache.clear_pattern(pattern)
        
        return {
            'success': result.get('pipeline_success', False),
            'message': 'Daily ETL completed',
            'details': result,
            'cache_cleared': True
        }
        
    except Exception as e:
        logger.error(f"Daily ETL task failed: {e}")
        
        self.update_state(
            state='FAILURE',
            meta={'message': f'Daily ETL failed: {str(e)}', 'error': str(e)}
        )
        
        return {
            'success': False,
            'message': f'Daily ETL failed: {str(e)}',
            'error': str(e)
        }

@celery_app.task(bind=True, name='update_league_leaders')
def update_league_leaders_task(self, stat_keys: list = None):
    """Task to update league leaders for specified stats."""
    try:
        logger.info("Starting update league leaders task")
        
        stat_keys = stat_keys or ['pts', 'reb', 'ast', 'stl', 'blk']
        
        self.update_state(
            state='PROGRESS',
            meta={'message': f'Updating leaders for {len(stat_keys)} stats...', 'progress': 0}
        )
        
        results = {}
        total_stats = len(stat_keys)
        
        for i, stat_key in enumerate(stat_keys):
            try:
                # This would typically fetch from NBA API and update database
                leaders = nba_api.get_league_leaders(stat_key)
                
                if leaders:
                    # Cache the results
                    cache_key = f"league_leaders_{stat_key}_2025"
                    cache.set(cache_key, leaders, 3600)  # 1 hour cache
                    
                    results[stat_key] = {
                        'success': True,
                        'count': len(leaders)
                    }
                else:
                    results[stat_key] = {
                        'success': False,
                        'error': 'No data returned'
                    }
                
                # Update progress
                progress = ((i + 1) / total_stats) * 100
                self.update_state(
                    state='PROGRESS',
                    meta={'message': f'Updated {stat_key} leaders', 'progress': progress}
                )
                
            except Exception as e:
                logger.error(f"Failed to update leaders for {stat_key}: {e}")
                results[stat_key] = {
                    'success': False,
                    'error': str(e)
                }
        
        successful_updates = sum(1 for r in results.values() if r['success'])
        
        logger.info(f"League leaders task completed: {successful_updates}/{total_stats} successful")
        
        return {
            'success': successful_updates > 0,
            'message': f'Updated {successful_updates}/{total_stats} stat leaders',
            'details': results
        }
        
    except Exception as e:
        logger.error(f"Update league leaders task failed: {e}")
        
        self.update_state(
            state='FAILURE',
            meta={'message': f'Task failed: {str(e)}', 'error': str(e)}
        )
        
        return {
            'success': False,
            'message': f'League leaders update failed: {str(e)}',
            'error': str(e)
        }

@celery_app.task(bind=True, name='update_team_standings')
def update_team_standings_task(self):
    """Task to update team standings."""
    try:
        logger.info("Starting update team standings task")
        
        self.update_state(
            state='PROGRESS',
            meta={'message': 'Fetching team standings...', 'progress': 0}
        )
        
        # This would fetch from NBA API and update database
        # For now, we'll simulate the process
        standings = nba_api.get_team_standings()
        
        if standings:
            # Cache the standings
            cache_key = "team_standings_2025"
            cache.set(cache_key, standings, 3600)  # 1 hour cache
            
            self.update_state(
                state='PROGRESS',
                meta={'message': 'Team standings updated', 'progress': 100}
            )
            
            logger.info(f"Team standings updated: {len(standings)} teams")
            
            return {
                'success': True,
                'message': 'Team standings updated successfully',
                'team_count': len(standings)
            }
        else:
            return {
                'success': False,
                'message': 'No standings data available'
            }
        
    except Exception as e:
        logger.error(f"Update team standings task failed: {e}")
        
        self.update_state(
            state='FAILURE',
            meta={'message': f'Task failed: {str(e)}', 'error': str(e)}
        )
        
        return {
            'success': False,
            'message': f'Team standings update failed: {str(e)}',
            'error': str(e)
        }

@celery_app.task(name='test_task')
def test_task(message: str = "Hello from Celery!"):
    """Simple test task to verify Celery is working."""
    try:
        logger.info(f"Test task executed with message: {message}")
        time.sleep(2)  # Simulate work
        return {
            'success': True,
            'message': message,
            'timestamp': time.time()
        }
        
    except Exception as e:
        logger.error(f"Test task failed: {e}")
        return {
            'success': False,
            'error': str(e)
        }

# Convenience functions for triggering tasks
def trigger_stats_fetch(incremental: bool = True):
    """Trigger stats fetch task."""
    return fetch_stats_task.delay(incremental)

def trigger_bet_evaluation():
    """Trigger bet evaluation task.""" 
    return evaluate_bets_task.delay()

def trigger_daily_etl():
    """Trigger daily ETL task."""
    return daily_etl_task.delay()

def trigger_league_leaders_update(stat_keys: list = None):
    """Trigger league leaders update."""
    return update_league_leaders_task.delay(stat_keys)

def trigger_standings_update():
    """Trigger team standings update."""
    return update_team_standings_task.delay()