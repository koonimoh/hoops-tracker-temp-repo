"""
Background tasks module for Hoops Tracker application.

This module contains Celery tasks for:
- Data fetching and ETL processes
- Cache warming and management
- Scheduled maintenance tasks
- Betting simulation and evaluation
"""

from .celery_app import celery_app
from .fetch_data import fetch_stats_task, evaluate_bets_task
from .cache_warmup import warm_cache_task, cleanup_cache_task

__all__ = [
    'celery_app',
    'fetch_stats_task',
    'evaluate_bets_task', 
    'warm_cache_task',
    'cleanup_cache_task'
]