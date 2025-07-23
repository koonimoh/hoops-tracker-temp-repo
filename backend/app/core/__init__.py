"""
Core module for Hoops Tracker application.
"""

from .config import settings
from .logging import logger
from .cache import cache, cached  # Export both the manager and decorator

__all__ = ['settings', 'logger', 'cache', 'cached']