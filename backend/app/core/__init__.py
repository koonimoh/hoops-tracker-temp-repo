"""
Core module for Hoops Tracker application.

This module contains core functionality including:
- Configuration management
- Logging setup
- Caching utilities
- Session management
"""

from .config import settings
from .logging import logger
from .cache import cache, cached

__all__ = ['settings', 'logger', 'cache', 'cached']