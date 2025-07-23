"""
Core module for Hoops Tracker application.

This module contains core functionality including:
- Configuration management
- Logging setup
- Caching utilities
- Session management
"""

from app.config import settings
from app.logging import logger
from app.cache import cache, cached

__all__ = ['settings', 'logger', 'cache', 'cached']

if __name__ == "__main__":
    # smoke‑test imports and basic behavior
    print("settings:", settings)
    print("logger methods:", [m for m in ("debug", "info", "warning") if hasattr(logger, m)])
    print("cache instance:", cache)
    print("cached decorator:", callable(cached))

    # verify that `cached` actually caches
    calls = {"count": 0}

    @cached("smoke_test_key")
    def expensive():
        calls["count"] += 1
        return "ok"

    # first call runs function
    assert expensive() == "ok"
    # second call should come from cache
    assert expensive() == "ok"
    assert calls["count"] == 1, f"cached did not work, count={calls['count']}"

    print("All core module tests passed ✅")
