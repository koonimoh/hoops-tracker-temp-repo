from pydantic_settings import BaseSettings
from typing import Optional
import os

class Settings(BaseSettings):
    # Supabase
    supabase_url: str
    supabase_key: str
    supabase_anon_key: str
    
    # Flask
    flask_secret_key: str
    flask_env: str = "development"
    flask_debug: bool = True
    
    # App settings
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    debug: bool = True
    
    # Session Management (simplified - using filesystem)
    permanent_session_lifetime: int = 86400  # 24 hours
    
    # Performance
    max_workers: int = 8
    thread_pool_size: int = 20
    
    # NBA API
    nba_api_rate_limit: int = 600
    nba_api_retry_attempts: int = 3
    nba_api_retry_delay: int = 2
    
    # Search
    search_cache_ttl: int = 1800  # 30 minutes
    fuzzy_search_threshold: float = 0.4
    
    # Cache (simplified)
    cache_default_timeout: int = 300  # 5 minutes

    class Config:
        env_file = ".env"
        case_sensitive = False

settings = Settings()