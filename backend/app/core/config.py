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
    
    # Session Management
    session_type: str = "redis"
    session_permanent: bool = True
    session_use_signer: bool = True
    session_key_prefix: str = "hoops_tracker:"
    permanent_session_lifetime: int = 86400
    
    # Redis
    redis_url: str
    redis_session_db: int = 1
    redis_cache_db: int = 2
    redis_celery_db: int = 3
    
    # Cache
    cache_type: str = "redis"
    cache_default_timeout: int = 300
    cache_key_prefix: str = "ht_cache:"
    
    # Celery
    celery_broker_url: str
    celery_result_backend: str
    
    # Performance
    max_workers: int = 8
    thread_pool_size: int = 20
    cache_preload_enabled: bool = True
    
    # NBA API
    nba_api_rate_limit: int = 600
    nba_api_retry_attempts: int = 3
    nba_api_retry_delay: int = 2
    
    # Search
    search_cache_ttl: int = 1800
    fuzzy_search_threshold: float = 0.4

    class Config:
        env_file = ".env"
        case_sensitive = False

settings = Settings()

# Redis connection configs
REDIS_SESSION_CONFIG = {
    'host': settings.redis_url.split('://')[1].split(':')[0],
    'port': int(settings.redis_url.split(':')[-1].split('/')[0]),
    'db': settings.redis_session_db
}

REDIS_CACHE_CONFIG = {
    'host': settings.redis_url.split('://')[1].split(':')[0],
    'port': int(settings.redis_url.split(':')[-1].split('/')[0]),
    'db': settings.redis_cache_db
}