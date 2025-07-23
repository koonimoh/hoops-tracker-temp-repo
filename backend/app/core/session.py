import redis
from flask_session import Session
from app.core.config import settings, REDIS_SESSION_CONFIG

def init_session(app):
    """Initialize Flask-Session with Redis"""
    
    # Configure session settings
    app.config['SESSION_TYPE'] = settings.session_type
    app.config['SESSION_PERMANENT'] = settings.session_permanent
    app.config['SESSION_USE_SIGNER'] = settings.session_use_signer
    app.config['SESSION_KEY_PREFIX'] = settings.session_key_prefix
    app.config['PERMANENT_SESSION_LIFETIME'] = settings.permanent_session_lifetime
    
    # Configure Redis for sessions
    app.config['SESSION_REDIS'] = redis.Redis(**REDIS_SESSION_CONFIG)
    
    # Initialize session
    Session(app)
    
    return app