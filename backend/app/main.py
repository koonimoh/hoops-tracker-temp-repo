"""
Main application entry point (simplified).
"""

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from app.core.config import settings
from app.core.logging import logger
from app.core.cache import cache
from app.routes import register_blueprints
from datetime import datetime

def create_app():
    """Create and configure Flask application."""
    app = Flask(__name__)
    
    # Basic Flask configuration
    app.config['SECRET_KEY'] = settings.flask_secret_key
    app.config['SESSION_TYPE'] = 'filesystem' 
    app.config['PERMANENT_SESSION_LIFETIME'] = settings.permanent_session_lifetime
    app.config['DEBUG'] = settings.debug
    
    # Initialize CORS
    CORS(app)
    
    # Initialize simple cache
    cache.init_app(app)
    
    # Register blueprints
    register_blueprints(app)
    
    # Register error handlers
    @app.errorhandler(404)
    def not_found(e):
        if request.path.startswith('/api/'):
            return jsonify({'error': 'Not found'}), 404
        return render_template('errors/404.html'), 404
    
    @app.errorhandler(500)
    def server_error(e):
        logger.error(f'Server Error: {e}')
        if request.path.startswith('/api/'):
            return jsonify({'error': 'Internal server error'}), 500
        return render_template('errors/500.html'), 500
    
    # Health check endpoint
    @app.route('/health')
    def health_check():
        try:
            # Simple health check without complex database testing
            from app.db.supabase import supabase
            # Just try to connect
            test_result = supabase.table('players').select('id').limit(1).execute()
            db_healthy = not bool(test_result.error)
        except Exception as e:
            logger.error(f"Health check DB error: {e}")
            db_healthy = False
        
        cache_stats = cache.get_stats()
        
        return jsonify({
            'status': 'healthy' if db_healthy else 'degraded',
            'database': {'connected': db_healthy},
            'cache': cache_stats,
            'timestamp': datetime.utcnow().isoformat()
        })
    
    logger.info("Flask application created successfully")
    return app

# Create the app instance
app = create_app()

if __name__ == '__main__':
    app.run(
        host=settings.app_host,
        port=settings.app_port,
        debug=settings.debug
    )