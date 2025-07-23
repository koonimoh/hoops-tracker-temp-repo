# backend/app/main.py
"""
Main application entry point.
"""

from flask import Flask, render_template, jsonify
from flask_cors import CORS
from app.core.config import settings
from app.core.logging import logger
from app.core.cache import cache
from app.db.supabase import db_manager
from app.routes import register_blueprints

def create_app():
    """Create and configure Flask application."""
    app = Flask(__name__)
    
    # Configure app
    app.config['SECRET_KEY'] = settings.secret_key
    app.config['SESSION_TYPE'] = 'filesystem'
    app.config['PERMANENT_SESSION_LIFETIME'] = settings.session_lifetime
    
    # Initialize CORS
    CORS(app, origins=settings.cors_origins)
    
    # Initialize cache
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
        # Create a 500.html template too
        return render_template('errors/500.html'), 500
    
    # Health check endpoint
    @app.route('/health')
    def health_check():
        db_health = db_manager.health_check()
        return jsonify({
            'status': 'healthy' if db_health['connected'] else 'unhealthy',
            'database': db_health,
            'timestamp': datetime.utcnow().isoformat()
        })
    
    logger.info("Flask application created successfully")
    return app

# Import at the end to avoid circular imports
from flask import request
from datetime import datetime

# Create the app instance
app = create_app()

if __name__ == '__main__':
    app.run(
        host=settings.app_host,
        port=settings.app_port,
        debug=settings.debug
    )