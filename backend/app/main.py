# Complete the main Flask application file
# backend/app/main.py
"""
Main Flask application factory and configuration.
"""

from flask import Flask, g, session
from flask_session import Session
import os
from app.core.config import settings
from app.core.logging import logger
from app.core.session import init_session
from app.routes import register_blueprints
from app.auth.auth_service import get_current_user

def create_app():
    """Create and configure the Flask application."""
    app = Flask(__name__)
    
    # Configuration
    app.config['SECRET_KEY'] = settings.flask_secret_key
    app.config['DEBUG'] = settings.flask_debug
    
    # Initialize session management
    init_session(app)
    
    # Register blueprints
    register_blueprints(app)
    
    # Global template variables
    @app.context_processor
    def inject_user():
        """Inject current user into all templates."""
        return {'current_user': get_current_user()}
    
    # Before request handler
    @app.before_request
    def before_request():
        """Set up global context before each request."""
        g.current_user = get_current_user()
    
    # Error handlers
    @app.errorhandler(404)
    def not_found(error):
        return render_template('errors/404.html'), 404
    
    @app.errorhandler(403)
    def forbidden(error):
        return render_template('errors/403.html'), 403
    
    @app.errorhandler(500)
    def internal_error(error):
        logger.error(f"Internal server error: {error}")
        return render_template('errors/500.html'), 500
    
    # Health check endpoint
    @app.route('/health')
    def health_check():
        """Health check endpoint for monitoring."""
        try:
            from app.db.supabase import test_db_connection
            db_healthy = test_db_connection()
            
            return {
                'status': 'healthy' if db_healthy else 'unhealthy',
                'database': 'connected' if db_healthy else 'disconnected',
                'timestamp': datetime.utcnow().isoformat()
            }, 200 if db_healthy else 503
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {'status': 'unhealthy', 'error': str(e)}, 503
    
    logger.info("Hoops Tracker application created successfully")
    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=8000, debug=settings.flask_debug)