""" Routes module for Hoops Tracker application.
"""

from .auth_routes import auth_bp
from .api_routes import api_bp
from .main_routes import main_bp
from .admin_routes import admin_bp

def register_blueprints(app):
    """Register all blueprints with the Flask app."""
    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(api_bp, url_prefix='/api')
    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(main_bp)