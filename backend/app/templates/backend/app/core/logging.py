"""
Logging configuration for Hoops Tracker application.
"""

import logging
import sys
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from app.core.config import settings

def setup_logging():
    """Setup logging configuration for the application."""
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO if settings.flask_env == 'production' else logging.DEBUG)
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler for general logs
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, 'hoops_tracker.log'),
        maxBytes=10485760,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # Error file handler
    error_handler = RotatingFileHandler(
        os.path.join(log_dir, 'errors.log'),
        maxBytes=10485760,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    root_logger.addHandler(error_handler)
    
    # Daily rotating handler for audit logs
    audit_handler = TimedRotatingFileHandler(
        os.path.join(log_dir, 'audit.log'),
        when='midnight',
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    audit_handler.setLevel(logging.INFO)
    audit_handler.setFormatter(formatter)
    
    # Create audit logger
    audit_logger = logging.getLogger('audit')
    audit_logger.addHandler(audit_handler)
    audit_logger.propagate = False
    
    return logging.getLogger("hoops_tracker")

# Create the main logger
logger = setup_logging()

class CustomLogger:
    """Custom logger wrapper with additional functionality."""
    
    def __init__(self, name="hoops_tracker"):
        self.logger = logging.getLogger(name)
        self.audit_logger = logging.getLogger('audit')
    
    def debug(self, message, *args, **kwargs):
        """Log debug message."""
        self.logger.debug(message, *args, **kwargs)
    
    def info(self, message, *args, **kwargs):
        """Log info message."""
        self.logger.info(message, *args, **kwargs)
    
    def warning(self, message, *args, **kwargs):
        """Log warning message."""
        self.logger.warning(message, *args, **kwargs)
    
    def error(self, message, *args, **kwargs):
        """Log error message."""
        self.logger.error(message, *args, **kwargs)
    
    def critical(self, message, *args, **kwargs):
        """Log critical message."""
        self.logger.critical(message, *args, **kwargs)
    
    def audit(self, action, user_id=None, details=None):
        """Log audit event."""
        audit_message = f"Action: {action}"
        if user_id:
            audit_message += f" | User: {user_id}"
        if details:
            audit_message += f" | Details: {details}"
        
        self.audit_logger.info(audit_message)
    
    def performance(self, operation, duration, details=None):
        """Log performance metrics."""
        perf_message = f"Performance: {operation} took {duration:.3f}s"
        if details:
            perf_message += f" | {details}"
        
        if duration > 1.0:  # Log slow operations as warnings
            self.logger.warning(perf_message)
        else:
            self.logger.info(perf_message)
    
    def api_call(self, endpoint, method, status_code, duration):
        """Log API call information."""
        self.logger.info(
            f"API Call: {method} {endpoint} - {status_code} - {duration:.3f}s"
        )
    
    def user_action(self, user_id, action, resource=None, result="success"):
        """Log user actions for analytics."""
        message = f"User {user_id} performed {action}"
        if resource:
            message += f" on {resource}"
        message += f" - {result}"
        
        self.audit(action, user_id, f"Resource: {resource}, Result: {result}")

# Create enhanced logger instance
logger = CustomLogger()

# Module level functions for backward compatibility
def log_performance(operation, duration, details=None):
    """Log performance metrics."""
    logger.performance(operation, duration, details)

def log_api_call(endpoint, method, status_code, duration):
    """Log API call information."""
    logger.api_call(endpoint, method, status_code, duration)

def log_user_action(user_id, action, resource=None, result="success"):
    """Log user actions."""
    logger.user_action(user_id, action, resource, result)

def log_audit(action, user_id=None, details=None):
    """Log audit events."""
    logger.audit(action, user_id, details)