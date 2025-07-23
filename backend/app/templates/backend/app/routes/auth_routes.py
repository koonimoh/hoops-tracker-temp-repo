# backend/app/routes/auth_routes.py
"""
Authentication routes for login, register, logout.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.auth.auth_service import auth_service
from app.auth.decorators import login_required
from app.core.logging import logger

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """User login page and handler."""
    if request.method == 'GET':
        return render_template('login.html')
    
    try:
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')
        
        if not email or not password:
            flash('Please provide both email and password.', 'error')
            return render_template('login.html')
        
        result = auth_service.login_user(email, password)
        
        if result['success']:
            flash('Welcome back!', 'success')
            next_page = request.args.get('next')
            return redirect(next_page if next_page else url_for('main.dashboard'))
        else:
            flash(result['error'], 'error')
            return render_template('login.html')
            
    except Exception as e:
        logger.error(f"Login error: {e}")
        flash('An error occurred during login. Please try again.', 'error')
        return render_template('login.html')

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    """User registration page and handler."""
    if request.method == 'GET':
        return render_template('register.html')
    
    try:
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')
        full_name = request.form.get('full_name', '').strip()
        
        # Validation
        if not all([email, password, confirm_password]):
            flash('Please fill in all required fields.', 'error')
            return render_template('register.html')
        
        if password != confirm_password:
            flash('Passwords do not match.', 'error')
            return render_template('register.html')
        
        if len(password) < 8:
            flash('Password must be at least 8 characters long.', 'error')
            return render_template('register.html')
        
        result = auth_service.register_user(email, password, full_name)
        
        if result['success']:
            flash('Registration successful! Please check your email to verify your account.', 'success')
            return redirect(url_for('auth.login'))
        else:
            flash(result['error'], 'error')
            return render_template('register.html')
            
    except Exception as e:
        logger.error(f"Registration error: {e}")
        flash('An error occurred during registration. Please try again.', 'error')
        return render_template('register.html')

@auth_bp.route('/logout')
@login_required
def logout():
    """User logout handler."""
    try:
        auth_service.logout_user()
        flash('You have been logged out successfully.', 'info')
    except Exception as e:
        logger.error(f"Logout error: {e}")
        flash('An error occurred during logout.', 'error')
    
    return redirect(url_for('main.index'))

@auth_bp.route('/profile')
@login_required
def profile():
    """User profile page."""
    from app.auth.auth_service import get_current_user
    
    user = get_current_user()
    if not user:
        return redirect(url_for('auth.login'))
    
    return render_template('profile.html', user=user)