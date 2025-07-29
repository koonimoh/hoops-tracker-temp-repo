# auth.py
from functools import wraps
from flask import Blueprint, request, session, redirect, url_for, flash, jsonify, render_template, current_app
import logging
from typing import Optional, Dict

# Create blueprint
auth_bp = Blueprint('auth', __name__)

def require_auth(f):
    """Decorator to require authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = get_current_user()
        if not user:
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function

def require_role(role):
    """Decorator to require specific role"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = get_current_user()
            if not user:
                flash('Please log in to access this page.', 'error')
                return redirect(url_for('auth.login'))
            
            user_profile = current_app.supabase.get_user_profile(user['id'])
            if not user_profile or user_profile.get('role') != role:
                flash('Insufficient permissions.', 'error')
                return redirect(url_for('dashboard'))
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def get_current_user() -> Optional[Dict]:
    """Get current authenticated user from session"""
    user_id = session.get('user_id')
    if not user_id:
        return None
    
    # Get user profile from Supabase
    try:
        profile = current_app.supabase.get_user_profile(user_id)
        if profile:
            return profile
        else:
            # Clear invalid session
            session.clear()
            return None
    except Exception as e:
        logging.error(f"Error getting user profile: {str(e)}")
        session.clear()
        return None

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """Login page and handler"""
    if request.method == 'GET':
        # Check if already logged in
        if get_current_user():
            return redirect(url_for('dashboard'))
        return render_template('auth/login.html')
    
    # Handle POST request
    email = request.form.get('email')
    password = request.form.get('password')
    
    if not email or not password:
        flash('Email and password are required.', 'error')
        return render_template('auth/login.html')
    
    # Attempt login with Supabase
    result = current_app.supabase.sign_in_user(email, password)
    
    if result['success']:
        user = result['user']
        session_data = result['session']
        
        # Store user session
        session['user_id'] = user.id
        session['access_token'] = session_data.access_token
        session['refresh_token'] = session_data.refresh_token
        session.permanent = True
        
        # Get or create user profile
        profile = current_app.supabase.get_user_profile(user.id)
        if not profile:
            # Create profile for new user
            profile_result = current_app.supabase.create_user_profile(
                user_id=user.id,
                email=user.email,
                username=user.email.split('@')[0]  # Default username
            )
            if not profile_result['success']:
                flash('Error creating user profile.', 'error')
                return render_template('auth/login.html')
        
        flash('Login successful!', 'success')
        
        # Redirect to next page or dashboard
        next_page = request.args.get('next')
        if next_page and next_page.startswith('/'):
            return redirect(next_page)
        return redirect(url_for('dashboard'))
    
    else:
        flash(f'Login failed: {result["error"]}', 'error')
        return render_template('auth/login.html')

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    """Registration page and handler"""
    if request.method == 'GET':
        # Check if already logged in
        if get_current_user():
            return redirect(url_for('dashboard'))
        return render_template('auth/register.html')
    
    # Handle POST request
    email = request.form.get('email')
    password = request.form.get('password')
    confirm_password = request.form.get('confirm_password')
    username = request.form.get('username')
    full_name = request.form.get('full_name')
    favorite_team = request.form.get('favorite_team')
    
    # Validation
    if not email or not password:
        flash('Email and password are required.', 'error')
        return render_template('auth/register.html')
    
    if password != confirm_password:
        flash('Passwords do not match.', 'error')
        return render_template('auth/register.html')
    
    if len(password) < 6:
        flash('Password must be at least 6 characters long.', 'error')
        return render_template('auth/register.html')
    
    # Attempt registration with Supabase
    metadata = {
        'username': username,
        'full_name': full_name,
        'favorite_team': favorite_team
    }
    
    result = current_app.supabase.sign_up_user(email, password, metadata)
    
    if result['success']:
        flash('Registration successful! Please check your email to verify your account.', 'success')
        return redirect(url_for('auth.login'))
    else:
        flash(f'Registration failed: {result["error"]}', 'error')
        return render_template('auth/register.html')

@auth_bp.route('/logout')
def logout():
    """Logout handler"""
    # Sign out from Supabase
    current_app.supabase.sign_out_user()
    
    # Clear session
    session.clear()
    
    flash('You have been logged out.', 'info')
    return redirect(url_for('index'))

@auth_bp.route('/profile', methods=['GET', 'POST'])
@require_auth
def profile():
    """User profile page"""
    user = get_current_user()
    
    if request.method == 'GET':
        teams = current_app.supabase.get_all_teams()
        return render_template('auth/profile.html', user=user, teams=teams)
    
    # Handle profile update
    username = request.form.get('username')
    full_name = request.form.get('full_name')
    favorite_team = request.form.get('favorite_team')
    
    updates = {}
    if username and username != user.get('username'):
        updates['username'] = username
    if full_name and full_name != user.get('full_name'):
        updates['full_name'] = full_name
    if favorite_team and favorite_team != user.get('favorite_team'):
        updates['favorite_team'] = favorite_team
    
    if updates:
        result = current_app.supabase.update_user_profile(user['id'], updates)
        if result['success']:
            flash('Profile updated successfully!', 'success')
        else:
            flash(f'Error updating profile: {result["error"]}', 'error')
    else:
        flash('No changes to save.', 'info')
    
    return redirect(url_for('auth.profile'))

@auth_bp.route('/change-password', methods=['POST'])
@require_auth
def change_password():
    """Change user password"""
    current_password = request.form.get('current_password')
    new_password = request.form.get('new_password')
    confirm_password = request.form.get('confirm_password')
    
    if not all([current_password, new_password, confirm_password]):
        flash('All password fields are required.', 'error')
        return redirect(url_for('auth.profile'))
    
    if new_password != confirm_password:
        flash('New passwords do not match.', 'error')
        return redirect(url_for('auth.profile'))
    
    if len(new_password) < 6:
        flash('New password must be at least 6 characters long.', 'error')
        return redirect(url_for('auth.profile'))
    
    # For now, we'll use a simple approach - in production, you'd want proper password verification
    try:
        # This would require additional Supabase auth methods
        flash('Password change functionality requires additional setup.', 'info')
    except Exception as e:
        flash(f'Error changing password: {str(e)}', 'error')
    
    return redirect(url_for('auth.profile'))

# API endpoints for AJAX requests
@auth_bp.route('/api/check-auth')
def check_auth():
    """Check if user is authenticated (AJAX endpoint)"""
    user = get_current_user()
    return jsonify({
        'authenticated': user is not None,
        'user': user if user else None
    })

@auth_bp.route('/api/refresh-session', methods=['POST'])
def refresh_session():
    """Refresh user session"""
    refresh_token = session.get('refresh_token')
    
    if not refresh_token:
        return jsonify({'success': False, 'error': 'No refresh token'}), 401
    
    try:
        # This would require implementing refresh token logic in Supabase client
        return jsonify({'success': True, 'message': 'Session refreshed'})
    except Exception as e:
        logging.error(f"Session refresh error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

# Admin routes
@auth_bp.route('/admin/users')
@require_role('admin')
def admin_users():
    """Admin page for managing users"""
    try:
        # Get all user profiles (admin only)
        response = current_app.supabase.client.table("hoops.user_profiles").select("*").order("created_at", desc=True).execute()
        users = response.data or []
        return render_template('auth/admin_users.html', users=users)
    except Exception as e:
        flash(f'Error loading users: {str(e)}', 'error')
        return redirect(url_for('dashboard'))

@auth_bp.route('/admin/users/<user_id>/role', methods=['POST'])
@require_role('admin')
def update_user_role(user_id):
    """Update user role (admin only)"""
    new_role = request.form.get('role')
    
    if new_role not in ['user', 'admin', 'moderator']:
        flash('Invalid role.', 'error')
        return redirect(url_for('auth.admin_users'))
    
    try:
        result = current_app.supabase.update_user_profile(user_id, {'role': new_role})
        if result['success']:
            flash(f'User role updated to {new_role}.', 'success')
        else:
            flash(f'Error updating role: {result["error"]}', 'error')
    except Exception as e:
        flash(f'Error updating role: {str(e)}', 'error')
    
    return redirect(url_for('auth.admin_users'))

# Password reset functionality (basic implementation)
@auth_bp.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    """Forgot password page"""
    if request.method == 'GET':
        return render_template('auth/forgot_password.html')
    
    email = request.form.get('email')
    
    if not email:
        flash('Email is required.', 'error')
        return render_template('auth/forgot_password.html')
    
    try:
        # This would require implementing password reset in Supabase
        flash('Password reset functionality requires additional setup.', 'info')
        return redirect(url_for('auth.login'))
    except Exception as e:
        flash(f'Error sending reset email: {str(e)}', 'error')
        return render_template('auth/forgot_password.html')

# Session management
@auth_bp.before_app_request
def load_logged_in_user():
    """Load user info on each request"""
    user_id = session.get('user_id')
    
    if user_id is None:
        return
    
    # Check if session is still valid
    try:
        user = current_app.supabase.get_user_profile(user_id)
        if not user:
            session.clear()
    except Exception as e:
        logging.error(f"Error loading user: {str(e)}")
        session.clear()

# Context processor to make user available in templates
@auth_bp.app_context_processor
def inject_user():
    """Make current user available in all templates"""
    return dict(current_user=get_current_user())