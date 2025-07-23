# backend/app/routes/auth_routes.py
"""
Authentication routes using proper Supabase Auth methods.
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
            if result.get('confirmation_required'):
                flash('Registration successful! Please check your email to confirm your account before signing in.', 'info')
            else:
                flash('Registration successful! You can now sign in.', 'success')
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

@auth_bp.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    """Forgot password page and handler using Supabase Auth."""
    if request.method == 'GET':
        return render_template('forgot_password.html')
    
    try:
        email = request.form.get('email', '').strip().lower()
        
        if not email:
            flash('Please enter your email address.', 'error')
            return render_template('forgot_password.html')
        
        result = auth_service.send_password_reset(email)
        flash(result['message'], 'info')
        return redirect(url_for('auth.login'))
        
    except Exception as e:
        logger.error(f"Forgot password error: {e}")
        flash('An error occurred. Please try again.', 'error')
        return render_template('forgot_password.html')

@auth_bp.route('/reset-password', methods=['GET', 'POST'])
def reset_password():
    """Password reset page and handler using Supabase Auth."""
    if request.method == 'GET':
        # Get tokens from URL parameters (sent by Supabase)
        access_token = request.args.get('access_token')
        refresh_token = request.args.get('refresh_token')
        
        if not access_token:
            flash('Invalid reset link. Please request a new password reset.', 'error')
            return redirect(url_for('auth.forgot_password'))
        
        return render_template('reset_password.html', 
                             access_token=access_token, 
                             refresh_token=refresh_token)
    
    try:
        access_token = request.form.get('access_token', '')
        password = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')
        
        if not password or not confirm_password:
            flash('Please fill in all fields.', 'error')
            return render_template('reset_password.html', access_token=access_token)
        
        if password != confirm_password:
            flash('Passwords do not match.', 'error')
            return render_template('reset_password.html', access_token=access_token)
        
        if len(password) < 8:
            flash('Password must be at least 8 characters long.', 'error')
            return render_template('reset_password.html', access_token=access_token)
        
        result = auth_service.reset_password(access_token, password)
        
        if result['success']:
            flash(result['message'], 'success')
            return redirect(url_for('auth.login'))
        else:
            flash(result['error'], 'error')
            return render_template('reset_password.html', access_token=access_token)
        
    except Exception as e:
        logger.error(f"Reset password error: {e}")
        flash('An error occurred. Please try again.', 'error')
        return redirect(url_for('auth.forgot_password'))

@auth_bp.route('/confirm')
def confirm_email():
    """Email confirmation handler."""
    try:
        # Get tokens from URL parameters (sent by Supabase)
        access_token = request.args.get('access_token')
        refresh_token = request.args.get('refresh_token')
        
        if not access_token or not refresh_token:
            flash('Invalid confirmation link.', 'error')
            return redirect(url_for('auth.login'))
        
        result = auth_service.confirm_email(access_token, refresh_token)
        
        if result['success']:
            flash(result['message'], 'success')
        else:
            flash(result['error'], 'error')
        
        return redirect(url_for('auth.login'))
        
    except Exception as e:
        logger.error(f"Email confirmation error: {e}")
        flash('An error occurred during email confirmation.', 'error')
        return redirect(url_for('auth.login'))

@auth_bp.route('/resend-confirmation', methods=['POST'])
def resend_confirmation():
    """Resend email confirmation."""
    try:
        email = request.form.get('email', '').strip().lower()
        
        if not email:
            flash('Please enter your email address.', 'error')
            return redirect(url_for('auth.login'))
        
        result = auth_service.resend_confirmation(email)
        flash(result['message'], 'info')
        return redirect(url_for('auth.login'))
        
    except Exception as e:
        logger.error(f"Resend confirmation error: {e}")
        flash('An error occurred. Please try again.', 'error')
        return redirect(url_for('auth.login'))

@auth_bp.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    """User profile page and update handler."""
    from app.auth.auth_service import get_current_user
    
    user = get_current_user()
    if not user:
        return redirect(url_for('auth.login'))
    
    if request.method == 'GET':
        return render_template('profile.html', user=user)
    
    try:
        display_name = request.form.get('display_name', '').strip()
        
        if display_name:
            # Update user profile in database
            from app.db.supabase import supabase
            from datetime import datetime
            
            result = supabase.table('user_profiles').update({
                'display_name': display_name,
                'updated_at': datetime.utcnow().isoformat()
            }).eq('user_id', user['user_id']).execute()
            
            if result.error:
                flash('Failed to update profile.', 'error')
            else:
                flash('Profile updated successfully!', 'success')
                # Clear cache to reflect changes
                from app.core.cache import cache
                cache_key = f"user_session:{user['user_id']}"
                cache.delete(cache_key)
        
        return redirect(url_for('auth.profile'))
        
    except Exception as e:
        logger.error(f"Profile update error: {e}")
        flash('An error occurred while updating your profile.', 'error')
        return redirect(url_for('auth.profile'))

@auth_bp.route('/change-password', methods=['POST'])
@login_required  
def change_password():
    """Change user password using Supabase Auth."""
    try:
        user = get_current_user()
        if not user:
            return redirect(url_for('auth.login'))
        
        current_password = request.form.get('current_password', '')
        new_password = request.form.get('new_password', '')
        confirm_password = request.form.get('confirm_password', '')
        
        if not all([current_password, new_password, confirm_password]):
            flash('Please fill in all password fields.', 'error')
            return redirect(url_for('auth.profile'))
        
        if new_password != confirm_password:
            flash('New passwords do not match.', 'error')
            return redirect(url_for('auth.profile'))
        
        if len(new_password) < 8:
            flash('New password must be at least 8 characters long.', 'error')
            return redirect(url_for('auth.profile'))
        
        result = auth_service.change_password(user['user_id'], current_password, new_password)
        
        if result['success']:
            flash(result['message'], 'success')
        else:
            flash(result['error'], 'error')
        
        return redirect(url_for('auth.profile'))
        
    except Exception as e:
        logger.error(f"Change password error: {e}")
        flash('An error occurred while changing your password.', 'error')
        return redirect(url_for('auth.profile'))