# backend/app/auth/auth_service.py
"""
Authentication service using Supabase Auth with proper password reset implementation.
"""

import hashlib
import secrets
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from flask import session, request, g, url_for
from functools import wraps
from app.db.supabase import supabase
from app.core.logging import logger
from app.core.cache import cache

class AuthService:
    """Enhanced authentication service with Supabase Auth."""
    
    def __init__(self):
        self.session_timeout = 7200  # 2 hours
    
    def register_user(self, email: str, password: str, full_name: str = None) -> Dict[str, Any]:
        """Register a new user with Supabase Auth."""
        try:
            # Create user in Supabase Auth
            result = supabase.auth.sign_up({
                'email': email,
                'password': password,
                'options': {
                    'data': {
                        'full_name': full_name or email.split('@')[0],
                        'display_name': full_name or email.split('@')[0]
                    },
                    # Optional: Set redirect URL for email confirmation
                    'email_redirect_to': f"{request.host_url}auth/confirm"
                }
            })
            
            if result.error:
                logger.error(f"User registration failed: {result.error.message}")
                return {
                    'success': False,
                    'error': result.error.message
                }
            
            # Check if email confirmation is required
            if result.user and not result.session:
                logger.info(f"User registered successfully, email confirmation required: {email}")
                return {
                    'success': True,
                    'user': result.user,
                    'confirmation_required': True,
                    'message': 'Please check your email to confirm your account.'
                }
            
            logger.info(f"User registered successfully: {email}")
            return {
                'success': True,
                'user': result.user,
                'session': result.session,
                'confirmation_required': False
            }
            
        except Exception as e:
            logger.error(f"Registration error: {e}")
            return {
                'success': False,
                'error': 'Registration failed. Please try again.'
            }
    
    def login_user(self, email: str, password: str) -> Dict[str, Any]:
        """Authenticate user and create session."""
        try:
            # Authenticate with Supabase
            result = supabase.auth.sign_in_with_password({
                'email': email,
                'password': password
            })
            
            if result.error:
                logger.warning(f"Login failed for {email}: {result.error.message}")
                return {
                    'success': False,
                    'error': 'Invalid email or password'
                }
            
            if not result.user.email_confirmed_at:
                return {
                    'success': False,
                    'error': 'Please confirm your email address before signing in.'
                }
            
            # Get user profile with role information
            user_profile = self.get_user_profile(result.user.id)
            
            # Update login tracking
            self._update_login_tracking(result.user.id)
            
            # Create Flask session
            session['user_id'] = result.user.id
            session['user_email'] = result.user.email
            session['access_token'] = result.session.access_token
            session['refresh_token'] = result.session.refresh_token
            session['role'] = user_profile.get('role_name', 'user') if user_profile else 'user'
            session['permissions'] = self.get_user_permissions(result.user.id)
            
            # Cache user session data
            cache_key = f"user_session:{result.user.id}"
            cache.set(cache_key, {
                'user_id': result.user.id,
                'email': result.user.email,
                'role': session['role'],
                'permissions': session['permissions'],
                'last_activity': datetime.utcnow().isoformat()
            }, timeout=self.session_timeout)
            
            logger.info(f"User logged in successfully: {email}")
            return {
                'success': True,
                'user': result.user,
                'profile': user_profile,
                'session': result.session
            }
            
        except Exception as e:
            logger.error(f"Login error: {e}")
            return {
                'success': False,
                'error': 'Login failed. Please try again.'
            }
    
    def send_password_reset(self, email: str) -> Dict[str, Any]:
        """Send password reset email using Supabase Auth."""
        try:
            result = supabase.auth.reset_password_email(
                email,
                {
                    'redirect_to': f"{request.host_url}auth/reset-password"
                }
            )
            
            if result.error:
                logger.error(f"Password reset email failed: {result.error.message}")
                # For security, don't reveal if email exists or not
                return {
                    'success': True,
                    'message': 'If an account with this email exists, you will receive a password reset link.'
                }
            
            logger.info(f"Password reset email sent successfully to: {email}")
            return {
                'success': True,
                'message': 'If an account with this email exists, you will receive a password reset link.'
            }
            
        except Exception as e:
            logger.error(f"Password reset error: {e}")
            return {
                'success': True,  # Still return success for security
                'message': 'If an account with this email exists, you will receive a password reset link.'
            }
    
    def reset_password(self, access_token: str, new_password: str) -> Dict[str, Any]:
        """Reset password using Supabase Auth."""
        try:
            # Set the session with the access token from the reset link
            result = supabase.auth.set_session(access_token, refresh_token=None)
            
            if result.error:
                logger.error(f"Invalid reset token: {result.error.message}")
                return {
                    'success': False,
                    'error': 'Invalid or expired reset link. Please request a new one.'
                }
            
            # Update the password
            update_result = supabase.auth.update_user({
                'password': new_password
            })
            
            if update_result.error:
                logger.error(f"Password update failed: {update_result.error.message}")
                return {
                    'success': False,
                    'error': 'Failed to update password. Please try again.'
                }
            
            logger.info(f"Password reset successfully for user: {update_result.user.email}")
            return {
                'success': True,
                'message': 'Password updated successfully. Please sign in with your new password.'
            }
            
        except Exception as e:
            logger.error(f"Password reset error: {e}")
            return {
                'success': False,
                'error': 'An error occurred while resetting your password.'
            }
    
    def change_password(self, user_id: str, current_password: str, new_password: str) -> Dict[str, Any]:
        """Change user password (requires current password verification)."""
        try:
            # Get current user
            user = self.get_current_user()
            if not user or user['user_id'] != user_id:
                return {
                    'success': False,
                    'error': 'Unauthorized'
                }
            
            # Verify current password by attempting to sign in
            email = user.get('email')
            if not email:
                return {
                    'success': False,
                    'error': 'Unable to verify current password'
                }
            
            verify_result = supabase.auth.sign_in_with_password({
                'email': email,
                'password': current_password
            })
            
            if verify_result.error:
                return {
                    'success': False,
                    'error': 'Current password is incorrect'
                }
            
            # Update password
            update_result = supabase.auth.update_user({
                'password': new_password
            })
            
            if update_result.error:
                logger.error(f"Password change failed: {update_result.error.message}")
                return {
                    'success': False,
                    'error': 'Failed to update password. Please try again.'
                }
            
            logger.info(f"Password changed successfully for user: {email}")
            return {
                'success': True,
                'message': 'Password updated successfully.'
            }
            
        except Exception as e:
            logger.error(f"Password change error: {e}")
            return {
                'success': False,
                'error': 'An error occurred while changing your password.'
            }
    
    def confirm_email(self, access_token: str, refresh_token: str) -> Dict[str, Any]:
        """Confirm email using tokens from confirmation link."""
        try:
            result = supabase.auth.set_session(access_token, refresh_token)
            
            if result.error:
                logger.error(f"Email confirmation failed: {result.error.message}")
                return {
                    'success': False,
                    'error': 'Invalid or expired confirmation link.'
                }
            
            # Create user profile if it doesn't exist
            self._create_user_profile(result.user)
            
            logger.info(f"Email confirmed successfully for: {result.user.email}")
            return {
                'success': True,
                'user': result.user,
                'session': result.session,
                'message': 'Email confirmed successfully! You can now sign in.'
            }
            
        except Exception as e:
            logger.error(f"Email confirmation error: {e}")
            return {
                'success': False,
                'error': 'An error occurred during email confirmation.'
            }
    
    def resend_confirmation(self, email: str) -> Dict[str, Any]:
        """Resend email confirmation."""
        try:
            result = supabase.auth.resend(
                type='signup',
                email=email,
                options={
                    'email_redirect_to': f"{request.host_url}auth/confirm"
                }
            )
            
            if result.error:
                logger.error(f"Resend confirmation failed: {result.error.message}")
                # Don't reveal if email exists
                return {
                    'success': True,
                    'message': 'If an account with this email exists, a new confirmation email has been sent.'
                }
            
            return {
                'success': True,
                'message': 'If an account with this email exists, a new confirmation email has been sent.'
            }
            
        except Exception as e:
            logger.error(f"Resend confirmation error: {e}")
            return {
                'success': True,  # Still return success for security
                'message': 'If an account with this email exists, a new confirmation email has been sent.'
            }
    
    def logout_user(self) -> bool:
        """Logout user and clear session."""
        try:
            user_id = session.get('user_id')
            
            if user_id:
                # Clear cache
                cache_key = f"user_session:{user_id}"
                cache.delete(cache_key)
                
                # Sign out from Supabase
                supabase.auth.sign_out()
                
                logger.info(f"User logged out: {user_id}")
            
            # Clear Flask session
            session.clear()
            return True
            
        except Exception as e:
            logger.error(f"Logout error: {e}")
            return False
    
    def _create_user_profile(self, user) -> None:
        """Create user profile after email confirmation."""
        try:
            # Check if profile already exists
            existing = supabase.table('user_profiles').select('id').eq('user_id', user.id).execute()
            
            if existing.data:
                return  # Profile already exists
            
            # Get default role
            default_role = supabase.table('user_roles').select('id').eq('name', 'user').single().execute()
            
            profile_data = {
                'user_id': user.id,
                'email': user.email,
                'display_name': user.user_metadata.get('display_name', user.email.split('@')[0]),
                'full_name': user.user_metadata.get('full_name'),
                'role_id': default_role.data['id'] if default_role.data else None,
                'is_active': True,
                'email_confirmed': True,
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }
            
            result = supabase.table('user_profiles').insert(profile_data).execute()
            
            if result.error:
                logger.error(f"Failed to create user profile: {result.error}")
            else:
                logger.info(f"User profile created for: {user.email}")
                
        except Exception as e:
            logger.error(f"Error creating user profile: {e}")
    
    def get_current_user(self) -> Optional[Dict[str, Any]]:
        """Get current authenticated user from session."""
        try:
            user_id = session.get('user_id')
            if not user_id:
                return None
            
            # Check cache first
            cache_key = f"user_session:{user_id}"
            cached_user = cache.get(cache_key)
            
            if cached_user:
                # Update last activity
                cached_user['last_activity'] = datetime.utcnow().isoformat()
                cache.set(cache_key, cached_user, timeout=self.session_timeout)
                return cached_user
            
            # Get from database
            user_profile = self.get_user_profile(user_id)
            if user_profile:
                user_data = {
                    'user_id': user_id,
                    'email': session.get('user_email'),
                    'role': user_profile.get('role_name'),
                    'permissions': self.get_user_permissions(user_id),
                    'profile': user_profile,
                    'last_activity': datetime.utcnow().isoformat()
                }
                
                # Update cache
                cache.set(cache_key, user_data, timeout=self.session_timeout)
                return user_data
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting current user: {e}")
            return None
    
    def get_user_profile(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user profile with role information."""
        try:
            result = supabase.table('user_profiles').select(
                '*, user_roles(name, description, permissions)'
            ).eq('user_id', user_id).eq('is_active', True).single().execute()
            
            if result.error or not result.data:
                return None
            
            profile = result.data
            if profile.get('user_roles'):
                profile['role_name'] = profile['user_roles']['name']
                profile['role_permissions'] = profile['user_roles']['permissions']
            else:
                profile['role_name'] = 'user'
                profile['role_permissions'] = {}
            
            return profile
            
        except Exception as e:
            logger.error(f"Error getting user profile: {e}")
            return None
    
    def get_user_permissions(self, user_id: str) -> List[str]:
        """Get list of user permissions."""
        try:
            # Try to use the SQL function if it exists
            result = supabase.rpc('get_user_permissions', {
                'user_uuid': user_id
            }).execute()
            
            if result.error:
                logger.warning(f"SQL function not available, using fallback: {result.error}")
                # Fallback to basic role-based permissions
                user_profile = self.get_user_profile(user_id)
                if user_profile and user_profile.get('role_name'):
                    return self._get_default_permissions(user_profile['role_name'])
                return []
            
            return [perm['permission_name'] for perm in result.data or []]
            
        except Exception as e:
            logger.error(f"Error getting user permissions: {e}")
            # Fallback to basic permissions
            return ['players.read', 'bets.read']
    
    def _get_default_permissions(self, role_name: str) -> List[str]:
        """Get default permissions for a role (fallback)."""
        role_permissions = {
            'admin': ['all'],
            'premium': ['bets.create', 'bets.read', 'bets.update', 'watchlist.create', 'watchlist.read', 'players.read'],
            'user': ['bets.read', 'watchlist.read', 'players.read'],
            'guest': ['players.read']
        }
        return role_permissions.get(role_name, ['players.read'])
    
    def has_permission(self, user_id: str, permission: str) -> bool:
        """Check if user has specific permission."""
        try:
            # Use cached permissions if available
            current_user = self.get_current_user()
            if current_user and current_user.get('user_id') == user_id:
                permissions = current_user.get('permissions', [])
                # Check for admin permission
                if 'all' in permissions:
                    return True
                return permission in permissions
            
            # Fallback check
            permissions = self.get_user_permissions(user_id)
            return 'all' in permissions or permission in permissions
            
        except Exception as e:
            logger.error(f"Error checking permission: {e}")
            return False
    
    def _update_login_tracking(self, user_id: str):
        """Update user login tracking."""
        try:
            # Get current login count
            profile_result = supabase.table('user_profiles').select('login_count').eq(
                'user_id', user_id
            ).single().execute()
            
            current_count = 0
            if profile_result.data:
                current_count = profile_result.data.get('login_count', 0)
            
            # Update login tracking
            supabase.table('user_profiles').update({
                'last_login_at': datetime.utcnow().isoformat(),
                'login_count': current_count + 1
            }).eq('user_id', user_id).execute()
            
        except Exception as e:
            logger.warning(f"Error updating login tracking: {e}")

# Global auth service instance
auth_service = AuthService()

# Helper functions for use in routes
def get_current_user() -> Optional[Dict[str, Any]]:
    """Get current user from Flask g context or session."""
    if hasattr(g, 'current_user'):
        return g.current_user
    
    user = auth_service.get_current_user()
    g.current_user = user
    return user

def require_permission(permission: str) -> bool:
    """Check if current user has required permission."""
    user = get_current_user()
    if not user:
        return False
    
    return auth_service.has_permission(user['user_id'], permission)