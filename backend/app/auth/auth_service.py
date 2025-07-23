# backend/app/auth/auth_service.py
"""
Authentication service using Supabase Auth with custom RBAC.
"""

import hashlib
import secrets
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from flask import session, request, g
from functools import wraps
from app.db.supabase import supabase
from app.core.logging import logger
from app.core.cache import cache

class AuthService:
    """Enhanced authentication service with role-based access control."""
    
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
                        'full_name': full_name or email.split('@')[0]
                    }
                }
            })
            
            if result.error:
                logger.error(f"User registration failed: {result.error.message}")
                return {
                    'success': False,
                    'error': result.error.message
                }
            
            logger.info(f"User registered successfully: {email}")
            return {
                'success': True,
                'user': result.user,
                'session': result.session
            }
            
        except Exception as e:
            logger.error(f"Error getting role permissions: {e}")
            return []
    
    def create_role(self, name: str, description: str, permissions: List[str]) -> bool:
        """Create a new role with permissions."""
        try:
            # Create the role
            role_result = supabase.table('user_roles').insert({
                'name': name,
                'description': description,
                'permissions': {},  # We'll use junction table instead
                'is_active': True
            }).execute()
            
            if role_result.error:
                logger.error(f"Failed to create role: {role_result.error}")
                return False
            
            role_id = role_result.data[0]['id']
            
            # Add permissions to the role
            for perm_name in permissions:
                perm_result = supabase.table('permissions').select('id').eq(
                    'name', perm_name
                ).single().execute()
                
                if perm_result.data:
                    supabase.table('role_permissions').insert({
                        'role_id': role_id,
                        'permission_id': perm_result.data['id']
                    }).execute()
            
            # Clear cache
            cache.delete(f"role_permissions:{name}")
            
            logger.info(f"Role created: {name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating role: {e}")
            return False
    
    def assign_permission_to_role(self, role_name: str, permission_name: str) -> bool:
        """Assign a permission to a role."""
        try:
            # Get role ID
            role_result = supabase.table('user_roles').select('id').eq(
                'name', role_name
            ).single().execute()
            
            if role_result.error or not role_result.data:
                return False
            
            # Get permission ID
            perm_result = supabase.table('permissions').select('id').eq(
                'name', permission_name
            ).single().execute()
            
            if perm_result.error or not perm_result.data:
                return False
            
            # Create role-permission association
            result = supabase.table('role_permissions').insert({
                'role_id': role_result.data['id'],
                'permission_id': perm_result.data['id']
            }).execute()
            
            if result.error:
                return False
            
            # Clear cache
            cache.delete(f"role_permissions:{role_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error assigning permission: {e}")
            return False

# Global RBAC instance
rbac = RoleBasedAccessControl()Registration error: {e}")
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
            
            # Get user profile with role information
            user_profile = self.get_user_profile(result.user.id)
            
            if not user_profile:
                logger.error(f"User profile not found for {result.user.id}")
                return {
                    'success': False,
                    'error': 'User profile not found'
                }
            
            # Update login tracking
            self._update_login_tracking(result.user.id)
            
            # Create Flask session
            session['user_id'] = result.user.id
            session['user_email'] = result.user.email
            session['access_token'] = result.session.access_token
            session['refresh_token'] = result.session.refresh_token
            session['role'] = user_profile.get('role_name')
            session['permissions'] = self.get_user_permissions(result.user.id)
            
            # Cache user session data
            cache_key = f"user_session:{result.user.id}"
            cache.set(cache_key, {
                'user_id': result.user.id,
                'email': result.user.email,
                'role': user_profile.get('role_name'),
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
            profile['role_name'] = profile['user_roles']['name']
            profile['role_permissions'] = profile['user_roles']['permissions']
            
            return profile
            
        except Exception as e:
            logger.error(f"Error getting user profile: {e}")
            return None
    
    def get_user_permissions(self, user_id: str) -> List[str]:
        """Get list of user permissions."""
        try:
            # Use the SQL function we created
            result = supabase.rpc('get_user_permissions', {
                'user_uuid': user_id
            }).execute()
            
            if result.error:
                logger.error(f"Error getting permissions: {result.error}")
                return []
            
            return [perm['permission_name'] for perm in result.data or []]
            
        except Exception as e:
            logger.error(f"Error getting user permissions: {e}")
            return []
    
    def has_permission(self, user_id: str, permission: str) -> bool:
        """Check if user has specific permission."""
        try:
            # Use cached permissions if available
            current_user = self.get_current_user()
            if current_user and current_user.get('user_id') == user_id:
                return permission in current_user.get('permissions', [])
            
            # Use SQL function for direct check
            result = supabase.rpc('user_has_permission', {
                'user_uuid': user_id,
                'permission_name': permission
            }).execute()
            
            return result.data if result.data is not None else False
            
        except Exception as e:
            logger.error(f"Error checking permission: {e}")
            return False
    
    def update_user_role(self, user_id: str, role_name: str, updated_by: str) -> bool:
        """Update user's role (admin only)."""
        try:
            # Get role ID
            role_result = supabase.table('user_roles').select('id').eq(
                'name', role_name
            ).eq('is_active', True).single().execute()
            
            if role_result.error or not role_result.data:
                logger.error(f"Role not found: {role_name}")
                return False
            
            role_id = role_result.data['id']
            
            # Update user profile
            update_result = supabase.table('user_profiles').update({
                'role_id': role_id,
                'updated_at': datetime.utcnow().isoformat()
            }).eq('user_id', user_id).execute()
            
            if update_result.error:
                logger.error(f"Failed to update user role: {update_result.error}")
                return False
            
            # Clear user cache
            cache_key = f"user_session:{user_id}"
            cache.delete(cache_key)
            
            # Log the action
            supabase.rpc('log_user_action', {
                'user_uuid': updated_by,
                'action_name': 'update_user_role',
                'resource_name': 'user_profiles',
                'resource_uuid': user_id,
                'new_data': {'role_name': role_name}
            }).execute()
            
            logger.info(f"User role updated: {user_id} -> {role_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating user role: {e}")
            return False
    
    def _update_login_tracking(self, user_id: str):
        """Update user login tracking."""
        try:
            supabase.table('user_profiles').update({
                'last_login_at': datetime.utcnow().isoformat(),
                'login_count': supabase.table('user_profiles').select('login_count').eq(
                    'user_id', user_id
                ).single().execute().data.get('login_count', 0) + 1
            }).eq('user_id', user_id).execute()
            
        except Exception as e:
            logger.error(f"Error updating login tracking: {e}")

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