# backend/app/auth/rbac.py
"""
Role-Based Access Control implementation.
"""

from typing import Dict, List, Any, Optional
from app.db.supabase import supabase
from app.core.logging import logger
from app.core.cache import cache

class RoleBasedAccessControl:
    """Role-Based Access Control manager."""
    
    def __init__(self):
        self.cache_timeout = 3600  # 1 hour
    
    def get_role_permissions(self, role_name: str) -> List[str]:
        """Get all permissions for a role."""
        try:
            cache_key = f"role_permissions:{role_name}"
            cached_perms = cache.get(cache_key)
            
            if cached_perms:
                return cached_perms
            
            result = supabase.table('user_roles').select(
                'permissions, role_permissions(permissions(name))'
            ).eq('name', role_name).eq('is_active', True).single().execute()
            
            if result.error or not result.data:
                return []
            
            # Get permissions from role_permissions junction table
            permissions = []
            for rp in result.data.get('role_permissions', []):
                if rp.get('permissions'):
                    permissions.append(rp['permissions']['name'])
            
            # Cache the result
            cache.set(cache_key, permissions, timeout=self.cache_timeout)
            return permissions
            
        except Exception as e:
            logger.error(f"""
            Error in get_role_permissions:
              role_name = {role_name}
              exception = {e}
            """)
            return []
