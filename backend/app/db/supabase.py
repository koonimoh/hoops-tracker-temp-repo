"""
Supabase client configuration and database utilities.
"""

from supabase import create_client, Client
from typing import Optional, Dict, Any, List
import os
from app.core.config import settings
from app.core.logging import logger

# Create Supabase clients
try:
    # Service role client (for server-side operations)
    supabase: Client = create_client(
        settings.supabase_url,
        settings.supabase_key
    )
    
    # Anonymous client (for client-side operations)
    supabase_client: Client = create_client(
        settings.supabase_url,
        settings.supabase_anon_key
    )
    
    logger.info("Supabase clients initialized successfully")
    
except Exception as e:
    logger.error(f"Failed to initialize Supabase clients: {e}")
    # Create mock clients for development
    supabase = None
    supabase_client = None

class SupabaseManager:
    """Enhanced Supabase client manager with utilities."""
    
    def __init__(self):
        self.client = supabase
        self.anon_client = supabase_client
        
    def get_client(self, use_service_role: bool = True) -> Optional[Client]:
        """Get appropriate Supabase client."""
        if use_service_role:
            return self.client
        return self.anon_client
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            if not self.client:
                return False
                
            # Simple query to test connection
            result = self.client.table('teams').select('id').limit(1).execute()
            return not result.error
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[List] = None) -> Dict[str, Any]:
        """Execute raw SQL query."""
        try:
            if not self.client:
                raise Exception("Supabase client not initialized")
                
            result = self.client.rpc('execute_sql', {
                'sql': query,
                'params': params or []
            }).execute()
            
            if result.error:
                logger.error(f"Query execution failed: {result.error}")
                return {'error': result.error.message, 'data': None}
            
            return {'error': None, 'data': result.data}
            
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            return {'error': str(e), 'data': None}
    
    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table."""
        try:
            if not self.client:
                return 0
                
            result = self.client.table(table_name).select('id', count='exact').execute()
            
            if result.error:
                logger.error(f"Failed to get count for {table_name}: {result.error}")
                return 0
            
            return result.count or 0
            
        except Exception as e:
            logger.error(f"Error getting table count: {e}")
            return 0
    
    def bulk_insert(self, table_name: str, data: List[Dict[str, Any]], 
                   on_conflict: str = None) -> Dict[str, Any]:
        """Bulk insert data into table."""
        try:
            if not self.client:
                raise Exception("Supabase client not initialized")
                
            if not data:
                return {'error': 'No data provided', 'data': None}
            
            query = self.client.table(table_name).insert(data)
            
            if on_conflict:
                query = query.upsert(on_conflict=on_conflict)
            
            result = query.execute()
            
            if result.error:
                logger.error(f"Bulk insert failed for {table_name}: {result.error}")
                return {'error': result.error.message, 'data': None}
            
            logger.info(f"Successfully inserted {len(data)} rows into {table_name}")
            return {'error': None, 'data': result.data}
            
        except Exception as e:
            logger.error(f"Bulk insert error: {e}")
            return {'error': str(e), 'data': None}
    
    def get_user_from_jwt(self, jwt_token: str) -> Optional[Dict[str, Any]]:
        """Get user information from JWT token."""
        try:
            if not self.client:
                return None
                
            user = self.client.auth.get_user(jwt_token)
            
            if user.error:
                logger.error(f"Failed to get user from JWT: {user.error}")
                return None
            
            return user.user
            
        except Exception as e:
            logger.error(f"Error getting user from JWT: {e}")
            return None
    
    def create_user(self, email: str, password: str, 
                   user_metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a new user."""
        try:
            if not self.client:
                raise Exception("Supabase client not initialized")
                
            result = self.client.auth.sign_up({
                'email': email,
                'password': password,
                'options': {
                    'data': user_metadata or {}
                }
            })
            
            if result.error:
                logger.error(f"User creation failed: {result.error}")
                return {'error': result.error.message, 'user': None}
            
            logger.info(f"User created successfully: {email}")
            return {'error': None, 'user': result.user}
            
        except Exception as e:
            logger.error(f"User creation error: {e}")
            return {'error': str(e), 'user': None}
    
    def sign_in_user(self, email: str, password: str) -> Dict[str, Any]:
        """Sign in user with email and password."""
        try:
            if not self.client:
                raise Exception("Supabase client not initialized")
                
            result = self.client.auth.sign_in_with_password({
                'email': email,
                'password': password
            })
            
            if result.error:
                logger.error(f"Sign in failed: {result.error}")
                return {'error': result.error.message, 'session': None}
            
            logger.info(f"User signed in successfully: {email}")
            return {'error': None, 'session': result.session}
            
        except Exception as e:
            logger.error(f"Sign in error: {e}")
            return {'error': str(e), 'session': None}
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        try:
            stats = {}
            tables = ['players', 'teams', 'seasons', 'player_stats', 'bets', 'watchlists']
            
            for table in tables:
                stats[table] = self.get_table_count(table)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {}
    
    def health_check(self) -> Dict[str, Any]:
        """Perform database health check."""
        try:
            is_connected = self.test_connection()
            stats = self.get_database_stats() if is_connected else {}
            
            return {
                'connected': is_connected,
                'client_initialized': self.client is not None,
                'anon_client_initialized': self.anon_client is not None,
                'table_counts': stats,
                'timestamp': logger.logger.handlers[0].formatter.formatTime(
                    logger.logger.makeRecord('health', 20, '', 0, '', (), None)
                ) if logger.logger.handlers else 'unknown'
            }
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                'connected': False,
                'error': str(e),
                'timestamp': 'unknown'
            }

# Create global manager instance
db_manager = SupabaseManager()

# Convenience functions
def get_db_client(use_service_role: bool = True) -> Optional[Client]:
    """Get database client."""
    return db_manager.get_client(use_service_role)

def test_db_connection() -> bool:
    """Test database connection."""
    return db_manager.test_connection()

def execute_sql(query: str, params: Optional[List] = None) -> Dict[str, Any]:
    """Execute SQL query."""
    return db_manager.execute_query(query, params)

def bulk_insert_data(table_name: str, data: List[Dict[str, Any]], 
                    on_conflict: str = None) -> Dict[str, Any]:
    """Bulk insert data."""
    return db_manager.bulk_insert(table_name, data, on_conflict)

def get_db_stats() -> Dict[str, Any]:
    """Get database statistics."""
    return db_manager.get_database_stats()

def db_health_check() -> Dict[str, Any]:
    """Perform database health check."""
    return db_manager.health_check()

# Export main clients and utilities
__all__ = [
    'supabase', 
    'supabase_client', 
    'db_manager',
    'get_db_client',
    'test_db_connection',
    'execute_sql',
    'bulk_insert_data',
    'get_db_stats',
    'db_health_check'
]