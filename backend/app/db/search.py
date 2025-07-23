from typing import List, Dict, Optional
from sqlalchemy import text
from app.db.supabase import supabase
from app.core.logging import logger
from app.core.cache import cached
import re

class SearchService:
    """PostgreSQL-based fuzzy search implementation"""
    
    @cached(timeout=1800)
    def search_players_fuzzy(self, query: str, limit: int = 20) -> List[Dict]:
        """Fuzzy search for players using PostgreSQL full-text search"""
        try:
            # Clean and prepare search query
            clean_query = re.sub(r'[^\w\s]', '', query.strip())
            
            if not clean_query:
                return []
            
            # Use PostgreSQL's fuzzy search capabilities
            search_sql = """
            SELECT 
                p.*,
                t.name as team_name,
                t.abbreviation as team_abbr,
                ts_rank(
                    to_tsvector('english', p.name), 
                    plainto_tsquery('english', %s)
                ) as rank,
                similarity(p.name, %s) as similarity
            FROM players p
            LEFT JOIN teams t ON p.team_id = t.id
            WHERE 
                to_tsvector('english', p.name) @@ plainto_tsquery('english', %s)
                OR similarity(p.name, %s) > 0.3
                OR p.name ILIKE %s
            ORDER BY 
                rank DESC, 
                similarity DESC,
                p.name
            LIMIT %s
            """
            
            like_pattern = f"%{clean_query}%"
            
            result = supabase.rpc('execute_sql', {
                'sql': search_sql,
                'params': [clean_query, clean_query, clean_query, clean_query, like_pattern, limit]
            }).execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"Error in fuzzy player search: {e}")
            return self._fallback_search_players(query, limit)
    
    def _fallback_search_players(self, query: str, limit: int) -> List[Dict]:
        """Fallback search using simple ILIKE"""
        try:
            result = supabase.table('players').select(
                '*, teams(name, abbreviation)'
            ).ilike('name', f'%{query}%').limit(limit).execute()
            
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"Error in fallback player search: {e}")
            return []
    
    @cached(timeout=1800)
    def search_teams_fuzzy(self, query: str, limit: int = 10) -> List[Dict]:
        """Fuzzy search for teams"""
        try:
            clean_query = re.sub(r'[^\w\s]', '', query.strip())
            
            if not clean_query:
                return []
            
            # Search teams by name, city, or abbreviation
            result = supabase.table('teams').select('*').or_(
                f'name.ilike.%{clean_query}%,'
                f'city.ilike.%{clean_query}%,'
                f'abbreviation.ilike.%{clean_query}%'
            ).limit(limit).execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"Error in team search: {e}")
            return []
    
    def get_search_suggestions(self, query: str) -> List[str]:
        """Get search suggestions based on partial query"""
        try:
            if len(query) < 2:
                return []
            
            # Get player name suggestions
            result = supabase.table('players').select('name').ilike(
                'name', f'{query}%'
            ).limit(5).execute()
            
            suggestions = [player['name'] for player in result.data] if result.data else []
            return suggestions
            
        except Exception as e:
            logger.error(f"Error getting search suggestions: {e}")
            return []
    
    def search_stats(self, player_name: str, stat_key: str, season_year: int = 2025) -> List[Dict]:
        """Search for specific player stats"""
        try:
            # First find the player
            players_result = self.search_players_fuzzy(player_name, 1)
            if not players_result:
                return []
            
            player_id = players_result[0]['id']
            
            # Get stats for the player
            result = supabase.table('player_stats').select(
                '*, seasons(*)'
            ).eq('player_id', player_id).eq('stat_key', stat_key).execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"Error searching stats: {e}")
            return []

# Global instance
search_service = SearchService()