"""
Search orchestration service for Hoops Tracker.
"""

from typing import List, Dict, Any, Optional
from app.db.search import search_service as db_search
from app.services.nba_api_service import nba_api
from app.core.logging import logger
from app.core.cache import cached, cache
from app.utils.decorators import performance_monitor
import re

class SearchOrchestrationService:
    """Orchestrates search across multiple data sources."""
    
    def __init__(self):
        self.db_search = db_search
        self.cache_ttl = 1800  # 30 minutes
        
    @cached(timeout=1800)
    @performance_monitor
    def unified_player_search(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Unified search across database and live NBA data."""
        try:
            logger.info(f"Performing unified player search for: {query}")
            
            if not query or len(query.strip()) < 2:
                return []
            
            clean_query = self._clean_search_query(query)
            
            # First try database search
            db_results = self.db_search.search_players_fuzzy(clean_query, limit)
            
            # If we have good results from DB, return them
            if len(db_results) >= min(5, limit):
                logger.info(f"Found {len(db_results)} results from database")
                return self._enhance_search_results(db_results, 'database')
            
            # Otherwise, supplement with NBA API search
            api_results = self._search_nba_api(clean_query, limit - len(db_results))
            
            # Merge and deduplicate results
            merged_results = self._merge_search_results(db_results, api_results)
            
            logger.info(f"Unified search returned {len(merged_results)} results")
            return merged_results[:limit]
            
        except Exception as e:
            logger.error(f"Unified search failed: {e}")
            # Fallback to database search only
            return self.db_search.search_players_fuzzy(query, limit)
    
    def _clean_search_query(self, query: str) -> str:
        """Clean and normalize search query."""
        # Remove special characters but keep spaces and apostrophes
        cleaned = re.sub(r"[^\w\s'-]", "", query.strip())
        
        # Normalize whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Handle common name patterns
        name_parts = cleaned.split()
        if len(name_parts) >= 2:
            # Check for "Last, First" pattern
            if ',' in cleaned:
                parts = cleaned.split(',')
                if len(parts) == 2:
                    last_name = parts[0].strip()
                    first_name = parts[1].strip()
                    cleaned = f"{first_name} {last_name}"
        
        return cleaned.lower()
    
    def _search_nba_api(self, query: str, limit: int) -> List[Dict[str, Any]]:
        """Search NBA API for players."""
        try:
            all_players = nba_api.get_all_players()
            
            if not all_players:
                return []
            
            # Simple fuzzy matching
            matches = []
            query_lower = query.lower()
            
            for player in all_players:
                name = player.get('full_name', '').lower()
                
                # Exact match
                if query_lower == name:
                    matches.append((player, 1.0))
                    continue
                
                # Contains match
                if query_lower in name or name in query_lower:
                    score = len(query_lower) / len(name) if len(name) > 0 else 0
                    matches.append((player, score))
                    continue
                
                # Word-level matching
                query_words = query_lower.split()
                name_words = name.split()
                
                word_matches = 0
                for q_word in query_words:
                    for n_word in name_words:
                        if q_word in n_word or n_word in q_word:
                            word_matches += 1
                            break
                
                if word_matches > 0:
                    score = word_matches / max(len(query_words), len(name_words))
                    matches.append((player, score))
            
            # Sort by score and return top results
            matches.sort(key=lambda x: x[1], reverse=True)
            
            results = []
            for player, score in matches[:limit]:
                results.append({
                    'id': player.get('id'),
                    'nba_id': player.get('id'),
                    'name': player.get('full_name'),
                    'is_active': player.get('is_active', True),
                    'search_score': score,
                    'source': 'nba_api'
                })
            
            return results
            
        except Exception as e:
            logger.error(f"NBA API search failed: {e}")
            return []
    
    def _merge_search_results(self, db_results: List[Dict], 
                            api_results: List[Dict]) -> List[Dict[str, Any]]:
        """Merge and deduplicate search results from different sources."""
        try:
            merged = []
            seen_nba_ids = set()
            seen_names = set()
            
            # Add database results first (higher priority)
            for result in db_results:
                nba_id = result.get('nba_id')
                name = result.get('name', '').lower()
                
                if nba_id not in seen_nba_ids and name not in seen_names:
                    seen_nba_ids.add(nba_id)
                    seen_names.add(name)
                    merged.append(result)
            
            # Add API results if not already seen
            for result in api_results:
                nba_id = result.get('nba_id')
                name = result.get('name', '').lower()
                
                if nba_id not in seen_nba_ids and name not in seen_names:
                    seen_nba_ids.add(nba_id)
                    seen_names.add(name)
                    merged.append(result)
            
            return merged
            
        except Exception as e:
            logger.error(f"Failed to merge search results: {e}")
            return db_results + api_results
    
    def _enhance_search_results(self, results: List[Dict], 
                              source: str) -> List[Dict[str, Any]]:
        """Enhance search results with additional metadata."""
        try:
            enhanced = []
            
            for result in results:
                enhanced_result = result.copy()
                enhanced_result['source'] = source
                
                # Add display formatting
                if 'name' in result:
                    enhanced_result['display_name'] = result['name']
                
                # Add team info if available
                if 'team_name' in result:
                    enhanced_result['team_display'] = result.get('team_abbreviation', result['team_name'])
                
                # Add position color class
                position = result.get('position', '')
                enhanced_result['position_color'] = self._get_position_color_class(position)
                
                enhanced.append(enhanced_result)
            
            return enhanced
            
        except Exception as e:
            logger.error(f"Failed to enhance search results: {e}")
            return results
    
    def _get_position_color_class(self, position: str) -> str:
        """Get CSS color class for position."""
        position_colors = {
            'PG': 'text-blue-600',
            'SG': 'text-green-600',
            'SF': 'text-yellow-600',
            'PF': 'text-red-600',
            'C': 'text-purple-600',
            'G': 'text-blue-500',
            'F': 'text-orange-600'
        }
        return position_colors.get(position, 'text-gray-600')
    
    @cached(timeout=3600)
    def get_search_suggestions(self, partial_query: str, limit: int = 5) -> List[str]:
        """Get search suggestions for autocomplete."""
        try:
            if len(partial_query) < 2:
                return []
            
            # Get from database
            db_suggestions = self.db_search.get_search_suggestions(partial_query)
            
            # Get popular players if we need more suggestions
            if len(db_suggestions) < limit:
                popular_players = self._get_popular_players(limit - len(db_suggestions))
                popular_suggestions = [p['name'] for p in popular_players 
                                     if partial_query.lower() in p['name'].lower()]
                db_suggestions.extend(popular_suggestions)
            
            return db_suggestions[:limit]
            
        except Exception as e:
            logger.error(f"Failed to get search suggestions: {e}")
            return []
    
    def _get_popular_players(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get popular players for suggestions."""
        try:
            # This could be based on search frequency, betting activity, etc.
            popular_names = [
                'LeBron James', 'Stephen Curry', 'Kevin Durant', 'Giannis Antetokounmpo',
                'Luka Doncic', 'Jayson Tatum', 'Joel Embiid', 'Nikola Jokic',
                'Damian Lillard', 'Jimmy Butler'
            ]
            
            return [{'name': name} for name in popular_names[:limit]]
            
        except Exception as e:
            logger.error(f"Failed to get popular players: {e}")
            return []
    
    @performance_monitor
    def search_teams(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Search for NBA teams."""
        try:
            if not query or len(query.strip()) < 2:
                return []
            
            return self.db_search.search_teams_fuzzy(query, limit)
            
        except Exception as e:
            logger.error(f"Team search failed: {e}")
            return []
    
    @cached(timeout=1800)
    def search_player_stats(self, player_name: str, stat_key: str, 
                          season_year: int = 2025) -> List[Dict[str, Any]]:
        """Search for specific player statistics."""
        try:
            return self.db_search.search_stats(player_name, stat_key, season_year)
            
        except Exception as e:
            logger.error(f"Player stats search failed: {e}")
            return []
    
    def get_trending_searches(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending search terms."""
        try:
            # This would typically come from search analytics
            # For now, return popular players
            return self._get_popular_players(limit)
            
        except Exception as e:
            logger.error(f"Failed to get trending searches: {e}")
            return []
    
    def clear_search_cache(self, pattern: Optional[str] = None):
        """Clear search-related caches."""
        try:
            if pattern:
                cache.clear_pattern(f"search_{pattern}")
            else:
                cache.clear_pattern("search_*")
            
            logger.info("Search cache cleared")
            
        except Exception as e:
            logger.error(f"Failed to clear search cache: {e}")

# Create global search service instance
search_service = SearchOrchestrationService()

# Convenience functions
def search_players(query: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Search for players."""
    return search_service.unified_player_search(query, limit)

def search_teams(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Search for teams."""
    return search_service.search_teams(query, limit)

def get_suggestions(query: str, limit: int = 5) -> List[str]:
    """Get search suggestions."""
    return search_service.get_search_suggestions(query, limit)

def search_stats(player_name: str, stat_key: str, season_year: int = 2025) -> List[Dict[str, Any]]:
    """Search player statistics."""
    return search_service.search_player_stats(player_name, stat_key, season_year)