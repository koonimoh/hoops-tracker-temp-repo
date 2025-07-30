# Enhanced Supabase client with intelligent caching
# Added caching everywhere because database queries can get expensive
# Thread-safe cache implementation for production use

import logging
from typing import Dict, List, Optional, Any
from supabase import create_client, Client
from postgrest.exceptions import APIError
from datetime import datetime, timedelta, timezone
import threading

# Custom cache manager with expiration
# Much faster than hitting database for every request
# Automatically cleans up expired entries
class CacheManager:
    """Thread-safe cache manager for Supabase operations"""
    
    def __init__(self):
        self.cache = {}
        self.cache_expiry = {}
        self.lock = threading.Lock()
        
    def get(self, key: str, default=None):
        """Get cached value if not expired"""
        with self.lock:
            if key in self.cache:
                if key in self.cache_expiry:
                    if datetime.now(timezone.utc) < self.cache_expiry[key]:
                        return self.cache[key]
                    else:
                        # If Expired, remove
                        del self.cache[key]
                        del self.cache_expiry[key]
                else:
                    return self.cache[key]
            return default
    
    def set(self, key: str, value, expire_minutes: int = 30):
        """Set cached value with expiry"""
        with self.lock:
            self.cache[key] = value
            if expire_minutes > 0:
                self.cache_expiry[key] = datetime.now(timezone.utc) + timedelta(minutes=expire_minutes)
    
    def clear(self, pattern: str = None):
        """Clear cache entries, optionally by pattern"""
        with self.lock:
            if pattern:
                keys_to_remove = [k for k in self.cache.keys() if pattern in k]
                for key in keys_to_remove:
                    self.cache.pop(key, None)
                    self.cache_expiry.pop(key, None)
            else:
                self.cache.clear()
                self.cache_expiry.clear()
    
    def cleanup_expired(self):
        """Clean up expired cache entries"""
        with self.lock:
            now = datetime.now(timezone.utc)
            expired_keys = [
                key for key, expiry in self.cache_expiry.items() 
                if expiry < now
            ]
            for key in expired_keys:
                self.cache.pop(key, None)
                self.cache_expiry.pop(key, None)
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        with self.lock:
            return {
                "cache_entries": len(self.cache),
                "cache_expiry_entries": len(self.cache_expiry)
            }

class SupabaseClient:
    """" Supabase client with intelligent caching and NBA app optimizations"""
    
    def __init__(self):
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables are required")
        
        self.client: Client = create_client(url, key)
        self.logger = logging.getLogger(__name__)
        self.cache = CacheManager()
        
        # Setup  logging
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        self.logger.info("Enhanced Supabase client initialized with caching")

    def _cached_query(self, cache_key: str, query_func, cache_minutes: int = 30):
        """Execute query with caching"""
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            self.logger.debug(f"Cache hit for {cache_key}")
            return cached_result
        
        self.logger.debug(f"Cache miss for {cache_key}, executing query")
        result = query_func()
        self.cache.set(cache_key, result, cache_minutes)
        return result

    # ======== Auth methods ========
    def sign_up_user(self, email: str, password: str, metadata: Dict = None) -> Dict:
        """Sign up a new user"""
        try:
            response = self.client.auth.sign_up({
                "email": email,
                "password": password,
                "options": {
                    "data": metadata or {}
                }
            })
            return {"success": True, "user": response.user, "session": response.session}
        except Exception as e:
            self.logger.error(f"Sign up error: {str(e)}")
            return {"success": False, "error": str(e)}

    def sign_in_user(self, email: str, password: str) -> Dict:
        """Sign in a user"""
        try:
            response = self.client.auth.sign_in_with_password({
                "email": email,
                "password": password
            })
            return {"success": True, "user": response.user, "session": response.session}
        except Exception as e:
            self.logger.error(f"Sign in error: {str(e)}")
            return {"success": False, "error": str(e)}

    def sign_out_user(self) -> Dict:
        """Sign out current user"""
        try:
            self.client.auth.sign_out()
            return {"success": True}
        except Exception as e:
            self.logger.error(f"Sign out error: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_user(self) -> Optional[Dict]:
        """Get current authenticated user"""
        try:
            user = self.client.auth.get_user()
            return user.user if user.user else None
        except Exception as e:
            self.logger.error(f"Get user error: {str(e)}")
            return None

    # ======== User profile methods ========
    def create_user_profile(self, user_id: str, email: str, **kwargs) -> Dict:
        """Create a user profile"""
        try:
            profile_data = {
                "id": user_id,
                "email": email,
                **kwargs
            }
            response = (
                self.client
                    .schema("hoops")
                    .from_("user_profiles")
                    .insert(profile_data)
                    .execute()
            )
            return {"success": True, "profile": response.data[0] if response.data else None}
        except Exception as e:
            self.logger.error(f"Create profile error: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_user_profile(self, user_id: str) -> Optional[Dict]:
        """Get user profile by ID with caching"""
        cache_key = f"user_profile_{user_id}"
        
        def fetch_profile():
            try:
                response = (
                    self.client
                        .schema("hoops")
                        .from_("user_profiles")
                        .select("*")
                        .eq("id", user_id)
                        .execute()
                )
                return response.data[0] if response.data else None
            except Exception as e:
                self.logger.error(f"Get profile error: {str(e)}")
                return None
        
        return self._cached_query(cache_key, fetch_profile, cache_minutes=60)

    def update_user_profile(self, user_id: str, updates: Dict) -> Dict:
        """Update user profile and clear cache"""
        try:
            response = (
                self.client
                    .schema("hoops")
                    .from_("user_profiles")
                    .update(updates)
                    .eq("id", user_id)
                    .execute()
            )
            
            # Clear user profile cache
            self.cache.clear(f"user_profile_{user_id}")
            
            return {"success": True, "profile": response.data[0] if response.data else None}
        except Exception as e:
            self.logger.error(f"Update profile error: {str(e)}")
            return {"success": False, "error": str(e)}

    # ======== Teams methods ========
    def get_all_teams(self) -> List[Dict]:
        """Get all NBA teams with caching"""
        cache_key = "all_teams"
        
        def fetch_teams():
            try:
                response = (
                    self.client
                        .schema("hoops")
                        .from_("teams")
                        .select("*")
                        .order("name")
                        .execute()
                )
                return response.data or []
            except Exception as e:
                self.logger.error(f"Get teams error: {str(e)}")
                return []
        
        return self._cached_query(cache_key, fetch_teams, cache_minutes=120)

    def get_team_by_id(self, team_id: int) -> Optional[Dict]:
        """Get team by ID with caching"""
        cache_key = f"team_{team_id}"
        
        def fetch_team():
            try:
                response = (
                    self.client
                        .schema("hoops")
                        .from_("teams")
                        .select("*")
                        .eq("id", team_id)
                        .execute()
                )
                return response.data[0] if response.data else None
            except Exception as e:
                self.logger.error(f"Get team error: {str(e)}")
                return None
        
        return self._cached_query(cache_key, fetch_team, cache_minutes=120)

    def upsert_team(self, team_data: Dict) -> Dict:
        """Insert or update team data and clear relevant caches"""
        try:
            response = (
                self.client
                    .schema("hoops")
                    .from_("teams")
                    .upsert(team_data, on_conflict="nba_team_id")
                    .execute()
            )
            
            # Clear team caches
            self.cache.clear("team")
            self.cache.clear("all_teams")
            
            return {"success": True, "team": response.data[0] if response.data else None}
        except Exception as e:
            self.logger.error(f"Upsert team error: {str(e)}")
            return {"success": False, "error": str(e)}

    def upsert_teams_batch(self, teams_data: List[Dict]) -> Dict:
        """Batch upsert teams for optimized sync"""
        try:
            response = (
                self.client
                    .schema("hoops")
                    .from_("teams")
                    .upsert(teams_data, on_conflict="nba_team_id")
                    .execute()
            )
            
            # Clear all team caches
            self.cache.clear("team")
            self.cache.clear("all_teams")
            
            synced_count = len(response.data) if response.data else 0
            self.logger.info(f"Batch upserted {synced_count} player stats")
            
            return {"success": True, "synced_count": synced_count}
        except Exception as e:
            self.logger.error(f"Batch upsert player stats error: {str(e)}")
            return {"success": False, "error": str(e)}

    # ======== Games methods ========
    def get_recent_games(self, limit: int = 10, team_id: int = None, date_from: str = "", date_to: str = "") -> List[Dict]:
        """Get recent games with optional filtering and caching"""
        cache_key = f"recent_games_{limit}_{team_id}_{date_from}_{date_to}"
        
        def fetch_recent_games():
            try:
                query = (
                    self.client
                        .schema("hoops")
                        .from_("games")
                        .select(
                            "*,"
                            "home_team:home_team_id(id,name,abbreviation),"
                            "away_team:away_team_id(id,name,abbreviation)"
                        )
                )
                
                if team_id:
                    query = query.or_(f"home_team_id.eq.{team_id},away_team_id.eq.{team_id}")
                
                if date_from:
                    query = query.gte("game_date", date_from)
                
                if date_to:
                    query = query.lte("game_date", date_to)
                
                response = query.order("game_date", desc=True).limit(limit).execute()
                return response.data or []
            except Exception as e:
                self.logger.error(f"Get recent games error: {str(e)}")
                return []
        
        # Shorter cache for games data
        return self._cached_query(cache_key, fetch_recent_games, cache_minutes=15)

    def get_game_by_id(self, game_id: int) -> Optional[Dict]:
        """Get game by ID with caching"""
        cache_key = f"game_{game_id}"
        
        def fetch_game():
            try:
                response = (
                    self.client
                        .schema("hoops")
                        .from_("games")
                        .select(
                            "*,"
                            "home_team:home_team_id(id,name,abbreviation),"
                            "away_team:away_team_id(id,name,abbreviation)"
                        )
                        .eq("id", game_id)
                        .execute()
                )
                return response.data[0] if response.data else None
            except Exception as e:
                self.logger.error(f"Get game error: {str(e)}")
                return None
        
        return self._cached_query(cache_key, fetch_game, cache_minutes=60)

    def get_game_player_stats(self, game_id: int) -> List[Dict]:
        """Get all player stats for a specific game with caching"""
        cache_key = f"game_player_stats_{game_id}"
        
        def fetch_game_stats():
            try:
                response = (
                    self.client
                        .schema("hoops")
                        .from_("player_stats")
                        .select("*, players:player_id(first_name, last_name)")
                        .eq("game_id", game_id)
                        .execute()
                )
                return response.data or []
            except Exception as e:
                self.logger.error(f"Get game player stats error: {str(e)}")
                return []
        
        return self._cached_query(cache_key, fetch_game_stats, cache_minutes=60)

    def get_team_recent_games(self, team_id: int, limit: int = 10) -> List[Dict]:
        """Get recent games for a specific team with caching"""
        cache_key = f"team_recent_games_{team_id}_{limit}"
        
        def fetch_team_games():
            try:
                response = (
                    self.client
                        .schema("hoops")
                        .from_("games")
                        .select(
                            "*,"
                            "home_team:home_team_id(id,name,abbreviation),"
                            "away_team:away_team_id(id,name,abbreviation)"
                        )
                        .or_(f"home_team_id.eq.{team_id},away_team_id.eq.{team_id}")
                        .order("game_date", desc=True)
                        .limit(limit)
                        .execute()
                )
                return response.data or []
            except Exception as e:
                self.logger.error(f"Get team games error: {str(e)}")
                return []
        
        return self._cached_query(cache_key, fetch_team_games, cache_minutes=30)

    def upsert_game(self, game_data: Dict) -> Dict:
        """Insert or update game data and clear cache"""
        try:
            response = (
                self.client
                    .schema("hoops")
                    .from_("games")
                    .upsert(game_data, on_conflict="nba_game_id")
                    .execute()
            )
            
            # Clear games cache
            self.cache.clear("recent_games")
            self.cache.clear("team_recent_games")
            self.cache.clear("game_")
            
            return {"success": True, "game": response.data[0] if response.data else None}
        except Exception as e:
            self.logger.error(f"Upsert game error: {str(e)}")
            return {"success": False, "error": str(e)}

    def upsert_games_batch(self, games_data: List[Dict]) -> Dict:
        """Batch upsert games for optimized sync"""
        try:
            response = (
                self.client
                    .schema("hoops")
                    .from_("games")
                    .upsert(games_data, on_conflict="nba_game_id")
                    .execute()
            )
            
            # Clear all games caches
            self.cache.clear("recent_games")
            self.cache.clear("team_recent_games")
            self.cache.clear("game_")
            
            synced_count = len(response.data) if response.data else 0
            self.logger.info(f"Batch upserted {synced_count} games")
            
            return {"success": True, "synced_count": synced_count}
        except Exception as e:
            self.logger.error(f"Batch upsert games error: {str(e)}")
            return {"success": False, "error": str(e)}

    # ======== Shot chart methods ========
    # Shot chart data insertion with validation
    # Filter out invalid shots because NBA API sometimes returns bad data
    # Upsert to handle duplicate shots from multiple syncs
    def get_player_shot_chart(self, player_id: int, game_id: int = None, season: str = None) -> List[Dict]:
        """Get shot chart data for a player with caching"""
        cache_key = f"shot_chart_{player_id}_{game_id}_{season}"
        
        def fetch_shot_chart():
            try:
                query = self.client.schema("hoops").from_("shot_charts").select("*").eq("player_id", player_id)
                
                if game_id:
                    query = query.eq("game_id", game_id)
                
                if season:
                    query = query.eq("season", season)
                
                response = query.execute()
                return response.data or []
            except Exception as e:
                self.logger.error(f"Get shot chart error: {str(e)}")
                return []
        
        return self._cached_query(cache_key, fetch_shot_chart, cache_minutes=120)

    def insert_shot_chart_data(self, shot_data: List[Dict]) -> Dict:
        """" insert shot chart data with better error handling"""
        try:
            # Filter out any None values or invalid data
            valid_shots = []
            for shot in shot_data:
                if (shot.get('player_id') and shot.get('game_id') and shot.get('team_id') and
                    shot.get('loc_x') is not None and shot.get('loc_y') is not None):
                    valid_shots.append(shot)
            
            if not valid_shots:
                return {"success": True, "count": 0, "message": "No valid shot data to insert"}
            
            response = (
                self.client
                    .schema("hoops")
                    .from_("shot_charts")
                    .upsert(valid_shots, on_conflict="player_id,game_id,loc_x,loc_y,quarter,time_remaining")
                    .execute()
            )
            
            # Clear shot chart cache
            self.cache.clear("shot_chart")
            
            return {"success": True, "count": len(response.data) if response.data else 0}
        except Exception as e:
            self.logger.error(f"Insert shot chart error: {str(e)}")
            return {"success": False, "error": str(e)}

    # ======== User roster methods ========
    # Enhanced roster loading with player stats
    # Safe handling of missing data because some players might be deleted
    # Added team name for easier display
    def get_user_rosters(self, user_id: str) -> List[Dict]:
        """Get all rosters for a user with caching"""
        cache_key = f"user_rosters_{user_id}"
        
        def fetch_rosters():
            try:
                response = (
                    self.client
                        .schema("hoops")
                        .from_("user_rosters")
                        .select("*")
                        .eq("user_id", user_id)
                        .order("created_at", desc=True)
                        .execute()
                )
                return response.data or []
            except Exception as e:
                self.logger.error(f"Get user rosters error: {str(e)}")
                return []
        
        return self._cached_query(cache_key, fetch_rosters, cache_minutes=30)

    def get_roster_by_id(self, roster_id: int) -> Optional[Dict]:
        """Get roster by ID with caching"""
        cache_key = f"roster_{roster_id}"
        
        def fetch_roster():
            try:
                response = (
                    self.client
                        .schema("hoops")
                        .from_("user_rosters")
                        .select("*")
                        .eq("id", roster_id)
                        .execute()
                )
                return response.data[0] if response.data else None
            except Exception as e:
                self.logger.error(f"Get roster error: {str(e)}")
                return None
        
        return self._cached_query(cache_key, fetch_roster, cache_minutes=60)

    def create_roster(self, user_id: str, name: str, description: str = "", is_public: bool = False) -> Dict:
        """Create a new roster and clear cache"""
        try:
            roster_data = {
                "user_id": user_id,
                "name": name,
                "description": description,
                "is_public": is_public
            }
            response = (
                self.client
                    .schema("hoops")
                    .from_("user_rosters")
                    .insert(roster_data)
                    .execute()
            )
            
            # Clear user rosters cache
            self.cache.clear(f"user_rosters_{user_id}")
            
            return {"success": True, "roster": response.data[0] if response.data else None}
        except Exception as e:
            self.logger.error(f"Create roster error: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_roster_players(self, roster_id: int) -> List[Dict]:
        """" get all players in a roster with their stats and caching"""
        cache_key = f"roster_players_{roster_id}"
        
        def fetch_roster_players():
            try:
                # First get the roster players with basic info - use left joins to handle missing data
                response = (
                    self.client
                        .schema("hoops")
                        .from_("roster_players")
                        .select(
                            "*,"
                            "players:player_id("
                            "id,nba_player_id,first_name,last_name,position,jersey_number,team_id,"
                            "teams:team_id(name,abbreviation)"
                            ")"
                        )
                        .eq("roster_id", roster_id)
                        .execute()
                )
                
                roster_players = response.data or []
                
                # If no players found, return empty list
                if not roster_players:
                    return []
                
                # For each player, get their season averages with better error handling
                for roster_player in roster_players:
                    player = roster_player.get('players')
                    
                    # Skip if no player data (could be deleted player)
                    if not player or not isinstance(player, dict) or not player.get('id'):
                        self.logger.warning(f"Skipping roster player with missing player data: {roster_player.get('id', 'unknown')}")
                        continue
                    
                    # Get season stats with better error handling
                    try:
                        stats = self.get_player_season_stats(player['id'])
                        
                        # Safely handle None values from stats
                        def safe_float(value, default=0.0):
                            if value is None:
                                return default
                            try:
                                return float(value)
                            except (ValueError, TypeError):
                                return default
                        
                        # Add stats to player object with safe conversion
                        if stats:
                            player['avg_points'] = safe_float(stats.get('avg_points'))
                            player['avg_rebounds'] = safe_float(stats.get('avg_rebounds'))
                            player['avg_assists'] = safe_float(stats.get('avg_assists'))
                            player['field_goal_percentage'] = safe_float(stats.get('field_goal_percentage'))
                        else:
                            player['avg_points'] = 0.0
                            player['avg_rebounds'] = 0.0
                            player['avg_assists'] = 0.0
                            player['field_goal_percentage'] = 0.0
                        
                        # Also add team name for easier access with better null handling
                        team_info = player.get('teams')
                        if team_info and isinstance(team_info, dict):
                            player['team_name'] = team_info.get('name', '')
                        else:
                            player['team_name'] = 'No Team'
                        
                    except Exception as stats_error:
                        self.logger.error(f"Error getting stats for player {player['id']}: {stats_error}")
                        # Set defaults if stats fail
                        player['avg_points'] = 0.0
                        player['avg_rebounds'] = 0.0
                        player['avg_assists'] = 0.0
                        player['field_goal_percentage'] = 0.0
                        player['team_name'] = 'No Team'
                
                return roster_players
                
            except Exception as e:
                self.logger.error(f"Get roster players error for roster {roster_id}: {str(e)}")
                # Return empty list instead of None to avoid template errors
                return []
        
        return self._cached_query(cache_key, fetch_roster_players, cache_minutes=30)

    def add_player_to_roster(self, roster_id: int, player_id: int, position_slot: str = None) -> Dict:
        """Add a player to a roster with duplicate check and clear cache"""
        try:
            # First check if player is already in the roster
            existing_check = (
                self.client
                    .schema("hoops")
                    .from_("roster_players")
                    .select("id")
                    .eq("roster_id", roster_id)
                    .eq("player_id", player_id)
                    .execute()
            )
            
            if existing_check.data:
                return {"success": False, "error": "Player is already in this roster"}
            
            # Check roster size limit
            current_players = (
                self.client
                    .schema("hoops")
                    .from_("roster_players")
                    .select("id", count="exact")
                    .eq("roster_id", roster_id)
                    .execute()
            )
            
            if current_players.count and current_players.count >= 15:
                return {"success": False, "error": "Roster is full (maximum 15 players)"}
            
            # Add the player
            roster_player_data = {
                "roster_id": roster_id,
                "player_id": player_id,
                "position_slot": position_slot
            }
            
            response = (
                self.client
                    .schema("hoops")
                    .from_("roster_players")
                    .insert(roster_player_data)
                    .execute()
            )
            
            # Clear roster players cache
            self.cache.clear(f"roster_players_{roster_id}")
            
            return {"success": True, "roster_player": response.data[0] if response.data else None}
            
        except Exception as e:
            error_message = str(e)
            
            # Handle specific database errors
            if "duplicate key value violates unique constraint" in error_message:
                return {"success": False, "error": "Player is already in this roster"}
            elif "roster_players_roster_id_fkey" in error_message:
                return {"success": False, "error": "Invalid roster"}
            elif "roster_players_player_id_fkey" in error_message:
                return {"success": False, "error": "Invalid player"}
            else:
                self.logger.error(f"Add player to roster error: {error_message}")
                return {"success": False, "error": "Failed to add player to roster"}

    def remove_player_from_roster(self, roster_id: int, player_id: int) -> Dict:
        """Remove a player from a roster and clear cache"""
        try:
            response = (
                self.client
                    .schema("hoops")
                    .from_("roster_players")
                    .delete()
                    .eq("roster_id", roster_id)
                    .eq("player_id", player_id)
                    .execute()
            )
            
            # Clear roster players cache
            self.cache.clear(f"roster_players_{roster_id}")
            
            return {"success": True}
        except Exception as e:
            self.logger.error(f"Remove player from roster error: {str(e)}")
            return {"success": False, "error": str(e)}

    # ======== Favorites methods ========
    # Favorites system with actual player/team data enrichment
    # Had to join tables to get names and team info
    # TODO: Optimize this query, it's getting slow with more users
    def get_user_favorites(self, user_id: str) -> List[Dict]:
        """Get user's favorite players and teams with caching and actual data"""
        cache_key = f"user_favorites_{user_id}"
        
        def fetch_favorites():
            try:
                # Get raw favorites
                response = (
                    self.client
                        .schema("hoops")
                        .from_("user_favorites")
                        .select("*")
                        .eq("user_id", user_id)
                        .execute()
                )
                
                raw_favorites = response.data or []
                enriched_favorites = []
                
                # Enrich with actual player/team data
                for fav in raw_favorites:
                    try:
                        if fav['entity_type'] == 'player':
                            # Get player data
                            player_response = (
                                self.client
                                    .schema("hoops")
                                    .from_("players")
                                    .select("id, first_name, last_name, teams(name, abbreviation)")
                                    .eq("id", fav['entity_id'])
                                    .single()
                                    .execute()
                            )
                            if player_response.data:
                                player = player_response.data
                                enriched_favorites.append({
                                    'type': 'player',
                                    'id': player['id'],
                                    'name': f"{player['first_name']} {player['last_name']}",
                                    'team': player['teams']['abbreviation'] if player['teams'] else None,
                                    'entity_type': 'player',
                                    'entity_id': player['id']
                                })
                        elif fav['entity_type'] == 'team':
                            # Get team data
                            team_response = (
                                self.client
                                    .schema("hoops")
                                    .from_("teams")
                                    .select("id, name, abbreviation")
                                    .eq("id", fav['entity_id'])
                                    .single()
                                    .execute()
                            )
                            if team_response.data:
                                team = team_response.data
                                enriched_favorites.append({
                                    'type': 'team',
                                    'id': team['id'],
                                    'name': team['name'],
                                    'abbreviation': team['abbreviation'],
                                    'entity_type': 'team',
                                    'entity_id': team['id']
                                })
                    except Exception as e:
                        self.logger.warning(f"Error enriching favorite {fav}: {str(e)}")
                        continue
                
                return enriched_favorites
                
            except Exception as e:
                self.logger.error(f"Get favorites error: {str(e)}")
                return []
        
        return self._cached_query(cache_key, fetch_favorites, cache_minutes=60)

    def add_favorite(self, user_id: str, entity_type: str, entity_id: int) -> Dict:
        """Add a favorite player or team and clear cache"""
        try:
            logging.info(f"Adding favorite - user_id: {user_id}, entity_type: {entity_type}, entity_id: {entity_id}")
            
            # Check if already exists
            existing = (
                self.client
                    .schema("hoops")
                    .from_("user_favorites")
                    .select("id")
                    .eq("user_id", user_id)
                    .eq("entity_type", entity_type)
                    .eq("entity_id", entity_id)
                    .execute()
            )
            
            if existing.data:
                logging.info(f"Favorite already exists for user {user_id}")
                return {"success": False, "error": "Already in favorites"}
            
            favorite_data = {
                "user_id": user_id,
                "entity_type": entity_type,
                "entity_id": entity_id
            }
            
            response = (
                self.client
                    .schema("hoops")
                    .from_("user_favorites")
                    .insert(favorite_data)
                    .execute()
            )
            
            # Clear favorites cache
            self.cache.clear(f"user_favorites_{user_id}")
            
            logging.info(f"Successfully added favorite for user {user_id}")
            return {"success": True, "favorite": response.data[0] if response.data else None}
            
        except Exception as e:
            self.logger.error(f"Add favorite error: {str(e)}")
            return {"success": False, "error": str(e)}

    def remove_favorite(self, user_id: str, entity_type: str, entity_id: int) -> Dict:
        """Remove a favorite and clear cache"""
        try:
            response = (
                self.client
                    .schema("hoops")
                    .from_("user_favorites")
                    .delete()
                    .eq("user_id", user_id)
                    .eq("entity_type", entity_type)
                    .eq("entity_id", entity_id)
                    .execute()
            )
            
            # Clear favorites cache
            self.cache.clear(f"user_favorites_{user_id}")
            
            return {"success": True}
        except Exception as e:
            self.logger.error(f"Remove favorite error: {str(e)}")
            return {"success": False, "error": str(e)}

    # ======== Team stats methods ========
    def get_team_season_stats(self, team_id: int, season: str = "2024-25") -> Optional[Dict]:
        """Get team season stats with caching"""
        cache_key = f"team_season_stats_{team_id}_{season}"
        
        def fetch_team_stats():
            try:
                # Get team record
                record = self.get_team_record(team_id)
                
                # Add additional team metrics if available
                # For now, return basic record data
                return {
                    "wins": record.get("wins", 0),
                    "losses": record.get("losses", 0),
                    "win_percentage": record.get("win_percentage", 0.0),
                    "points_per_game": 0.0,  # Could be calculated from game data
                    "points_allowed_per_game": 0.0  # Could be calculated from game data
                }
            except Exception as e:
                self.logger.error(f"Get team season stats error: {str(e)}")
                return {
                    "wins": 0,
                    "losses": 0,
                    "win_percentage": 0.0,
                    "points_per_game": 0.0,
                    "points_allowed_per_game": 0.0
                }
        
        return self._cached_query(cache_key, fetch_team_stats, cache_minutes=60)

    # ======== Data sync logging ========
    def log_sync_start(self, sync_type: str) -> int:
        """Log the start of a data sync operation"""
        try:
            log_data = {
                "sync_type": sync_type,
                "status": "started"
            }
            response = (
                self.client
                    .schema("hoops")
                    .from_("data_sync_log")
                    .insert(log_data)
                    .execute()
            )
            return response.data[0]["id"] if response.data else None
        except Exception as e:
            self.logger.error(f"Log sync start error: {str(e)}")
            return None

    def log_sync_complete(self, log_id: int, records_processed: int = 0) -> None:
        """Log the completion of a data sync operation"""
        try:
            update_data = {
                "status": "completed",
                "records_processed": records_processed,
                "completed_at": datetime.now(timezone.utc).isoformat()
            }
            self.client\
                .schema("hoops")\
                .from_("data_sync_log")\
                .update(update_data)\
                .eq("id", log_id)\
                .execute()
        except Exception as e:
            self.logger.error(f"Log sync complete error: {str(e)}")

    def log_sync_error(self, log_id: int, error_message: str) -> None:
        """Log a sync operation error"""
        try:
            update_data = {
                "status": "failed",
                "error_message": error_message,
                "completed_at": datetime.now(timezone.utc).isoformat()
            }
            self.client\
                .schema("hoops")\
                .from_("data_sync_log")\
                .update(update_data)\
                .eq("id", log_id)\
                .execute()
        except Exception as e:
            self.logger.error(f"Log sync error: {str(e)}")

    def get_last_sync_log(self) -> Optional[Dict]:
        """Get the most recent sync log entry with caching"""
        cache_key = "last_sync_log"
        
        def fetch_last_sync():
            try:
                response = (
                    self.client
                        .schema("hoops")
                        .from_("data_sync_log")
                        .select("*")
                        .order("started_at", desc=True)
                        .limit(1)
                        .execute()
                )
                return response.data[0] if response.data else None
            except Exception as e:
                self.logger.error(f"Get last sync log error: {str(e)}")
                return None
        
        return self._cached_query(cache_key, fetch_last_sync, cache_minutes=5)

    # ======== Cache management methods ========
    def clear_cache(self, pattern: str = None):
        """Clear cache entries, optionally by pattern"""
        self.cache.clear(pattern)
        self.logger.info(f"Cache cleared for pattern: {pattern}" if pattern else "All cache cleared")

    def cleanup_expired_cache(self):
        """Clean up expired cache entries"""
        self.cache.cleanup_expired()

    def get_cache_stats(self) -> Dict:
        """Get cache statistics for debugging"""
        return self.cache.get_stats()

    def get_team_record(self, team_id: int) -> Dict:
        """Get team win/loss record with caching"""
        cache_key = f"team_record_{team_id}"
        
        def fetch_record():
            try:
                # Try the database function first
                try:
                    response = (
                        self.client
                            .schema("hoops")
                            .rpc("get_team_record", {"p_team_id": team_id})
                            .execute()
                    )
                    
                    if response.data and len(response.data) > 0:
                        record = response.data[0]
                        return {
                            "wins": record.get('wins', 0),
                            "losses": record.get('losses', 0),
                            "win_percentage": float(record.get('win_percentage', 0))
                        }
                except Exception as rpc_error:
                    self.logger.debug(f"RPC function failed for team {team_id}, calculating manually: {rpc_error}")
                
                # Fallback: Calculate manually
                home_games = (
                    self.client
                        .schema("hoops")
                        .from_("games")
                        .select("home_score, away_score")
                        .eq("home_team_id", team_id)
                        .eq("status", "Final")
                        .not_.is_("home_score", "null")
                        .not_.is_("away_score", "null")
                        .execute()
                )
                
                away_games = (
                    self.client
                        .schema("hoops")
                        .from_("games")
                        .select("home_score, away_score")
                        .eq("away_team_id", team_id)
                        .eq("status", "Final")
                        .not_.is_("home_score", "null")
                        .not_.is_("away_score", "null")
                        .execute()
                )
                
                wins = 0
                losses = 0
                
                # Count home wins/losses
                for game in home_games.data or []:
                    home_score = int(game.get("home_score", 0))
                    away_score = int(game.get("away_score", 0))
                    if home_score > away_score:
                        wins += 1
                    elif away_score > home_score:
                        losses += 1
                
                # Count away wins/losses
                for game in away_games.data or []:
                    home_score = int(game.get("home_score", 0))
                    away_score = int(game.get("away_score", 0))
                    if away_score > home_score:
                        wins += 1
                    elif home_score > away_score:
                        losses += 1
                
                total_games = wins + losses
                win_percentage = wins / total_games if total_games > 0 else 0.0
                
                return {
                    "wins": wins,
                    "losses": losses,
                    "win_percentage": win_percentage
                }
                
            except Exception as e:
                self.logger.error(f"Get team record error: {str(e)}")
                return {"wins": 0, "losses": 0, "win_percentage": 0.0}
        
        return self._cached_query(cache_key, fetch_record, cache_minutes=30)
        
        
    # Added pagination because loading all players crashes the browser
    # Search functionality needed custom SQL queries
    # ======== Players methods ========
    def get_players_paginated(self, page: int = 1, per_page: int = 20, search: str = "", team_id: int = None, position: str = "") -> Dict:
        """Get paginated list of players with caching for popular queries"""
        # Create cache key based on parameters
        cache_key = f"players_page_{page}_{per_page}_{search}_{team_id}_{position}"
        
        def fetch_players():
            try:
                offset = (page - 1) * per_page
                
                query = (
                    self.client
                        .schema("hoops")
                        .from_("players")
                        .select("*,teams:team_id(id,name,abbreviation,city)", count="exact")
                )
                
                if search:
                    query = query.or_(f"first_name.ilike.%{search}%,last_name.ilike.%{search}%")
                
                if team_id:
                    query = query.eq("team_id", team_id)
                
                if position:
                    query = query.eq("position", position)
                
                response = query.eq("is_active", True).order("last_name").range(offset, offset + per_page - 1).execute()
                
                total_count = response.count or 0
                total_pages = (total_count + per_page - 1) // per_page
                
                return {
                    "players": response.data or [],
                    "pagination": {
                        "current_page": page,
                        "per_page": per_page,
                        "total_count": total_count,
                        "total_pages": total_pages,
                        "has_next": page < total_pages,
                        "has_prev": page > 1
                    }
                }
            except Exception as e:
                self.logger.error(f"Get players error: {str(e)}")
                return {"players": [], "pagination": {"current_page": 1, "total_pages": 1}}
        
        # Cache for shorter time for search queries
        cache_minutes = 15 if search else 30
        return self._cached_query(cache_key, fetch_players, cache_minutes)

    def get_player_by_id(self, player_id: int) -> Optional[Dict]:
        """Get player by ID with team info and caching"""
        cache_key = f"player_{player_id}"
        
        def fetch_player():
            try:
                response = (
                    self.client
                        .schema("hoops")
                        .from_("players")
                        .select("*,teams:team_id(id,name,abbreviation,city,conference,division)")
                        .eq("id", player_id)
                        .execute()
                )
                return response.data[0] if response.data else None
            except Exception as e:
                self.logger.error(f"Get player error: {str(e)}")
                return None
        
        return self._cached_query(cache_key, fetch_player, cache_minutes=60)

    def upsert_player(self, player_data: Dict) -> Dict:
        """Insert or update player data and clear relevant caches"""
        try:
            response = (
                self.client
                    .schema("hoops")
                    .from_("players")
                    .upsert(player_data, on_conflict="nba_player_id")
                    .execute()
            )
            
            # Clear player caches
            self.cache.clear("player")
            self.cache.clear("players_page")
            
            return {"success": True, "player": response.data[0] if response.data else None}
        except Exception as e:
            self.logger.error(f"Upsert player error: {str(e)}")
            return {"success": False, "error": str(e)}

    def upsert_players_batch(self, players_data: List[Dict]) -> Dict:
        """Batch upsert players for optimized sync"""
        try:
            response = (
                self.client
                    .schema("hoops")
                    .from_("players")
                    .upsert(players_data, on_conflict="nba_player_id")
                    .execute()
            )
            
            # Clear all player caches
            self.cache.clear("player")
            self.cache.clear("players_page")
            
            synced_count = len(response.data) if response.data else 0
            self.logger.info(f"Batch upserted {synced_count} players")
            
            return {"success": True, "synced_count": synced_count}
        except Exception as e:
            self.logger.error(f"Batch upsert players error: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_team_roster(self, team_id: int) -> List[Dict]:
        """Get all players for a team with caching"""
        cache_key = f"team_roster_{team_id}"
        
        def fetch_roster():
            try:
                response = (
                    self.client
                        .schema("hoops")
                        .from_("players")
                        .select("*")
                        .eq("team_id", team_id)
                        .eq("is_active", True)
                        .order("jersey_number")
                        .execute()
                )
                return response.data or []
            except Exception as e:
                self.logger.error(f"Get roster error: {str(e)}")
                return []
        
        return self._cached_query(cache_key, fetch_roster, cache_minutes=60)

    # ======== Enhanced season stats methods ========
    def upsert_player_season_stats(self, stats_data: Dict) -> Dict:
        """Insert or update player season stats and clear cache"""
        try:
            response = (
                self.client
                    .schema("hoops")
                    .from_("player_season_stats")
                    .upsert(stats_data, on_conflict="player_id,season")
                    .execute()
            )
            
            # Clear season stats cache for this player
            player_id = stats_data.get('player_id')
            if player_id:
                self.cache.clear(f"player_season_stats_{player_id}")
            
            return {"success": True, "stats": response.data[0] if response.data else None}
        except Exception as e:
            self.logger.error(f"Upsert player season stats error: {str(e)}")
            return {"success": False, "error": str(e)}

    def upsert_player_season_stats_batch(self, stats_data_list: List[Dict]) -> Dict:
        """Batch upsert player season stats for optimized sync"""
        try:
            response = (
                self.client
                    .schema("hoops")
                    .from_("player_season_stats")
                    .upsert(stats_data_list, on_conflict="player_id,season")
                    .execute()
            )
            
            # Clear season stats cache
            self.cache.clear("player_season_stats")
            
            synced_count = len(response.data) if response.data else 0
            self.logger.info(f"Batch upserted {synced_count} season stats")
            
            return {"success": True, "synced_count": synced_count}
        except Exception as e:
            self.logger.error(f"Batch upsert season stats error: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_player_season_stats(self, player_id: int, season: str = "2024-25") -> Optional[Dict]:
        """" get player season averages with caching and fallback options"""
        cache_key = f"player_season_stats_{player_id}_{season}"
        
        def fetch_stats():
            try:
                self.logger.debug(f"Looking for stats for player_id={player_id}, season={season}")
                
                # First try the dedicated season stats table
                try:
                    response = (
                        self.client
                            .schema("hoops")
                            .from_("player_season_stats")
                            .select("*")
                            .eq("player_id", player_id)
                            .eq("season", season)
                            .execute()
                    )
                    
                    if response.data and len(response.data) > 0:
                        stats = response.data[0]
                        return {
                            "avg_points": float(stats.get('points_per_game', 0)),
                            "avg_rebounds": float(stats.get('rebounds_per_game', 0)),
                            "avg_assists": float(stats.get('assists_per_game', 0)),
                            "games_played": int(stats.get('games_played', 0)),
                            "field_goal_percentage": float(stats.get('field_goal_percentage', 0)),
                            "three_point_percentage": float(stats.get('three_point_percentage', 0)),
                            "free_throw_percentage": float(stats.get('free_throw_percentage', 0))
                        }
                        
                except Exception as season_stats_error:
                    self.logger.debug(f"Season stats table failed for player {player_id}, trying RPC: {season_stats_error}")
                
                # Try the RPC function if it exists
                try:
                    response = (
                        self.client
                            .schema("hoops")
                            .rpc("get_player_season_averages", {
                                "p_player_id": player_id,
                                "p_season": season
                            })
                            .execute()
                    )
                    
                    if response.data and len(response.data) > 0:
                        stats = response.data[0]
                        return {
                            "avg_points": float(stats.get('avg_points', 0)) if stats.get('avg_points') is not None else 0.0,
                            "avg_rebounds": float(stats.get('avg_rebounds', 0)) if stats.get('avg_rebounds') is not None else 0.0,
                            "avg_assists": float(stats.get('avg_assists', 0)) if stats.get('avg_assists') is not None else 0.0,
                            "games_played": int(stats.get('games_played', 0))
                        }
                        
                except Exception as rpc_error:
                    self.logger.debug(f"RPC function failed for player {player_id}, calculating manually: {rpc_error}")
                
                # Fallback: Calculate averages manually from player_stats table
                response = (
                    self.client
                        .schema("hoops")
                        .from_("player_stats")
                        .select("points, rebounds, assists, minutes_played, field_goals_made, field_goals_attempted, three_pointers_made, three_pointers_attempted, free_throws_made, free_throws_attempted")
                        .eq("player_id", player_id)
                        .execute()
                )
                
                stats_data = response.data or []
                
                if not stats_data:
                    return {
                        "avg_points": 0.0,
                        "avg_rebounds": 0.0,
                        "avg_assists": 0.0,
                        "games_played": 0,
                        "field_goal_percentage": 0.0,
                        "three_point_percentage": 0.0,
                        "free_throw_percentage": 0.0
                    }
                
                # Calculate averages safely
                def safe_sum(items, key):
                    return sum(item.get(key, 0) or 0 for item in items)
                
                def safe_percentage(made, attempted):
                    total_made = safe_sum(stats_data, made)
                    total_attempted = safe_sum(stats_data, attempted)
                    return round(total_made / total_attempted, 3) if total_attempted > 0 else 0.0
                
                total_points = safe_sum(stats_data, 'points')
                total_rebounds = safe_sum(stats_data, 'rebounds')
                total_assists = safe_sum(stats_data, 'assists')
                games_played = len(stats_data)
                
                return {
                    "avg_points": round(total_points / games_played, 1) if games_played > 0 else 0.0,
                    "avg_rebounds": round(total_rebounds / games_played, 1) if games_played > 0 else 0.0,
                    "avg_assists": round(total_assists / games_played, 1) if games_played > 0 else 0.0,
                    "games_played": games_played,
                    "field_goal_percentage": safe_percentage('field_goals_made', 'field_goals_attempted'),
                    "three_point_percentage": safe_percentage('three_pointers_made', 'three_pointers_attempted'),
                    "free_throw_percentage": safe_percentage('free_throws_made', 'free_throws_attempted')
                }
                
            except Exception as e:
                self.logger.error(f"Get player stats error for player {player_id}: {str(e)}")
                return {
                    "avg_points": 0.0,
                    "avg_rebounds": 0.0,
                    "avg_assists": 0.0,
                    "games_played": 0,
                    "field_goal_percentage": 0.0,
                    "three_point_percentage": 0.0,
                    "free_throw_percentage": 0.0
                }
        
        return self._cached_query(cache_key, fetch_stats, cache_minutes=60)

    def get_player_recent_games(self, player_id: int, limit: int = 10) -> List[Dict]:
        """Get player's recent game stats with caching"""
        cache_key = f"player_recent_games_{player_id}_{limit}"
        
        def fetch_recent_games():
            try:
                response = (
                    self.client
                        .schema("hoops")
                        .from_("player_stats")
                        .select(
                            "*,"
                            "games:game_id("
                            "id,game_date,"
                            "home_team:home_team_id(name,abbreviation),"
                            "away_team:away_team_id(name,abbreviation)"
                            ")"
                        )
                        .eq("player_id", player_id)
                        .order("games(game_date)", desc=True)
                        .limit(limit)
                        .execute()
                )
                return response.data or []
            except Exception as e:
                self.logger.error(f"Get player recent games error: {str(e)}")
                return []
        
        return self._cached_query(cache_key, fetch_recent_games, cache_minutes=30)

    def upsert_player_stats(self, stats_data: Dict) -> Dict:
        """Insert or update player stats and clear cache"""
        try:
            response = (
                self.client
                    .schema("hoops")
                    .from_("player_stats")
                    .upsert(stats_data, on_conflict="player_id,game_id")
                    .execute()
            )
            
            # Clear player stats cache
            player_id = stats_data.get('player_id')
            if player_id:
                self.cache.clear(f"player_recent_games_{player_id}")
                self.cache.clear(f"player_season_stats_{player_id}")
            
            return {"success": True, "stats": response.data[0] if response.data else None}
        except Exception as e:
            self.logger.error(f"Upsert player stats error: {str(e)}")
            return {"success": False, "error": str(e)}

    def upsert_player_stats_batch(self, stats_data_list: List[Dict]) -> Dict:
        """Batch upsert player stats for optimized sync"""
        try:
            response = (
                self.client
                    .schema("hoops")
                    .from_("player_stats")
                    .upsert(stats_data_list, on_conflict="player_id,game_id")
                    .execute()
            )
            
            # Clear player stats caches
            self.cache.clear("player_recent_games")
            self.cache.clear("player_season_stats")
            
            synced_count = len(response.data) if response.data else 0             
            self.logger.info(f"Batch upserted {synced_count} player stats")

            return {"success": True, "synced_count": synced_count}
        except Exception as e:
            self.logger.error(f"Batch upsert player stats error: {str(e)}")
            return {"success": False, "error": str(e)}