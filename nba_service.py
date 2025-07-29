# nba_service.py - Optimized NBA Service with Intelligent Caching
import time
import logging
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta, timezone
import pandas as pd
from nba_api.stats.endpoints import (
    commonteamroster, playercareerstats, teamgamelog,
    playergamelog, shotchartdetail, leaguegamefinder,
    teamdetails, commonplayerinfo, playerdashboardbygeneralsplits,
    teamdashboardbygeneralsplits
)
from nba_api.stats.static import teams, players
from nba_api.live.nba.endpoints import scoreboard
import threading

class Config:
    """Configuration class with caching and rate limiting"""
    
    NBA_API_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    # Global rate limiting
    API_CALL_DELAY = 0.8  # Increased from 0.6 to be safer
    LAST_API_CALL = 0
    API_LOCK = threading.Lock()
    
    # NBA team conference mappings (cached)
    TEAM_CONFERENCES = {
        # Eastern Conference
        'Atlanta Hawks': 'Eastern', 'Boston Celtics': 'Eastern', 
        'Brooklyn Nets': 'Eastern', 'Charlotte Hornets': 'Eastern',
        'Chicago Bulls': 'Eastern', 'Cleveland Cavaliers': 'Eastern',
        'Detroit Pistons': 'Eastern', 'Indiana Pacers': 'Eastern',
        'Miami Heat': 'Eastern', 'Milwaukee Bucks': 'Eastern',
        'New York Knicks': 'Eastern', 'Orlando Magic': 'Eastern',
        'Philadelphia 76ers': 'Eastern', 'Toronto Raptors': 'Eastern',
        'Washington Wizards': 'Eastern',
        
        # Western Conference  
        'Dallas Mavericks': 'Western', 'Denver Nuggets': 'Western',
        'Golden State Warriors': 'Western', 'Houston Rockets': 'Western',
        'Los Angeles Clippers': 'Western', 'Los Angeles Lakers': 'Western',
        'Memphis Grizzlies': 'Western', 'Minnesota Timberwolves': 'Western',
        'New Orleans Pelicans': 'Western', 'Oklahoma City Thunder': 'Western',
        'Phoenix Suns': 'Western', 'Portland Trail Blazers': 'Western',
        'Sacramento Kings': 'Western', 'San Antonio Spurs': 'Western',
        'Utah Jazz': 'Western'
    }
    
    @classmethod
    def get_team_conference(cls, team_name: str) -> Optional[str]:
        """Get conference for a team name"""
        return cls.TEAM_CONFERENCES.get(team_name, 'Eastern')
    
    @classmethod
    def get_current_season(cls) -> str:
        """Get current NBA season string with better logic"""
        now = datetime.now()
        
        # NBA season typically runs October to June
        # July-September is off-season
        if now.month >= 10:  # October-December: new season starting
            return f"{now.year}-{str(now.year + 1)[2:]}"
        elif now.month <= 6:  # January-June: season in progress  
            return f"{now.year - 1}-{str(now.year)[2:]}"
        else:  # July-September: off-season, return most recent completed season
            return f"{now.year - 1}-{str(now.year)[2:]}"
    
    @classmethod
    def get_seasons_to_try(cls) -> List[str]:
        """Get list of seasons to try for data sync - more comprehensive"""
        current = cls.get_current_season()
        current_year = int(current.split('-')[0])
        
        seasons = []
        # Try current season and previous 4 seasons for better data coverage
        for i in range(5):
            year = current_year - i
            season_str = f"{year}-{str(year + 1)[2:]}"
            seasons.append(season_str)
        
        return seasons


class IntelligentCache:
    """Intelligent caching system to reduce API calls"""
    
    def __init__(self):
        self.cache = {}
        self.cache_expiry = {}
        self.id_mappings = {
            'nba_team_to_internal': {},
            'nba_player_to_internal': {},
            'nba_game_to_internal': {}
        }
        self.lock = threading.Lock()
        
    def get(self, key: str, default=None):
        """Get cached value if not expired"""
        with self.lock:
            if key in self.cache:
                if key in self.cache_expiry:
                    if datetime.now(timezone.utc) < self.cache_expiry[key]:
                        return self.cache[key]
                    else:
                        # Expired, remove
                        del self.cache[key]
                        del self.cache_expiry[key]
                else:
                    return self.cache[key]
            return default
    
    def set(self, key: str, value, expire_minutes: int = 60):
        """Set cached value with expiry"""
        with self.lock:
            self.cache[key] = value
            if expire_minutes > 0:
                self.cache_expiry[key] = datetime.now(timezone.utc) + timedelta(minutes=expire_minutes)
    
    def cache_id_mapping(self, mapping_type: str, nba_id: int, internal_id: int):
        """Cache ID mapping to reduce DB lookups"""
        with self.lock:
            if mapping_type in self.id_mappings:
                self.id_mappings[mapping_type][nba_id] = internal_id
    
    def get_id_mapping(self, mapping_type: str, nba_id: int) -> Optional[int]:
        """Get cached ID mapping"""
        with self.lock:
            return self.id_mappings.get(mapping_type, {}).get(nba_id)
    
    def clear_expired(self):
        """Clear expired cache entries"""
        with self.lock:
            now = datetime.now(timezone.utc)
            expired_keys = [
                key for key, expiry in self.cache_expiry.items() 
                if expiry < now
            ]
            for key in expired_keys:
                self.cache.pop(key, None)
                self.cache_expiry.pop(key, None)


class NBAService:
    """Optimized NBA service with intelligent caching and rate limiting"""
    
    def __init__(self, supabase_client=None):
        self.supabase = supabase_client
        self.logger = logging.getLogger(__name__)
        self.headers = Config.NBA_API_HEADERS
        self.cache = IntelligentCache()
        
        # Setup enhanced logging
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        self.logger.info("NBAService initialized with intelligent caching")
        
    def set_supabase_client(self, supabase_client):
        """Set the Supabase client after initialization"""
        self.supabase = supabase_client
        self.logger.info("Supabase client set")
    
    def _global_rate_limit_delay(self):
        """Global rate limiting across all service instances"""
        with Config.API_LOCK:
            now = time.time()
            time_since_last = now - Config.LAST_API_CALL
            
            if time_since_last < Config.API_CALL_DELAY:
                sleep_time = Config.API_CALL_DELAY - time_since_last
                self.logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
            
            Config.LAST_API_CALL = time.time()
    
    def _cached_api_call(self, cache_key: str, api_call_func, cache_minutes: int = 30, max_retries: int = 3):
        """Make API call with caching and enhanced error handling"""
        # Check cache first
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            self.logger.debug(f"Cache hit for {cache_key}")
            return cached_result
        
        # Make API call with rate limiting and retries
        self.logger.debug(f"Cache miss for {cache_key}, making API call")
        
        for attempt in range(max_retries):
            try:
                self._global_rate_limit_delay()
                
                # Add extra delay for shot chart requests (they're more resource intensive)
                if 'shot_chart' in cache_key:
                    time.sleep(1.5)
                
                result = api_call_func()
                self.cache.set(cache_key, result, cache_minutes)
                self.logger.debug(f"Cached result for {cache_key}")
                return result
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # Handle specific timeout errors
                if 'timeout' in error_msg or 'timed out' in error_msg:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 5  # Exponential backoff
                        self.logger.warning(f"Timeout on attempt {attempt + 1} for {cache_key}, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        self.logger.error(f"Final timeout for {cache_key} after {max_retries} attempts")
                        
                # Handle rate limiting errors
                elif 'rate limit' in error_msg or '429' in error_msg or 'too many requests' in error_msg:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 10  # Longer wait for rate limits
                        self.logger.warning(f"Rate limited on attempt {attempt + 1} for {cache_key}, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        self.logger.error(f"Rate limit exceeded for {cache_key} after {max_retries} attempts")
                
                # Handle connection errors
                elif 'connection' in error_msg:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 3
                        self.logger.warning(f"Connection error on attempt {attempt + 1} for {cache_key}, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        self.logger.error(f"Connection failed for {cache_key} after {max_retries} attempts")
                
                # For other errors, don't retry
                self.logger.error(f"API call failed for {cache_key}: {e}")
                break
        
        # If all retries failed, raise the last exception
        raise
        
    def sync_teams(self) -> Dict:
        """Optimized team sync with batch operations"""
        if not self.supabase:
            return {"success": False, "error": "Supabase client not initialized"}
        
        self.logger.info("Starting optimized teams sync")
        log_id = None
        try:
            log_id = self.supabase.log_sync_start("teams")
        except AttributeError:
            self.logger.warning("Supabase logging not available")
        
        try:
            # Get NBA teams (cached)
            nba_teams = self._cached_api_call(
                "nba_teams_static",
                lambda: teams.get_teams(),
                cache_minutes=1440  # 24 hours
            )
            
            self.logger.info(f"Processing {len(nba_teams)} teams")
            teams_data = []
            
            # Process teams in smaller batches to avoid overwhelming the API
            batch_size = 5
            for i in range(0, len(nba_teams), batch_size):
                batch = nba_teams[i:i + batch_size]
                
                for team in batch:
                    try:
                        # Check for stop signal
                        if self.should_stop_sync():
                            self.logger.info("Team sync stopped by admin")
                            break
                        
                        # Use cached API call for team details
                        cache_key = f"team_details_{team['id']}"
                        team_info_df = self._cached_api_call(
                            cache_key,
                            lambda: teamdetails.TeamDetails(team_id=team['id']).get_data_frames()[0],
                            cache_minutes=60
                        )
                        
                        if not team_info_df.empty:
                            team_row = team_info_df.iloc[0]
                            
                            # Normalize conference
                            conference = str(team_row.get('CONFERENCE', '')).strip()
                            if conference.lower() in ['east', 'eastern']:
                                conference = 'Eastern'
                            elif conference.lower() in ['west', 'western']:
                                conference = 'Western'
                            else:
                                conference = Config.get_team_conference(team['full_name']) or 'Eastern'
                            
                            team_data = {
                                "nba_team_id": team['id'],
                                "name": team['full_name'],
                                "abbreviation": team['abbreviation'],
                                "city": team['city'],
                                "conference": conference,
                                "division": str(team_row.get('DIVISION', '')).strip(),
                                "founded_year": team_row.get('FOUNDED', None)
                            }
                            
                            teams_data.append(team_data)
                            self.logger.debug(f"Prepared team data for {team['full_name']}")
                            
                    except Exception as e:
                        self.logger.error(f"Error processing team {team.get('full_name', 'Unknown')}: {e}")
                        continue
                
                # Brief pause between batches
                if i + batch_size < len(nba_teams):
                    time.sleep(1)
            
            # Batch upsert all teams
            synced_count = 0
            if teams_data:
                try:
                    # Check if batch upsert is available
                    if hasattr(self.supabase, 'upsert_teams_batch'):
                        result = self.supabase.upsert_teams_batch(teams_data)
                        synced_count = result.get("synced_count", 0)
                        self.logger.info(f"Batch upserted {synced_count} teams")
                    else:
                        # Fallback to individual upserts
                        for team_data in teams_data:
                            try:
                                result = self.supabase.upsert_team(team_data)
                                if result.get("success", False):
                                    synced_count += 1
                                    # Cache the ID mapping
                                    if 'team' in result and result['team']:
                                        self.cache.cache_id_mapping(
                                            'nba_team_to_internal', 
                                            team_data['nba_team_id'], 
                                            result['team']['id']
                                        )
                            except Exception as e:
                                self.logger.error(f"Error upserting team {team_data['name']}: {e}")
                        
                        self.logger.info(f"Individual upserted {synced_count} teams")
                        
                except Exception as e:
                    self.logger.error(f"Error in batch team upsert: {e}")
                    return {"success": False, "error": str(e)}
            
            try:
                if log_id:
                    self.supabase.log_sync_complete(log_id, synced_count)
            except AttributeError:
                pass
                
            self.logger.info(f"Teams sync completed: {synced_count} teams synced")
            return {"success": True, "synced_count": synced_count}
            
        except Exception as e:
            self.logger.error(f"Teams sync error: {e}")
            try:
                if log_id:
                    self.supabase.log_sync_error(log_id, str(e))
            except AttributeError:
                pass
            return {"success": False, "error": str(e)}
    
    def sync_players(self, team_id: int = None) -> Dict:
        """Optimized players sync with batch operations"""
        if not self.supabase:
            return {"success": False, "error": "Supabase client not initialized"}
        
        self.logger.info(f"Starting optimized players sync (team_id: {team_id})")
        log_id = None
        try:
            log_id = self.supabase.log_sync_start("players")
        except AttributeError:
            pass
            
        synced_count = 0
        
        try:
            # Get teams to sync
            if team_id:
                teams_to_sync = [{"id": team_id, "nba_team_id": team_id}]
            else:
                try:
                    teams_to_sync = self.supabase.get_all_teams()
                    self.logger.info(f"Found {len(teams_to_sync)} teams to sync players for")
                except AttributeError:
                    # Fallback to NBA API teams (limited)
                    nba_teams = teams.get_teams()
                    teams_to_sync = [{"id": t['id'], "nba_team_id": t['id']} for t in nba_teams[:10]]
                    self.logger.warning(f"Fallback: processing {len(teams_to_sync)} teams")
            
            players_data = []
            
            # Process teams in smaller batches
            for team in teams_to_sync:
                try:
                    if self.should_stop_sync():
                        self.logger.info("Player sync stopped by admin")
                        break
                    
                    nba_team_id = team.get("nba_team_id", team["id"])
                    cache_key = f"team_roster_{nba_team_id}"
                    
                    # Get roster with caching
                    roster_df = self._cached_api_call(
                        cache_key,
                        lambda: commonteamroster.CommonTeamRoster(team_id=nba_team_id).get_data_frames()[0],
                        cache_minutes=30
                    )
                    
                    self.logger.debug(f"Processing {len(roster_df)} players for team {team.get('name', nba_team_id)}")
                    
                    for _, row in roster_df.iterrows():
                        try:
                            player_id = row['PLAYER_ID']
                            
                            # Get player info with caching
                            info_cache_key = f"player_info_{player_id}"
                            info_df = self._cached_api_call(
                                info_cache_key,
                                lambda: commonplayerinfo.CommonPlayerInfo(player_id=player_id).get_data_frames()[0],
                                cache_minutes=60
                            )
                            
                            info = info_df.iloc[0] if not info_df.empty else {}
                            
                            # Parse player data safely
                            player_data = self._parse_player_data(row, info, team["id"])
                            if player_data:
                                players_data.append(player_data)
                                
                        except Exception as e:
                            self.logger.error(f"Error processing player {row.get('PLAYER', 'Unknown')}: {e}")
                            continue
                    
                    # Brief pause between teams
                    time.sleep(0.5)
                    
                except Exception as e:
                    self.logger.error(f"Error fetching roster for team {team.get('name', 'Unknown')}: {e}")
                    continue
            
            # Batch upsert all players
            if players_data:
                try:
                    if hasattr(self.supabase, 'upsert_players_batch'):
                        result = self.supabase.upsert_players_batch(players_data)
                        synced_count = result.get("synced_count", 0)
                        self.logger.info(f"Batch upserted {synced_count} players")
                    else:
                        # Fallback to individual upserts
                        for player_data in players_data:
                            try:
                                result = self.supabase.upsert_player(player_data)
                                if result.get("success", False):
                                    synced_count += 1
                                    # Cache ID mapping
                                    if 'player' in result and result['player']:
                                        self.cache.cache_id_mapping(
                                            'nba_player_to_internal',
                                            player_data['nba_player_id'],
                                            result['player']['id']
                                        )
                            except Exception as e:
                                self.logger.error(f"Error upserting player: {e}")
                        
                        self.logger.info(f"Individual upserted {synced_count} players")
                        
                except Exception as e:
                    self.logger.error(f"Error in batch player upsert: {e}")
                    return {"success": False, "error": str(e)}
            
            try:
                if log_id:
                    self.supabase.log_sync_complete(log_id, synced_count)
            except AttributeError:
                pass
                
            self.logger.info(f"Players sync completed: {synced_count} players synced")
            return {"success": True, "synced_count": synced_count}
        
        except Exception as e:
            self.logger.error(f"Players sync error: {e}")
            try:
                if log_id:
                    self.supabase.log_sync_error(log_id, str(e))
            except AttributeError:
                pass
            return {"success": False, "error": str(e)}
    
    def _parse_player_data(self, row, info, team_id: int) -> Optional[Dict]:
        """Parse player data safely with better error handling"""
        try:
            # Parse jersey number safely
            num = row.get('NUM')
            jersey_number = None
            if pd.notna(num):
                num_str = str(num).strip()
                if num_str.isdigit():
                    jersey_number = int(num_str)
            
            # Parse birth date safely
            birth_iso = None
            bd = info.get('BIRTHDATE')
            if pd.notna(bd):
                try:
                    dt = datetime.strptime(bd, '%Y-%m-%dT%H:%M:%S')
                    birth_iso = dt.date().isoformat()
                except (ValueError, TypeError):
                    self.logger.debug(f"Could not parse birth date: {bd}")
            
            # Parse experience safely
            exp_years = 0
            exp = row.get('EXP')
            if pd.notna(exp):
                exp_str = str(exp).strip()
                if exp_str.isdigit():
                    exp_years = int(exp_str)
            
            # Parse height safely
            height_inches = None
            h = info.get('HEIGHT')
            if pd.notna(h) and '-' in str(h):
                try:
                    ft, inch = str(h).split('-')
                    height_inches = int(ft) * 12 + int(inch)
                except (ValueError, IndexError):
                    self.logger.debug(f"Could not parse height: {h}")
            
            # Parse weight safely
            weight_lbs = None
            w = info.get('WEIGHT')
            if pd.notna(w):
                try:
                    weight_lbs = int(float(str(w)))
                except (ValueError, TypeError):
                    self.logger.debug(f"Could not parse weight: {w}")
            
            return {
                "nba_player_id": int(row['PLAYER_ID']),
                "team_id": team_id,
                "first_name": str(row.get('PLAYER', '')).split(' ')[0],
                "last_name": ' '.join(str(row.get('PLAYER', '')).split(' ')[1:]),
                "position": str(row.get('POSITION', '')),
                "jersey_number": jersey_number,
                "height_inches": height_inches,
                "weight_lbs": weight_lbs,
                "birth_date": birth_iso,
                "experience_years": exp_years,
                "college": str(row.get('SCHOOL', '')),
                "is_active": True
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing player data: {e}")
            return None
    
    def should_stop_sync(self) -> bool:
        """Check if sync should be stopped - improved implementation"""
        try:
            from flask import current_app
            return current_app.sync_status.get("stopped", False)
        except (RuntimeError, AttributeError):
            # Not in app context or sync_status not available
            return False
            
    #########
    #########    
    ########## Continuation of NBAService class - Player Stats and Games methods
    
    def sync_player_stats_enhanced(self, player_id: int = None, season: str = None, max_players: int = None) -> Dict:
        """Optimized player stats sync with intelligent batching"""
        if not self.supabase:
            return {"success": False, "error": "Supabase client not initialized"}
        
        if not season:
            season = Config.get_current_season()
        
        seasons_to_try = Config.get_seasons_to_try()
        
        # If max_players is None, process all players
        if max_players is None:
            max_players = 1000  # Set high number to effectively remove limit
        
        self.logger.info(f"Starting optimized player stats sync - Season: {season}, Max players: {max_players}")
        log_id = None
        try:
            log_id = self.supabase.log_sync_start("player_stats")
        except AttributeError:
            pass
            
        synced_count = 0
        stats_data = []
        
        try:
            # Get players to sync stats for
            if player_id:
                players = [{"id": player_id, "nba_player_id": player_id}]
                self.logger.info(f"Syncing stats for single player: {player_id}")
            else:
                try:
                    response = (
                        self.supabase.client
                            .schema("hoops")
                            .from_("players")
                            .select("id, nba_player_id, first_name, last_name")
                            .eq("is_active", True)
                            .execute()
                    )
                    players = response.data or []
                    
                    # Apply limit after fetching if specified and reasonable
                    if max_players and max_players < len(players) and max_players < 1000:
                        players = players[:max_players]
                    
                    self.logger.info(f"Found {len(players)} active players to sync stats for")
                    
                except Exception as e:
                    self.logger.error(f"Error fetching players from database: {e}")
                    return {"success": False, "error": "Could not fetch players"}
            
            # Process players in smaller batches to avoid overwhelming the API
            batch_size = 15  # Optimized batch size for speed
            total_batches = (len(players) + batch_size - 1) // batch_size
            
            for batch_num, i in enumerate(range(0, len(players), batch_size), 1):
                batch_players = players[i:i + batch_size]
                
                self.logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_players)} players)")
                
                for player in batch_players:
                    if self.should_stop_sync():
                        self.logger.info("Player stats sync stopped by admin")
                        break
                        
                    self.logger.debug(f"Processing stats for player {player.get('first_name', '')} {player.get('last_name', '')} (ID: {player['nba_player_id']})")
                    
                    stats_synced = False
                    
                    # Try multiple seasons for better data coverage
                    for season_attempt in seasons_to_try:
                        if self.should_stop_sync() or stats_synced:
                            break
                            
                        try:
                            cache_key = f"player_dashboard_{player['nba_player_id']}_{season_attempt}"
                            
                            # Use cached API call for player stats
                            dashboard_df = self._cached_api_call(
                                cache_key,
                                lambda: playerdashboardbygeneralsplits.PlayerDashboardByGeneralSplits(
                                    player_id=player["nba_player_id"],
                                    season=season_attempt
                                ).get_data_frames()[0],
                                cache_minutes=30
                            )
                            
                            if not dashboard_df.empty:
                                stats_row = dashboard_df.iloc[0]
                                games_played = int(stats_row.get('GP', 0))
                                
                                if games_played > 0:
                                    # Calculate per-game averages
                                    stats_record = {
                                        "player_id": player["id"],
                                        "season": season_attempt,
                                        "games_played": games_played,
                                        "minutes_per_game": self._safe_divide(float(stats_row.get('MIN', 0)), games_played),
                                        "points_per_game": self._safe_divide(float(stats_row.get('PTS', 0)), games_played),
                                        "rebounds_per_game": self._safe_divide(float(stats_row.get('REB', 0)), games_played),
                                        "assists_per_game": self._safe_divide(float(stats_row.get('AST', 0)), games_played),
                                        "steals_per_game": self._safe_divide(float(stats_row.get('SL', 0)), games_played),
                                        "blocks_per_game": self._safe_divide(float(stats_row.get('BLK', 0)), games_played),
                                        "turnovers_per_game": self._safe_divide(float(stats_row.get('TOV', 0)), games_played),
                                        "field_goal_percentage": float(stats_row.get('FG_PCT', 0)),
                                        "three_point_percentage": float(stats_row.get('FG3_PCT', 0)),
                                        "free_throw_percentage": float(stats_row.get('FT_PCT', 0))
                                    }
                                    
                                    stats_data.append(stats_record)
                                    stats_synced = True
                                    self.logger.debug(f"Added season stats for {player.get('first_name', '')} {player.get('last_name', '')} ({season_attempt})")
                                    break
                                    
                        except Exception as e:
                            self.logger.debug(f"No stats found for player {player['nba_player_id']} in {season_attempt}: {e}")
                            continue
                    
                    if not stats_synced:
                        self.logger.warning(f"No stats found for player {player.get('first_name', '')} {player.get('last_name', '')} in any season")
                
                # Brief pause between batches - optimized for speed
                if i + batch_size < len(players):
                    time.sleep(1.5)
            
            # Batch upsert all season stats
            if stats_data:
                try:
                    if hasattr(self.supabase, 'upsert_player_season_stats_batch'):
                        result = self.supabase.upsert_player_season_stats_batch(stats_data)
                        synced_count = result.get("synced_count", 0)
                        self.logger.info(f"Batch upserted {synced_count} season stats records")
                    else:
                        # Fallback to individual upserts
                        for stats_record in stats_data:
                            try:
                                result = self.supabase.upsert_player_season_stats(stats_record)
                                if result.get("success", False):
                                    synced_count += 1
                            except Exception as e:
                                self.logger.error(f"Error upserting season stats: {e}")
                        
                        self.logger.info(f"Individual upserted {synced_count} season stats records")
                        
                except Exception as e:
                    self.logger.error(f"Error in batch season stats upsert: {e}")
            
            try:
                if log_id:
                    self.supabase.log_sync_complete(log_id, synced_count)
            except AttributeError:
                pass
            
            self.logger.info(f"Player stats sync completed: {synced_count} records synced")
            return {"success": True, "synced_count": synced_count}
            
        except Exception as e:
            self.logger.error(f"Player stats sync error: {e}")
            try:
                if log_id:
                    self.supabase.log_sync_error(log_id, str(e))
            except AttributeError:
                pass
            return {"success": False, "error": str(e)}
    
    def sync_recent_games_enhanced(self, days_back: int = 30, max_games: int = 200) -> Dict:
        """Optimized games sync with intelligent batching"""
        if not self.supabase:
            return {"success": False, "error": "Supabase client not initialized"}
        
        self.logger.info(f"Starting optimized games sync - Days back: {days_back}, Max games: {max_games}")
        log_id = None
        try:
            log_id = self.supabase.log_sync_start("games")
        except AttributeError:
            pass
            
        synced_count = 0
        
        try:
            current_season = Config.get_current_season()
            seasons_to_try = Config.get_seasons_to_try()
            season_types = ["Regular Season", "Playoffs"]
            
            games_collected = []
            
            for season in seasons_to_try:
                if len(games_collected) >= max_games:
                    break
                    
                for season_type in season_types:
                    if len(games_collected) >= max_games or self.should_stop_sync():
                        break
                        
                    try:
                        cache_key = f"league_games_{season}_{season_type.replace(' ', '_')}"
                        
                        # Use cached API call for games
                        games_df = self._cached_api_call(
                            cache_key,
                            lambda: leaguegamefinder.LeagueGameFinder(
                                season_nullable=season,
                                season_type_nullable=season_type
                            ).get_data_frames()[0],
                            cache_minutes=15  # Shorter cache for games
                        )
                        
                        if not games_df.empty:
                            # Sort by game date descending to get most recent first
                            games_df = games_df.sort_values('GAME_DATE', ascending=False)
                            
                            # Limit games per season/type
                            remaining_needed = max_games - len(games_collected)
                            season_games = games_df.head(remaining_needed * 2)  # *2 because each game has 2 rows
                            
                            game_ids_processed = set()
                            
                            for _, game_row in season_games.iterrows():
                                if len(games_collected) >= max_games or self.should_stop_sync():
                                    break
                                    
                                game_id = game_row['GAME_ID']
                                
                                if game_id in game_ids_processed:
                                    continue
                                    
                                game_ids_processed.add(game_id)
                                
                                # Find both teams for this game
                                game_teams = games_df[games_df['GAME_ID'] == game_id]
                                
                                if len(game_teams) >= 2:
                                    team1 = game_teams.iloc[0]
                                    team2 = game_teams.iloc[1]
                                    
                                    # Parse game data safely
                                    game_data = self._parse_game_data(team1, team2, season, season_type)
                                    if game_data:
                                        games_collected.append(game_data)
                                        self.logger.debug(f"Collected game: {game_data['nba_game_id']}")
                            
                            self.logger.info(f"Collected {len([g for g in games_collected if g['season'] == season and g['season_type'] == season_type])} games from {season_type} {season}")
                            
                    except Exception as e:
                        self.logger.error(f"Error fetching games from {season_type} {season}: {e}")
                        continue
            
            # Batch upsert all games
            if games_collected:
                try:
                    if hasattr(self.supabase, 'upsert_games_batch'):
                        result = self.supabase.upsert_games_batch(games_collected)
                        synced_count = result.get("synced_count", 0)
                        self.logger.info(f"Batch upserted {synced_count} games")
                    else:
                        # Fallback to individual upserts
                        for game_data in games_collected:
                            try:
                                result = self.supabase.upsert_game(game_data)
                                if result.get("success", False):
                                    synced_count += 1
                                    # Cache ID mapping
                                    if 'game' in result and result['game']:
                                        self.cache.cache_id_mapping(
                                            'nba_game_to_internal',
                                            game_data['nba_game_id'],
                                            result['game']['id']
                                        )
                            except Exception as e:
                                self.logger.error(f"Error upserting game {game_data.get('nba_game_id')}: {e}")
                        
                        self.logger.info(f"Individual upserted {synced_count} games")
                        
                except Exception as e:
                    self.logger.error(f"Error in batch games upsert: {e}")
            
            try:
                if log_id:
                    self.supabase.log_sync_complete(log_id, synced_count)
            except AttributeError:
                pass
                
            self.logger.info(f"Games sync completed: {synced_count} games synced (attempted {len(games_collected)})")
            return {"success": True, "synced_count": synced_count}
            
        except Exception as e:
            self.logger.error(f"Games sync error: {e}")
            try:
                if log_id:
                    self.supabase.log_sync_error(log_id, str(e))
            except AttributeError:
                pass
            return {"success": False, "error": str(e)}
    
    def _parse_game_data(self, team1, team2, season: str, season_type: str) -> Optional[Dict]:
        """Parse game data safely with better error handling"""
        try:
            # Determine home/away from matchup
            if '@' in str(team1['MATCHUP']):
                away_team_nba_id = team1['TEAM_ID']
                home_team_nba_id = team2['TEAM_ID']
                away_score = team1['PTS']
                home_score = team2['PTS']
            else:
                home_team_nba_id = team1['TEAM_ID']
                away_team_nba_id = team2['TEAM_ID']
                home_score = team1['PTS']
                away_score = team2['PTS']
            
            # Get internal team IDs (with caching)
            home_team_id = self._get_team_id_by_nba_id(home_team_nba_id)
            away_team_id = self._get_team_id_by_nba_id(away_team_nba_id)
            
            if not home_team_id or not away_team_id:
                self.logger.debug(f"Could not find internal team IDs for game {team1['GAME_ID']}")
                return None
            
            return {
                "nba_game_id": str(team1['GAME_ID']),
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "game_date": str(team1['GAME_DATE']),
                "season": season,
                "season_type": season_type,
                "status": "Final",
                "home_score": int(home_score) if home_score else 0,
                "away_score": int(away_score) if away_score else 0
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing game data: {e}")
            return None
    
    # Replace the sync_shot_chart_data_enhanced method in your nba_service.py
    def sync_shot_chart_data_enhanced(self, player_id: int, season: str = None, max_shots: int = 1000) -> Dict:
        """Optimized shot chart sync with intelligent caching and better error handling"""
        if not self.supabase:
            return {"success": False, "error": "Supabase client not initialized"}
        
        if not season:
            season = Config.get_current_season()
        
        self.logger.info(f"Starting shot chart sync for player {player_id} - Season: {season}, Max shots: {max_shots}")
        
        try:
            seasons_to_try = Config.get_seasons_to_try()
            shot_records = []
            
            for season_attempt in seasons_to_try:
                if len(shot_records) >= max_shots or self.should_stop_sync():
                    break
                    
                # Try both Regular Season and Playoffs
                season_types = ['Regular Season', 'Playoffs']
                
                for season_type in season_types:
                    if len(shot_records) >= max_shots:
                        break
                        
                    try:
                        cache_key = f"shot_chart_{player_id}_{season_attempt}_{season_type.replace(' ', '_')}"
                        
                        # Enhanced API call with proper parameters and longer timeout
                        shot_df = self._cached_api_call(
                            cache_key,
                            lambda: self._make_shot_chart_request(player_id, season_attempt, season_type),
                            cache_minutes=60
                        )
                        
                        if not shot_df.empty:
                            # Sort by game date to get most recent shots first
                            if 'GAME_DATE' in shot_df.columns:
                                shot_df = shot_df.sort_values('GAME_DATE', ascending=False)
                            
                            self.logger.info(f"Found {len(shot_df)} shots for player {player_id} in {season_attempt} {season_type}")
                            
                            # Process shots with better error handling
                            for _, shot in shot_df.iterrows():
                                if len(shot_records) >= max_shots:
                                    break
                                    
                                shot_record = self._parse_shot_data(shot, player_id, season_attempt, season_type)
                                if shot_record:
                                    shot_records.append(shot_record)
                            
                            self.logger.info(f"Successfully processed {len([r for r in shot_records if r['season'] == season_attempt and r['season_type'] == season_type])} shots from {season_attempt} {season_type}")
                            
                    except Exception as e:
                        self.logger.debug(f"No shot data found for player {player_id} in {season_attempt} {season_type}: {e}")
                        continue
                
                # If we got shots from this season, we can break (most recent season has data)
                if shot_records:
                    break
            
            if not shot_records:
                self.logger.warning(f"No shot data found for player {player_id} in any season/type")
                return {"success": True, "synced_count": 0, "message": "No shot data found for player"}
            
            # Insert in optimized batches
            count = 0
            batch_size = 50
            for i in range(0, len(shot_records), batch_size):
                batch = shot_records[i:i + batch_size]
                try:
                    res = self.supabase.insert_shot_chart_data(batch)
                    if res.get("success", False):
                        count += res.get("count", 0)
                        self.logger.debug(f"Inserted batch {i//batch_size + 1}: {res.get('count', 0)} shots")
                except Exception as e:
                    self.logger.error(f"Error inserting shot batch: {e}")
                    continue
            
            self.logger.info(f"Shot chart sync completed: {count} shots synced for player {player_id}")
            return {"success": True, "synced_count": count}
            
        except Exception as e:
            self.logger.error(f"Shot chart sync error: {e}")
            return {"success": False, "error": str(e)}

    def _make_shot_chart_request(self, player_id: int, season: str, season_type: str):
        """Make shot chart API request with proper parameters and error handling"""
        try:
            # Get player's team ID for better API results
            team_id = 0  # Default to 0 for all teams
            
            # Try to get the player's current team
            try:
                if self.supabase:
                    response = (
                        self.supabase.client
                            .schema("hoops")
                            .from_("players")
                            .select("teams:team_id(nba_team_id)")
                            .eq("nba_player_id", player_id)
                            .execute()
                    )
                    if response.data and response.data[0].get('teams'):
                        team_id = response.data[0]['teams']['nba_team_id']
            except Exception:
                pass  # Fall back to team_id=0
            
            # Make the API call with proper parameters
            endpoint = shotchartdetail.ShotChartDetail(
                team_id=team_id,
                player_id=player_id,
                season_nullable=season,
                season_type_all_star=season_type,  # Fixed parameter name
                context_measure_simple='FGA',  # Field Goal Attempts
                timeout=45  # Longer timeout
            )
            
            # Get the shot data
            data_frames = endpoint.get_data_frames()
            
            if data_frames and len(data_frames) > 0:
                return data_frames[0]
            else:
                # Return empty DataFrame if no data
                import pandas as pd
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.debug(f"Shot chart API request failed: {e}")
            import pandas as pd
            return pd.DataFrame()


            
            
    def _parse_shot_data(self, shot, player_id: int, season: str, season_type: str) -> Optional[Dict]:
        """Parse shot data safely with enhanced error handling"""
        try:
            # Get our internal IDs (with caching) - with better error handling
            game_id_raw = shot.get('GAME_ID')
            team_id_raw = shot.get('TEAM_ID') 
            
            if pd.isna(game_id_raw) or pd.isna(team_id_raw):
                self.logger.debug(f"Missing game_id or team_id in shot data")
                return None
                
            gid = self._get_game_id_by_nba_id(str(game_id_raw))
            tid = self._get_team_id_by_nba_id(int(team_id_raw))
            pid = self._get_player_id_by_nba_id(player_id)
            
            if not (gid and tid and pid):
                self.logger.debug(f"Missing internal IDs for shot: game={gid}, team={tid}, player={pid}")
                return None
            
            # Handle shot made flag safely
            shot_made_flag = shot.get('SHOT_MADE_FLAG', 0)
            if pd.isna(shot_made_flag):
                shot_made_flag = 0
            shot_made = bool(int(shot_made_flag) == 1)
            
            # Handle shot value safely  
            shot_value = shot.get('SHOT_VALUE', 0)
            if pd.isna(shot_value):
                shot_value = 0
            points_earned = int(shot_value) if shot_made else 0
            
            # Handle coordinates safely
            loc_x = shot.get('LOC_X', 0)
            loc_y = shot.get('LOC_Y', 0)
            shot_distance = shot.get('SHOT_DISTANCE', 0)
            
            if pd.isna(loc_x):
                loc_x = 0
            if pd.isna(loc_y):  
                loc_y = 0
            if pd.isna(shot_distance):
                shot_distance = 0
                
            # Handle period safely
            period = shot.get('PERIOD', 1)
            if pd.isna(period):
                period = 1
            quarter = int(period)
            
            # Handle time_remaining safely
            time_remaining = ""
            minutes_remaining = shot.get('MINUTES_REMAINING')
            seconds_remaining = shot.get('SECONDS_REMAINING')
            
            if pd.notna(minutes_remaining) and pd.notna(seconds_remaining):
                try:
                    mins = int(minutes_remaining)
                    secs = int(seconds_remaining)
                    time_remaining = f"{mins}:{secs:02d}"
                except (ValueError, TypeError):
                    time_remaining = "0:00"
            
            # Handle text fields safely
            action_type = shot.get('ACTION_TYPE', '')
            if pd.isna(action_type):
                action_type = ''
                
            shot_zone = shot.get('SHOT_ZONE_BASIC', '')
            if pd.isna(shot_zone):
                shot_zone = ''
            
            return {
                "player_id": pid,
                "game_id": gid,
                "team_id": tid,
                "shot_made": shot_made,
                "shot_type": str(action_type),
                "shot_zone": str(shot_zone),
                "shot_distance": float(shot_distance),
                "loc_x": float(loc_x),
                "loc_y": float(loc_y),
                "quarter": quarter,
                "time_remaining": time_remaining,
                "points_earned": points_earned,
                "season": season,
                "season_type": season_type
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing shot data: {e}")
            return None
            
            
    ########## 
    ########## 
    ########## Continuation of NBAService class - Helper methods and completion
    
    # Update sync_all_data_enhanced() method in nba_service.py

    def sync_all_data_enhanced(self, include_shot_charts: bool = False, max_players_for_shots: int = 10) -> Dict:
        """Optimized full data sync with optional shot charts"""
        self.logger.info("Starting optimized full data sync")
        
        results = {
            "teams": {"success": False, "synced_count": 0},
            "players": {"success": False, "synced_count": 0},
            "games": {"success": False, "synced_count": 0},
            "player_stats": {"success": False, "synced_count": 0},
            "shot_charts": {"success": False, "synced_count": 0}  # Added
        }
        
        try:
            # 1. Sync teams first (foundational data)
            self.logger.info("Step 1/5: Syncing teams...")
            results["teams"] = self.sync_teams()
            
            if results["teams"]["success"]:
                # 2. Sync players (depends on teams)
                self.logger.info("Step 2/5: Syncing players...")
                results["players"] = self.sync_players()
            else:
                self.logger.error("Teams sync failed, skipping players")
            
            # 3. Sync games (independent of players but helps with stats)
            self.logger.info("Step 3/5: Syncing recent games...")
            results["games"] = self.sync_recent_games_enhanced()
            
            # 4. Sync player stats (depends on players and benefits from games)
            if results["players"]["success"]:
                self.logger.info("Step 4/5: Syncing player stats...")
                results["player_stats"] = self.sync_player_stats_enhanced(max_players=50)
            else:
                self.logger.warning("Players sync failed, skipping player stats")
            
            # 5. Sync shot charts (optional, requires games and players)
            if include_shot_charts and results["players"]["success"] and results["games"]["success"]:
                self.logger.info(f"Step 5/5: Syncing shot charts for top {max_players_for_shots} players...")
                shot_results = self._sync_shot_charts_for_top_players(max_players_for_shots)
                results["shot_charts"] = shot_results
            else:
                if include_shot_charts:
                    self.logger.warning("Skipping shot charts due to failed dependencies")
                results["shot_charts"] = {"success": True, "synced_count": 0, "message": "Skipped"}
            
            self.logger.info("Optimized full data sync completed")
            
        except Exception as e:
            self.logger.error(f"Full sync error: {e}")
            results["error"] = str(e)
        
        return results

    def _sync_shot_charts_for_top_players(self, max_players: int = 10) -> Dict:
        """Sync shot charts for most active players"""
        try:
            # Get top players with recent stats
            response = (
                self.supabase.client
                    .schema("hoops")
                    .from_("player_season_stats")
                    .select("player_id, players:player_id(nba_player_id, first_name, last_name)")
                    .gte("games_played", 5)  # Players with at least 5 games
                    .order("points_per_game", desc=True)
                    .limit(max_players)
                    .execute()
            )
            
            top_players = response.data or []
            total_shots_synced = 0
            players_synced = 0
            
            seasons_to_try = ['2024-25', '2023-24']
            
            for player_stat in top_players:
                if self.should_stop_sync():
                    break
                    
                player_info = player_stat.get('players')
                if not player_info or not player_info.get('nba_player_id'):
                    continue
                
                nba_player_id = player_info['nba_player_id']
                player_name = f"{player_info.get('first_name', '')} {player_info.get('last_name', '')}"
                
                self.logger.info(f"Syncing shot chart for {player_name} (NBA ID: {nba_player_id})")
                
                # Try multiple seasons
                for season in seasons_to_try:
                    try:
                        result = self.sync_shot_chart_data_enhanced(
                            nba_player_id, 
                            season=season, 
                            max_shots=200  # Limit shots per player
                        )
                        
                        if result.get('success') and result.get('synced_count', 0) > 0:
                            total_shots_synced += result.get('synced_count', 0)
                            players_synced += 1
                            self.logger.info(f"Synced {result.get('synced_count', 0)} shots for {player_name}")
                            break  # Found data for this season
                            
                    except Exception as e:
                        self.logger.error(f"Error syncing shots for {player_name} in {season}: {str(e)}")
                        continue
                
                # Rate limiting between players
                time.sleep(2)
            
            return {
                "success": True, 
                "synced_count": total_shots_synced,
                "players_synced": players_synced,
                "total_players_attempted": len(top_players)
            }
            
        except Exception as e:
            self.logger.error(f"Error syncing shot charts for top players: {str(e)}")
            return {"success": False, "error": str(e)}
    
    # Backward compatibility methods
    def sync_player_stats(self, player_id: int = None, season: str = None) -> Dict:
        """Backward compatibility method"""
        return self.sync_player_stats_enhanced(player_id, season)
    
    def sync_recent_games(self, days_back: int = 7, max_games: int = 100) -> Dict:
        """Backward compatibility method"""
        return self.sync_recent_games_enhanced(days_back, max_games)
    
    def sync_shot_chart_data(self, player_id: int, season: str = None, max_shots: int = 500) -> Dict:
        """Backward compatibility method"""
        return self.sync_shot_chart_data_enhanced(player_id, season, max_shots)
    
    def sync_all_data(self) -> Dict:
        """Backward compatibility method"""
        return self.sync_all_data_enhanced()
    
    # Optimized Helper methods with caching
    def _get_team_id_by_nba_id(self, nba_team_id: int) -> Optional[int]:
        """Get team ID with caching to reduce database calls"""
        # Check cache first
        cached_id = self.cache.get_id_mapping('nba_team_to_internal', nba_team_id)
        if cached_id:
            return cached_id
        
        try:
            response = (
                self.supabase.client
                    .schema("hoops")
                    .from_("teams")
                    .select("id")
                    .eq("nba_team_id", nba_team_id)
                    .execute()
            )
            
            if response.data:
                team_id = response.data[0]["id"]
                # Cache the mapping
                self.cache.cache_id_mapping('nba_team_to_internal', nba_team_id, team_id)
                return team_id
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting team ID for NBA team {nba_team_id}: {e}")
            return None
    
    def _get_player_id_by_nba_id(self, nba_player_id: int) -> Optional[int]:
        """Get player ID with caching to reduce database calls"""
        # Check cache first
        cached_id = self.cache.get_id_mapping('nba_player_to_internal', nba_player_id)
        if cached_id:
            return cached_id
        
        try:
            response = (
                self.supabase.client
                    .schema("hoops")
                    .from_("players")
                    .select("id")
                    .eq("nba_player_id", nba_player_id)
                    .execute()
            )
            
            if response.data:
                player_id = response.data[0]["id"]
                # Cache the mapping
                self.cache.cache_id_mapping('nba_player_to_internal', nba_player_id, player_id)
                return player_id
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting player ID for NBA player {nba_player_id}: {e}")
            return None
    
    def _get_game_id_by_nba_id(self, nba_game_id: str) -> Optional[int]:
        """Get game ID with caching to reduce database calls"""
        # Check cache first
        cached_id = self.cache.get_id_mapping('nba_game_to_internal', nba_game_id)
        if cached_id:
            return cached_id
        
        try:
            response = (
                self.supabase.client
                    .schema("hoops")
                    .from_("games")
                    .select("id")
                    .eq("nba_game_id", str(nba_game_id))
                    .execute()
            )
            
            if response.data:
                game_id = response.data[0]["id"]
                # Cache the mapping
                self.cache.cache_id_mapping('nba_game_to_internal', nba_game_id, game_id)
                return game_id
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting game ID for NBA game {nba_game_id}: {e}")
            return None
    
    def _safe_divide(self, numerator: float, denominator: float) -> float:
        """Safely divide two numbers, returning 0 if denominator is 0"""
        try:
            if denominator == 0:
                return 0.0
            return round(numerator / denominator, 2)
        except (TypeError, ValueError):
            return 0.0
    
    def _parse_minutes(self, minutes_str: str) -> int:
        """Parse minutes string (e.g., '32:45') to total minutes"""
        try:
            if not minutes_str or pd.isna(minutes_str):
                return 0
            s = str(minutes_str)
            if ':' in s:
                return int(s.split(':')[0])
            return int(float(s))
        except (ValueError, TypeError, IndexError):
            return 0
    
    def get_player_headshot_url(self, nba_player_id: int) -> str:
        """Generate NBA player headshot URL"""
        return f"https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/{nba_player_id}.png"
    
    def get_team_logo_url(self, nba_team_id: int) -> str:
        """Generate NBA team logo URL"""
        return f"https://cdn.nba.com/logos/nba/{nba_team_id}/primary/L/logo.svg"

    def get_team_id_from_row(self, row):
        """Robust method to get team ID from row with multiple possible column names"""
        possible_names = ['TEAM_ID', 'Team_ID', 'team_id', 'TeamID']
        for name in possible_names:
            if name in row and pd.notna(row[name]):
                return self._get_team_id_by_nba_id(row[name])
        return None

    def get_game_id_from_row(self, row):
        """Robust method to get game ID from row with multiple possible column names"""
        possible_names = ['GAME_ID', 'Game_ID', 'game_id', 'GameID']
        for name in possible_names:
            if name in row and pd.notna(row[name]):
                return self._get_game_id_by_nba_id(row[name])
        return None
    
    def _sync_player_game_stats(self, player: Dict, season: str) -> None:
        """Optimized individual game stats sync with caching"""
        try:
            cache_key = f"player_gamelog_{player['nba_player_id']}_{season}"
            
            # Get player game log with caching
            gamelog_df = self._cached_api_call(
                cache_key,
                lambda: playergamelog.PlayerGameLog(
                    player_id=player["nba_player_id"],
                    season=season
                ).get_data_frames()[0],
                cache_minutes=15
            )
            
            if gamelog_df.empty:
                self.logger.debug(f"No game log found for player {player['nba_player_id']} in {season}")
                return
            
            # Process recent games (limit to avoid overload)
            recent_games = gamelog_df.head(20)
            game_stats_data = []
            
            for _, game_row in recent_games.iterrows():
                try:
                    # Get internal IDs with caching
                    game_id = self._get_game_id_by_nba_id(game_row['GAME_ID'])
                    team_id = self._get_team_id_by_nba_id(game_row['TEAM_ID'])
                    
                    if not game_id or not team_id:
                        self.logger.debug(f"Missing IDs for game {game_row['GAME_ID']}: game_id={game_id}, team_id={team_id}")
                        continue
                    
                    # Parse minutes played safely
                    minutes_played = self._parse_minutes(game_row.get('MIN', '0:00'))
                    
                    stats_data = {
                        "player_id": player["id"],
                        "game_id": game_id,
                        "team_id": team_id,
                        "minutes_played": minutes_played,
                        "points": int(game_row.get('PTS', 0)),
                        "rebounds": int(game_row.get('REB', 0)),
                        "assists": int(game_row.get('AST', 0)),
                        "steals": int(game_row.get('STL', 0)),
                        "blocks": int(game_row.get('BLK', 0)),
                        "turnovers": int(game_row.get('TOV', 0)),
                        "field_goals_made": int(game_row.get('FGM', 0)),
                        "field_goals_attempted": int(game_row.get('FGA', 0)),
                        "three_pointers_made": int(game_row.get('FG3M', 0)),
                        "three_pointers_attempted": int(game_row.get('FG3A', 0)),
                        "free_throws_made": int(game_row.get('FTM', 0)),
                        "free_throws_attempted": int(game_row.get('FTA', 0)),
                        "personal_fouls": int(game_row.get('PF', 0)),
                        "plus_minus": int(game_row.get('PLUS_MINUS', 0)) if pd.notna(game_row.get('PLUS_MINUS')) else 0
                    }
                    
                    game_stats_data.append(stats_data)
                    
                except Exception as e:
                    self.logger.error(f"Error processing game stats for game {game_row.get('GAME_ID', 'Unknown')}: {e}")
                    continue
            
            # Batch upsert game stats if we have any
            if game_stats_data:
                try:
                    if hasattr(self.supabase, 'upsert_player_stats_batch'):
                        self.supabase.upsert_player_stats_batch(game_stats_data)
                        self.logger.debug(f"Batch upserted {len(game_stats_data)} game stats for player {player['nba_player_id']}")
                    else:
                        # Fallback to individual upserts
                        for stats_data in game_stats_data:
                            try:
                                self.supabase.upsert_player_stats(stats_data)
                            except Exception as e:
                                self.logger.error(f"Error upserting individual game stats: {e}")
                        
                        self.logger.debug(f"Individual upserted {len(game_stats_data)} game stats for player {player['nba_player_id']}")
                        
                except Exception as e:
                    self.logger.error(f"Error upserting game stats batch: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error syncing game stats for player {player.get('nba_player_id', 'Unknown')}: {e}")
    
    def clear_cache(self):
        """Clear all cached data"""
        self.cache.clear_expired()
        self.logger.info("Cache cleared")
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics for debugging"""
        with self.cache.lock:
            return {
                "cache_entries": len(self.cache.cache),
                "cache_expiry_entries": len(self.cache.cache_expiry),
                "id_mappings": {
                    "teams": len(self.cache.id_mappings.get('nba_team_to_internal', {})),
                    "players": len(self.cache.id_mappings.get('nba_player_to_internal', {})),
                    "games": len(self.cache.id_mappings.get('nba_game_to_internal', {}))
                }
            }