# parallel_sync.py - Optimized Parallel Sync Service
import threading
import concurrent.futures
import time
import logging
from typing import Dict, List, Optional, Callable
from datetime import datetime, timezone
from queue import Queue, Empty

class ParallelSyncService:
    """Optimized parallel processing service for NBA data synchronization"""
    
    def __init__(self, supabase_client, nba_service, max_workers=3):
        self.supabase = supabase_client
        self.nba_service = nba_service
        self.max_workers = max_workers  # Reduced default workers
        self.logger = logging.getLogger(__name__)
        
        # Setup enhanced logging
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        # Thread-safe job management
        self.active_jobs = {}
        self.job_lock = threading.Lock()
        
        # Global rate limiting to coordinate with NBA service
        self.rate_limiter = threading.Semaphore(1)  # Only 1 API call at a time across all workers
        self.last_api_call = 0
        self.api_call_lock = threading.Lock()
        
        self.logger.info(f"ParallelSyncService initialized with {max_workers} workers")

    def _global_rate_limit(self):
        """Global rate limiting across all parallel workers"""
        with self.api_call_lock:
            now = time.time()
            time_since_last = now - self.last_api_call
            min_delay = 1.0  # Increased delay for parallel operations
            
            if time_since_last < min_delay:
                sleep_time = min_delay - time_since_last
                self.logger.debug(f"Parallel rate limiting: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
            
            self.last_api_call = time.time()

    def sync_teams_parallel(self) -> str:
        """Sync teams with optimized parallel processing"""
        job_id = self._create_job('teams_parallel', self._sync_teams_worker)
        return job_id

    def sync_players_parallel(self, batch_size: int = 3, max_teams: int = 5) -> str:
        """Sync players in smaller parallel batches"""
        job_id = self._create_job('players_parallel', self._sync_players_worker, {'batch_size': batch_size, 'max_teams': max_teams})
        return job_id

    def sync_player_stats_parallel(self, player_ids: List[int] = None, batch_size: int = 5) -> str:
        """Sync player stats in smaller parallel batches"""
        job_id = self._create_job('player_stats_parallel', self._sync_player_stats_worker, {
            'player_ids': player_ids,
            'batch_size': batch_size
        })
        return job_id

    def sync_shot_charts_parallel(self, player_ids: List[int], season: str = "2024-25") -> str:
        """Sync shot chart data with conservative parallel processing"""
        job_id = self._create_job('shot_charts_parallel', self._sync_shot_charts_worker, {
            'player_ids': player_ids,
            'season': season
        })
        return job_id

    def sync_all_parallel(self) -> str:
        """Run complete data sync with optimized parallel processing"""
        job_id = self._create_job('sync_all_parallel', self._sync_all_worker)
        return job_id

    def _create_job(self, job_type: str, worker_func: Callable, params: Dict = None) -> str:
        """Create and queue a new sync job"""
        job_id = f"{job_type}_{int(time.time())}"
        
        job_data = {
            'id': job_id,
            'type': job_type,
            'worker_func': worker_func,
            'params': params or {},
            'status': 'queued',
            'created_at': datetime.now(timezone.utc),
            'progress': 0,
            'result': None,
            'error': None,
            'message': f'Starting {job_type}...'
        }
        
        with self.job_lock:
            self.active_jobs[job_id] = job_data
        
        # Start job in background thread
        thread = threading.Thread(target=self._execute_job, args=(job_id,))
        thread.daemon = True
        thread.start()
        
        self.logger.info(f"Created parallel job: {job_id}")
        return job_id

    def _execute_job(self, job_id: str):
        """Execute a job in a separate thread with better error handling"""
        try:
            with self.job_lock:
                if job_id not in self.active_jobs:
                    return
                
                job = self.active_jobs[job_id]
                job['status'] = 'running'
                job['started_at'] = datetime.now(timezone.utc)
                job['message'] = f"Running {job['type']}..."
            
            self.logger.info(f"Starting execution of job {job_id}")
            
            # Execute the worker function
            result = job['worker_func'](job_id, job['params'])
            
            with self.job_lock:
                job['status'] = 'completed'
                job['completed_at'] = datetime.now(timezone.utc)
                job['result'] = result
                job['progress'] = 100
                job['message'] = f"Completed {job['type']}"
            
            self.logger.info(f"Job {job_id} completed successfully")
                
        except Exception as e:
            self.logger.error(f"Job {job_id} failed: {str(e)}")
            with self.job_lock:
                job['status'] = 'failed'
                job['error'] = str(e)
                job['completed_at'] = datetime.now(timezone.utc)
                job['message'] = f"Failed: {str(e)}"

    def _sync_teams_worker(self, job_id: str, params: Dict) -> Dict:
        """Optimized worker function for teams sync"""
        from nba_api.stats.static import teams
        
        log_id = None
        try:
            log_id = self.supabase.log_sync_start("teams_parallel")
        except:
            pass
        
        try:
            self._update_job_progress(job_id, 10, "Fetching NBA teams...")
            nba_teams = teams.get_teams()
            total_teams = len(nba_teams)
            synced_count = 0
            
            self.logger.info(f"Processing {total_teams} teams in parallel")
            
            # Process teams in smaller batches with conservative parallelism
            batch_size = 3  # Smaller batches
            max_concurrent = min(self.max_workers, 2)  # Limit concurrent workers
            
            for i in range(0, total_teams, batch_size):
                if self._should_stop_job(job_id):
                    break
                    
                batch_teams = nba_teams[i:i + batch_size]
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                    future_to_team = {
                        executor.submit(self._sync_single_team, team): team 
                        for team in batch_teams
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_team, timeout=120):
                        try:
                            result = future.result(timeout=30)
                            if result.get('success'):
                                synced_count += 1
                        except Exception as e:
                            team = future_to_team[future]
                            self.logger.error(f"Error syncing team {team.get('full_name', 'Unknown')}: {str(e)}")
                
                # Update progress
                progress = min(90, int((i + batch_size) / total_teams * 80) + 10)
                self._update_job_progress(job_id, progress, f"Synced {synced_count}/{total_teams} teams")
                
                # Longer pause between batches for teams
                time.sleep(2)
            
            if log_id:
                self.supabase.log_sync_complete(log_id, synced_count)
            
            self._update_job_progress(job_id, 100, f"Completed: {synced_count} teams synced")
            return {"success": True, "synced_count": synced_count, "total": total_teams}
            
        except Exception as e:
            if log_id:
                try:
                    self.supabase.log_sync_error(log_id, str(e))
                except:
                    pass
            raise e

    def _sync_single_team(self, team: Dict) -> Dict:
        """Sync a single team with enhanced rate limiting"""
        try:
            # Global rate limiting
            self._global_rate_limit()
            
            from nba_api.stats.endpoints import teamdetails
            team_details_response = teamdetails.TeamDetails(team_id=team['id'])
            team_info = team_details_response.get_data_frames()[0]
            
            if not team_info.empty:
                team_row = team_info.iloc[0]
                
                # Normalize conference
                conference = str(team_row.get('CONFERENCE', '')).strip()
                if conference.lower() in ['east', 'eastern']:
                    conference = 'Eastern'
                elif conference.lower() in ['west', 'western']:
                    conference = 'Western'
                else:
                    from nba_service import Config
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
                
                return self.supabase.upsert_team(team_data)
            
            return {"success": False, "error": "No team data"}
            
        except Exception as e:
            self.logger.error(f"Error syncing team {team.get('full_name', 'Unknown')}: {str(e)}")
            return {"success": False, "error": str(e)}

    def _sync_players_worker(self, job_id: str, params: Dict) -> Dict:
        """Optimized worker function for players sync"""
        batch_size = params.get('batch_size', 3)  # Smaller default
        
        log_id = None
        try:
            log_id = self.supabase.log_sync_start("players_parallel")
        except:
            pass
        
        try:
            self._update_job_progress(job_id, 10, "Fetching teams...")
            
            # Get teams to process (limit for parallel processing)
            max_teams = params.get('max_teams', 5)
            teams = self.supabase.get_all_teams()[:max_teams]  # Use configurable limit
            total_teams = len(teams)
            synced_count = 0
            
            self.logger.info(f"Processing {total_teams} teams for player sync")
            
            # Process teams in very small batches
            max_concurrent = min(self.max_workers, 2)
            
            for i in range(0, total_teams, batch_size):
                if self._should_stop_job(job_id):
                    break
                    
                batch_teams = teams[i:i + batch_size]
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                    future_to_team = {
                        executor.submit(self._sync_team_roster, team): team 
                        for team in batch_teams
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_team, timeout=300):
                        try:
                            result = future.result(timeout=60)
                            synced_count += result.get('synced_count', 0)
                        except Exception as e:
                            team = future_to_team[future]
                            self.logger.error(f"Error syncing roster for {team.get('name', 'Unknown')}: {str(e)}")
                
                # Update progress
                progress = min(90, int((i + batch_size) / total_teams * 80) + 10)
                self._update_job_progress(job_id, progress, f"Processed {i + batch_size}/{total_teams} teams")
                
                # Longer pause between team batches
                time.sleep(3)
            
            if log_id:
                self.supabase.log_sync_complete(log_id, synced_count)
            
            self._update_job_progress(job_id, 100, f"Completed: {synced_count} players synced")
            return {"success": True, "synced_count": synced_count, "total_teams": total_teams}
            
        except Exception as e:
            if log_id:
                try:
                    self.supabase.log_sync_error(log_id, str(e))
                except:
                    pass
            raise e

    def _sync_team_roster(self, team: Dict) -> Dict:
        """Sync roster for a single team with conservative rate limiting"""
        try:
            from nba_api.stats.endpoints import commonteamroster, commonplayerinfo
            import pandas as pd
            
            # Conservative rate limiting
            self._global_rate_limit()
            
            nba_team_id = team.get("nba_team_id", team["id"])
            roster_response = commonteamroster.CommonTeamRoster(team_id=nba_team_id)
            roster_df = roster_response.get_data_frames()[0]
            
            synced_count = 0
            players_data = []
            
            # Process players with rate limiting
            for _, player_row in roster_df.iterrows():
                try:
                    # Rate limit each player info call
                    self._global_rate_limit()
                    
                    player_id = player_row['PLAYER_ID']
                    player_info_response = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
                    player_info_df = player_info_response.get_data_frames()[0]
                    
                    if not player_info_df.empty:
                        player_info = player_info_df.iloc[0]
                        
                        # Parse player data safely (same as NBA service)
                        player_data = self._parse_player_data_safe(player_row, player_info, team["id"])
                        if player_data:
                            players_data.append(player_data)
                    
                except Exception as e:
                    self.logger.error(f"Error processing player {player_row.get('PLAYER', 'Unknown')}: {str(e)}")
                    continue
            
            # Batch upsert players if we have data
            if players_data:
                try:
                    if hasattr(self.supabase, 'upsert_players_batch'):
                        result = self.supabase.upsert_players_batch(players_data)
                        synced_count = result.get("synced_count", 0)
                    else:
                        # Fallback to individual upserts
                        for player_data in players_data:
                            try:
                                result = self.supabase.upsert_player(player_data)
                                if result.get("success", False):
                                    synced_count += 1
                            except Exception as e:
                                self.logger.error(f"Error upserting player: {str(e)}")
                except Exception as e:
                    self.logger.error(f"Error in batch player upsert: {str(e)}")
            
            self.logger.info(f"Synced {synced_count} players for team {team.get('name', 'Unknown')}")
            return {"success": True, "synced_count": synced_count}
            
        except Exception as e:
            self.logger.error(f"Error syncing roster for team {team.get('name', 'Unknown')}: {str(e)}")
            return {"success": False, "error": str(e)}

    def _parse_player_data_safe(self, row, info, team_id: int) -> Optional[Dict]:
        """Safely parse player data (copied from NBA service)"""
        try:
            import pandas as pd
            from datetime import datetime, timezone
            
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
                    pass
            
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
                    pass
            
            # Parse weight safely
            weight_lbs = None
            w = info.get('WEIGHT')
            if pd.notna(w):
                try:
                    weight_lbs = int(float(str(w)))
                except (ValueError, TypeError):
                    pass
            
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
            self.logger.error(f"Error parsing player data: {str(e)}")
            return None

    def _sync_player_stats_worker(self, job_id: str, params: Dict) -> Dict:
        """Conservative worker function for player stats sync"""
        player_ids = params.get('player_ids')
        batch_size = params.get('batch_size', 5)  # Very small batch
        
        log_id = None
        try:
            log_id = self.supabase.log_sync_start("player_stats_parallel")
        except:
            pass
        
        try:
            self._update_job_progress(job_id, 10, "Fetching players...")
            
            # Get players to process (limited)
            if player_ids:
                players = [{"id": pid, "nba_player_id": pid} for pid in player_ids]
            else:
                # Get limited active players
                try:
                    response = (
                        self.supabase.client
                            .schema("hoops")
                            .from_("players")
                            .select("id, nba_player_id, first_name, last_name")
                            .eq("is_active", True)
                            .limit(50)  # Very limited for parallel
                            .execute()
                    )
                    players = response.data or []
                except Exception as e:
                    self.logger.error(f"Error fetching players: {str(e)}")
                    return {"success": False, "error": "Could not fetch players"}
            
            total_players = len(players)
            synced_count = 0
            
            self.logger.info(f"Processing stats for {total_players} players")
            
            # Process in very small batches
            max_concurrent = min(self.max_workers, 2)
            
            for i in range(0, total_players, batch_size):
                if self._should_stop_job(job_id):
                    break
                    
                batch_players = players[i:i + batch_size]
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                    future_to_player = {
                        executor.submit(self._sync_player_stats_single, player): player 
                        for player in batch_players
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_player, timeout=180):
                        try:
                            result = future.result(timeout=45)
                            synced_count += result.get('synced_count', 0)
                        except Exception as e:
                            player = future_to_player[future]
                            self.logger.error(f"Error syncing stats for player {player.get('nba_player_id', 'Unknown')}: {str(e)}")
                
                # Update progress
                progress = min(90, int((i + batch_size) / total_players * 80) + 10)
                self._update_job_progress(job_id, progress, f"Processed {i + batch_size}/{total_players} players")
                
                # Longer pause for stats
                time.sleep(5)
            
            if log_id:
                self.supabase.log_sync_complete(log_id, synced_count)
            
            self._update_job_progress(job_id, 100, f"Completed: {synced_count} stats synced")
            return {"success": True, "synced_count": synced_count, "total_players": total_players}
            
        except Exception as e:
            if log_id:
                try:
                    self.supabase.log_sync_error(log_id, str(e))
                except:
                    pass
            raise e

    def _sync_player_stats_single(self, player: Dict) -> Dict:
        """Sync stats for a single player with conservative approach"""
        try:
            from nba_api.stats.endpoints import playerdashboardbygeneralsplits
            from nba_service import Config
            
            # Conservative rate limiting
            self._global_rate_limit()
            
            seasons_to_try = Config.get_seasons_to_try()
            stats_synced = 0
            
            # Try only current season for parallel processing
            for season in seasons_to_try[:1]:  # Only try current season
                try:
                    dashboard_response = playerdashboardbygeneralsplits.PlayerDashboardByGeneralSplits(
                        player_id=player["nba_player_id"],
                        season=season
                    )
                    
                    dashboard_df = dashboard_response.get_data_frames()[0]
                    
                    if not dashboard_df.empty:
                        stats_row = dashboard_df.iloc[0]
                        games_played = int(stats_row.get('GP', 0))
                        
                        if games_played > 0:
                            stats_data = {
                                "player_id": player["id"],
                                "season": season,
                                "games_played": games_played,
                                "minutes_per_game": self._safe_divide(float(stats_row.get('MIN', 0)), games_played),
                                "points_per_game": self._safe_divide(float(stats_row.get('PTS', 0)), games_played),
                                "rebounds_per_game": self._safe_divide(float(stats_row.get('REB', 0)), games_played),
                                "assists_per_game": self._safe_divide(float(stats_row.get('AST', 0)), games_played),
                                "steals_per_game": self._safe_divide(float(stats_row.get('STL', 0)), games_played),
                                "blocks_per_game": self._safe_divide(float(stats_row.get('BLK', 0)), games_played),
                                "turnovers_per_game": self._safe_divide(float(stats_row.get('TOV', 0)), games_played),
                                "field_goal_percentage": float(stats_row.get('FG_PCT', 0)),
                                "three_point_percentage": float(stats_row.get('FG3_PCT', 0)),
                                "free_throw_percentage": float(stats_row.get('FT_PCT', 0))
                            }
                            
                            try:
                                result = self.supabase.upsert_player_season_stats(stats_data)
                                if result.get("success", False):
                                    stats_synced = 1
                                    break
                            except Exception as e:
                                self.logger.error(f"Error upserting season stats: {str(e)}")
                
                except Exception as e:
                    self.logger.debug(f"No stats for player {player['nba_player_id']} in {season}: {str(e)}")
                    continue
            
            return {"success": True, "synced_count": stats_synced}
            
        except Exception as e:
            self.logger.error(f"Error syncing stats for player {player.get('nba_player_id', 'Unknown')}: {str(e)}")
            return {"success": False, "error": str(e)}

    def _safe_divide(self, numerator: float, denominator: float) -> float:
        """Safely divide two numbers"""
        try:
            if denominator == 0:
                return 0.0
            return round(numerator / denominator, 2)
        except (TypeError, ValueError):
            return 0.0

    def _sync_shot_charts_worker(self, job_id: str, params: Dict) -> Dict:
        """Very conservative worker function for shot charts sync"""
        player_ids = params.get('player_ids', [])
        season = params.get('season', '2024-25')
        
        log_id = None
        try:
            log_id = self.supabase.log_sync_start("shot_charts_parallel")
        except:
            pass
        
        try:
            total_players = len(player_ids)
            synced_count = 0
            
            self.logger.info(f"Processing shot charts for {total_players} players")
            
            # Very conservative: only 1-2 workers for shot charts
            max_concurrent = min(self.max_workers, 1)  # Sequential for shot charts
            batch_size = 1  # One at a time
            
            for i in range(0, total_players, batch_size):
                if self._should_stop_job(job_id):
                    break
                    
                batch_players = player_ids[i:i + batch_size]
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                    future_to_player = {
                        executor.submit(self.nba_service.sync_shot_chart_data_enhanced, player_id, season, 500): player_id 
                        for player_id in batch_players
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_player, timeout=300):
                        try:
                            result = future.result(timeout=90)
                            if result.get('success'):
                                synced_count += result.get('synced_count', 0)
                        except Exception as e:
                            player_id = future_to_player[future]
                            self.logger.error(f"Error syncing shot chart for player {player_id}: {str(e)}")
                
                # Update progress
                progress = min(90, int((i + batch_size) / total_players * 80) + 10)
                self._update_job_progress(job_id, progress, f"Processed {i + batch_size}/{total_players} players")
                
                # Very long pause for shot charts
                time.sleep(10)
            
            if log_id:
                self.supabase.log_sync_complete(log_id, synced_count)
            
            self._update_job_progress(job_id, 100, f"Completed: {synced_count} shots synced")
            return {"success": True, "synced_count": synced_count, "total_players": total_players}
            
        except Exception as e:
            if log_id:
                try:
                    self.supabase.log_sync_error(log_id, str(e))
                except:
                    pass
            raise e

    def _sync_all_worker(self, job_id: str, params: Dict) -> Dict:
        """Conservative worker function for complete sync"""
        results = {
            "teams": {"success": False, "synced_count": 0},
            "players": {"success": False, "synced_count": 0},
            "games": {"success": False, "synced_count": 0},
            "player_stats": {"success": False, "synced_count": 0}
        }
        
        try:
            # 1. Sync teams (parallel)
            self._update_job_progress(job_id, 10, "Syncing teams...")
            teams_result = self._sync_teams_worker(f"{job_id}_teams", {})
            results["teams"] = teams_result
            
            if teams_result["success"]:
                # 2. Sync players (parallel, very limited)
                self._update_job_progress(job_id, 30, "Syncing players...")
                players_result = self._sync_players_worker(f"{job_id}_players", {"batch_size": 2})
                results["players"] = players_result
            
            # 3. Sync recent games (sequential)
            self._update_job_progress(job_id, 60, "Syncing recent games...")
            games_result = self.nba_service.sync_recent_games_enhanced(max_games=50)
            results["games"] = games_result
            
            if results["players"]["success"] and results["games"]["success"]:
                # 4. Sync player stats (parallel, very limited)
                self._update_job_progress(job_id, 80, "Syncing player stats...")
                stats_result = self._sync_player_stats_worker(f"{job_id}_stats", {"batch_size": 3})
                results["player_stats"] = stats_result
            
            self._update_job_progress(job_id, 100, "Full sync completed")
            return results
            
        except Exception as e:
            self.logger.error(f"Full sync error: {str(e)}")
            raise e

    def _update_job_progress(self, job_id: str, progress: int, message: str = None):
        """Update job progress"""
        with self.job_lock:
            if job_id in self.active_jobs:
                self.active_jobs[job_id]['progress'] = progress
                if message:
                    self.active_jobs[job_id]['message'] = message
                    self.logger.debug(f"Job {job_id}: {progress}% - {message}")

    def _should_stop_job(self, job_id: str) -> bool:
        """Check if job should be stopped"""
        try:
            from flask import current_app
            return current_app.sync_status.get("stopped", False)
        except (RuntimeError, AttributeError):
            return False

    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Get status of a specific job"""
        with self.job_lock:
            job = self.active_jobs.get(job_id, {})
            if job:
                job_copy = job.copy()
                # Convert datetime to string for JSON serialization
                for key in ['created_at', 'started_at', 'completed_at']:
                    if key in job_copy and job_copy[key]:
                        job_copy[key] = job_copy[key].isoformat()
                return job_copy
            return None

    def get_all_jobs(self) -> Dict:
        """Get status of all jobs"""
        with self.job_lock:
            all_jobs = {}
            for job_id, job in self.active_jobs.items():
                job_copy = job.copy()
                # Convert datetime to string for JSON serialization
                for key in ['created_at', 'started_at', 'completed_at']:
                    if key in job_copy and job_copy[key]:
                        job_copy[key] = job_copy[key].isoformat()
                all_jobs[job_id] = job_copy
            return all_jobs

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job (best effort)"""
        with self.job_lock:
            if job_id in self.active_jobs:
                job = self.active_jobs[job_id]
                if job['status'] in ['queued', 'running']:
                    job['status'] = 'cancelled'
                    job['completed_at'] = datetime.now(timezone.utc)
                    job['message'] = "Cancelled by admin"
                    return True
        return False

    def cleanup_completed_jobs(self, max_age_hours: int = 24):
        """Clean up old completed jobs"""
        from datetime import timedelta
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        
        with self.job_lock:
            jobs_to_remove = []
            for job_id, job in self.active_jobs.items():
                if (job['status'] in ['completed', 'failed', 'cancelled'] and 
                    job.get('completed_at', datetime.now(timezone.utc)) < cutoff_time):
                    jobs_to_remove.append(job_id)
            
            for job_id in jobs_to_remove:
                del self.active_jobs[job_id]
        
        if jobs_to_remove:
            self.logger.info(f"Cleaned up {len(jobs_to_remove)} old jobs")
        
        return len(jobs_to_remove)