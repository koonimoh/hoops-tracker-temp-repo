import time
import asyncio
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from nba_api.stats.endpoints import (
    playercareerstats, leagueleaders, teamgamelog,
    playergamelog, leaguestandings, commonplayerinfo as playerinfo
)
from nba_api.stats.static import players, teams
from nba_api.live.nba.endpoints import scoreboard
from app.core.config import settings
from app.core.logging import logger
from app.core.cache import cache as cached
from app.utils.decorators import rate_limit, retry_on_failure

class NBAApiService:
    def __init__(self):
        self.rate_limit = settings.nba_api_rate_limit
        self.retry_attempts = settings.nba_api_retry_attempts
        self.retry_delay = settings.nba_api_retry_delay
        self.executor = ThreadPoolExecutor(max_workers=settings.max_workers)
    
    @cached(timeout=3600)  # Cache for 1 hour
    def get_all_players(self) -> List[Dict]:
        """Get all NBA players (cached)"""
        try:
            return players.get_players()
        except Exception as e:
            logger.error(f"Error fetching all players: {e}")
            return []
    
    @cached(timeout=3600)  # Cache for 1 hour
    def get_all_teams(self) -> List[Dict]:
        """Get all NBA teams (cached)"""
        try:
            return teams.get_teams()
        except Exception as e:
            logger.error(f"Error fetching all teams: {e}")
            return []
    
    def find_player_by_name(self, name: str) -> Optional[Dict]:
        """Find player by name with fuzzy matching"""
        all_players = self.get_all_players()
        
        # Exact match first
        for player in all_players:
            if player['full_name'].lower() == name.lower():
                return player
        
        # Fuzzy match
        name_lower = name.lower()
        matches = []
        for player in all_players:
            full_name = player['full_name'].lower()
            if name_lower in full_name or full_name in name_lower:
                matches.append(player)
        
        return matches[0] if matches else None
    
    @rate_limit(calls=10, period=60)  # Rate limiting decorator
    @retry_on_failure(max_attempts=3, delay=2)
    def get_player_career_stats(self, player_id: int) -> Dict:
        """Get player career statistics"""
        try:
            career = playercareerstats.PlayerCareerStats(player_id=str(player_id))
            return {
                'regular_season': career.get_data_frames()[0].to_dict('records'),
                'playoffs': career.get_data_frames()[1].to_dict('records') if len(career.get_data_frames()) > 1 else []
            }
        except Exception as e:
            logger.error(f"Error fetching career stats for player {player_id}: {e}")
            raise
    
    @rate_limit(calls=10, period=60)
    @retry_on_failure(max_attempts=3, delay=2)
    def get_player_game_log(self, player_id: int, season: str = "2024-25") -> List[Dict]:
        """Get player game log for current season"""
        try:
            game_log = playergamelog.PlayerGameLog(
                player_id=str(player_id),
                season=season,
                season_type_all_star='Regular Season'
            )
            return game_log.get_data_frames()[0].to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching game log for player {player_id}: {e}")
            raise
    
    @cached(timeout=1800)  # Cache for 30 minutes
    def get_league_leaders(self, stat_category: str = "PTS", season: str = "2024-25") -> List[Dict]:
        """Get league leaders for specified stat"""
        try:
            leaders = leagueleaders.LeagueLeaders(
                stat_category_abbreviation=stat_category,
                season=season,
                season_type_all_star='Regular Season'
            )
            return leaders.get_data_frames()[0].to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching league leaders for {stat_category}: {e}")
            return []
    
    @cached(timeout=3600)  # Cache for 1 hour
    def get_team_standings(self, season: str = "2024-25") -> Dict:
        """Get current team standings"""
        try:
            standings = leaguestandings.LeagueStandings(season=season)
            return standings.get_data_frames()[0].to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching standings: {e}")
            return []
    
    def get_live_scoreboard(self) -> Dict:
        """Get today's live scoreboard"""
        try:
            games = scoreboard.ScoreBoard()
            return games.get_dict()
        except Exception as e:
            logger.error(f"Error fetching live scoreboard: {e}")
            return {}
    
    def fetch_multiple_players_parallel(self, player_ids: List[int]) -> List[Dict]:
        """Fetch multiple players' data in parallel"""
        results = []
        
        # Submit tasks to thread pool
        future_to_player = {
            self.executor.submit(self.get_player_career_stats, player_id): player_id
            for player_id in player_ids
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_player):
            player_id = future_to_player[future]
            try:
                result = future.result()
                results.append({
                    'player_id': player_id,
                    'data': result
                })
            except Exception as e:
                logger.error(f"Error fetching data for player {player_id}: {e}")
                results.append({
                    'player_id': player_id,
                    'data': None,
                    'error': str(e)
                })
        
        return results

# Global instance
nba_api = NBAApiService()