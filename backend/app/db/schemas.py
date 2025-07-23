"""
Database schemas and Pydantic models for Hoops Tracker.
"""

from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, date
from uuid import UUID, uuid4
from enum import Enum

# Enums for type safety
class ConferenceEnum(str, Enum):
    EAST = "East"
    WEST = "West"

class SeasonTypeEnum(str, Enum):
    REGULAR = "Regular Season"
    PLAYOFFS = "Playoffs"
    PRESEASON = "Preseason"

class BetSideEnum(str, Enum):
    OVER = "over"
    UNDER = "under"

class BetStatusEnum(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"
    WON = "won"
    LOST = "lost"
    PUSH = "push"
    CANCELLED = "cancelled"

class StatKeyEnum(str, Enum):
    POINTS = "pts"
    REBOUNDS = "reb" 
    ASSISTS = "ast"
    STEALS = "stl"
    BLOCKS = "blk"
    TURNOVERS = "tov"
    PERSONAL_FOULS = "pf"
    FIELD_GOALS_MADE = "fg_made"
    FIELD_GOALS_ATTEMPTED = "fg_att"
    FIELD_GOAL_PERCENTAGE = "fg_pct"
    THREE_POINTERS_MADE = "fg3_made"
    THREE_POINTERS_ATTEMPTED = "fg3_att"
    THREE_POINT_PERCENTAGE = "fg3_pct"
    FREE_THROWS_MADE = "ft_made"
    FREE_THROWS_ATTEMPTED = "ft_att"
    FREE_THROW_PERCENTAGE = "ft_pct"
    OFFENSIVE_REBOUNDS = "oreb"
    DEFENSIVE_REBOUNDS = "dreb"
    MINUTES = "min"

# Base schemas
class TimestampMixin(BaseModel):
    """Mixin for created_at and updated_at timestamps."""
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)

class UUIDMixin(BaseModel):
    """Mixin for UUID primary key."""
    id: UUID = Field(default_factory=uuid4)

# Team schemas
class TeamBase(BaseModel):
    """Base team schema."""
    nba_id: int = Field(..., description="Official NBA team ID")
    name: str = Field(..., min_length=1, max_length=255)
    abbreviation: str = Field(..., min_length=2, max_length=10)
    city: str = Field(..., min_length=1, max_length=255)
    state: Optional[str] = Field(None, max_length=100)
    conference: ConferenceEnum
    division: Optional[str] = Field(None, max_length=20)
    logo_url: Optional[str] = None
    primary_color: Optional[str] = Field(None, regex=r'^#[0-9A-Fa-f]{6}$')
    secondary_color: Optional[str] = Field(None, regex=r'^#[0-9A-Fa-f]{6}$')
    founded_year: Optional[int] = Field(None, ge=1946, le=2030)
    arena_name: Optional[str] = Field(None, max_length=255)
    arena_capacity: Optional[int] = Field(None, ge=0)

class TeamCreate(TeamBase):
    """Schema for creating a team."""
    pass

class TeamUpdate(BaseModel):
    """Schema for updating a team."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    abbreviation: Optional[str] = Field(None, min_length=2, max_length=10)
    city: Optional[str] = Field(None, min_length=1, max_length=255)
    state: Optional[str] = Field(None, max_length=100)
    conference: Optional[ConferenceEnum] = None
    division: Optional[str] = Field(None, max_length=20)
    logo_url: Optional[str] = None
    primary_color: Optional[str] = Field(None, regex=r'^#[0-9A-Fa-f]{6}$')
    secondary_color: Optional[str] = Field(None, regex=r'^#[0-9A-Fa-f]{6}$')
    arena_name: Optional[str] = Field(None, max_length=255)
    arena_capacity: Optional[int] = Field(None, ge=0)

class Team(UUIDMixin, TeamBase, TimestampMixin):
    """Complete team schema."""
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }

# Player schemas
class PlayerBase(BaseModel):
    """Base player schema."""
    nba_id: int = Field(..., description="Official NBA player ID")
    name: str = Field(..., min_length=1, max_length=255)
    team_id: Optional[UUID] = None
    position: Optional[str] = Field(None, max_length=10)
    jersey_number: Optional[int] = Field(None, ge=0, le=99)
    height: Optional[str] = Field(None, max_length=10)
    weight: Optional[str] = Field(None, max_length=10)
    birth_date: Optional[date] = None
    college: Optional[str] = Field(None, max_length=255)
    country: Optional[str] = Field(None, max_length=100)
    draft_year: Optional[int] = Field(None, ge=1950, le=2030)
    draft_round: Optional[int] = Field(None, ge=1, le=10)
    draft_number: Optional[int] = Field(None, ge=1, le=100)
    is_active: bool = Field(default=True)

class PlayerCreate(PlayerBase):
    """Schema for creating a player."""
    pass

class PlayerUpdate(BaseModel):
    """Schema for updating a player."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    team_id: Optional[UUID] = None
    position: Optional[str] = Field(None, max_length=10)
    jersey_number: Optional[int] = Field(None, ge=0, le=99)
    height: Optional[str] = Field(None, max_length=10)
    weight: Optional[str] = Field(None, max_length=10)
    birth_date: Optional[date] = None
    college: Optional[str] = Field(None, max_length=255)
    country: Optional[str] = Field(None, max_length=100)
    is_active: Optional[bool] = None

class Player(UUIDMixin, PlayerBase, TimestampMixin):
    """Complete player schema."""
    # Optional relationship data
    team: Optional[Team] = None
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }

# Season schemas
class SeasonBase(BaseModel):
    """Base season schema."""
    year: int = Field(..., ge=1946, le=2050)
    season_type: SeasonTypeEnum = SeasonTypeEnum.REGULAR
    is_current: bool = Field(default=False)
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    games_played: int = Field(default=0, ge=0)
    total_games: int = Field(default=82, ge=0)

class SeasonCreate(SeasonBase):
    """Schema for creating a season."""
    pass

class Season(UUIDMixin, SeasonBase, TimestampMixin):
    """Complete season schema."""
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }

# Player stats schemas
class PlayerStatBase(BaseModel):
    """Base player stat schema."""
    player_id: UUID
    season_id: UUID
    team_id: Optional[UUID] = None
    game_id: Optional[str] = Field(None, max_length=50)
    game_date: Optional[date] = None
    opponent_team_id: Optional[UUID] = None
    is_home: bool = Field(default=True)
    stat_key: StatKeyEnum
    stat_value: float = Field(..., ge=0)
    minutes_played: Optional[float] = Field(None, ge=0)
    is_starter: bool = Field(default=False)
    plus_minus: Optional[int] = None
    efficiency_rating: Optional[float] = Field(None, ge=0)
    usage_rate: Optional[float] = Field(None, ge=0, le=100)

class PlayerStatCreate(PlayerStatBase):
    """Schema for creating player stats."""
    pass

class PlayerStat(UUIDMixin, PlayerStatBase, TimestampMixin):
    """Complete player stat schema."""
    # Optional relationship data
    player: Optional[Player] = None
    season: Optional[Season] = None
    team: Optional[Team] = None
    opponent_team: Optional[Team] = None
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }

# Team standings schemas
class TeamStandingBase(BaseModel):
    """Base team standing schema."""
    team_id: UUID
    season_id: UUID
    wins: int = Field(default=0, ge=0)
    losses: int = Field(default=0, ge=0)
    ties: int = Field(default=0, ge=0)
    gb: float = Field(default=0.0, ge=0)
    streak: Optional[str] = Field(None, max_length=10)
    home_wins: int = Field(default=0, ge=0)
    home_losses: int = Field(default=0, ge=0)
    away_wins: int = Field(default=0, ge=0)
    away_losses: int = Field(default=0, ge=0)
    conference_wins: int = Field(default=0, ge=0)
    conference_losses: int = Field(default=0, ge=0)
    division_wins: int = Field(default=0, ge=0)
    division_losses: int = Field(default=0, ge=0)
    last_10_wins: int = Field(default=0, ge=0, le=10)
    last_10_losses: int = Field(default=0, ge=0, le=10)
    strength_of_schedule: Optional[float] = Field(None, ge=0, le=1)
    point_differential: Optional[float] = None

class TeamStandingCreate(TeamStandingBase):
    """Schema for creating team standings."""
    pass

class TeamStanding(UUIDMixin, TeamStandingBase, TimestampMixin):
    """Complete team standing schema."""
    # Computed fields
    games_played: Optional[int] = None
    pct: Optional[float] = None
    
    # Optional relationship data
    team: Optional[Team] = None
    season: Optional[Season] = None
    
    @validator('pct', always=True)
    def calculate_pct(cls, v, values):
        """Calculate winning percentage."""
        wins = values.get('wins', 0)
        losses = values.get('losses', 0)
        ties = values.get('ties', 0)
        total_games = wins + losses + ties
        
        if total_games == 0:
            return 0.0
        
        return round((wins + ties * 0.5) / total_games, 3)
    
    @validator('games_played', always=True)
    def calculate_games_played(cls, v, values):
        """Calculate total games played."""
        return values.get('wins', 0) + values.get('losses', 0) + values.get('ties', 0)
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }

# Betting schemas
class BetBase(BaseModel):
    """Base bet schema."""
    user_id: UUID
    player_id: UUID
    stat_key: StatKeyEnum
    threshold: float = Field(..., gt=0)
    side: BetSideEnum
    stake: float = Field(default=10.0, gt=0, le=1000)
    odds: Optional[float] = Field(None, gt=0)
    potential_payout: Optional[float] = Field(None, gt=0)
    status: BetStatusEnum = BetStatusEnum.PENDING
    result_value: Optional[float] = Field(None, ge=0)
    confidence_score: Optional[float] = Field(None, ge=0, le=1)
    game_date: Optional[date] = None
    resolved_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    notes: Optional[str] = Field(None, max_length=500)
    source: str = Field(default="manual", max_length=50)

class BetCreate(BetBase):
    """Schema for creating a bet."""
    pass

class BetUpdate(BaseModel):
    """Schema for updating a bet."""
    status: Optional[BetStatusEnum] = None
    result_value: Optional[float] = Field(None, ge=0)
    resolved_at: Optional[datetime] = None
    notes: Optional[str] = Field(None, max_length=500)

class Bet(UUIDMixin, BetBase, TimestampMixin):
    """Complete bet schema."""
    # Optional relationship data
    player: Optional[Player] = None
    
    @validator('potential_payout', always=True)
    def calculate_potential_payout(cls, v, values):
        """Calculate potential payout based on stake and odds."""
        stake = values.get('stake')
        odds = values.get('odds')
        
        if stake and odds:
            return round(stake * odds, 2)
        return v
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }

# Watchlist schemas
class WatchlistBase(BaseModel):
    """Base watchlist schema."""
    user_id: UUID
    player_id: UUID
    priority: int = Field(default=3, ge=1, le=5)
    tags: Optional[List[str]] = Field(default_factory=list)
    notes: Optional[str] = Field(None, max_length=500)
    notify_on_games: bool = Field(default=True)
    notify_on_stats: bool = Field(default=False)
    notify_threshold: Optional[float] = Field(None, ge=0)
    view_count: int = Field(default=0, ge=0)
    last_viewed_at: Optional[datetime] = None

class WatchlistCreate(WatchlistBase):
    """Schema for creating watchlist entry."""
    pass

class WatchlistUpdate(BaseModel):
    """Schema for updating watchlist entry."""
    priority: Optional[int] = Field(None, ge=1, le=5)
    tags: Optional[List[str]] = None
    notes: Optional[str] = Field(None, max_length=500)
    notify_on_games: Optional[bool] = None
    notify_on_stats: Optional[bool] = None
    notify_threshold: Optional[float] = Field(None, ge=0)

class Watchlist(UUIDMixin, WatchlistBase, TimestampMixin):
    """Complete watchlist schema."""
    # Optional relationship data
    player: Optional[Player] = None
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }

# Roster schemas
class RosterBase(BaseModel):
    """Base roster schema."""
    user_id: UUID
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    roster_type: str = Field(default="fantasy", max_length=20)
    max_players: int = Field(default=15, ge=1, le=20)
    salary_cap: Optional[float] = Field(None, gt=0)
    is_public: bool = Field(default=False)
    is_active: bool = Field(default=True)
    is_template: bool = Field(default=False)
    total_value: float = Field(default=0.0, ge=0)
    performance_score: Optional[float] = Field(None, ge=0)

class RosterCreate(RosterBase):
    """Schema for creating a roster."""
    pass

class RosterUpdate(BaseModel):
    """Schema for updating a roster."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    max_players: Optional[int] = Field(None, ge=1, le=20)
    salary_cap: Optional[float] = Field(None, gt=0)
    is_public: Optional[bool] = None
    is_active: Optional[bool] = None

class Roster(UUIDMixin, RosterBase, TimestampMixin):
    """Complete roster schema."""
    # Optional relationship data
    players: Optional[List[Player]] = None
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }

# Roster players schemas
class RosterPlayerBase(BaseModel):
    """Base roster player schema."""
    roster_id: UUID
    player_id: UUID
    position_type: Optional[str] = Field(None, max_length=20)
    roster_position: Optional[int] = Field(None, ge=1, le=20)
    salary: Optional[float] = Field(None, ge=0)
    projected_points: Optional[float] = Field(None, ge=0)
    actual_points: float = Field(default=0.0, ge=0)
    games_played: int = Field(default=0, ge=0)

class RosterPlayerCreate(RosterPlayerBase):
    """Schema for creating roster player."""
    pass

class RosterPlayer(UUIDMixin, RosterPlayerBase, TimestampMixin):
    """Complete roster player schema."""
    # Optional relationship data
    roster: Optional[Roster] = None
    player: Optional[Player] = None
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }

# Search and API response schemas
class SearchResult(BaseModel):
    """Generic search result schema."""
    id: UUID
    name: str
    type: str  # 'player', 'team', etc.
    score: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None

class PaginatedResponse(BaseModel):
    """Paginated response schema."""
    items: List[Any]
    total: int
    page: int = Field(default=1, ge=1)
    per_page: int = Field(default=20, ge=1, le=100)
    pages: int
    has_next: bool
    has_prev: bool

class APIResponse(BaseModel):
    """Standard API response schema."""
    success: bool = True
    message: Optional[str] = None
    data: Optional[Any] = None
    errors: Optional[List[str]] = None
    meta: Optional[Dict[str, Any]] = None

# Statistics and analytics schemas
class PlayerStatsAggregated(BaseModel):
    """Aggregated player statistics."""
    player_id: UUID
    season_id: UUID
    games_played: int = Field(default=0, ge=0)
    minutes_per_game: Optional[float] = Field(None, ge=0)
    points_per_game: Optional[float] = Field(None, ge=0)
    rebounds_per_game: Optional[float] = Field(None, ge=0)
    assists_per_game: Optional[float] = Field(None, ge=0)
    steals_per_game: Optional[float] = Field(None, ge=0)
    blocks_per_game: Optional[float] = Field(None, ge=0)
    turnovers_per_game: Optional[float] = Field(None, ge=0)
    field_goal_percentage: Optional[float] = Field(None, ge=0, le=100)
    three_point_percentage: Optional[float] = Field(None, ge=0, le=100)
    free_throw_percentage: Optional[float] = Field(None, ge=0, le=100)
    efficiency_rating: Optional[float] = Field(None, ge=0)
    usage_rate: Optional[float] = Field(None, ge=0, le=100)
    
    class Config:
        from_attributes = True
        json_encoders = {
            UUID: lambda v: str(v)
        }

class BettingOdds(BaseModel):
    """Betting odds calculation result."""
    player_id: UUID
    stat_key: StatKeyEnum
    threshold: float
    over_odds: float = Field(..., gt=0)
    under_odds: float = Field(..., gt=0)
    over_probability: float = Field(..., ge=0, le=1)
    under_probability: float = Field(..., ge=0, le=1)
    confidence: str = Field(..., regex=r'^(Low|Medium|High|Very High)
    )
    sample_size: int = Field(..., ge=0)
    recent_form: Optional[float] = Field(None, ge=0, le=1)
    historical_average: Optional[float] = Field(None, ge=0)
    standard_deviation: Optional[float] = Field(None, ge=0)
    
    class Config:
        json_encoders = {
            UUID: lambda v: str(v)
        }

# Export all schemas
__all__ = [
    # Enums
    'ConferenceEnum', 'SeasonTypeEnum', 'BetSideEnum', 'BetStatusEnum', 'StatKeyEnum',
    
    # Mixins
    'TimestampMixin', 'UUIDMixin',
    
    # Team schemas
    'TeamBase', 'TeamCreate', 'TeamUpdate', 'Team',
    
    # Player schemas  
    'PlayerBase', 'PlayerCreate', 'PlayerUpdate', 'Player',
    
    # Season schemas
    'SeasonBase', 'SeasonCreate', 'Season',
    
    # Player stats schemas
    'PlayerStatBase', 'PlayerStatCreate', 'PlayerStat',
    
    # Team standings schemas
    'TeamStandingBase', 'TeamStandingCreate', 'TeamStanding',
    
    # Betting schemas
    'BetBase', 'BetCreate', 'BetUpdate', 'Bet',
    
    # Watchlist schemas
    'WatchlistBase', 'WatchlistCreate', 'WatchlistUpdate', 'Watchlist',
    
    # Roster schemas
    'RosterBase', 'RosterCreate', 'RosterUpdate', 'Roster',
    'RosterPlayerBase', 'RosterPlayerCreate', 'RosterPlayer',
    
    # API schemas
    'SearchResult', 'PaginatedResponse', 'APIResponse',
    
    # Analytics schemas
    'PlayerStatsAggregated', 'BettingOdds'
]