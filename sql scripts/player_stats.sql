CREATE TABLE player_stats (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    player_id UUID NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    season_id UUID NOT NULL REFERENCES seasons(id) ON DELETE CASCADE,
    team_id UUID REFERENCES teams(id),
    game_id VARCHAR(50),
    game_date DATE,
    opponent_team_id UUID REFERENCES teams(id),
    is_home BOOLEAN DEFAULT true,
    
    -- Basic stats
    stat_key VARCHAR(20) NOT NULL CHECK (stat_key IN (
        'pts', 'reb', 'ast', 'stl', 'blk', 'tov', 'pf',
        'fg_made', 'fg_att', 'fg_pct', 'fg3_made', 'fg3_att', 'fg3_pct',
        'ft_made', 'ft_att', 'ft_pct', 'oreb', 'dreb', 'min'
    )),
    stat_value DECIMAL(10,2) NOT NULL,
    
    -- Additional context
    minutes_played DECIMAL(5,2),
    is_starter BOOLEAN DEFAULT false,
    plus_minus INTEGER,
    
    -- Performance metrics (calculated)
    efficiency_rating DECIMAL(8,2),
    usage_rate DECIMAL(5,2),
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Comprehensive indexing strategy for stats
CREATE UNIQUE INDEX idx_player_stats_unique ON player_stats(player_id, season_id, stat_key, game_date, game_id);

-- Performance indexes
CREATE INDEX idx_player_stats_player_id ON player_stats(player_id);
CREATE INDEX idx_player_stats_season_id ON player_stats(season_id);
CREATE INDEX idx_player_stats_stat_key ON player_stats(stat_key);
CREATE INDEX idx_player_stats_game_date ON player_stats(game_date DESC);
CREATE INDEX idx_player_stats_team_id ON player_stats(team_id);

-- Composite indexes for common queries
CREATE INDEX idx_player_stats_player_season ON player_stats(player_id, season_id);
CREATE INDEX idx_player_stats_player_stat_season ON player_stats(player_id, stat_key, season_id);
CREATE INDEX idx_player_stats_season_stat_value ON player_stats(season_id, stat_key, stat_value DESC);
CREATE INDEX idx_player_stats_date_range ON player_stats(game_date, stat_key) WHERE game_date IS NOT NULL;

-- Partial indexes for active data
CREATE INDEX idx_player_stats_recent ON player_stats(game_date DESC, stat_key) 
    WHERE game_date >= CURRENT_DATE - INTERVAL '30 days';