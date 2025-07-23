CREATE TABLE team_standings (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    team_id UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    season_id UUID NOT NULL REFERENCES seasons(id) ON DELETE CASCADE,
    
    -- Basic standings
    wins INTEGER NOT NULL DEFAULT 0,
    losses INTEGER NOT NULL DEFAULT 0,
    ties INTEGER DEFAULT 0,
    
    -- Calculated fields (using generated columns for consistency)
    games_played INTEGER GENERATED ALWAYS AS (wins + losses + ties) STORED,
    pct DECIMAL(4,3) GENERATED ALWAYS AS (
        CASE 
            WHEN (wins + losses + ties) = 0 THEN 0.000
            ELSE ROUND((wins + ties * 0.5)::DECIMAL / (wins + losses + ties), 3)
        END
    ) STORED,
    
    -- Advanced standings metrics
    gb DECIMAL(3,1) DEFAULT 0.0,
    streak VARCHAR(10),
    home_wins INTEGER DEFAULT 0,
    home_losses INTEGER DEFAULT 0,
    away_wins INTEGER DEFAULT 0,
    away_losses INTEGER DEFAULT 0,
    
    -- Conference/Division records
    conference_wins INTEGER DEFAULT 0,
    conference_losses INTEGER DEFAULT 0,
    division_wins INTEGER DEFAULT 0,
    division_losses INTEGER DEFAULT 0,
    
    -- Recent performance
    last_10_wins INTEGER DEFAULT 0,
    last_10_losses INTEGER DEFAULT 0,
    
    -- Strength metrics
    strength_of_schedule DECIMAL(4,3),
    point_differential DECIMAL(6,1),
    
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Ensure unique team per season
CREATE UNIQUE INDEX idx_team_standings_unique ON team_standings(team_id, season_id);
CREATE INDEX idx_team_standings_season ON team_standings(season_id);
CREATE INDEX idx_team_standings_pct ON team_standings(season_id, pct DESC);
CREATE INDEX idx_team_standings_conference ON team_standings(season_id, pct DESC) 
    WHERE EXISTS (SELECT 1 FROM teams t WHERE t.id = team_id);