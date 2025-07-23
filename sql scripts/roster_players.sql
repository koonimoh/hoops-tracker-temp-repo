CREATE TABLE roster_players (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    roster_id UUID NOT NULL REFERENCES rosters(id) ON DELETE CASCADE,
    player_id UUID NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    
    -- Roster position details
    position_type VARCHAR(20),
    roster_position INTEGER,
    salary DECIMAL(10,2),
    projected_points DECIMAL(8,2),
    
    -- Performance tracking
    actual_points DECIMAL(8,2) DEFAULT 0,
    games_played INTEGER DEFAULT 0,
    
    added_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT roster_players_unique UNIQUE(roster_id, player_id)
);

-- Roster players indexes
CREATE INDEX idx_roster_players_roster_id ON roster_players(roster_id);
CREATE INDEX idx_roster_players_player_id ON roster_players(player_id);
CREATE INDEX idx_roster_players_position ON roster_players(roster_id, position_type);