CREATE TABLE seasons (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    year INTEGER NOT NULL,
    season_type VARCHAR(20) DEFAULT 'Regular Season' CHECK (season_type IN ('Regular Season', 'Playoffs', 'Preseason')),
    is_current BOOLEAN DEFAULT false,
    start_date DATE,
    end_date DATE,
    games_played INTEGER DEFAULT 0,
    total_games INTEGER DEFAULT 82,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Ensure only one current season per type
CREATE UNIQUE INDEX idx_seasons_current ON seasons(is_current, season_type) WHERE is_current = true;
CREATE INDEX idx_seasons_year ON seasons(year);
CREATE INDEX idx_seasons_type ON seasons(season_type);