CREATE TABLE players (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    nba_id INTEGER UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    team_id UUID REFERENCES teams(id),
    position VARCHAR(10),
    jersey_number INTEGER,
    height VARCHAR(10),
    weight VARCHAR(10),
    birth_date DATE,
    college VARCHAR(255),
    country VARCHAR(100),
    draft_year INTEGER,
    draft_round INTEGER,
    draft_number INTEGER,
    is_active BOOLEAN DEFAULT true,
    search_vector tsvector,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Advanced indexes for search performance
CREATE INDEX idx_players_nba_id ON players(nba_id);
CREATE INDEX idx_players_name_gin ON players USING gin(name gin_trgm_ops);
CREATE INDEX idx_players_name_gist ON players USING gist(name gist_trgm_ops);
CREATE INDEX idx_players_team_id ON players(team_id);
CREATE INDEX idx_players_position ON players(position);
CREATE INDEX idx_players_active ON players(is_active) WHERE is_active = true;

-- Full-text search index
CREATE INDEX idx_players_search_vector ON players USING gin(search_vector);

-- Composite index for common queries
CREATE INDEX idx_players_active_team ON players(is_active, team_id) WHERE is_active = true;

-- Create search vector trigger function
CREATE OR REPLACE FUNCTION update_players_search_vector() 
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := setweight(to_tsvector('english', COALESCE(NEW.name,'')), 'A') ||
                        setweight(to_tsvector('english', COALESCE(NEW.position,'')), 'B') ||
                        setweight(to_tsvector('english', COALESCE(NEW.college,'')), 'C');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger
CREATE TRIGGER players_search_vector_update
    BEFORE INSERT OR UPDATE ON players
    FOR EACH ROW EXECUTE FUNCTION update_players_search_vector();