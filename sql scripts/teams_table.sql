CREATE TABLE teams (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    nba_id INTEGER UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    abbreviation VARCHAR(10) NOT NULL,
    city VARCHAR(255) NOT NULL,
    state VARCHAR(100),
    conference VARCHAR(10) CHECK (conference IN ('East', 'West')),
    division VARCHAR(20),
    logo_url TEXT,
    primary_color VARCHAR(7),
    secondary_color VARCHAR(7),
    founded_year INTEGER,
    arena_name VARCHAR(255),
    arena_capacity INTEGER,
    search_vector tsvector,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for teams
CREATE INDEX idx_teams_nba_id ON teams(nba_id);
CREATE INDEX idx_teams_abbreviation ON teams(abbreviation);
CREATE INDEX idx_teams_conference ON teams(conference);
CREATE INDEX idx_teams_division ON teams(division);
CREATE INDEX idx_teams_name_gin ON teams USING gin(name gin_trgm_ops);
CREATE INDEX idx_teams_city_gin ON teams USING gin(city gin_trgm_ops);
CREATE INDEX idx_teams_search_vector ON teams USING gin(search_vector);

-- Team search vector function
CREATE OR REPLACE FUNCTION update_teams_search_vector() 
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := setweight(to_tsvector('english', COALESCE(NEW.name,'')), 'A') ||
                        setweight(to_tsvector('english', COALESCE(NEW.city,'')), 'A') ||
                        setweight(to_tsvector('english', COALESCE(NEW.abbreviation,'')), 'B') ||
                        setweight(to_tsvector('english', COALESCE(NEW.division,'')), 'C');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER teams_search_vector_update
    BEFORE INSERT OR UPDATE ON teams
    FOR EACH ROW EXECUTE FUNCTION update_teams_search_vector();