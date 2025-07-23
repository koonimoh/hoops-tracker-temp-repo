CREATE TABLE rosters (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    
    -- Roster configuration
    roster_type VARCHAR(20) DEFAULT 'fantasy' CHECK (roster_type IN ('fantasy', 'draft', 'favorites', 'analysis')),
    max_players INTEGER DEFAULT 15,
    salary_cap DECIMAL(12,2),
    
    -- Status and sharing
    is_public BOOLEAN DEFAULT false,
    is_active BOOLEAN DEFAULT true,
    is_template BOOLEAN DEFAULT false,
    
    -- Performance tracking
    total_value DECIMAL(12,2) DEFAULT 0,
    performance_score DECIMAL(8,2),
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Roster indexes
CREATE INDEX idx_rosters_user_id ON rosters(user_id);
CREATE INDEX idx_rosters_public ON rosters(is_public) WHERE is_public = true;
CREATE INDEX idx_rosters_type ON rosters(roster_type);
CREATE INDEX idx_rosters_active ON rosters(user_id, is_active) WHERE is_active = true;