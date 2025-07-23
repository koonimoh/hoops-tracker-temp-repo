CREATE TABLE bets (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    player_id UUID NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    stat_key VARCHAR(20) NOT NULL,
    threshold DECIMAL(10,2) NOT NULL,
    side VARCHAR(10) NOT NULL CHECK (side IN ('over', 'under')),
    
    -- Betting details
    odds DECIMAL(8,4),
    stake DECIMAL(10,2) DEFAULT 0.00,
    potential_payout DECIMAL(10,2),
    
    -- Status and resolution
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'active', 'won', 'lost', 'push', 'cancelled')),
    result_value DECIMAL(10,2),
    confidence_score DECIMAL(3,2), -- AI confidence if implemented
    
    -- Timestamps
    placed_at TIMESTAMPTZ DEFAULT NOW(),
    game_date DATE,
    resolved_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    
    -- Metadata
    notes TEXT,
    source VARCHAR(50) DEFAULT 'manual',
    
    CONSTRAINT bets_valid_threshold CHECK (threshold > 0),
    CONSTRAINT bets_valid_odds CHECK (odds IS NULL OR odds > 0)
);

-- Performance indexes for bets
CREATE INDEX idx_bets_user_id ON bets(user_id);
CREATE INDEX idx_bets_player_id ON bets(player_id);
CREATE INDEX idx_bets_status ON bets(status);
CREATE INDEX idx_bets_placed_at ON bets(placed_at DESC);
CREATE INDEX idx_bets_game_date ON bets(game_date) WHERE game_date IS NOT NULL;

-- Composite indexes for common queries
CREATE INDEX idx_bets_user_status ON bets(user_id, status);
CREATE INDEX idx_bets_user_recent ON bets(user_id, placed_at DESC);
CREATE INDEX idx_bets_pending_resolution ON bets(status, game_date) WHERE status = 'pending';