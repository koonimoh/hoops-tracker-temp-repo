CREATE TABLE watchlists (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    player_id UUID NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    
    -- Watchlist metadata
    priority INTEGER DEFAULT 1 CHECK (priority BETWEEN 1 AND 5),
    tags TEXT[],
    notes TEXT,
    
    -- Notification preferences
    notify_on_games BOOLEAN DEFAULT true,
    notify_on_stats BOOLEAN DEFAULT false,
    notify_threshold DECIMAL(5,2),
    
    -- Activity tracking
    view_count INTEGER DEFAULT 0,
    last_viewed_at TIMESTAMPTZ,
    added_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT watchlists_unique_user_player UNIQUE(user_id, player_id)
);

-- Indexes for watchlists
CREATE INDEX idx_watchlists_user_id ON watchlists(user_id);
CREATE INDEX idx_watchlists_player_id ON watchlists(player_id);
CREATE INDEX idx_watchlists_added_at ON watchlists(added_at DESC);
CREATE INDEX idx_watchlists_priority ON watchlists(user_id, priority DESC);
CREATE INDEX idx_watchlists_tags ON watchlists USING gin(tags);