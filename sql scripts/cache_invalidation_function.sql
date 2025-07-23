CREATE OR REPLACE FUNCTION invalidate_player_cache(player_uuid UUID)
RETURNS VOID AS $$
BEGIN
    -- This function can be called from application code to trigger cache invalidation
    -- The actual cache clearing will be handled by the application layer
    
    -- Update the player's updated_at timestamp to signal cache invalidation
    UPDATE players 
    SET updated_at = NOW() 
    WHERE id = player_uuid;
    
    -- Log the cache invalidation
    INSERT INTO cache_invalidation_log (table_name, record_id, invalidated_at)
    VALUES ('players', player_uuid, NOW());
END;
$$ LANGUAGE plpgsql;

-- Create cache invalidation log table
CREATE TABLE cache_invalidation_log (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    record_id UUID,
    invalidated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_cache_invalidation_log_table ON cache_invalidation_log(table_name, invalidated_at);