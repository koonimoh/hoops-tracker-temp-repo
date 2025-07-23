CREATE OR REPLACE FUNCTION update_team_standings()
RETURNS TRIGGER AS $$
BEGIN
    -- Recalculate games behind (GB) for all teams in the same season
    UPDATE team_standings ts1
    SET gb = GREATEST(0, 
        (SELECT (wins - losses) FROM team_standings ts2 
         WHERE ts2.season_id = ts1.season_id 
         ORDER BY pct DESC, wins DESC LIMIT 1) - (ts1.wins - ts1.losses)
    ) / 2.0
    WHERE ts1.season_id = NEW.season_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER team_standings_update_gb
    AFTER INSERT OR UPDATE ON team_standings
    FOR EACH ROW EXECUTE FUNCTION update_team_standings();