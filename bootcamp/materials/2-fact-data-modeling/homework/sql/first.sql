-- 1. Deduplicate game_details
-- De-duplicate explicitly based on game_id, team_id, and player_id
CREATE TABLE deduped_game_details AS
SELECT DISTINCT ON (game_id, team_id, player_id) *
FROM game_details;
