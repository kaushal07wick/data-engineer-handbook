-- Code for defining "plays_in" edge (between player and game)

-- Step 1: Preview the raw game details table
SELECT * FROM game_details;

-- Step 2: Build "plays_in" edge — each player appears in a specific game
-- This version deduplicates player-game pairs before generating edge properties
SELECT 
    player_id AS subject_identifier, -- Player node (edge subject)
    'player'::vertex_type AS subject_type,
    game_id AS object_identifier, -- Game node (edge object)
    'game'::vertex_type AS object_type,
    'plays_in'::edge_type AS edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
    ) -- Properties of the relationship (player's context in that game)
FROM deduped
WHERE row_num = 1;

-- Step 3: Check for duplicate player-game records
SELECT player_id, game_id, COUNT(1)
FROM game_details
GROUP BY 1,2;

-- Step 4: Query for each player’s max points in any single game
SELECT 
    v.properties->>'player_name', -- Extract player name from JSON
    MAX(CAST(e.properties->>'pts' AS INTEGER)) -- Max points scored by the player
FROM vertices v 
JOIN edges e ON e.subject_identifier = v.identifier AND e.subject_type = v.type
GROUP BY 1
ORDER BY 2 DESC;

-- Step 5: Insert "plays_in", "plays_against", and "shares_team" edges between players
-- Deduplicate entries (keep only one record per player-game pair)
INSERT INTO edges
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) AS row_num 
    FROM game_details   
),
filtered AS (
    SELECT * FROM deduped
    WHERE row_num = 1
),
aggregated AS (
    -- Compare every pair of players within the same game
    -- Create "shares_team" if same team, otherwise "plays_against"
    SELECT 
        f1.player_id AS subject_player_id,
        f2.player_id AS object_player_id,
        CASE 
            WHEN f1.team_abbreviation = f2.team_abbreviation 
                THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END AS edge_type,
        MAX(f1.player_name) AS subject_player_name,
        MAX(f2.player_name) AS object_player_name,
        COUNT(1) AS num_games, -- Number of games they played together
        SUM(f1.pts) AS left_points, -- Total points for subject
        SUM(f2.pts) AS right_points -- Total points for object
    FROM filtered f1 
    JOIN filtered f2
        ON f1.game_id = f2.game_id -- Same game
       AND f1.player_id > f2.player_id -- Avoid self-pairs and duplicates
    GROUP BY 
        f1.player_id,
        f2.player_id,
        edge_type
)
-- Final edge insert with relationship properties between players
SELECT 
    subject_player_id AS subject_identifier,
    'player'::vertex_type AS subject_type,
    object_player_id AS object_identifier,
    'player'::vertex_type AS object_type,
    edge_type AS edge_type,
    json_build_object(
        'num_games', num_games,
        'subject_pts', left_points,
        'object_pts', right_points
    ) AS properties
FROM aggregated;

-- Step 6: Review all edges created (including "plays_in", "shares_team", "plays_against")
SELECT * FROM edges;

-- Step 7: Query edge data for advanced analysis
-- Calculate each player's total edge connections to others and related stats
SELECT
    v.properties->>'player_name', -- Player name
    e.object_identifier, -- Other player connected by edge
    CAST(v.properties->>'number_of_games' AS REAL) /
        CASE 
            WHEN CAST(v.properties->>'number_of_games' AS REAL) = 0 THEN 1 
            ELSE CAST(v.properties->>'number_of_games' AS REAL) 
        END AS normalized_game_ratio, -- Always 1.0; placeholder for potential future calc
    e.properties->>'subject_points', -- Player’s points across shared games
    e.properties->>'num_games' -- Number of games between the two players
FROM vertices v 
JOIN edges e 
    ON v.identifier = e.subject_identifier 
    AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type;
