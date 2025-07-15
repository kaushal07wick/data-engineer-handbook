-- Code from Module 2: Graph-based Modelling

-- Step 1: Create an ENUM type to represent vertex types in the graph
CREATE TYPE vertex_type
AS ENUM ('player', 'team', 'game');

-- Step 2: Insert 'game' vertices into the graph
-- Each row represents a game, storing points and the winning team as JSON properties
INSERT INTO vertices
SELECT 
    game_id AS identifier, -- Unique ID for the game node
    'game'::vertex_type AS type, -- Set the vertex type to 'game'
    json_build_object( -- JSON properties associated with the game
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning_team', CASE 
            WHEN home_team_wins = 1 THEN home_team_id 
            ELSE visitor_team_id 
        END
    ) AS properties
FROM games;

-- Step 3: Insert 'player' vertices with aggregated stats
-- Each player node includes total games played, total points, and associated teams
INSERT INTO vertices
WITH players_agg AS (
    SELECT
        player_id AS identifier, -- Unique ID for the player node
        MAX(player_name) as player_name, -- Get player name (in case of duplicates)
        COUNT(1) as number_of_games, -- Total games played
        SUM(pts) AS total_points, -- Total points scored
        array_agg(DISTINCT team_id) as teams -- All teams the player has played on
    FROM game_details
    GROUP BY player_id
)
SELECT identifier, 
    'player'::vertex_type, -- Set the vertex type to 'player'
    json_build_object(
        'player_name', player_name,
        'number_of_games', number_of_games,
        'total_points', total_points,
        'teams', teams
    )
FROM players_agg;

-- Step 4: Create the 'vertices' table to hold all nodes in the graph
CREATE TABLE vertices (
    identifier TEXT, -- Unique ID for each vertex
    type vertex_type, -- ENUM value indicating vertex type (player, team, game)
    properties JSON, -- JSON object holding attributes
    PRIMARY KEY (identifier, type) -- Composite key to ensure uniqueness
);

-- Step 5: Create an ENUM type for different edge relationships
CREATE TYPE edge_type AS
    ENUM ('plays_against', 'shares_team', 'plays_in', 'plays_on');

-- Step 6: Create the 'edges' table to represent relationships between vertices
CREATE TABLE edges (
    subject_identifier TEXT, -- ID of the starting vertex
    subject_type vertex_type, -- Type of the starting vertex
    object_identifier TEXT, -- ID of the target vertex
    object_type vertex_type, -- Type of the target vertex
    edge_type edge_type, -- Relationship type
    properties JSON, -- JSON metadata about the edge
    PRIMARY KEY (subject_identifier, subject_type, object_identifier, object_type, edge_type)
);

-- Step 7: (Optional) Preview the teams table
SELECT * FROM teams;

-- Step 8: Insert 'team' vertices into the graph
-- Deduplicate in case of multiple records per team_id
WITH teams_deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY team_id) AS row_num
    FROM teams
)
INSERT INTO vertices
SELECT 
    team_id AS identifier, -- Unique ID for the team node
    'team'::vertex_type AS type, -- Set the vertex type to 'team'
    json_build_object( -- JSON properties describing the team
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
    )
FROM teams_deduped
WHERE row_num = 1; -- Only keep the first occurrence per team_id
