-- View all records in the 'games' table
-- This table likely contains metadata about each game (e.g., game ID, teams, points, dates)
SELECT * FROM games;

-- View all records in the 'teams' table
-- This table contains information about each team (e.g., team ID, abbreviation, city)
SELECT * FROM teams;


-- Count how many vertices exist for each type (player, team, game)
-- Useful for validating that the graph has the expected number of nodes per category
SELECT type, COUNT(1)
FROM vertices
GROUP BY 1;
