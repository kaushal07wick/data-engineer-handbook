-- ================================================
-- Aggregate `game_details` with GROUPING SETS
-- ================================================
-- This query answers:
-- 1. Who scored the most points playing for one team?         (player + team)
-- 2. Who scored the most points in one season?                (player + season)
-- 3. Which team has won the most games?                       (team only)
-- It also counts wins by checking home/away team and home_team_wins.

SELECT
    gd.player_id,            -- Player's unique ID
    gd.player_name,          -- Player's name
    gd.team_id,              -- Team's unique ID
    gd.team_abbreviation,    -- Short form of the team name
    g.season,                -- NBA season of the game

    -- Total points scored in the grouping dimension
    SUM(gd.pts) AS total_points,

    -- Total wins in the grouping dimension
    SUM(
        CASE 
            -- Win condition for home team player
            WHEN (gd.team_id = g.home_team_id AND g.home_team_wins = 1) 
            -- Win condition for away team player
              OR (gd.team_id = g.visitor_team_id AND g.home_team_wins = 0) 
            THEN 1 ELSE 0 
        END
    ) AS total_wins,

    -- GROUPING() helps identify which grouping set produced the row
    -- e.g., 0 = all columns present, 1 = missing one dimension, etc.
    GROUPING(gd.player_id, gd.team_id, g.season) AS grouping_code

FROM game_details gd
JOIN games g 
    ON gd.game_id = g.game_id

-- GROUPING SETS efficiently generates multiple aggregation combinations in one pass
GROUP BY GROUPING SETS (
    (gd.player_id, gd.player_name, gd.team_id, gd.team_abbreviation),  -- player & team
    (gd.player_id, gd.player_name, g.season),                          -- player & season
    (gd.team_id, gd.team_abbreviation)                                 -- team only
)

-- Sorting so similar grouping sets are together and within them sorted by total points
ORDER BY grouping_code, total_points DESC;
