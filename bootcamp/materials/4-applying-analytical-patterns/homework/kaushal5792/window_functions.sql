-- ===========================
-- Part 1: Most games won in a 90-game stretch
-- ===========================

WITH team_games AS (
    -- Create a unified table of all games from each team's perspective
    -- First part: home team perspective
    SELECT
        g.season,
        g.game_id,
        g.game_date_est,
        g.home_team_id AS team_id,
        CASE WHEN g.home_team_wins = 1 THEN 1 ELSE 0 END AS win_flag -- 1 if home team won, else 0
    FROM games g

    UNION ALL

    -- Second part: away team perspective
    SELECT
        g.season,
        g.game_id,
        g.game_date_est,
        g.visitor_team_id AS team_id,
        CASE WHEN g.home_team_wins = 0 THEN 1 ELSE 0 END AS win_flag -- 1 if away team won, else 0
    FROM games g
),
rolling_wins AS (
    -- For each team, calculate a rolling sum of wins in the last 90 games
    SELECT
        team_id,
        game_id,
        game_date_est,
        SUM(win_flag) OVER (
            PARTITION BY team_id
            ORDER BY game_date_est
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS wins_last_90
    FROM team_games
)
-- Find the maximum wins any team has achieved in a 90-game window
SELECT
    team_id,
    MAX(wins_last_90) AS max_wins_in_90_games
FROM rolling_wins
GROUP BY team_id
ORDER BY max_wins_in_90_games DESC;


-- ===========================
-- Part 2: Longest streak of LeBron James scoring over 10 points
-- ===========================

WITH lebron_games AS (
    -- Get all games played by LeBron James along with points scored
    SELECT
        gd.game_id,
        g.game_date_est,
        gd.player_name,
        gd.pts,
        CASE WHEN gd.pts > 10 THEN 1 ELSE 0 END AS over_10_flag -- 1 if > 10 points
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
    WHERE gd.player_name = 'LeBron James'
),
streak_groups AS (
    -- Identify "streak groups" by counting how many times he failed to score over 10 points
    -- The count increases each time he scores 10 or fewer, effectively breaking the streak
    SELECT
        *,
        SUM(CASE WHEN over_10_flag = 0 THEN 1 ELSE 0 END) 
            OVER (ORDER BY game_date_est) AS streak_break_count
    FROM lebron_games
),
streak_lengths AS (
    -- Count the number of consecutive games (islands) where he scored over 10 points
    SELECT
        streak_break_count,
        COUNT(*) AS streak_length
    FROM streak_groups
    WHERE over_10_flag = 1
    GROUP BY streak_break_count
)
-- Find the longest such streak
SELECT
    MAX(streak_length) AS longest_over_10_streak
FROM streak_lengths;
