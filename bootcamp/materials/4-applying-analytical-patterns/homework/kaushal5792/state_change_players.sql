-- =====================================================
-- Compare player activity status between two seasons
-- =====================================================
-- Goal:
-- Identify if players are new, retired, continuing, 
-- returning from retirement, or staying retired.
-- =====================================================

WITH previous_season AS (
    -- Get player names and active status for the 2021 season
    SELECT player_name, is_active
    FROM players
    WHERE current_season = 2021
),
current_season_data AS (
    -- Get player names and active status for the 2022 season
    SELECT player_name, is_active
    FROM players
    WHERE current_season = 2022
),
all_players AS (
    -- Union of both seasons' players
    -- Ensures we include players who appear in only one of the seasons
    SELECT player_name FROM previous_season
    UNION
    SELECT player_name FROM current_season_data
)
SELECT
    ap.player_name,

    -- Classify the change in player's status between seasons
    CASE
        -- Appears in 2022 but not in 2021 → New player
        WHEN ps.player_name IS NULL AND cs.is_active = TRUE
            THEN 'New'

        -- Was active in 2021 but not present in 2022 → Retired
        WHEN ps.is_active = TRUE AND cs.player_name IS NULL
            THEN 'Retired'

        -- Active in both seasons → Continued playing
        WHEN ps.is_active = TRUE AND cs.is_active = TRUE
            THEN 'Continued Playing'

        -- Was inactive in 2021 but active in 2022 → Returned from retirement
        WHEN ps.is_active = FALSE AND cs.is_active = TRUE
            THEN 'Returned from Retirement'

        -- Inactive in both seasons → Stayed retired
        WHEN ps.is_active = FALSE AND cs.is_active = FALSE
            THEN 'Stayed Retired'
    END AS state_change

FROM all_players ap
LEFT JOIN previous_season ps 
    ON ap.player_name = ps.player_name
LEFT JOIN current_season_data cs 
    ON ap.player_name = cs.player_name;
