-- This query builds SCD (Slowly Changing Dimension) history records by detecting changes in an actor's quality_class or is_active status over the years.

-- Step 1: Detect if a change has occurred compared to the previous year's record
WITH streak_started AS (
    SELECT
        actor,
        current_year,
        quality_class,
        is_active,
        -- Check if either the quality_class or is_active has changed from the previous record
        -- Also handle the first occurrence where no previous record exists (LAG returns NULL)
        LAG(quality_class) OVER (PARTITION BY actor ORDER BY current_year) <> quality_class
            OR LAG(is_active) OVER (PARTITION BY actor ORDER BY current_year) <> is_active
            OR LAG(quality_class) OVER (PARTITION BY actor ORDER BY current_year) IS NULL
            OR LAG(is_active) OVER (PARTITION BY actor ORDER BY current_year) IS NULL
            AS did_change  -- Flag to indicate a change has started
    FROM actors
),

-- Step 2: Assign a unique streak ID for each unchanged sequence
streak_identified AS (
    SELECT
        actor,
        quality_class,
        is_active,
        current_year,
        -- Create a streak ID that increments each time a change is detected
        SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
            OVER (PARTITION BY actor ORDER BY current_year) AS streak_id
    FROM streak_started
),

-- Step 3: Aggregate streaks to get start and end years
aggregated AS (
    SELECT
        actor,
        quality_class,
        is_active,
        MIN(current_year) AS start_date,  -- First year of the streak
        MAX(current_year) AS end_date     -- Last year of the streak
    FROM streak_identified
    GROUP BY actor, quality_class, is_active, streak_id  -- Group by streak to consolidate years
)

-- Step 4: Insert the aggregated SCD records into the history table
INSERT INTO actors_history_scd (
    actor,
    quality_class,
    is_active,
    start_date,
    end_date,
    is_current  -- Mark whether this is the most recent/current record
)
SELECT
    actor,
    quality_class,
    is_active,
    start_date,
    end_date,
    -- Determine if this is the current record by checking if it extends to the most recent year in the source table
    CASE 
        WHEN end_date = (SELECT MAX(current_year) FROM actors) THEN TRUE
        ELSE FALSE
    END AS is_current
FROM aggregated;

-- Step 5: View the final SCD history table
SELECT * FROM actors_history_scd;
