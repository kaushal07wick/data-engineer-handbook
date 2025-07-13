-- This query updates the 'actors' table with new film records for the year 1974.
-- It carries forward actors from 1973 and updates their film lists, quality classifications, and activity status.

-- Optional: Retrieve the earliest year from 'actor_films' (commented out for now)
-- SELECT MIN(year) FROM actor_films;

-- Insert or update actor records for the current year (1974)
INSERT INTO actors (
    actorid,
    filmid,
    actor,
    film,
    quality_class,
    is_active,
    current_year
)

-- Define CTE for actors' data from the previous year (1973)
WITH yesterday AS (
    SELECT * FROM actors
    WHERE current_year = 1973 
),

-- Define CTE for films released in the current year (1974)
today AS (
    SELECT * FROM actor_films
    WHERE year = 1974
)

-- Combine yesterday's and today's data to build the updated actor records
SELECT  
    -- Use today's actorid if available, otherwise fallback to yesterday's
    COALESCE(t.actorid, y.actorid) AS actorid,

    -- Use today's filmid if available, otherwise fallback to yesterday's
    COALESCE(t.filmid, y.filmid) AS filmid,

    -- Use today's actor name if available, otherwise fallback to yesterday's
    COALESCE(t.actor, y.actor) AS actor,

    -- Film history management:
    -- If actor had no films yesterday, start new film array with today's film
    -- If actor already had films, append today's film to their existing film array
    -- If no new film, retain the previous film list
    CASE 
        WHEN y.film IS NULL THEN 
            ARRAY[ROW(
                t.film,
                t.votes,
                t.rating,
                t.filmid::TEXT
            )::film_struct]
        WHEN t.film IS NOT NULL THEN 
            y.film || ARRAY[ROW(
                t.film,
                t.votes,
                t.rating,
                t.filmid::TEXT
            )::film_struct]
        ELSE y.film
    END AS film,

    -- Update the actor's quality classification based on the latest film rating
    -- If no new film, retain the previous quality class
    CASE 
        WHEN t.film IS NOT NULL THEN
            CASE 
                WHEN t.rating > 8 THEN 'star'
                WHEN t.rating > 7 THEN 'good'
                WHEN t.rating > 6 THEN 'average'
                ELSE 'bad'
            END::quality_class
        ELSE y.quality_class
    END AS quality_class,

    -- Mark actor as active if they have a new film this year, otherwise inactive
    CASE 
        WHEN t.film IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_active,

    -- Use the current year if present (from today's data), otherwise increment last year's year by 1
    COALESCE(t.year, y.current_year + 1) AS current_year

FROM today t
-- Full outer join to ensure we include:
-- 1. Actors with new films this year (left side)
-- 2. Actors from last year with no new films (right side)
FULL OUTER JOIN yesterday y
    ON t.actorid = y.actorid
    AND t.filmid = y.filmid

-- Conflict handling: if actor-film combination already exists, update the record
ON CONFLICT (actorid, filmid) DO UPDATE
SET
    actor = EXCLUDED.actor,
    film = EXCLUDED.film,
    quality_class = EXCLUDED.quality_class,
    is_active = EXCLUDED.is_active,
    current_year = EXCLUDED.current_year;

-- Display the updated 'actors' table
SELECT * FROM actors;
