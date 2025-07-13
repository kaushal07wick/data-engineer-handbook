CREATE TYPE scd_type AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER,
    is_current BOOLEAN
);


WITH last_year_scd AS (
    -- Get records that were active last year (ending in 1971)
    SELECT *
    FROM actors_history_scd
    WHERE end_date = 1971
),

historical_scd AS (
    -- Keep the historical records that ended before last year
    SELECT
        actor,
        quality_class,
        is_active,
        start_date,
        end_date
    FROM actors_history_scd
    WHERE end_date < 1971
),

this_year_data AS (
    -- Current year's data (1972)
    SELECT *
    FROM actors
    WHERE current_year = 1972
),

unchanged_records AS (
    -- Records where quality_class and is_active have not changed
    SELECT
        ty.actor,
        ty.quality_class,
        ty.is_active,
        ly.start_date,
        ty.current_year AS end_date
    FROM this_year_data ty
    JOIN last_year_scd ly
        ON ty.actor = ly.actor
    WHERE ty.quality_class = ly.quality_class
      AND ty.is_active = ly.is_active
),

changed_records AS (
    -- Records where either quality_class or is_active changed
    SELECT
        ty.actor,
        ARRAY[
            ROW(ly.quality_class, ly.is_active, ly.start_date, 1971)::scd_type,
            ROW(ty.quality_class, ty.is_active, ty.current_year, ty.current_year)::scd_type
        ] AS scd_array
    FROM this_year_data ty
    JOIN last_year_scd ly
        ON ty.actor = ly.actor
    WHERE ty.quality_class <> ly.quality_class
       OR ty.is_active <> ly.is_active
),

unnested_changed_records AS (
    -- Flatten the changed records array into individual rows, with explicit aliasing
    SELECT
        actor,
        scd_elem.quality_class,
        scd_elem.is_active,
        scd_elem.start_date,
        scd_elem.end_date
    FROM changed_records,
         UNNEST(scd_array) AS scd_elem(quality_class, is_active, start_date, end_date)
),

new_records AS (
    -- Actors that are completely new this year
    SELECT
        ty.actor,
        ty.quality_class,
        ty.is_active,
        ty.current_year AS start_date,
        ty.current_year AS end_date
    FROM this_year_data ty
    LEFT JOIN last_year_scd ly
        ON ty.actor = ly.actor
    WHERE ly.actor IS NULL
) INSERT INTO actors_history_scd (actor, quality_class, is_active, start_date, end_date)
SELECT * FROM (
    SELECT * FROM historical_scd
    UNION ALL
    SELECT * FROM unchanged_records
    UNION ALL
    SELECT * FROM unnested_changed_records
    UNION ALL
    SELECT * FROM new_records
) final_records;

SELECT * FROM actors_history_scd;