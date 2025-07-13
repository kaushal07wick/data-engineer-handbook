-- Create a Slowly Changing Dimension (SCD) history table for actors.
-- This table tracks changes in actors' quality classifications and activity status over time.

CREATE TABLE actors_history_scd (            

    -- The name of the actor (can appear multiple times if their status changes over time)
    actor TEXT,                   

    -- The quality classification of the actor (star, good, average, bad)
    quality_class quality_class,  

    -- Indicates if the actor was active during the recorded time period
    is_active BOOLEAN,           

    -- The year when this specific record period started
    start_date INTEGER,             

    -- The year when this specific record period ended
    end_date INTEGER,                    

    -- Indicates whether this record is the actor's current status (TRUE) or a historical record (FALSE)
    is_current BOOLEAN  

);

-- Select all records from the history table for review
SELECT * FROM actors_history_scd;

-- Create indexes AFTER the tables are created
CREATE INDEX idx_actor ON actors_history_scd (actor);
CREATE INDEX idx_is_current ON actors_history_scd (is_current);