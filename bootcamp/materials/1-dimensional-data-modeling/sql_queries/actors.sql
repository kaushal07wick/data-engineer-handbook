CREATE TYPE film_struct AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TYPE quality_class AS ENUM('star', 'good', 'average', 'bad');

CREATE TABLE actors (
    actorid TEXT,
    filmid TEXT,
    actor TEXT,
    film film_struct[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY (actorid, filmid)
);
