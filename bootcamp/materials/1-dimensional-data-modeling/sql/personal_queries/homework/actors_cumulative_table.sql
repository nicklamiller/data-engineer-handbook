-- Clean up types and tables.
-- DROP TABLE IF EXISTS actors CASCADE;
-- DROP TYPE IF EXISTS films CASCADE;
-- DROP TYPE IF EXISTS quality_class CASCADE;

-- Create needed types and tables.
CREATE TYPE films AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
);
CREATE TYPE quality_class AS ENUM (
	'star', 'good', 'average', 'bad'
);
CREATE TABLE actors (
	actor_id TEXT,
	actor_name TEXT,
	year INTEGER,
	films films[],
	quality_class quality_class,
	is_active BOOLEAN,
	current_year INTEGER,
	PRIMARY KEY(actor_name, current_year)
);

-- Build cumulative actors table.
WITH cleaned_actor_films AS (
	SELECT
		actorid AS actor_id,
		actor AS actor_name,
		year,
		votes,
		rating,
		filmid,
		film
	FROM actor_films
)
, previous_year AS (
	SELECT *
	FROM actors
	WHERE current_year = 1969
)
, current_year AS (
	SELECT
		actor_id,
		MAX(actor_name) AS actor_name,
		year,
		ARRAY_AGG(ROW(
			film,
			votes,
			rating,
			filmid
		)::films) AS films,
		AVG(rating) AS avg_rating
	FROM cleaned_actor_films
	GROUP BY actor_id, year
	HAVING year = 1970
)
, combined AS (
	SELECT
		COALESCE(c.actor_id, p.actor_id) AS actor_id,
		COALESCE(c.actor_name, p.actor_name) AS actor_name,
		COALESCE(c.year, p.year) AS year,
		CASE
			WHEN p.films IS NULL THEN c.films
			WHEN c.year IS NOT NULL THEN p.films || c.films
			ELSE p.films
		END AS films,
		CASE
			WHEN avg_rating > 8 THEN 'star'
			WHEN avg_rating > 7 AND avg_rating <= 8 THEN 'good'
			WHEN avg_rating > 6 AND avg_rating <= 7 THEN 'average'
			ELSE 'bad'
		END::quality_class AS quality_class,
	(c.year IS NOT NULL) AS is_active,
	COALESCE(c.year, p.current_year + 1) AS current_year
	FROM current_year c
	FULL OUTER JOIN previous_year p
		USING(actor_id)
)

INSERT INTO actors
SELECT * FROM combined;

SELECT * FROM actors;

