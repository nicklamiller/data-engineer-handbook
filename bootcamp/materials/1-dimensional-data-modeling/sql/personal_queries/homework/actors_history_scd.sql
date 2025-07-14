DROP TABLE IF EXISTS actors_history_scd;
CREATE TABLE IF NOT EXISTS actors_history_scd (
	actor_id TEXT,
	actor_name TEXT,
	quality_class quality_class,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER,
	current_year INTEGER,
	PRIMARY KEY(actor_id, start_year)
);

CREATE TYPE scd_type AS (
	quality_class quality_class,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER
);

-- Backfill query
INSERT INTO actors_history_scd
WITH with_previous AS (
	SELECT
		actor_id,
		actor_name,
		current_year,
		quality_class,
		is_active,
		LAG(quality_class, 1) OVER(PARTITION BY actor_id ORDER BY current_year) AS quality_class_previous,
		LAG(is_active, 1) OVER(PARTITION BY actor_id ORDER BY current_year) AS is_active_previous
	FROM actors
	WHERE current_year <= 1977
	ORDER BY actor_name ASC	
)
, with_indicators AS (
	SELECT
		*,
		CASE
			WHEN is_active <> is_active_previous OR quality_class <> quality_class_previous THEN 1
			ELSE 0
		END AS change_indicator
	FROM with_previous
)
, with_streaks AS (
	SELECT
		*,
		SUM(change_indicator) OVER(PARTITION BY actor_id ORDER BY current_year) AS streak_identifier
	FROM with_indicators
)
SELECT
	actor_id,
	MAX(actor_name) AS actor_name,
	quality_class,
	is_active,
	MIN(current_year) AS start_year,
	MAX(current_year) AS end_year,
	1977 AS current_year
FROM with_streaks
GROUP BY
	actor_id,
	streak_identifier,
	quality_class,
	is_active;


-- Incremental query
WITH last_year_scd AS (
	SELECT *
	FROM actors_history_scd
	WHERE current_year = 1977
)
, historical_scd AS (
	SELECT
		actor_id,
		actor_name,
		quality_class,
		is_active,
		start_year,
		end_year
	FROM actors_history_scd
	WHERE current_year = 1977
		AND end_year < 1977
)
, this_year AS (
	SELECT *
	FROM actors
	WHERE current_year = 1978
)
, unchanged_records AS (
	SELECT
		ty.actor_id,
		ty.actor_name,
		ty.quality_class,
		ty.is_active,
		ly.start_year,
		ty.current_year AS end_year
	FROM this_year ty
	LEFT JOIN last_year_scd ly USING(actor_id)
	WHERE
		ty.is_active = ly.is_active
		AND ty.quality_class = ly.quality_class
)
, changed_records AS (
	SELECT
		ty.actor_id,
		ty.actor_name,
		UNNEST(ARRAY[
			ROW(
				ly.quality_class,
				ly.is_active,
				ly.start_year,
				ly.end_year
			)::scd_type,
			ROW(
				ty.quality_class,
				ty.is_active,
				ty.current_year,
				ty.current_year
			)::scd_type
		]) AS records
	FROM this_year ty
	LEFT JOIN last_year_scd ly USING(actor_id)
	WHERE
		ty.is_active <> ly.is_active
		AND ty.quality_class <> ly.quality_class
)
, unnested_changed_records AS (
	SELECT
		actor_id,
		actor_name,
		(records::scd_type).quality_class,
		(records::scd_type).is_active,
		(records::scd_type).start_year,
		(records::scd_type).end_year
	FROM changed_records
)
, new_records AS (
	SELECT
		ty.actor_id,
		ty.actor_name,
		ty.quality_class,
		ty.is_active,
		ty.current_year,
		ty.current_year AS end_year
	FROM this_year ty
	LEFT JOIN last_year_scd ly
		ON ty.actor_id = ly.actor_id
	WHERE ly.actor_id IS NULL
)

SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;
