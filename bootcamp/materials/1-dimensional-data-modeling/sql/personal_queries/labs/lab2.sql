-- The output of this query is a type 2 SCD table modeling the scoring_class and is_active SCD's of the players table.
-- The players_scd creates the initial SCD table, and the final table combines new data with this SCD table.
-- NOTE: The final table doesn't contain the current_season column, which is included in the players_scd table.
-- NOTE: Aside from this, they contain the same information about the players.

-- Run the below snippet by itself once. This creates the custom types we'll use.
CREATE TYPE scd_type AS (
	scoring_class scoring_class,
	is_active BOOLEAN,
	start_season INTEGER,
	end_season INTEGER
);

-- Run below snippet however many times you want. This recreates the type 2 SCD table from scratch and adds new season data to it.
DROP TABLE IF EXISTS players_scd;
CREATE TABLE IF NOT EXISTS players_scd (
	player_name TEXT,
	scoring_class scoring_class,
	is_active BOOLEAN,
	start_season INTEGER,
	end_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY(player_name, start_season)
);

INSERT INTO players_scd
WITH with_previous AS (
	SELECT
		player_name,
		current_season,
		scoring_class,
		is_active,
		LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
		LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
	FROM players
	WHERE current_season <= 2021
)
, with_indicators AS (
	SELECT
		*,
		CASE
			WHEN scoring_class <> previous_scoring_class THEN 1
			WHEN is_active <> previous_is_active THEN 1
			ELSE 0
		END AS change_indicator
	FROM with_previous
)
, with_streaks AS (
	SELECT
		*,
		SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
	FROM with_indicators
)

SELECT
	player_name,
	scoring_class,
	is_active,
	MIN(current_season) AS start_season,
	MAX(current_season) AS end_season,
	2021 AS current_season
FROM with_streaks
GROUP BY
	player_name,
	streak_identifier,
	is_active,
	scoring_class
ORDER BY streak_identifier DESC;

-- The below snippet creates the final table, which combines new data with the SCD table.
WITH last_season_scd AS (
	SELECT *
	FROM players_scd
	WHERE current_season = 2021
	AND end_season = 2021
)
, historical_scd AS (
	SELECT
		player_name,
		scoring_class,
		is_active,
		start_season,
		end_season
	FROM players_scd
	WHERE current_season = 2021
	AND end_season < 2021
)
, this_season_data AS (
	SELECT *
	FROM players
	WHERE current_season = 2022
)
, unchanged_records AS (
	SELECT
		ts.player_name,
		ts.scoring_class,
		ts.is_active,
		ls.start_season,
		ts.current_season AS end_season
	FROM this_season_data ts
	LEFT JOIN last_season_scd ls
		ON ts.player_name = ls.player_name
	WHERE
		ts.scoring_class = ls.scoring_class
		AND ts.is_active = ls.is_active
)
, changed_records AS (
	SELECT
		ts.player_name,
		ts.scoring_class,
		ts.is_active,
		ls.start_season,
		ts.current_season AS end_season,
		UNNEST(ARRAY[
			ROW(
				ls.scoring_class,
				ls.is_active,
				ls.start_season,
				ls.end_season
			)::scd_type,
			ROW(
				ts.scoring_class,
				ts.is_active,
				ts.current_season,
				ts.current_season
			)::scd_type
		]) AS records
	FROM this_season_data ts
	LEFT JOIN last_season_scd ls
		ON ts.player_name = ls.player_name
	WHERE
		(ts.scoring_class <> ls.scoring_class
		OR ts.is_active <> ls.is_active
		OR ls.player_name IS NULL)
)
, unnested_changed_records AS (
	SELECT
		player_name,
		(records::scd_type).scoring_class,
		(records::scd_type).is_active,
		(records::scd_type).start_season,
		(records::scd_type).end_season
	FROM changed_records
)
, new_records AS (
	SELECT
		ts.player_name,
		ts.scoring_class,
		ts.is_active,
		ts.current_season,
		ts.current_season AS end_season
	FROM this_season_data ts
	LEFT JOIN last_season_scd ls
		ON ts.player_name = ls.player_name
	WHERE ls.player_name IS NULL
)

SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;
