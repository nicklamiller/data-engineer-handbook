--------- A query that does state change tracking for `players` ---------
-- DDL
-- DROP TABLE IF EXISTS players_transitions; -- Only run this line for initialization/debugging
CREATE TABLE IF NOT EXISTS players_transitions (
	player_name TEXT NOT NULL,
	first_active_season INTEGER,
	last_active_season INTEGER,
	player_state TEXT,
	seasons_active INTEGER[],
	season INTEGER,
	PRIMARY KEY(player_name, season)
);

INSERT INTO players_transitions
-- incremental query, iterate through seasons
WITH yesterday AS (
	SELECT *
	FROM players_transitions
	WHERE season = 2021
)
, today AS (
	SELECT *
	FROM player_seasons
	WHERE season = 2022
		AND player_name IS NOT NULL	
)


SELECT
	COALESCE(t.player_name, y.player_name) AS player_name,
	COALESCE(y.first_active_season, t.season) AS first_active_season,
	COALESCE(t.season, y.first_active_season) AS last_active_season,
	CASE
		WHEN y.player_name IS NULL AND t.season IS NOT NULL THEN 'New'
		WHEN y.last_active_season = t.season - 1 THEN 'Continued Playing'
		WHEN y.last_active_season < t.season - 1 THEN 'Returned from Retirement'
		WHEN t.season IS NULL AND y.last_active_season = y.season THEN 'Retired'
		ELSE 'Stayed Retired'
	END AS player_state,
	COALESCE(
		y.seasons_active, ARRAY[]::INTEGER[]
	) || CASE
		WHEN t.player_name IS NOT NULL THEN ARRAY[t.season]
		ELSE ARRAY[]::INTEGER[]
	END AS seasons_active,
	COALESCE(t.season, y.season + 1) AS season
FROM today t
FULL OUTER JOIN yesterday y	USING(player_name);

--------- A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data ---------
-- '(overall)' below reflects the entire group for that dimension e.g. '(overall)' for player_name means
-- aggregating metrics across all players
WITH aggregated AS (
	SELECT
		COALESCE(gd.player_name, '(overall)') AS player_name,
		COALESCE(gd.team_abbreviation::TEXT, '(overall)') AS team_id,
		COALESCE(g.season::TEXT, '(overall)') AS season,
		SUM(gd.pts) AS total_points,
		COUNT(DISTINCT
			CASE
				WHEN gd.team_id = g.home_team_id AND g.home_team_wins = 1 THEN g.game_id
				WHEN gd.team_id = g.visitor_team_id AND g.home_team_wins = 0 THEN g.game_id
			END
		) AS total_games_won
	FROM game_details gd
	JOIN games g USING(game_id)
	GROUP BY GROUPING SETS (
		(gd.player_name, gd.team_abbreviation),
		(gd.player_name, g.season),
		(gd.team_abbreviation)
	)
)
-- Who scored the most points playing for one team?
, most_points_one_team AS (
	SELECT
	    player_name,
	    team_id,
	    total_points
	FROM aggregated
	WHERE
	    player_name <> '(overall)'
	    AND team_id <> '(overall)'
	    AND season = '(overall)'
	ORDER BY total_points DESC
	LIMIT 1
)
, most_points_one_season AS (
-- Who scored the most points in one season?
	SELECT
	    player_name,
	    season,
	    total_points
	FROM aggregated
	WHERE
	    player_name <> '(overall)'
	    AND season <> '(overall)'
	    AND team_id = '(overall)'
	ORDER BY total_points DESC
	LIMIT 1
)
, most_team_wins AS (
-- Which team has won the most games?
	SELECT
	    team_id,
	    total_games_won
	FROM aggregated
	WHERE
	    team_id <> '(overall)'
	    AND player_name = '(overall)'
	    AND season = '(overall)'
	ORDER BY total_games_won DESC
	LIMIT 1
)

SELECT *
FROM most_team_wins -- replace this with desired CTE above to answer specific questions

--------- A query that uses window functions on `game_details` ---------
-- What is the most games a team has won in a 90 game stretch? 
WITH team_games AS (
    SELECT
        g.game_date_est,
        g.team_id_home AS team_id,
        t.abbreviation,
        g.home_team_wins AS win
    FROM games g
    JOIN teams t ON g.team_id_home = t.team_id
    UNION ALL
    SELECT
        g.game_date_est,
        g.team_id_away AS team_id,
        t.abbreviation,
        CASE WHEN g.home_team_wins = 0 THEN 1 ELSE 0 END AS win
    FROM games g
    JOIN teams t ON g.team_id_away = t.team_id
)
, rolling_wins AS (
	SELECT
		team_id,
		abbreviation,
		game_date_est,
		SUM(win) OVER(
			PARTITION BY team_id
			ORDER BY game_date_est
			ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
		) AS wins_in_last_90_games
	FROM team_games
)


SELECT
	team_id,
	abbreviation,
    MAX(wins_in_last_90_games) AS max_wins_in_90_game_stretch
FROM rolling_wins
GROUP BY team_id, abbreviation
ORDER BY max_wins_in_90_game_stretch DESC
LIMIT 1;


-- How many games in a row did LeBron James score over 10 points a game?
WITH over_10 AS (
	SELECT
		player_name,
		game_date_est,
		CASE WHEN pts > 10 THEN 1 ELSE 0 END AS scored_over_10
	FROM game_details gd
	JOIN games g USING(game_id)
)
, streaks AS (
	SELECT
		player_name,
		scored_over_10,
		game_date_est,
		-- This row number is sequential over all of a player's games.
		ROW_NUMBER() OVER(PARTITION BY player_name ORDER BY game_date_est) AS overall_game_number,
		-- This row number is sequential but RESETS for each group (over 10 pts vs. not).
		ROW_NUMBER() OVER(PARTITION BY player_name, scored_over_10 ORDER BY game_date_est) AS group_game_number,
		-- The difference between these two numbers will be constant for any consecutive
        -- sequence of games where `scored_over_10` has the same value.
		ROW_NUMBER() OVER(PARTITION BY player_name ORDER BY game_date_est) -
			ROW_NUMBER() OVER(PARTITION BY player_name, scored_over_10 ORDER BY game_date_est) AS streak_id
	FROM over_10
)
, streak_lengths AS (
	SELECT
		player_name,
		streak_id,
		scored_over_10,
		COUNT(*) AS streak_length
	FROM streaks
	GROUP BY
		player_name,
		streak_id,
		scored_over_10
)

SELECT
	player_name,
	MAX(streak_length) AS longest_streak_over_10_points
FROM streak_lengths
WHERE scored_over_10 = 1
	AND player_name = 'LeBron James'
GROUP BY player_name
