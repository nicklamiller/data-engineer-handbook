-- Query to deduplicate `game_details`
WITH game_details_deduped AS (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) AS row_num
	FROM game_details
)

SELECT COUNT(*)
FROM game_details_deduped
WHERE row_num = 1;


---- `user_devices_cumulated` table ----
-- A DDL for an `user_devices_cumulated` table
DROP TABLE IF EXISTS user_devices_cumulated;
CREATE TABLE IF NOT EXISTS user_devices_cumulated (
	user_id NUMERIC,
	browser_type TEXT,
	date DATE,
	device_activity_datelist DATE[],
	PRIMARY KEY(user_id, browser_type, date)
)


-- A cumulative query to generate `device_activity_datelist` from `events`
WITH deduped_devices AS ( -- `devices` table contains exact duplicates
	SELECT DISTINCT *
	FROM devices
)
, user_devices AS (
	SELECT	
		e.user_id,
		d.browser_type,
		CAST(e.event_time AS DATE) AS date
	FROM events e
	INNER JOIN deduped_devices d USING(device_id)
	WHERE e.user_id IS NOT NULL
		AND d.browser_type IS NOT NULL
)
, yesterday AS (
	SELECT *
	FROM user_devices_cumulated
	WHERE CAST(date AS DATE) = DATE('2022-12-31')
)
, today AS (
	SELECT *
	FROM user_devices
	WHERE date = DATE('2023-01-01')
)

SELECT *
FROM today
