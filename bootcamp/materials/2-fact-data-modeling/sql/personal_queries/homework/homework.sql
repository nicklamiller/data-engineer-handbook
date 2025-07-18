-- Query to deduplicate `game_details`
WITH game_details_deduped AS (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) AS row_num
	FROM game_details
)

SELECT *
FROM game_details_deduped
WHERE row_num = 1;

-- A DDL for an `user_devices_cumulated` table
-- DROP TABLE IF EXISTS user_devices_cumulated;
CREATE TABLE IF NOT EXISTS user_devices_cumulated (
	user_id NUMERIC,
	browser_type TEXT,
	device_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY(user_id, browser_type, date)
);

-- A cumulative query to generate `device_activity_datelist` from `events`
INSERT INTO user_devices_cumulated
WITH deduped_devices AS ( -- `devices` table contains exact duplicates
	SELECT DISTINCT *
	FROM devices
)
, events_deduped AS (
	SELECT DISTINCT *  -- `events` table contains exact duplicates
	FROM events
)
, user_devices AS (
	SELECT	
		e.user_id,
		d.browser_type,
		CAST(e.event_time AS DATE) AS date_active
	FROM events_deduped e
	INNER JOIN deduped_devices d USING(device_id)
	WHERE e.user_id IS NOT NULL
		AND d.browser_type IS NOT NULL
	GROUP BY user_id, browser_type, date_active
)
, yesterday AS (
	SELECT *
	FROM user_devices_cumulated
	WHERE CAST(date AS DATE) = DATE('2023-01-30')
)
, today AS (
	SELECT *
	FROM user_devices
	WHERE date_active = DATE('2023-01-31')
)


SELECT
	COALESCE(t.user_id, y.user_id) AS user_id,
	COALESCE(t.browser_type, y.browser_type) AS browser_type,
	CASE
		WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL THEN y.device_activity_datelist
		ELSE ARRAY[t.date_active] || y.device_activity_datelist
	END AS device_activity_datelist,
	COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y USING(user_id, browser_type);


SELECT *
FROM user_devices_cumulated
ORDER BY date DESC;

-- Convert the `device_activity_datelist` column into a `datelist_int` column
WITH user_browser_type AS (
	SELECT *
	FROM user_devices_cumulated
	WHERE date = DATE('2023-01-31')
)
, series AS (
	SELECT *
	FROM generate_series(
		DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day'
	) AS series_date
)
, placeholder_ints AS (
	SELECT
		CASE
			WHEN device_activity_datelist @> ARRAY[DATE(series_date)]
				THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
			ELSE 0
		END AS placeholder_int_value,
		*
	FROM user_browser_type
	CROSS JOIN series
)

SELECT
	user_id,
	browser_type,
	CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int,
	MAX(device_activity_datelist) AS device_activity_datelist
FROM placeholder_ints
GROUP BY user_id, browser_type;

-- DDL for `hosts_cumulated` table
DROP TABLE IF EXISTS hosts_cumulated;
CREATE TABLE IF NOT EXISTS hosts_cumulated (
	host TEXT,
	host_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY(host, date)
);

-- The incremental query to generate `host_activity_datelist`
INSERT INTO hosts_cumulated
WITH events_deduped AS (
	SELECT DISTINCT *  -- `events` table contains exact duplicates
	FROM events
)
, yesterday AS (
	SELECT *
	FROM hosts_cumulated
	WHERE date = DATE('2023-01-30')
)
, today AS (
	SELECT DISTINCT
		host,
		CAST(event_time AS DATE) AS date_active
	FROM events_deduped
	WHERE
		CAST(event_time AS DATE) = DATE('2023-01-31')
)


SELECT
	COALESCE(t.host, y.host) AS host,
	CASE
		WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL THEN y.host_activity_datelist
		ELSE ARRAY[t.date_active] || y.host_activity_datelist
	END AS host_activity_datelist,
	COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y USING(host);

SELECT *
FROM hosts_cumulated;

-- A monthly, reduced fact table DDL `host_activity_reduced`
DROP TABLE IF EXISTS host_activity_reduced;
CREATE TABLE IF NOT EXISTS host_activity_reduced (
	host TEXT,
	month_start DATE,
	hit_array INTEGER[],
	unique_visitors_array INTEGER[],
	PRIMARY KEY(host, month_start)
);

-- A monthly, reduced fact table DDL `host_activity_reduced`/an incremental query that loads `host_activity_reduced` day-by-day
INSERT INTO host_activity_reduced
WITH events_deduped AS (
	SELECT DISTINCT *  -- `events` table contains exact duplicates
	FROM events
)
, daily_aggregate AS (
	SELECT
		host,
		CAST(event_time AS DATE) AS date,
		COUNT(1) AS hits,
		COUNT(DISTINCT user_id) AS unique_visitors
	FROM events_deduped
	WHERE CAST(event_time AS DATE) = DATE('2023-01-31')
	GROUP BY host, CAST(event_time AS DATE)
)
, yesterday_array AS (
	SELECT *
	FROM host_activity_reduced
	WHERE DATE(month_start) = DATE('2023-01-01')
)

SELECT
	COALESCE(da.host, ya.host) AS host,
	COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
	CASE
		WHEN ya.hit_array IS NOT NULL
			THEN ya.hit_array || ARRAY[COALESCE(da.hits, 0)]
		WHEN ya.hit_array IS NULL
			THEN ARRAY_FILL(
				0,
				ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', da.date)), 0)]
			) || ARRAY[COALESCE(da.hits, 0)]
	END AS hit_array,
	CASE
		WHEN ya.unique_visitors_array IS NOT NULL
			THEN ya.unique_visitors_array || ARRAY[COALESCE(da.unique_visitors, 0)]
		WHEN ya.unique_visitors_array IS NULL
			THEN ARRAY_FILL(
				0,
				ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', da.date)), 0)]
			) || ARRAY[COALESCE(da.unique_visitors, 0)]
	END AS unique_visitors_array
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya USING(host)
ON CONFLICT(host, month_start)
DO UPDATE SET
	hit_array = EXCLUDED.hit_array,
	unique_visitors_array = EXCLUDED.unique_visitors_array;

SELECT *
FROM host_activity_reduced;
