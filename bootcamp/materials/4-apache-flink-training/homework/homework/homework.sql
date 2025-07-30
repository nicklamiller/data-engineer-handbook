-- DDL - run this before starting `session_job.py`
CREATE TABLE IF NOT EXISTS processed_events_session_aggregated (
	event_hour TIMESTAMP(3),
	event_hour_end TIMESTAMP(3),
	ip VARCHAR,
	host VARCHAR,
	num_hits BIGINT
);

-- Ensure table is being populated
SELECT *
FROM processed_events_session_aggregated
ORDER BY ip DESC;

---- Analytics on sessions ----
-- What is the average number of web events of a session from a user on Tech Creator?
SELECT
	host,
	AVG(num_hits) AS avg_number_web_events
FROM processed_events_session_aggregated
WHERE host LIKE '%techcreator%'
GROUP BY host;

-- Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
SELECT
	host,
	COUNT(DISTINCT ip) AS number_distinct_users,
	COUNT(1) AS number_sessions,
	SUM(num_hits) AS total_number_web_events,
	AVG(num_hits) AS avg_number_web_events,
	MIN(num_hits) AS min_number_web_events,
	MAX(num_hits) AS max_number_web_events,
	PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY num_hits) AS median_number_web_events
FROM processed_events_session_aggregated
GROUP BY host;
