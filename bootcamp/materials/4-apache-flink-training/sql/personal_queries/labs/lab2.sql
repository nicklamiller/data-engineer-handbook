-- DDL
CREATE TABLE IF NOT EXISTS processed_events_aggregated (
	event_hour TIMESTAMP(3),
	host VARCHAR,
	num_hits BIGINT
);

CREATE TABLE IF NOT EXISTS processed_events_aggregated_source (
	event_hour TIMESTAMP(3),
	host VARCHAR,
	referrer VARCHAR,
	num_hits BIGINT
);

SELECT * FROM processed_events_aggregated;
SELECT * FROM processed_events_aggregated_source;
