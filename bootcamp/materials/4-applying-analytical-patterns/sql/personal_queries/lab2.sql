WITH combined AS (
	SELECT
		COALESCE(d.os_type, 'N/A') AS os_type,
		COALESCE(d.browser_type, 'N/A') AS browser_type,
		e.*,
		CASE
			WHEN referrer LIKE '%zachwilson%' THEN 'On Site'
			WHEN referrer LIKE '%eczachly%' THEN 'On Site'
			WHEN referrer LIKE '%datanegineer.io%' THEN 'On Site'
			WHEN referrer LIKE '%t.co%' THEN 'Twitter'
			WHEN referrer LIKE '%linkedin%' THEN 'LinkedIn'
			WHEN referrer LIKE '%instagram%' THEN 'Instagram'
			WHEN referrer IS NULL THEN 'Direct'
			ELSE 'Other'
		END AS referrer_mapped
	FROM events e
	JOIN devices d USING(device_id)
)
, grouping_table AS ( -- This CTE is just used to practice grouping sets/rollups
	SELECT
		COALESCE(referrer_mapped, '(overall)') AS referrer,
		COALESCE(browser_type, '(overall)') AS browser_type,
		COALESCE(os_type, '(overall)') AS os_type,
		COUNT(1) AS number_of_site_hits,
		COUNT(CASE WHEN url = '/signup' THEN 1 END) AS number_of_signup_visits,
		COUNT(CASE WHEN url = '/contact' THEN 1 END) AS number_of_contact_visits,
		COUNT(CASE WHEN url = '/login' THEN 1 END) AS number_of_login_visits,
		CAST(
			COUNT(CASE WHEN url = '/signup' THEN 1 END) AS REAL
		) / COUNT(1) AS pct_visited_signup
	FROM combined
	GROUP BY GROUPING SETS (
		(referrer_mapped),
		(os_type),
		(browser_type),
		(referrer_mapped),
		()
	)
	-- GROUP BY ROLLUP(referrer_mapped, browser_type, os_type)
	HAVING COUNT(1) > 100
	ORDER BY CAST(
		COUNT(CASE WHEN url = '/signup' THEN 1 END) AS REAL
	) / COUNT(1) DESC
)
, aggregated AS (
	SELECT
		c1.user_id,
		c1.url AS to_url,
		c2.url AS from_url,
		MIN(
			CAST(c1.event_time AS TIMESTAMP) - CAST(c2.event_time AS TIMESTAMP)
		) AS duration
	FROM combined c1
	JOIN combined c2
		ON c1.user_id = c2.user_id
		AND DATE(c1.event_time) = DATE(c2.event_time)
		AND c1.event_time > c2.event_time
	GROUP BY c1.user_id, c1.url, c2.url
)

SELECT
	to_url,
	from_url,
	COUNT(1) AS num_of_users,
	MIN(duration) AS min_duration,
	MAX(duration) AS max_duration,
	AVG(duration) AS avg_duration
FROM aggregated
GROUP BY to_url, from_url
HAVING COUNT(1) > 100
LIMIT 100
