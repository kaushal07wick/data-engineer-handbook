-- 6. Populate hosts_cumulated
INSERT INTO hosts_cumulated
SELECT
    host,
    ARRAY_AGG(DISTINCT DATE(CAST(event_time AS TIMESTAMP)) ORDER BY DATE(CAST(event_time AS TIMESTAMP))) AS host_activity_datelist,
    DATE '2023-01-31' AS snapshot_date
FROM events
GROUP BY host;

