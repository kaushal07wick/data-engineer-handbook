-- Populate user_devices_cumulated
INSERT INTO user_devices_cumulated (
    user_id,
    device_id,
    device_activity_datelist,
    snapshot_date
)
SELECT
    user_id,
    device_id,
    ARRAY_AGG(DISTINCT CAST(event_time AS DATE) ORDER BY CAST(event_time AS DATE)) AS device_activity_datelist,
    CURRENT_DATE AS snapshot_date
FROM events
WHERE user_id IS NOT NULL AND device_id IS NOT NULL
GROUP BY user_id, device_id;
