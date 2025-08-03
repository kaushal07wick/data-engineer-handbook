-- 4. Convert device_activity_datelist to datelist_int
WITH users AS (
    SELECT * FROM user_devices_cumulated
    WHERE snapshot_date = DATE '2023-01-31'
),
series AS (
    SELECT DATE(generate_series(DATE '2023-01-01', DATE '2023-01-31', INTERVAL '1 day')) AS series_date
),
placeholder_ints AS (
    SELECT
        u.user_id,
        u.device_id,
        CASE
            WHEN u.device_activity_datelist @> ARRAY[s.series_date]
            THEN CAST(POW(2, 31 - EXTRACT(DAY FROM s.series_date)::int + 1) AS BIGINT)
            ELSE 0
        END AS placeholder_int_value
    FROM users u
    CROSS JOIN series s
)
SELECT
    user_id,
    device_id,
    BIT_OR(placeholder_int_value)::bit(32) AS datelist_int
FROM placeholder_ints
GROUP BY user_id, device_id