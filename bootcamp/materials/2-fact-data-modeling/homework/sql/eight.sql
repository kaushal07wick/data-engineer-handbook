-- 8. Incrementally insert into host_activity_reduced
WITH daily_stats AS (
    SELECT
        host,
        DATE(CAST(event_time AS TIMESTAMP)) AS day,
        COUNT(*) AS hits,
        COUNT(DISTINCT user_id) AS unique_visitors
    FROM events
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE '2023-01-01'  -- Replace with your target date
    GROUP BY host, DATE(CAST(event_time AS TIMESTAMP))
),

existing AS (
    SELECT * 
    FROM host_activity_reduced
    WHERE month = DATE_TRUNC('month', DATE '2023-01-01')::DATE
),

upserts AS (
    SELECT 
        COALESCE(e.host, d.host) AS host,
        DATE_TRUNC('month', DATE '2023-01-01')::DATE AS month,

        CASE 
            WHEN e.hit_array IS NOT NULL THEN 
                e.hit_array || ARRAY[COALESCE(d.hits, 0)]
            ELSE 
                array_fill(0, ARRAY[EXTRACT(DAY FROM DATE '2023-01-01')::int - 1]) 
                || ARRAY[COALESCE(d.hits, 0)]
        END AS hit_array,

        CASE 
            WHEN e.unique_visitors_array IS NOT NULL THEN 
                e.unique_visitors_array || ARRAY[COALESCE(d.unique_visitors, 0)]
            ELSE 
                array_fill(0, ARRAY[EXTRACT(DAY FROM DATE '2023-01-01')::int - 1]) 
                || ARRAY[COALESCE(d.unique_visitors, 0)]
        END AS unique_visitors_array
    FROM daily_stats d
    FULL OUTER JOIN existing e USING (host)
) INSERT INTO host_activity_reduced (
    host, month, hit_array, unique_visitors_array
)
SELECT 
    host, month, hit_array, unique_visitors_array 
FROM upserts
ON CONFLICT (month, host) DO UPDATE 
SET 
    hit_array = EXCLUDED.hit_array,
    unique_visitors_array = EXCLUDED.unique_visitors_array;
