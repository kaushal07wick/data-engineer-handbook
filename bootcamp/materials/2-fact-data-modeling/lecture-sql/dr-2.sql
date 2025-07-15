SELECT * 
FROM users_cumulated
WHERE date = DATE('2023-01-31');

SELECT * FROM generate_series(DATE('2023-01-02'), DATE('2023-01-31'), INTERVAL '1 DAY');
