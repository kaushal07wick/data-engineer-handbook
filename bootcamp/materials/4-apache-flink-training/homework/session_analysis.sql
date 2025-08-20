-- Average number of events per session on Tech Creator
SELECT AVG(event_count) AS avg_events_per_session
FROM processed_sessions
WHERE host = 'techcreator.com';

-- Compare average session events across all hosts
SELECT host, AVG(event_count) AS avg_events_per_session
FROM processed_sessions
GROUP BY host;
