-- 5. hosts_cumulated DDL
CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    snapshot_date DATE,
    PRIMARY KEY (host, snapshot_date)
);