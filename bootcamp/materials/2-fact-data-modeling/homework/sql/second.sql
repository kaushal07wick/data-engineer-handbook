-- 2. user_devices_cumulated DDL
CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    device_id TEXT,
    device_activity_datelist DATE[],
    snapshot_date DATE,
    PRIMARY KEY (user_id, device_id, snapshot_date)
);