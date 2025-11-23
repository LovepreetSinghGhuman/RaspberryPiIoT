-- Create database
CREATE DATABASE PresenceMonitoring;

-- Switch to database
USE PresenceMonitoring;

-- Create main table
CREATE TABLE PresenceLogs (
    id INT IDENTITY(1,1) PRIMARY KEY,
    device_id NVARCHAR(100) NOT NULL,
    presence_status NVARCHAR(20) NOT NULL,
    is_present BIT NOT NULL,
    event_type NVARCHAR(50) NOT NULL,
    event_time DATETIME2 NOT NULL,
    received_time DATETIME2 DEFAULT GETUTCDATE(),
    additional_data NVARCHAR(MAX) NULL
);

-- Create indexes
CREATE INDEX IX_PresenceLogs_EventTime ON PresenceLogs(event_time);
CREATE INDEX IX_PresenceLogs_DeviceId ON PresenceLogs(device_id);
CREATE INDEX IX_PresenceLogs_PresenceStatus ON PresenceLogs(presence_status);

-- Create view for recent activity
CREATE VIEW RecentPresenceActivity AS
SELECT 
    device_id,
    presence_status,
    is_present,
    event_type,
    event_time,
    received_time
FROM PresenceLogs
WHERE event_time > DATEADD(HOUR, -24, GETUTCDATE())
ORDER BY event_time DESC;

-- Create view for presence statistics
CREATE VIEW PresenceStatistics AS
SELECT 
    device_id,
    COUNT(*) as total_events,
    SUM(CASE WHEN is_present = 1 THEN 1 ELSE 0 END) as presence_count,
    SUM(CASE WHEN is_present = 0 THEN 1 ELSE 0 END) as absence_count,
    MIN(event_time) as first_event,
    MAX(event_time) as last_event
FROM PresenceLogs
GROUP BY device_id;