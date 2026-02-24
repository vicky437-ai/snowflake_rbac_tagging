/*
================================================================================
CDC DATA PRESERVATION - INFRASTRUCTURE SETUP
================================================================================
Purpose      : Create audit logging table and monitoring views
Version      : 1.0 (Production)
Last Updated : 2026-02-23
================================================================================

PREREQUISITES:
- Execute 01_SET_PARAMETERS.sql first
- Target database and schema must exist

================================================================================
*/

-- =============================================================================
-- USE PARAMETERS FROM SESSION
-- =============================================================================
USE DATABASE IDENTIFIER($TARGET_DATABASE);
USE SCHEMA IDENTIFIER($TARGET_SCHEMA);
USE WAREHOUSE IDENTIFIER($CDC_WAREHOUSE);

-- =============================================================================
-- CDC PROCESSING AUDIT LOG TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS CDC_PROCESSING_LOG (
    LOG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    BATCH_ID VARCHAR(100) NOT NULL,
    TABLE_NAME VARCHAR(100) NOT NULL,
    SOURCE_DATABASE VARCHAR(100),
    SOURCE_SCHEMA VARCHAR(100),
    TARGET_DATABASE VARCHAR(100),
    TARGET_SCHEMA VARCHAR(100),
    ROWS_PROCESSED NUMBER DEFAULT 0,
    ROWS_INSERTED NUMBER DEFAULT 0,
    ROWS_UPDATED NUMBER DEFAULT 0,
    ROWS_DELETED NUMBER DEFAULT 0,
    PROCESSING_START TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESSING_END TIMESTAMP_NTZ,
    DURATION_SECONDS NUMBER,
    STATUS VARCHAR(50) NOT NULL,  -- SUCCESS, ERROR, NO_DATA, STREAM_STALE_RECOVERED
    ERROR_MESSAGE VARCHAR(4000),
    ERROR_CODE VARCHAR(50),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Audit log for CDC data preservation processing. Tracks all batch executions, row counts, and errors.'
DATA_RETENTION_TIME_IN_DAYS = 90;

-- Create index-like clustering for common query patterns
ALTER TABLE CDC_PROCESSING_LOG CLUSTER BY (PROCESSING_START, TABLE_NAME);

-- =============================================================================
-- CDC STREAM STATUS VIEW
-- =============================================================================
CREATE OR REPLACE VIEW CDC_STREAM_STATUS AS
SELECT 
    STREAM_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    TABLE_NAME AS SOURCE_TABLE,
    STALE AS IS_STALE,
    STALE_AFTER,
    CREATED,
    OWNER,
    COMMENT,
    CASE 
        WHEN STALE = TRUE THEN 'STALE - NEEDS ATTENTION'
        WHEN STALE_AFTER < DATEADD('day', 3, CURRENT_TIMESTAMP()) THEN 'WARNING - STALE SOON'
        ELSE 'HEALTHY'
    END AS HEALTH_STATUS
FROM TABLE(INFORMATION_SCHEMA.STREAMS())
WHERE STREAM_NAME LIKE '%_HIST_STREAM'
ORDER BY HEALTH_STATUS DESC, STREAM_NAME;

-- =============================================================================
-- CDC TASK STATUS VIEW
-- =============================================================================
CREATE OR REPLACE VIEW CDC_TASK_STATUS AS
SELECT 
    NAME AS TASK_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    STATE,
    SCHEDULE,
    WAREHOUSE,
    LAST_COMMITTED_ON,
    LAST_SUSPENDED_ON,
    CREATED,
    OWNER,
    CASE 
        WHEN STATE = 'suspended' THEN 'SUSPENDED - NOT RUNNING'
        WHEN STATE = 'started' THEN 'RUNNING'
        ELSE STATE
    END AS TASK_STATUS
FROM TABLE(INFORMATION_SCHEMA.TASKS())
WHERE NAME LIKE 'TASK_PROCESS_%'
ORDER BY TASK_STATUS, TASK_NAME;

-- =============================================================================
-- DAILY PROCESSING SUMMARY VIEW
-- =============================================================================
CREATE OR REPLACE VIEW CDC_DAILY_SUMMARY AS
SELECT 
    DATE(PROCESSING_START) AS PROCESSING_DATE,
    TABLE_NAME,
    COUNT(*) AS TOTAL_RUNS,
    SUM(ROWS_PROCESSED) AS TOTAL_ROWS_PROCESSED,
    SUM(ROWS_INSERTED) AS TOTAL_ROWS_INSERTED,
    SUM(ROWS_UPDATED) AS TOTAL_ROWS_UPDATED,
    SUM(ROWS_DELETED) AS TOTAL_ROWS_DELETED,
    AVG(DURATION_SECONDS) AS AVG_DURATION_SECS,
    MAX(DURATION_SECONDS) AS MAX_DURATION_SECS,
    COUNT(CASE WHEN STATUS = 'SUCCESS' THEN 1 END) AS SUCCESS_COUNT,
    COUNT(CASE WHEN STATUS = 'ERROR' THEN 1 END) AS ERROR_COUNT,
    COUNT(CASE WHEN STATUS = 'NO_DATA' THEN 1 END) AS NO_DATA_COUNT,
    COUNT(CASE WHEN STATUS = 'STREAM_STALE_RECOVERED' THEN 1 END) AS STALE_RECOVERY_COUNT
FROM CDC_PROCESSING_LOG
WHERE PROCESSING_START >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- =============================================================================
-- ERROR TRACKING VIEW
-- =============================================================================
CREATE OR REPLACE VIEW CDC_ERROR_LOG AS
SELECT 
    LOG_ID,
    BATCH_ID,
    TABLE_NAME,
    PROCESSING_START,
    PROCESSING_END,
    DURATION_SECONDS,
    STATUS,
    ERROR_CODE,
    ERROR_MESSAGE,
    CREATED_AT
FROM CDC_PROCESSING_LOG
WHERE STATUS IN ('ERROR', 'STREAM_STALE_RECOVERED')
ORDER BY PROCESSING_START DESC;

-- =============================================================================
-- TABLE SYNC STATUS VIEW
-- =============================================================================
CREATE OR REPLACE VIEW CDC_TABLE_SYNC_STATUS AS
SELECT 
    TABLE_NAME,
    MAX(PROCESSING_START) AS LAST_PROCESSED,
    DATEDIFF('minute', MAX(PROCESSING_START), CURRENT_TIMESTAMP()) AS MINUTES_SINCE_LAST_PROCESS,
    SUM(CASE WHEN PROCESSING_START >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) THEN ROWS_PROCESSED ELSE 0 END) AS ROWS_LAST_24H,
    COUNT(CASE WHEN PROCESSING_START >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) AND STATUS = 'ERROR' THEN 1 END) AS ERRORS_LAST_24H,
    CASE 
        WHEN DATEDIFF('minute', MAX(PROCESSING_START), CURRENT_TIMESTAMP()) > 30 THEN 'DELAYED'
        WHEN COUNT(CASE WHEN PROCESSING_START >= DATEADD('hour', -1, CURRENT_TIMESTAMP()) AND STATUS = 'ERROR' THEN 1 END) > 0 THEN 'ERROR'
        ELSE 'HEALTHY'
    END AS SYNC_STATUS
FROM CDC_PROCESSING_LOG
GROUP BY TABLE_NAME
ORDER BY SYNC_STATUS DESC, TABLE_NAME;

-- =============================================================================
-- HELPER PROCEDURE: LOG CDC PROCESSING
-- =============================================================================
CREATE OR REPLACE PROCEDURE LOG_CDC_PROCESSING(
    P_BATCH_ID VARCHAR,
    P_TABLE_NAME VARCHAR,
    P_ROWS_PROCESSED NUMBER,
    P_ROWS_INSERTED NUMBER,
    P_ROWS_UPDATED NUMBER,
    P_ROWS_DELETED NUMBER,
    P_PROCESSING_START TIMESTAMP_NTZ,
    P_STATUS VARCHAR,
    P_ERROR_MESSAGE VARCHAR,
    P_ERROR_CODE VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    INSERT INTO CDC_PROCESSING_LOG (
        BATCH_ID,
        TABLE_NAME,
        SOURCE_DATABASE,
        SOURCE_SCHEMA,
        TARGET_DATABASE,
        TARGET_SCHEMA,
        ROWS_PROCESSED,
        ROWS_INSERTED,
        ROWS_UPDATED,
        ROWS_DELETED,
        PROCESSING_START,
        PROCESSING_END,
        DURATION_SECONDS,
        STATUS,
        ERROR_MESSAGE,
        ERROR_CODE
    )
    VALUES (
        P_BATCH_ID,
        P_TABLE_NAME,
        $SOURCE_DATABASE,
        $SOURCE_SCHEMA,
        $TARGET_DATABASE,
        $TARGET_SCHEMA,
        P_ROWS_PROCESSED,
        P_ROWS_INSERTED,
        P_ROWS_UPDATED,
        P_ROWS_DELETED,
        P_PROCESSING_START,
        CURRENT_TIMESTAMP(),
        DATEDIFF('second', P_PROCESSING_START, CURRENT_TIMESTAMP()),
        P_STATUS,
        P_ERROR_MESSAGE,
        P_ERROR_CODE
    );
    
    RETURN 'LOG_ENTRY_CREATED';
END;
$$;

-- =============================================================================
-- VERIFICATION
-- =============================================================================
SELECT 'CDC_PROCESSING_LOG' AS OBJECT_NAME, 'TABLE' AS OBJECT_TYPE, 'CREATED' AS STATUS
UNION ALL
SELECT 'CDC_STREAM_STATUS', 'VIEW', 'CREATED'
UNION ALL
SELECT 'CDC_TASK_STATUS', 'VIEW', 'CREATED'
UNION ALL
SELECT 'CDC_DAILY_SUMMARY', 'VIEW', 'CREATED'
UNION ALL
SELECT 'CDC_ERROR_LOG', 'VIEW', 'CREATED'
UNION ALL
SELECT 'CDC_TABLE_SYNC_STATUS', 'VIEW', 'CREATED'
UNION ALL
SELECT 'LOG_CDC_PROCESSING', 'PROCEDURE', 'CREATED';

/*
================================================================================
INFRASTRUCTURE OBJECTS CREATED:
================================================================================
1. CDC_PROCESSING_LOG    - Audit table for all CDC processing runs
2. CDC_STREAM_STATUS     - View showing health of all CDC streams
3. CDC_TASK_STATUS       - View showing status of all CDC tasks
4. CDC_DAILY_SUMMARY     - View with daily processing statistics
5. CDC_ERROR_LOG         - View filtering only error records
6. CDC_TABLE_SYNC_STATUS - View showing sync health per table
7. LOG_CDC_PROCESSING    - Helper procedure for audit logging
================================================================================
*/
