/*
================================================================================
CDC PIPELINE MONITORING AND OBSERVABILITY FRAMEWORK - VERSION 4.1
================================================================================
Version:        4.1.0 (BUG FIX - 2026-03-25)
Created:        2026-02-25
Updated:        2026-03-25
Environment:    D_RAW.SADB -> D_BRONZE.SADB CDC Pipeline

CHANGES FROM V4.0:
    1. BUG FIX (CRITICAL): SP_CAPTURE_STREAM_HEALTH rewritten:
       - INFORMATION_SCHEMA.STREAMS does NOT exist in Snowflake
       - Replaced with SHOW STREAMS IN SCHEMA + TABLE(RESULT_SCAN()) [real-time]
       - Single SHOW STREAMS call outside loop (1 call, not 22)
       - Stored LAST_QUERY_ID() in variable to prevent overwrite in loop
       - Proper quoted column refs: "stale", "stale_after", "name"
    2. BUG FIX (SECURITY): Prerequisite grants hardened:
       - Replaced GRANT ALL ON SCHEMA with USAGE + CREATE TABLE/VIEW/PROCEDURE/TASK
       - Replaced GRANT ALL ON objects with specific SELECT/INSERT/UPDATE/DELETE
       - Added SECURITY REVIEW note on IMPORTED PRIVILEGES exposure
       - Removed stale INFORMATION_SCHEMA.STREAMS grant comments

CHANGES FROM V3.0 (carried forward):
    1. CDC_EXECUTION_LOG aligned to v2 production SP schema:
       - TABLE_NAME VARCHAR(256), BATCH_ID NOT NULL
       - START_TIME / END_TIME (replaces EXECUTION_START_TMS / EXECUTION_END_TMS)
       - Removed ROWS_FILTERED, EXECUTION_DURATION_SEC, SOURCE_STREAM_LAG_SEC
    2. Task naming: TASK_SP_PROCESS_{TABLE_NAME} (was TASK_PROCESS_{TABLE_NAME})
    3. SP_CAPTURE_TASK_HEALTH rewritten:
       - Uses TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) for real-time (0 latency)
       - Replaces SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY (45-min latency)
    4. SP_CAPTURE_DATA_QUALITY_METRICS improved:
       - Percentage-based DQ thresholds (>=99% HEALTHY, >=95% WARNING, <95% CRITICAL)
       - Replaces hardcoded absolute row diff thresholds (10/100)
    5. VW_TASK_EXECUTION_HISTORY updated to INFORMATION_SCHEMA for real-time

PREREQUISITE: Run MONITORING_OBSERVABILITY_PREREQUISITE_GRANTS.sql first!

Tables Monitored (22):
    1.  EQPMNT_AAR_BASE               12. TRAIN_PLAN
    2.  EQPMV_EQPMT_EVENT_TYPE        13. TRAIN_PLAN_EVENT
    3.  EQPMV_RFEQP_MVMNT_EVENT       14. TRAIN_PLAN_LEG
    4.  LCMTV_EMIS                     15. TRAIN_TYPE
    5.  LCMTV_MVMNT_EVENT             16. TRAIN_KIND
    6.  STNWYB_MSG_DN                  17. TRKFCG_FIXED_PLANT_ASSET
    7.  TRAIN_CNST_DTL_RAIL_EQPT      18. TRKFCG_FXPLA_TRACK_LCTN_DN
    8.  TRAIN_CNST_SMRY                19. TRKFCG_SBDVSN
    9.  TRAIN_OPTRN                    20. TRKFCG_SRVC_AREA
    10. TRAIN_OPTRN_EVENT              21. TRKFCG_TRACK_SGMNT_DN
    11. TRAIN_OPTRN_LEG                22. TRKFC_TRSTN
================================================================================
*/

-- =============================================================================
-- STEP 1: CREATE MONITORING SCHEMA
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS D_BRONZE.MONITORING
    DATA_RETENTION_TIME_IN_DAYS = 90
    COMMENT = 'CDC Pipeline Monitoring and Observability Schema v4.0';

-- =============================================================================
-- STEP 2: CREATE MONITORING CONFIGURATION TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS D_BRONZE.MONITORING.CDC_PIPELINE_CONFIG (
    CONFIG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    TABLE_NAME VARCHAR(100) NOT NULL,
    SOURCE_SCHEMA VARCHAR(100) DEFAULT 'D_RAW.SADB',
    TARGET_SCHEMA VARCHAR(100) DEFAULT 'D_BRONZE.SADB',
    STREAM_NAME VARCHAR(200) NOT NULL,
    PROCEDURE_NAME VARCHAR(200) NOT NULL,
    TASK_NAME VARCHAR(200) NOT NULL,
    PRIMARY_KEY_COLUMNS VARCHAR(500) NOT NULL,
    EXPECTED_COLUMNS NUMBER NOT NULL,
    SCHEDULE_MINUTES NUMBER DEFAULT 5,
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    ALERT_THRESHOLD_MINUTES NUMBER DEFAULT 30,
    FILTER_VALUES VARCHAR(500) DEFAULT 'TSDPRG,EMEPRG',
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT UQ_CDC_PIPELINE_CONFIG_TABLE UNIQUE (TABLE_NAME)
);

-- =============================================================================
-- STEP 2b: UPSERT 22 PIPELINE CONFIGURATIONS (TASK_SP_PROCESS_ naming)
-- =============================================================================
MERGE INTO D_BRONZE.MONITORING.CDC_PIPELINE_CONFIG tgt
USING (
    SELECT * FROM (VALUES
    ('EQPMNT_AAR_BASE',           'D_RAW.SADB.EQPMNT_AAR_BASE_BASE_HIST_STREAM',            'D_RAW.SADB.SP_PROCESS_EQPMNT_AAR_BASE',            'D_RAW.SADB.TASK_SP_PROCESS_EQPMNT_AAR_BASE',            'EQPMNT_ID',                                              88),
    ('EQPMV_EQPMT_EVENT_TYPE',    'D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE_HIST_STREAM',    'D_RAW.SADB.SP_PROCESS_EQPMV_EQPMT_EVENT_TYPE',    'D_RAW.SADB.TASK_SP_PROCESS_EQPMV_EQPMT_EVENT_TYPE',    'EQPMT_EVENT_TYPE_ID',                                     31),
    ('EQPMV_RFEQP_MVMNT_EVENT',   'D_RAW.SADB.EQPMV_RFEQP_MVMNT_EVENT_BASE_HIST_STREAM',  'D_RAW.SADB.SP_PROCESS_EQPMV_RFEQP_MVMNT_EVENT',  'D_RAW.SADB.TASK_SP_PROCESS_EQPMV_RFEQP_MVMNT_EVENT',  'EVENT_ID',                                                 97),
    ('LCMTV_EMIS',                 'D_RAW.SADB.LCMTV_EMIS_BASE_HIST_STREAM',                'D_RAW.SADB.SP_PROCESS_LCMTV_EMIS',                'D_RAW.SADB.TASK_SP_PROCESS_LCMTV_EMIS',                'MARK_CD,EQPUN_NBR',                                       91),
    ('LCMTV_MVMNT_EVENT',         'D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE_HIST_STREAM',          'D_RAW.SADB.SP_PROCESS_LCMTV_MVMNT_EVENT',          'D_RAW.SADB.TASK_SP_PROCESS_LCMTV_MVMNT_EVENT',          'EVENT_ID',                                                50),
    ('STNWYB_MSG_DN',              'D_RAW.SADB.STNWYB_MSG_DN_BASE_HIST_STREAM',              'D_RAW.SADB.SP_PROCESS_STNWYB_MSG_DN',              'D_RAW.SADB.TASK_SP_PROCESS_STNWYB_MSG_DN',              'STNWYB_MSG_VRSN_ID',                                    137),
    ('TRAIN_CNST_DTL_RAIL_EQPT',   'D_RAW.SADB.TRAIN_CNST_DTL_RAIL_EQPT_BASE_HIST_STREAM',  'D_RAW.SADB.SP_PROCESS_TRAIN_CNST_DTL_RAIL_EQPT',  'D_RAW.SADB.TASK_SP_PROCESS_TRAIN_CNST_DTL_RAIL_EQPT',  'TRAIN_CNST_SMRY_ID,TRAIN_CNST_SMRY_VRSN_NBR,SQNC_NBR',  84),
    ('TRAIN_CNST_SMRY',            'D_RAW.SADB.TRAIN_CNST_SMRY_BASE_HIST_STREAM',            'D_RAW.SADB.SP_PROCESS_TRAIN_CNST_SMRY',            'D_RAW.SADB.TASK_SP_PROCESS_TRAIN_CNST_SMRY',            'TRAIN_CNST_SMRY_ID,TRAIN_CNST_SMRY_VRSN_NBR',            94),
    ('TRAIN_OPTRN',                'D_RAW.SADB.TRAIN_OPTRN_BASE_HIST_STREAM',                'D_RAW.SADB.SP_PROCESS_TRAIN_OPTRN',                'D_RAW.SADB.TASK_SP_PROCESS_TRAIN_OPTRN',                'OPTRN_ID',                                                24),
    ('TRAIN_OPTRN_EVENT',          'D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE_HIST_STREAM',          'D_RAW.SADB.SP_PROCESS_TRAIN_OPTRN_EVENT',          'D_RAW.SADB.TASK_SP_PROCESS_TRAIN_OPTRN_EVENT',          'OPTRN_EVENT_ID',                                          35),
    ('TRAIN_OPTRN_LEG',            'D_RAW.SADB.TRAIN_OPTRN_LEG_BASE_HIST_STREAM',            'D_RAW.SADB.SP_PROCESS_TRAIN_OPTRN_LEG',            'D_RAW.SADB.TASK_SP_PROCESS_TRAIN_OPTRN_LEG',            'OPTRN_LEG_ID',                                            20),
    ('TRAIN_PLAN',                 'D_RAW.SADB.TRAIN_PLAN_BASE_HIST_STREAM',                  'D_RAW.SADB.SP_PROCESS_TRAIN_PLAN',                  'D_RAW.SADB.TASK_SP_PROCESS_TRAIN_PLAN',                  'TRAIN_PLAN_ID',                                           24),
    ('TRAIN_PLAN_EVENT',           'D_RAW.SADB.TRAIN_PLAN_EVENT_BASE_HIST_STREAM',            'D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_EVENT',            'D_RAW.SADB.TASK_SP_PROCESS_TRAIN_PLAN_EVENT',            'TRAIN_PLAN_EVENT_ID',                                     43),
    ('TRAIN_PLAN_LEG',             'D_RAW.SADB.TRAIN_PLAN_LEG_BASE_HIST_STREAM',              'D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_LEG',              'D_RAW.SADB.TASK_SP_PROCESS_TRAIN_PLAN_LEG',              'TRAIN_PLAN_LEG_ID',                                       23),
    ('TRAIN_TYPE',                 'D_RAW.SADB.TRAIN_TYPE_BASE_HIST_STREAM',                  'D_RAW.SADB.SP_PROCESS_TRAIN_TYPE',                  'D_RAW.SADB.TASK_SP_PROCESS_TRAIN_TYPE',                  'TRAIN_TYPE_CD',                                           15),
    ('TRAIN_KIND',                 'D_RAW.SADB.TRAIN_KIND_BASE_HIST_STREAM',                  'D_RAW.SADB.SP_PROCESS_TRAIN_KIND',                  'D_RAW.SADB.TASK_SP_PROCESS_TRAIN_KIND',                  'TRAIN_KIND_CD',                                           17),
    ('TRKFCG_FIXED_PLANT_ASSET',   'D_RAW.SADB.TRKFCG_FIXED_PLANT_ASSET_BASE_HIST_STREAM',  'D_RAW.SADB.SP_PROCESS_TRKFCG_FIXED_PLANT_ASSET',  'D_RAW.SADB.TASK_SP_PROCESS_TRKFCG_FIXED_PLANT_ASSET',  'GRPHC_OBJECT_VRSN_ID',                                    59),
    ('TRKFCG_FXPLA_TRACK_LCTN_DN', 'D_RAW.SADB.TRKFCG_FXPLA_TRACK_LCTN_DN_BASE_HIST_STREAM','D_RAW.SADB.SP_PROCESS_TRKFCG_FXPLA_TRACK_LCTN_DN','D_RAW.SADB.TASK_SP_PROCESS_TRKFCG_FXPLA_TRACK_LCTN_DN','GRPHC_OBJECT_VRSN_ID',                                    63),
    ('TRKFCG_SBDVSN',              'D_RAW.SADB.TRKFCG_SBDVSN_BASE_HIST_STREAM',              'D_RAW.SADB.SP_PROCESS_TRKFCG_SBDVSN',              'D_RAW.SADB.TASK_SP_PROCESS_TRKFCG_SBDVSN',              'GRPHC_OBJECT_VRSN_ID',                                    56),
    ('TRKFCG_SRVC_AREA',           'D_RAW.SADB.TRKFCG_SRVC_AREA_BASE_HIST_STREAM',            'D_RAW.SADB.SP_PROCESS_TRKFCG_SRVC_AREA',            'D_RAW.SADB.TASK_SP_PROCESS_TRKFCG_SRVC_AREA',            'GRPHC_OBJECT_VRSN_ID',                                    32),
    ('TRKFCG_TRACK_SGMNT_DN',      'D_RAW.SADB.TRKFCG_TRACK_SGMNT_DN_BASE_HIST_STREAM',      'D_RAW.SADB.SP_PROCESS_TRKFCG_TRACK_SGMNT_DN',      'D_RAW.SADB.TASK_SP_PROCESS_TRKFCG_TRACK_SGMNT_DN',      'GRPHC_OBJECT_VRSN_ID',                                    65),
    ('TRKFC_TRSTN',                'D_RAW.SADB.TRKFC_TRSTN_BASE_HIST_STREAM',                'D_RAW.SADB.SP_PROCESS_TRKFC_TRSTN',                'D_RAW.SADB.TASK_SP_PROCESS_TRKFC_TRSTN',                'SCAC_CD,FSAC_CD',                                         47)
    ) AS v(TABLE_NAME, STREAM_NAME, PROCEDURE_NAME, TASK_NAME, PRIMARY_KEY_COLUMNS, EXPECTED_COLUMNS)
) src ON tgt.TABLE_NAME = src.TABLE_NAME
WHEN NOT MATCHED THEN INSERT (TABLE_NAME, STREAM_NAME, PROCEDURE_NAME, TASK_NAME, PRIMARY_KEY_COLUMNS, EXPECTED_COLUMNS)
VALUES (src.TABLE_NAME, src.STREAM_NAME, src.PROCEDURE_NAME, src.TASK_NAME, src.PRIMARY_KEY_COLUMNS, src.EXPECTED_COLUMNS)
WHEN MATCHED THEN UPDATE SET
    tgt.STREAM_NAME = src.STREAM_NAME,
    tgt.PROCEDURE_NAME = src.PROCEDURE_NAME,
    tgt.TASK_NAME = src.TASK_NAME,
    tgt.PRIMARY_KEY_COLUMNS = src.PRIMARY_KEY_COLUMNS,
    tgt.EXPECTED_COLUMNS = src.EXPECTED_COLUMNS,
    tgt.UPDATED_AT = CURRENT_TIMESTAMP();

-- Cleanup legacy names from v2.0 (OPTRN -> TRAIN_OPTRN renames)
DELETE FROM D_BRONZE.MONITORING.CDC_PIPELINE_CONFIG
WHERE TABLE_NAME IN ('OPTRN', 'OPTRN_EVENT', 'OPTRN_LEG');

-- =============================================================================
-- STEP 3: CREATE EXECUTION LOG TABLE (v2 Production SP Schema)
-- =============================================================================
-- This schema EXACTLY matches the INSERT statement used in all 22 v2 SPs:
--   INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (
--       TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME,
--       ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED,
--       ERROR_MESSAGE, CREATED_AT
--   )
CREATE TABLE IF NOT EXISTS D_BRONZE.MONITORING.CDC_EXECUTION_LOG (
    LOG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    TABLE_NAME VARCHAR(256) NOT NULL,
    BATCH_ID VARCHAR(100) NOT NULL,
    EXECUTION_STATUS VARCHAR(20) NOT NULL,
    START_TIME TIMESTAMP_NTZ,
    END_TIME TIMESTAMP_NTZ,
    ROWS_PROCESSED NUMBER DEFAULT 0,
    ROWS_INSERTED NUMBER DEFAULT 0,
    ROWS_UPDATED NUMBER DEFAULT 0,
    ROWS_DELETED NUMBER DEFAULT 0,
    ERROR_MESSAGE VARCHAR(4000),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- STEP 4: CREATE STREAM HEALTH SNAPSHOT TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS D_BRONZE.MONITORING.CDC_STREAM_HEALTH_SNAPSHOT (
    SNAPSHOT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    SNAPSHOT_TMS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    TABLE_NAME VARCHAR(100) NOT NULL,
    STREAM_NAME VARCHAR(200) NOT NULL,
    IS_STALE BOOLEAN,
    STALE_AFTER TIMESTAMP_NTZ,
    HOURS_UNTIL_STALE NUMBER(10,2),
    HAS_PENDING_DATA BOOLEAN,
    STREAM_STATUS VARCHAR(20),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- STEP 5: CREATE TASK HEALTH SNAPSHOT TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS D_BRONZE.MONITORING.CDC_TASK_HEALTH_SNAPSHOT (
    SNAPSHOT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    SNAPSHOT_TMS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    TABLE_NAME VARCHAR(100) NOT NULL,
    TASK_NAME VARCHAR(200) NOT NULL,
    TASK_STATE VARCHAR(20),
    LAST_RUN_TMS TIMESTAMP_NTZ,
    NEXT_SCHEDULED_TMS TIMESTAMP_NTZ,
    LAST_RUN_STATUS VARCHAR(20),
    LAST_RUN_DURATION_SEC NUMBER,
    LAST_RETURN_VALUE VARCHAR(4000),
    MINUTES_SINCE_LAST_RUN NUMBER,
    IS_HEALTHY BOOLEAN,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- STEP 6: CREATE DATA QUALITY METRICS TABLE (EXCLUDED - FUTURE PHASE)
-- =============================================================================
-- NOTE: DQ functionality excluded from v4.1 deployment per decision on 2026-03-25.
-- Table DDL retained for future enablement. Uncomment when ready to activate.
-- =============================================================================
/*
CREATE TABLE IF NOT EXISTS D_BRONZE.MONITORING.CDC_DATA_QUALITY_METRICS (
    METRIC_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    SNAPSHOT_TMS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    TABLE_NAME VARCHAR(100) NOT NULL,
    SOURCE_ROW_COUNT NUMBER,
    SOURCE_FILTERED_COUNT NUMBER,
    TARGET_ROW_COUNT NUMBER,
    TARGET_ACTIVE_COUNT NUMBER,
    ROW_COUNT_DIFF NUMBER,
    ROW_COUNT_MATCH_PCT NUMBER(5,2),
    DELETED_RECORDS_COUNT NUMBER,
    LATEST_SOURCE_UPDATE TIMESTAMP_NTZ,
    LATEST_TARGET_UPDATE TIMESTAMP_NTZ,
    UPDATE_LAG_MINUTES NUMBER,
    DATA_QUALITY_STATUS VARCHAR(20),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
*/

-- =============================================================================
-- STEP 7: CREATE ALERT LOG TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS D_BRONZE.MONITORING.CDC_ALERT_LOG (
    ALERT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    ALERT_TMS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ALERT_TYPE VARCHAR(50) NOT NULL,
    ALERT_SEVERITY VARCHAR(20) NOT NULL,
    TABLE_NAME VARCHAR(100),
    ALERT_MESSAGE VARCHAR(4000) NOT NULL,
    ALERT_DETAILS VARIANT,
    IS_ACKNOWLEDGED BOOLEAN DEFAULT FALSE,
    ACKNOWLEDGED_BY VARCHAR(100),
    ACKNOWLEDGED_TMS TIMESTAMP_NTZ,
    RESOLUTION_NOTES VARCHAR(4000),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- STEP 8: CREATE SP - CAPTURE STREAM HEALTH (SHOW STREAMS + RESULT_SCAN)
-- =============================================================================
-- v4.1 FIX: INFORMATION_SCHEMA.STREAMS does NOT exist in Snowflake.
-- Snowflake exposes stream metadata ONLY via:
--   (a) SHOW STREAMS command + TABLE(RESULT_SCAN(LAST_QUERY_ID()))  [real-time]
--   (b) SNOWFLAKE.ACCOUNT_USAGE.STREAMS                            [up to 2hr lag]
-- This SP uses approach (a) for real-time stream health monitoring.
-- Column references use quoted names ("stale", "stale_after") to match
-- the SHOW STREAMS output schema exactly.
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_BRONZE.MONITORING.SP_CAPTURE_STREAM_HEALTH()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_snapshot_tms TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_table_name VARCHAR;
    v_stream_name VARCHAR;
    v_stream_short VARCHAR;
    v_is_stale BOOLEAN;
    v_stale_after TIMESTAMP_NTZ;
    v_hours_until_stale NUMBER;
    v_has_data BOOLEAN;
    v_stream_found BOOLEAN := FALSE;
    v_count NUMBER := 0;
    rs RESULTSET;
    c1 CURSOR FOR 
        SELECT TABLE_NAME, STREAM_NAME 
        FROM D_BRONZE.MONITORING.CDC_PIPELINE_CONFIG 
        WHERE IS_ACTIVE = TRUE;
BEGIN
    -- ================================================================
    -- Run SHOW STREAMS once for the entire schema, then query the
    -- result set per stream. This avoids running SHOW STREAMS 22
    -- times inside the loop.
    -- ================================================================
    SHOW STREAMS IN SCHEMA D_RAW.SADB;
    LET v_query_id VARCHAR := (SELECT LAST_QUERY_ID());

    OPEN c1;
    FOR rec IN c1 DO
        v_table_name := rec.TABLE_NAME;
        v_stream_name := rec.STREAM_NAME;
        v_stream_short := SPLIT_PART(rec.STREAM_NAME, '.', 3);
        v_stream_found := FALSE;
        
        BEGIN
            -- ================================================================
            -- Query RESULT_SCAN from the SHOW STREAMS output.
            -- SHOW STREAMS columns include: "name", "stale", "stale_after"
            -- (lowercase quoted identifiers in RESULT_SCAN output).
            -- ================================================================
            SELECT 
                TRUE,
                "stale"::BOOLEAN,
                "stale_after"::TIMESTAMP_NTZ
            INTO 
                v_stream_found,
                v_is_stale,
                v_stale_after
            FROM TABLE(RESULT_SCAN(:v_query_id))
            WHERE "name" = :v_stream_short
            LIMIT 1;

            -- Calculate hours until stale (NULL-safe)
            v_hours_until_stale := CASE 
                WHEN v_stale_after IS NOT NULL 
                THEN ROUND(TIMESTAMPDIFF(MINUTE, CURRENT_TIMESTAMP(), :v_stale_after) / 60.0, 2)
                ELSE NULL 
            END;

            -- Check for pending data using SYSTEM$STREAM_HAS_DATA
            v_has_data := (SELECT SYSTEM$STREAM_HAS_DATA(:v_stream_name));

            INSERT INTO D_BRONZE.MONITORING.CDC_STREAM_HEALTH_SNAPSHOT 
                (SNAPSHOT_TMS, TABLE_NAME, STREAM_NAME, IS_STALE, STALE_AFTER, 
                 HOURS_UNTIL_STALE, HAS_PENDING_DATA, STREAM_STATUS)
            VALUES 
                (:v_snapshot_tms, :v_table_name, :v_stream_name, :v_is_stale, :v_stale_after,
                 :v_hours_until_stale, :v_has_data, 
                 CASE WHEN :v_is_stale = TRUE THEN 'STALE' ELSE 'ACTIVE' END);
            
            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHER THEN
                INSERT INTO D_BRONZE.MONITORING.CDC_STREAM_HEALTH_SNAPSHOT 
                    (SNAPSHOT_TMS, TABLE_NAME, STREAM_NAME, STREAM_STATUS)
                VALUES 
                    (:v_snapshot_tms, :v_table_name, :v_stream_name, 'ERROR');
        END;
    END FOR;
    CLOSE c1;
    
    RETURN 'Stream health captured for ' || v_count || ' streams at ' || TO_VARCHAR(:v_snapshot_tms);
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- STEP 9: CREATE SP - CAPTURE TASK HEALTH (INFORMATION_SCHEMA - REAL-TIME)
-- =============================================================================
-- v4.0 FIX: Replaced SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY (45-min latency)
-- with TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) for real-time task status.
-- INFORMATION_SCHEMA returns data immediately with no latency.
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_BRONZE.MONITORING.SP_CAPTURE_TASK_HEALTH()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_snapshot_tms TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_count NUMBER := 0;
BEGIN
    INSERT INTO D_BRONZE.MONITORING.CDC_TASK_HEALTH_SNAPSHOT 
        (SNAPSHOT_TMS, TABLE_NAME, TASK_NAME, TASK_STATE, LAST_RUN_TMS, LAST_RUN_STATUS, 
         LAST_RUN_DURATION_SEC, LAST_RETURN_VALUE, MINUTES_SINCE_LAST_RUN, IS_HEALTHY)
    SELECT 
        :v_snapshot_tms,
        c.TABLE_NAME,
        c.TASK_NAME,
        th.STATE,
        th.COMPLETED_TIME,
        th.STATE,
        TIMESTAMPDIFF(SECOND, th.SCHEDULED_TIME, th.COMPLETED_TIME),
        th.RETURN_VALUE,
        TIMESTAMPDIFF(MINUTE, th.COMPLETED_TIME, CURRENT_TIMESTAMP()),
        CASE 
            WHEN th.STATE = 'SUCCEEDED' 
                 AND TIMESTAMPDIFF(MINUTE, th.COMPLETED_TIME, CURRENT_TIMESTAMP()) < c.ALERT_THRESHOLD_MINUTES 
            THEN TRUE
            ELSE FALSE
        END
    FROM D_BRONZE.MONITORING.CDC_PIPELINE_CONFIG c
    LEFT JOIN (
        SELECT 
            DATABASE_NAME, SCHEMA_NAME, NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, RETURN_VALUE,
            ROW_NUMBER() OVER (PARTITION BY DATABASE_NAME, SCHEMA_NAME, NAME ORDER BY COMPLETED_TIME DESC) AS RN
        FROM TABLE(D_RAW.INFORMATION_SCHEMA.TASK_HISTORY(
            SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP()),
            RESULT_LIMIT => 1000
        ))
        WHERE DATABASE_NAME = 'D_RAW' 
          AND SCHEMA_NAME = 'SADB'
    ) th ON th.DATABASE_NAME || '.' || th.SCHEMA_NAME || '.' || th.NAME = c.TASK_NAME
        AND th.RN = 1
    WHERE c.IS_ACTIVE = TRUE;
    
    v_count := SQLROWCOUNT;
    RETURN 'Task health captured for ' || v_count || ' tasks at ' || TO_VARCHAR(:v_snapshot_tms);
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- STEP 10: SP_CAPTURE_DATA_QUALITY_METRICS (EXCLUDED - FUTURE PHASE)
-- =============================================================================
-- NOTE: DQ SP excluded from v4.1 deployment per decision on 2026-03-25.
-- SP code retained for future enablement. Uncomment when ready to activate.
-- =============================================================================
/*
CREATE OR REPLACE PROCEDURE D_BRONZE.MONITORING.SP_CAPTURE_DATA_QUALITY_METRICS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_snapshot_tms TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_table_name VARCHAR;
    v_source_count NUMBER;
    v_source_filtered NUMBER;
    v_target_count NUMBER;
    v_target_active NUMBER;
    v_deleted_count NUMBER;
    v_latest_source TIMESTAMP_NTZ;
    v_latest_target TIMESTAMP_NTZ;
    v_match_pct NUMBER(5,2);
    v_count NUMBER := 0;
    v_sql VARCHAR;
    rs RESULTSET;
    c1 CURSOR FOR 
        SELECT TABLE_NAME, SOURCE_SCHEMA, TARGET_SCHEMA
        FROM D_BRONZE.MONITORING.CDC_PIPELINE_CONFIG 
        WHERE IS_ACTIVE = TRUE;
BEGIN
    OPEN c1;
    FOR rec IN c1 DO
        v_table_name := rec.TABLE_NAME;
        
        BEGIN
            -- Source total count (RESULTSET+CURSOR pattern required for dynamic SQL)
            v_sql := 'SELECT COUNT(*) AS CNT FROM ' || rec.SOURCE_SCHEMA || '.' || v_table_name || '_BASE';
            rs := (EXECUTE IMMEDIATE :v_sql);
            LET c_src CURSOR FOR rs;
            OPEN c_src;
            FETCH c_src INTO v_source_count;
            CLOSE c_src;
            
            -- Source filtered count (excludes TSDPRG/EMEPRG purge records)
            v_sql := 'SELECT COUNT(*) AS CNT FROM ' || rec.SOURCE_SCHEMA || '.' || v_table_name || '_BASE WHERE NVL(SNW_OPERATION_OWNER, '''') NOT IN (''TSDPRG'', ''EMEPRG'')';
            rs := (EXECUTE IMMEDIATE :v_sql);
            LET c_flt CURSOR FOR rs;
            OPEN c_flt;
            FETCH c_flt INTO v_source_filtered;
            CLOSE c_flt;
            
            -- Target total count
            v_sql := 'SELECT COUNT(*) AS CNT FROM ' || rec.TARGET_SCHEMA || '.' || v_table_name;
            rs := (EXECUTE IMMEDIATE :v_sql);
            LET c_tgt CURSOR FOR rs;
            OPEN c_tgt;
            FETCH c_tgt INTO v_target_count;
            CLOSE c_tgt;
            
            -- Target deleted count
            v_sql := 'SELECT COUNT(*) AS CNT FROM ' || rec.TARGET_SCHEMA || '.' || v_table_name || ' WHERE IS_DELETED = TRUE';
            rs := (EXECUTE IMMEDIATE :v_sql);
            LET c_del CURSOR FOR rs;
            OPEN c_del;
            FETCH c_del INTO v_deleted_count;
            CLOSE c_del;
            
            v_target_active := v_target_count - COALESCE(v_deleted_count, 0);
            
            -- Latest source timestamp for lag calculation
            v_sql := 'SELECT MAX(SNW_LAST_REPLICATED) AS MX FROM ' || rec.SOURCE_SCHEMA || '.' || v_table_name || '_BASE';
            rs := (EXECUTE IMMEDIATE :v_sql);
            LET c_ls CURSOR FOR rs;
            OPEN c_ls;
            FETCH c_ls INTO v_latest_source;
            CLOSE c_ls;
            
            -- Latest target timestamp
            v_sql := 'SELECT MAX(RECORD_UPDATED_AT) AS MX FROM ' || rec.TARGET_SCHEMA || '.' || v_table_name;
            rs := (EXECUTE IMMEDIATE :v_sql);
            LET c_lt CURSOR FOR rs;
            OPEN c_lt;
            FETCH c_lt INTO v_latest_target;
            CLOSE c_lt;
            
            -- Calculate match percentage (NULL-safe)
            v_match_pct := CASE 
                WHEN v_source_filtered > 0 
                THEN ROUND(v_target_active / v_source_filtered * 100, 2) 
                ELSE 100.00 
            END;
            
            INSERT INTO D_BRONZE.MONITORING.CDC_DATA_QUALITY_METRICS (
                SNAPSHOT_TMS, TABLE_NAME, SOURCE_ROW_COUNT, SOURCE_FILTERED_COUNT,
                TARGET_ROW_COUNT, TARGET_ACTIVE_COUNT,
                ROW_COUNT_DIFF, ROW_COUNT_MATCH_PCT, DELETED_RECORDS_COUNT,
                LATEST_SOURCE_UPDATE, LATEST_TARGET_UPDATE, UPDATE_LAG_MINUTES,
                DATA_QUALITY_STATUS
            )
            VALUES (
                :v_snapshot_tms, :v_table_name, :v_source_count, :v_source_filtered,
                :v_target_count, :v_target_active,
                :v_target_active - :v_source_filtered,
                :v_match_pct,
                COALESCE(:v_deleted_count, 0),
                :v_latest_source, :v_latest_target,
                TIMESTAMPDIFF(MINUTE, :v_latest_source, :v_latest_target),
                CASE 
                    WHEN :v_match_pct >= 99.00 THEN 'HEALTHY'
                    WHEN :v_match_pct >= 95.00 THEN 'WARNING'
                    ELSE 'CRITICAL'
                END
            );
            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHER THEN
                INSERT INTO D_BRONZE.MONITORING.CDC_DATA_QUALITY_METRICS (
                    SNAPSHOT_TMS, TABLE_NAME, DATA_QUALITY_STATUS
                ) VALUES (:v_snapshot_tms, :v_table_name, 'ERROR');
        END;
    END FOR;
    CLOSE c1;
    
    RETURN 'Data quality metrics captured for ' || v_count || ' tables at ' || TO_VARCHAR(:v_snapshot_tms);
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;
*/

-- =============================================================================
-- STEP 11: CREATE SP - GENERATE ALERTS (DEDUPLICATION + SEVERITY)
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_BRONZE.MONITORING.SP_GENERATE_ALERTS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_alert_count NUMBER := 0;
BEGIN
    -- Alert 1: Stale streams (CRITICAL)
    INSERT INTO D_BRONZE.MONITORING.CDC_ALERT_LOG (ALERT_TYPE, ALERT_SEVERITY, TABLE_NAME, ALERT_MESSAGE, ALERT_DETAILS)
    SELECT 
        'STREAM_STALE', 'CRITICAL', TABLE_NAME,
        'Stream ' || STREAM_NAME || ' is STALE for table ' || TABLE_NAME,
        OBJECT_CONSTRUCT('stream_name', STREAM_NAME, 'snapshot_tms', SNAPSHOT_TMS, 'stale_after', STALE_AFTER)
    FROM D_BRONZE.MONITORING.CDC_STREAM_HEALTH_SNAPSHOT
    WHERE SNAPSHOT_TMS = (SELECT MAX(SNAPSHOT_TMS) FROM D_BRONZE.MONITORING.CDC_STREAM_HEALTH_SNAPSHOT)
      AND STREAM_STATUS = 'STALE'
      AND NOT EXISTS (
          SELECT 1 FROM D_BRONZE.MONITORING.CDC_ALERT_LOG a
          WHERE a.ALERT_TYPE = 'STREAM_STALE' AND a.TABLE_NAME = CDC_STREAM_HEALTH_SNAPSHOT.TABLE_NAME
            AND a.IS_ACKNOWLEDGED = FALSE AND a.CREATED_AT > DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
      );
    v_alert_count := v_alert_count + SQLROWCOUNT;
    
    -- Alert 2: Failed/unhealthy tasks (CRITICAL)
    INSERT INTO D_BRONZE.MONITORING.CDC_ALERT_LOG (ALERT_TYPE, ALERT_SEVERITY, TABLE_NAME, ALERT_MESSAGE, ALERT_DETAILS)
    SELECT 
        'TASK_FAILURE', 'CRITICAL', TABLE_NAME,
        'Task ' || TASK_NAME || ' failed or unhealthy. Last return: ' || COALESCE(LAST_RETURN_VALUE, 'N/A'),
        OBJECT_CONSTRUCT('task_name', TASK_NAME, 'task_state', TASK_STATE, 'last_run', LAST_RUN_TMS, 'return_value', LAST_RETURN_VALUE)
    FROM D_BRONZE.MONITORING.CDC_TASK_HEALTH_SNAPSHOT
    WHERE SNAPSHOT_TMS = (SELECT MAX(SNAPSHOT_TMS) FROM D_BRONZE.MONITORING.CDC_TASK_HEALTH_SNAPSHOT)
      AND IS_HEALTHY = FALSE
      AND NOT EXISTS (
          SELECT 1 FROM D_BRONZE.MONITORING.CDC_ALERT_LOG a
          WHERE a.ALERT_TYPE = 'TASK_FAILURE' AND a.TABLE_NAME = CDC_TASK_HEALTH_SNAPSHOT.TABLE_NAME
            AND a.IS_ACKNOWLEDGED = FALSE AND a.CREATED_AT > DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
      );
    v_alert_count := v_alert_count + SQLROWCOUNT;
    
    -- Alert 3: Data quality issues (EXCLUDED - FUTURE PHASE)
    -- DQ alerts disabled until SP_CAPTURE_DATA_QUALITY_METRICS is enabled.
    -- Uncomment below when DQ functionality is activated.
    
    -- Alert 4: Approaching staleness warning (stream < 12 hours until stale)
    INSERT INTO D_BRONZE.MONITORING.CDC_ALERT_LOG (ALERT_TYPE, ALERT_SEVERITY, TABLE_NAME, ALERT_MESSAGE, ALERT_DETAILS)
    SELECT 
        'STREAM_STALENESS_WARNING', 'WARNING', TABLE_NAME,
        'Stream ' || STREAM_NAME || ' approaching staleness: ' || HOURS_UNTIL_STALE || ' hours remaining',
        OBJECT_CONSTRUCT('stream_name', STREAM_NAME, 'hours_until_stale', HOURS_UNTIL_STALE, 'stale_after', STALE_AFTER)
    FROM D_BRONZE.MONITORING.CDC_STREAM_HEALTH_SNAPSHOT
    WHERE SNAPSHOT_TMS = (SELECT MAX(SNAPSHOT_TMS) FROM D_BRONZE.MONITORING.CDC_STREAM_HEALTH_SNAPSHOT)
      AND STREAM_STATUS = 'ACTIVE'
      AND HOURS_UNTIL_STALE IS NOT NULL
      AND HOURS_UNTIL_STALE < 12
      AND NOT EXISTS (
          SELECT 1 FROM D_BRONZE.MONITORING.CDC_ALERT_LOG a
          WHERE a.ALERT_TYPE = 'STREAM_STALENESS_WARNING' AND a.TABLE_NAME = CDC_STREAM_HEALTH_SNAPSHOT.TABLE_NAME
            AND a.IS_ACKNOWLEDGED = FALSE AND a.CREATED_AT > DATEADD(HOUR, -4, CURRENT_TIMESTAMP())
      );
    v_alert_count := v_alert_count + SQLROWCOUNT;
    
    RETURN 'Generated ' || v_alert_count || ' new alerts';
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- STEP 12: CREATE MASTER MONITORING PROCEDURE
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_BRONZE.MONITORING.SP_RUN_MONITORING_CYCLE()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_result VARCHAR := '';
    v_step_result VARCHAR;
BEGIN
    CALL D_BRONZE.MONITORING.SP_CAPTURE_STREAM_HEALTH() INTO v_step_result;
    v_result := v_result || 'Stream: ' || v_step_result || ' | ';
    
    CALL D_BRONZE.MONITORING.SP_CAPTURE_TASK_HEALTH() INTO v_step_result;
    v_result := v_result || 'Task: ' || v_step_result || ' | ';
    
    -- DQ SP excluded from v4.1 deployment. Uncomment when ready to activate.
    -- CALL D_BRONZE.MONITORING.SP_CAPTURE_DATA_QUALITY_METRICS() INTO v_step_result;
    -- v_result := v_result || 'DQ: ' || v_step_result || ' | ';
    
    CALL D_BRONZE.MONITORING.SP_GENERATE_ALERTS() INTO v_step_result;
    v_result := v_result || 'Alerts: ' || v_step_result;
    
    RETURN v_result;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR in monitoring cycle: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- STEP 13: CREATE MONITORING TASK (RUNS EVERY 15 MINUTES)
-- =============================================================================
CREATE OR REPLACE TASK D_BRONZE.MONITORING.TASK_CDC_MONITORING_CYCLE
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '15 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Master CDC monitoring task v4.0 - captures health metrics and generates alerts for 22 pipelines'
AS
    CALL D_BRONZE.MONITORING.SP_RUN_MONITORING_CYCLE();

-- =============================================================================
-- STEP 14: CREATE OBSERVABILITY VIEWS
-- =============================================================================

-- VIEW 1: Pipeline Health Dashboard (primary operational view)
CREATE OR REPLACE VIEW D_BRONZE.MONITORING.VW_PIPELINE_HEALTH_DASHBOARD AS
SELECT 
    c.TABLE_NAME,
    c.SOURCE_SCHEMA || '.' || c.TABLE_NAME || '_BASE' AS SOURCE_TABLE,
    c.TARGET_SCHEMA || '.' || c.TABLE_NAME AS TARGET_TABLE,
    c.PRIMARY_KEY_COLUMNS,
    c.EXPECTED_COLUMNS,
    t.TASK_STATE,
    t.LAST_RUN_TMS,
    t.LAST_RETURN_VALUE,
    t.MINUTES_SINCE_LAST_RUN,
    t.IS_HEALTHY AS TASK_HEALTHY,
    s.STREAM_STATUS,
    s.IS_STALE,
    s.STALE_AFTER,
    s.HOURS_UNTIL_STALE,
    s.HAS_PENDING_DATA,
    CASE 
        WHEN s.STREAM_STATUS = 'STALE' THEN 'CRITICAL'
        WHEN t.IS_HEALTHY = TRUE THEN 'HEALTHY'
        WHEN t.IS_HEALTHY = FALSE THEN 'CRITICAL'
        ELSE 'WARNING'
    END AS OVERALL_HEALTH
FROM D_BRONZE.MONITORING.CDC_PIPELINE_CONFIG c
LEFT JOIN (
    SELECT * FROM D_BRONZE.MONITORING.CDC_TASK_HEALTH_SNAPSHOT
    WHERE SNAPSHOT_TMS = (SELECT MAX(SNAPSHOT_TMS) FROM D_BRONZE.MONITORING.CDC_TASK_HEALTH_SNAPSHOT)
) t ON c.TABLE_NAME = t.TABLE_NAME
LEFT JOIN (
    SELECT * FROM D_BRONZE.MONITORING.CDC_STREAM_HEALTH_SNAPSHOT
    WHERE SNAPSHOT_TMS = (SELECT MAX(SNAPSHOT_TMS) FROM D_BRONZE.MONITORING.CDC_STREAM_HEALTH_SNAPSHOT)
) s ON c.TABLE_NAME = s.TABLE_NAME
WHERE c.IS_ACTIVE = TRUE
ORDER BY 
    CASE OVERALL_HEALTH WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
    c.TABLE_NAME;

-- VIEW 2: Active Alerts
CREATE OR REPLACE VIEW D_BRONZE.MONITORING.VW_ACTIVE_ALERTS AS
SELECT 
    ALERT_ID, ALERT_TMS, ALERT_TYPE, ALERT_SEVERITY, TABLE_NAME, ALERT_MESSAGE, ALERT_DETAILS,
    TIMESTAMPDIFF(MINUTE, ALERT_TMS, CURRENT_TIMESTAMP()) AS MINUTES_OPEN
FROM D_BRONZE.MONITORING.CDC_ALERT_LOG
WHERE IS_ACKNOWLEDGED = FALSE
ORDER BY CASE ALERT_SEVERITY WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END, ALERT_TMS DESC;

-- VIEW 3: Task Execution History (INFORMATION_SCHEMA - real-time)
CREATE OR REPLACE VIEW D_BRONZE.MONITORING.VW_TASK_EXECUTION_HISTORY AS
SELECT 
    DATABASE_NAME || '.' || SCHEMA_NAME || '.' || NAME AS TASK_NAME,
    STATE AS EXECUTION_STATUS,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    TIMESTAMPDIFF(SECOND, SCHEDULED_TIME, COMPLETED_TIME) AS DURATION_SECONDS,
    RETURN_VALUE,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(D_RAW.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 1000
))
WHERE DATABASE_NAME = 'D_RAW' 
  AND SCHEMA_NAME = 'SADB'
ORDER BY COMPLETED_TIME DESC;

-- VIEW 4: 7-Day Pipeline Trend (EXCLUDED - FUTURE PHASE - depends on DQ metrics)
/*
CREATE OR REPLACE VIEW D_BRONZE.MONITORING.VW_PIPELINE_TREND_7D AS
SELECT 
    DATE(SNAPSHOT_TMS) AS METRIC_DATE, TABLE_NAME,
    AVG(SOURCE_ROW_COUNT) AS AVG_SOURCE_ROWS,
    AVG(SOURCE_FILTERED_COUNT) AS AVG_SOURCE_FILTERED,
    AVG(TARGET_ROW_COUNT) AS AVG_TARGET_ROWS,
    AVG(ROW_COUNT_DIFF) AS AVG_ROW_DIFF,
    AVG(ROW_COUNT_MATCH_PCT) AS AVG_MATCH_PCT,
    COUNT(CASE WHEN DATA_QUALITY_STATUS = 'HEALTHY' THEN 1 END) AS HEALTHY_SNAPSHOTS,
    COUNT(CASE WHEN DATA_QUALITY_STATUS = 'WARNING' THEN 1 END) AS WARNING_SNAPSHOTS,
    COUNT(CASE WHEN DATA_QUALITY_STATUS = 'CRITICAL' THEN 1 END) AS CRITICAL_SNAPSHOTS
FROM D_BRONZE.MONITORING.CDC_DATA_QUALITY_METRICS
WHERE SNAPSHOT_TMS >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY DATE(SNAPSHOT_TMS), TABLE_NAME
ORDER BY METRIC_DATE DESC, TABLE_NAME;
*/

-- VIEW 5: Pipeline Summary (Executive KPIs)
CREATE OR REPLACE VIEW D_BRONZE.MONITORING.VW_PIPELINE_SUMMARY AS
SELECT 
    COUNT(*) AS TOTAL_PIPELINES,
    SUM(CASE WHEN c.IS_ACTIVE THEN 1 ELSE 0 END) AS ACTIVE_PIPELINES,
    SUM(CASE WHEN t.IS_HEALTHY = TRUE THEN 1 ELSE 0 END) AS HEALTHY_TASKS,
    SUM(CASE WHEN t.IS_HEALTHY = FALSE THEN 1 ELSE 0 END) AS UNHEALTHY_TASKS,
    SUM(CASE WHEN s.STREAM_STATUS = 'STALE' THEN 1 ELSE 0 END) AS STALE_STREAMS,
    (SELECT COUNT(*) FROM D_BRONZE.MONITORING.CDC_ALERT_LOG WHERE IS_ACKNOWLEDGED = FALSE) AS OPEN_ALERTS
FROM D_BRONZE.MONITORING.CDC_PIPELINE_CONFIG c
LEFT JOIN (
    SELECT * FROM D_BRONZE.MONITORING.CDC_TASK_HEALTH_SNAPSHOT
    WHERE SNAPSHOT_TMS = (SELECT MAX(SNAPSHOT_TMS) FROM D_BRONZE.MONITORING.CDC_TASK_HEALTH_SNAPSHOT)
) t ON c.TABLE_NAME = t.TABLE_NAME
LEFT JOIN (
    SELECT * FROM D_BRONZE.MONITORING.CDC_STREAM_HEALTH_SNAPSHOT
    WHERE SNAPSHOT_TMS = (SELECT MAX(SNAPSHOT_TMS) FROM D_BRONZE.MONITORING.CDC_STREAM_HEALTH_SNAPSHOT)
) s ON c.TABLE_NAME = s.TABLE_NAME;

-- VIEW 6: Filter Impact Analysis (EXCLUDED - FUTURE PHASE - depends on DQ metrics)
/*
CREATE OR REPLACE VIEW D_BRONZE.MONITORING.VW_FILTER_IMPACT_ANALYSIS AS
SELECT 
    TABLE_NAME,
    SOURCE_ROW_COUNT AS TOTAL_SOURCE,
    SOURCE_FILTERED_COUNT AS AFTER_FILTER,
    SOURCE_ROW_COUNT - SOURCE_FILTERED_COUNT AS ROWS_EXCLUDED,
    CASE WHEN SOURCE_ROW_COUNT > 0 
         THEN ROUND((SOURCE_ROW_COUNT - SOURCE_FILTERED_COUNT) / SOURCE_ROW_COUNT * 100, 2) 
         ELSE 0 END AS EXCLUSION_PCT,
    TARGET_ACTIVE_COUNT AS TARGET_ACTIVE,
    ROW_COUNT_MATCH_PCT AS MATCH_PCT,
    DATA_QUALITY_STATUS,
    SNAPSHOT_TMS
FROM D_BRONZE.MONITORING.CDC_DATA_QUALITY_METRICS
WHERE SNAPSHOT_TMS = (SELECT MAX(SNAPSHOT_TMS) FROM D_BRONZE.MONITORING.CDC_DATA_QUALITY_METRICS)
ORDER BY EXCLUSION_PCT DESC;
*/

-- =============================================================================
-- STEP 15: CREATE UTILITY PROCEDURES
-- =============================================================================

-- Acknowledge an alert
CREATE OR REPLACE PROCEDURE D_BRONZE.MONITORING.SP_ACKNOWLEDGE_ALERT(
    P_ALERT_ID NUMBER, P_ACKNOWLEDGED_BY VARCHAR, P_RESOLUTION_NOTES VARCHAR DEFAULT NULL
)
RETURNS VARCHAR LANGUAGE SQL EXECUTE AS CALLER
AS
$$
BEGIN
    UPDATE D_BRONZE.MONITORING.CDC_ALERT_LOG
    SET IS_ACKNOWLEDGED = TRUE, 
        ACKNOWLEDGED_BY = :P_ACKNOWLEDGED_BY,
        ACKNOWLEDGED_TMS = CURRENT_TIMESTAMP(), 
        RESOLUTION_NOTES = :P_RESOLUTION_NOTES
    WHERE ALERT_ID = :P_ALERT_ID;
    IF (SQLROWCOUNT > 0) THEN RETURN 'Alert ' || :P_ALERT_ID || ' acknowledged by ' || :P_ACKNOWLEDGED_BY; 
    ELSE RETURN 'Alert ' || :P_ALERT_ID || ' not found'; END IF;
EXCEPTION WHEN OTHER THEN RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- Cleanup old monitoring data
CREATE OR REPLACE PROCEDURE D_BRONZE.MONITORING.SP_CLEANUP_OLD_MONITORING_DATA(P_RETENTION_DAYS NUMBER DEFAULT 90)
RETURNS VARCHAR LANGUAGE SQL EXECUTE AS CALLER
AS
$$
DECLARE
    v_cutoff TIMESTAMP_NTZ;
    v_d1 NUMBER := 0; v_d2 NUMBER := 0; v_d3 NUMBER := 0; v_d4 NUMBER := 0; v_d5 NUMBER := 0;
BEGIN
    v_cutoff := DATEADD(DAY, -:P_RETENTION_DAYS, CURRENT_TIMESTAMP());
    DELETE FROM D_BRONZE.MONITORING.CDC_STREAM_HEALTH_SNAPSHOT WHERE CREATED_AT < :v_cutoff; v_d1 := SQLROWCOUNT;
    DELETE FROM D_BRONZE.MONITORING.CDC_TASK_HEALTH_SNAPSHOT WHERE CREATED_AT < :v_cutoff; v_d2 := SQLROWCOUNT;
    -- DQ cleanup excluded from v4.1. Uncomment when DQ is enabled.
    -- DELETE FROM D_BRONZE.MONITORING.CDC_DATA_QUALITY_METRICS WHERE CREATED_AT < :v_cutoff; v_d3 := SQLROWCOUNT;
    DELETE FROM D_BRONZE.MONITORING.CDC_ALERT_LOG WHERE CREATED_AT < :v_cutoff AND IS_ACKNOWLEDGED = TRUE; v_d4 := SQLROWCOUNT;
    DELETE FROM D_BRONZE.MONITORING.CDC_EXECUTION_LOG WHERE CREATED_AT < :v_cutoff; v_d5 := SQLROWCOUNT;
    RETURN 'Cleanup done (>' || :P_RETENTION_DAYS || 'd). Stream=' || v_d1 || ', Task=' || v_d2 || ', Alert=' || v_d4 || ', ExecLog=' || v_d5;
EXCEPTION WHEN OTHER THEN RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- STEP 16: CREATE CLEANUP TASK (WEEKLY)
-- =============================================================================
CREATE OR REPLACE TASK D_BRONZE.MONITORING.TASK_CDC_MONITORING_CLEANUP
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = 'USING CRON 0 2 * * SUN America/Chicago'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Weekly cleanup of monitoring data older than 90 days'
AS CALL D_BRONZE.MONITORING.SP_CLEANUP_OLD_MONITORING_DATA(90);

-- =============================================================================
-- STEP 17: RESUME MONITORING TASKS
-- =============================================================================
ALTER TASK D_BRONZE.MONITORING.TASK_CDC_MONITORING_CYCLE RESUME;
ALTER TASK D_BRONZE.MONITORING.TASK_CDC_MONITORING_CLEANUP RESUME;

-- =============================================================================
-- STEP 18: RUN INITIAL MONITORING CYCLE (UNCOMMENT TO EXECUTE)
-- =============================================================================
-- CALL D_BRONZE.MONITORING.SP_RUN_MONITORING_CYCLE();

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- SELECT * FROM D_BRONZE.MONITORING.CDC_PIPELINE_CONFIG ORDER BY TABLE_NAME;
-- SELECT COUNT(*) FROM D_BRONZE.MONITORING.CDC_PIPELINE_CONFIG; -- Expect 22
-- SELECT * FROM D_BRONZE.MONITORING.VW_PIPELINE_HEALTH_DASHBOARD;
-- SELECT * FROM D_BRONZE.MONITORING.VW_ACTIVE_ALERTS;
-- SELECT * FROM D_BRONZE.MONITORING.VW_PIPELINE_SUMMARY;
-- DQ views excluded from v4.1:
-- SELECT * FROM D_BRONZE.MONITORING.VW_FILTER_IMPACT_ANALYSIS;  -- FUTURE PHASE
-- SELECT * FROM D_BRONZE.MONITORING.VW_TASK_EXECUTION_HISTORY;
-- SELECT * FROM D_BRONZE.MONITORING.VW_PIPELINE_TREND_7D;     -- FUTURE PHASE