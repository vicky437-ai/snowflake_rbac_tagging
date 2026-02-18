/*
================================================================================
TRKFC_TRSTN CDC - ROLLBACK / CLEANUP SCRIPT
================================================================================
Purpose      : Safely removes all TRKFC_TRSTN CDC objects if deployment fails
Version      : 1.0.0
Usage        : Run this script to rollback a failed or unwanted deployment
================================================================================

WARNING: This script will remove CDC infrastructure objects!
         - Task will be stopped and dropped
         - Procedure will be dropped
         - Stream will be dropped
         - V1 table can optionally be preserved or dropped

PREREQUISITE: Review the configuration and set PRESERVE_V1_DATA appropriately
================================================================================
*/

-- =============================================================================
-- CONFIGURATION - MODIFY FOR YOUR ENVIRONMENT
-- =============================================================================
SET V_DATABASE = 'D_BRONZE';           -- Target database
SET V_SCHEMA = 'SADB';                 -- Target schema

-- CRITICAL SETTING: Set to TRUE to keep V1 data, FALSE to completely remove
SET V_PRESERVE_V1_DATA = TRUE;         -- TRUE = Keep V1 table with historical data
                                       -- FALSE = Drop V1 table (DATA LOSS!)

-- =============================================================================
-- CREATE ROLLBACK LOG TABLE
-- =============================================================================
CREATE OR REPLACE TEMPORARY TABLE _ROLLBACK_LOG (
    STEP_ORDER NUMBER,
    OBJECT_TYPE VARCHAR(50),
    OBJECT_NAME VARCHAR(200),
    ACTION VARCHAR(50),
    STATUS VARCHAR(20),
    DETAILS VARCHAR(500),
    EXECUTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- DISPLAY ROLLBACK PLAN
-- =============================================================================
SELECT '================================================================================' AS "=";
SELECT '                    TRKFC_TRSTN CDC - ROLLBACK SCRIPT                          ' AS "TITLE";
SELECT '================================================================================' AS "=";

SELECT 
    $V_DATABASE || '.' || $V_SCHEMA AS "TARGET_LOCATION",
    CASE WHEN $V_PRESERVE_V1_DATA THEN 'YES - V1 data will be PRESERVED' 
         ELSE 'NO - V1 table will be DROPPED (DATA LOSS!)' END AS "PRESERVE_V1_DATA",
    CURRENT_TIMESTAMP() AS "EXECUTION_TIME";

SELECT '================================================================================' AS "=";
SELECT '                         STARTING ROLLBACK                                     ' AS "STATUS";
SELECT '================================================================================' AS "=";

-- =============================================================================
-- STEP 1: SUSPEND AND DROP TASK (Must be first - has dependencies)
-- =============================================================================
DECLARE
    v_task_exists BOOLEAN := FALSE;
    v_task_name VARCHAR := '';
BEGIN
    v_task_name := $V_DATABASE || '.' || $V_SCHEMA || '.TASK_PROCESS_TRKFC_TRSTN_CDC';
    
    -- Check if task exists
    EXECUTE IMMEDIATE 'SHOW TASKS LIKE ''TASK_PROCESS_TRKFC_TRSTN_CDC'' IN SCHEMA ' || $V_DATABASE || '.' || $V_SCHEMA;
    CREATE OR REPLACE TEMPORARY TABLE _TASK_CHECK AS SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    SELECT COUNT(*) > 0 INTO v_task_exists FROM _TASK_CHECK;
    DROP TABLE IF EXISTS _TASK_CHECK;
    
    IF (v_task_exists) THEN
        -- Suspend task first (required before drop)
        BEGIN
            EXECUTE IMMEDIATE 'ALTER TASK ' || v_task_name || ' SUSPEND';
            INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
            VALUES (1, 'TASK', v_task_name, 'SUSPEND', 'SUCCESS', 'Task suspended successfully');
        EXCEPTION
            WHEN OTHER THEN
                INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
                VALUES (1, 'TASK', v_task_name, 'SUSPEND', 'SKIPPED', 'Task may already be suspended: ' || SQLERRM);
        END;
        
        -- Drop task
        BEGIN
            EXECUTE IMMEDIATE 'DROP TASK IF EXISTS ' || v_task_name;
            INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
            VALUES (2, 'TASK', v_task_name, 'DROP', 'SUCCESS', 'Task dropped successfully');
        EXCEPTION
            WHEN OTHER THEN
                INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
                VALUES (2, 'TASK', v_task_name, 'DROP', 'ERROR', 'Failed to drop task: ' || SQLERRM);
        END;
    ELSE
        INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
        VALUES (1, 'TASK', v_task_name, 'CHECK', 'SKIPPED', 'Task does not exist - nothing to drop');
    END IF;
END;

-- =============================================================================
-- STEP 2: DROP STORED PROCEDURE
-- =============================================================================
DECLARE
    v_proc_exists BOOLEAN := FALSE;
    v_proc_name VARCHAR := '';
BEGIN
    v_proc_name := $V_DATABASE || '.' || $V_SCHEMA || '.SP_PROCESS_TRKFC_TRSTN_CDC';
    
    -- Check if procedure exists
    EXECUTE IMMEDIATE '
    SELECT COUNT(*) FROM ' || $V_DATABASE || '.INFORMATION_SCHEMA.PROCEDURES 
    WHERE PROCEDURE_SCHEMA = ''' || $V_SCHEMA || ''' 
      AND PROCEDURE_NAME = ''SP_PROCESS_TRKFC_TRSTN_CDC''';
    
    CREATE OR REPLACE TEMPORARY TABLE _PROC_CHECK AS SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    SELECT COUNT(*) > 0 INTO v_proc_exists FROM _PROC_CHECK WHERE "COUNT(*)" > 0;
    DROP TABLE IF EXISTS _PROC_CHECK;
    
    IF (v_proc_exists) THEN
        BEGIN
            EXECUTE IMMEDIATE 'DROP PROCEDURE IF EXISTS ' || v_proc_name || '()';
            INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
            VALUES (3, 'PROCEDURE', v_proc_name, 'DROP', 'SUCCESS', 'Procedure dropped successfully');
        EXCEPTION
            WHEN OTHER THEN
                INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
                VALUES (3, 'PROCEDURE', v_proc_name, 'DROP', 'ERROR', 'Failed to drop procedure: ' || SQLERRM);
        END;
    ELSE
        INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
        VALUES (3, 'PROCEDURE', v_proc_name, 'CHECK', 'SKIPPED', 'Procedure does not exist - nothing to drop');
    END IF;
END;

-- =============================================================================
-- STEP 3: DROP STREAM
-- =============================================================================
DECLARE
    v_stream_exists BOOLEAN := FALSE;
    v_stream_name VARCHAR := '';
BEGIN
    v_stream_name := $V_DATABASE || '.' || $V_SCHEMA || '.TRKFC_TRSTN_BASE_HIST_STREAM';
    
    -- Check if stream exists
    EXECUTE IMMEDIATE '
    SELECT COUNT(*) FROM ' || $V_DATABASE || '.INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = ''' || $V_SCHEMA || ''' 
      AND TABLE_NAME = ''TRKFC_TRSTN_BASE_HIST_STREAM''
      AND TABLE_TYPE = ''STREAM''';
    
    CREATE OR REPLACE TEMPORARY TABLE _STREAM_CHECK AS SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    SELECT COUNT(*) > 0 INTO v_stream_exists FROM _STREAM_CHECK WHERE "COUNT(*)" > 0;
    DROP TABLE IF EXISTS _STREAM_CHECK;
    
    IF (v_stream_exists) THEN
        BEGIN
            EXECUTE IMMEDIATE 'DROP STREAM IF EXISTS ' || v_stream_name;
            INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
            VALUES (4, 'STREAM', v_stream_name, 'DROP', 'SUCCESS', 'Stream dropped successfully');
        EXCEPTION
            WHEN OTHER THEN
                INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
                VALUES (4, 'STREAM', v_stream_name, 'DROP', 'ERROR', 'Failed to drop stream: ' || SQLERRM);
        END;
    ELSE
        INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
        VALUES (4, 'STREAM', v_stream_name, 'CHECK', 'SKIPPED', 'Stream does not exist - nothing to drop');
    END IF;
END;

-- =============================================================================
-- STEP 4: DISABLE CHANGE TRACKING (Optional - keeps source table intact)
-- =============================================================================
DECLARE
    v_table_exists BOOLEAN := FALSE;
    v_table_name VARCHAR := '';
BEGIN
    v_table_name := $V_DATABASE || '.' || $V_SCHEMA || '.TRKFC_TRSTN_BASE';
    
    -- Check if source table exists
    EXECUTE IMMEDIATE '
    SELECT COUNT(*) FROM ' || $V_DATABASE || '.INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = ''' || $V_SCHEMA || ''' 
      AND TABLE_NAME = ''TRKFC_TRSTN_BASE''
      AND TABLE_TYPE = ''BASE TABLE''';
    
    CREATE OR REPLACE TEMPORARY TABLE _TABLE_CHECK AS SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    SELECT COUNT(*) > 0 INTO v_table_exists FROM _TABLE_CHECK WHERE "COUNT(*)" > 0;
    DROP TABLE IF EXISTS _TABLE_CHECK;
    
    IF (v_table_exists) THEN
        BEGIN
            -- Note: We do NOT disable change tracking - it doesn't hurt to leave it on
            -- and the customer may want it for other purposes
            INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
            VALUES (5, 'TABLE', v_table_name, 'PRESERVE', 'SUCCESS', 
                    'Source table preserved - change tracking left enabled (harmless)');
        EXCEPTION
            WHEN OTHER THEN
                INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
                VALUES (5, 'TABLE', v_table_name, 'CHECK', 'ERROR', 'Error checking source table: ' || SQLERRM);
        END;
    ELSE
        INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
        VALUES (5, 'TABLE', v_table_name, 'CHECK', 'SKIPPED', 'Source table does not exist');
    END IF;
END;

-- =============================================================================
-- STEP 5: HANDLE V1 TABLE (Based on PRESERVE_V1_DATA setting)
-- =============================================================================
DECLARE
    v_v1_exists BOOLEAN := FALSE;
    v_v1_name VARCHAR := '';
    v_v1_row_count NUMBER := 0;
    v_preserve BOOLEAN;
BEGIN
    v_v1_name := $V_DATABASE || '.' || $V_SCHEMA || '.TRKFC_TRSTN_V1';
    v_preserve := $V_PRESERVE_V1_DATA;
    
    -- Check if V1 table exists
    EXECUTE IMMEDIATE '
    SELECT COUNT(*) FROM ' || $V_DATABASE || '.INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = ''' || $V_SCHEMA || ''' 
      AND TABLE_NAME = ''TRKFC_TRSTN_V1''';
    
    CREATE OR REPLACE TEMPORARY TABLE _V1_CHECK AS SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    SELECT COUNT(*) > 0 INTO v_v1_exists FROM _V1_CHECK WHERE "COUNT(*)" > 0;
    DROP TABLE IF EXISTS _V1_CHECK;
    
    IF (v_v1_exists) THEN
        -- Get row count for logging
        BEGIN
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_v1_name;
            CREATE OR REPLACE TEMPORARY TABLE _V1_COUNT AS SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
            SELECT "COUNT(*)" INTO v_v1_row_count FROM _V1_COUNT;
            DROP TABLE IF EXISTS _V1_COUNT;
        EXCEPTION
            WHEN OTHER THEN
                v_v1_row_count := -1;
        END;
        
        IF (v_preserve) THEN
            -- PRESERVE V1 DATA
            INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
            VALUES (6, 'TABLE', v_v1_name, 'PRESERVE', 'SUCCESS', 
                    'V1 table PRESERVED with ' || v_v1_row_count || ' rows of historical data');
        ELSE
            -- DROP V1 TABLE (DATA LOSS!)
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS ' || v_v1_name;
                INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
                VALUES (6, 'TABLE', v_v1_name, 'DROP', 'SUCCESS', 
                        'V1 table DROPPED - ' || v_v1_row_count || ' rows of historical data DELETED');
            EXCEPTION
                WHEN OTHER THEN
                    INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
                    VALUES (6, 'TABLE', v_v1_name, 'DROP', 'ERROR', 'Failed to drop V1 table: ' || SQLERRM);
            END;
        END IF;
    ELSE
        INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
        VALUES (6, 'TABLE', v_v1_name, 'CHECK', 'SKIPPED', 'V1 table does not exist - nothing to handle');
    END IF;
END;

-- =============================================================================
-- STEP 6: DROP TEST RESULTS TABLE (If exists from testing)
-- =============================================================================
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS ' || $V_DATABASE || '.' || $V_SCHEMA || '._TEST_RESULTS_TRKFC_TRSTN';
    INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
    VALUES (7, 'TABLE', $V_DATABASE || '.' || $V_SCHEMA || '._TEST_RESULTS_TRKFC_TRSTN', 'DROP', 'SUCCESS', 
            'Test results table dropped (if existed)');
EXCEPTION
    WHEN OTHER THEN
        INSERT INTO _ROLLBACK_LOG (STEP_ORDER, OBJECT_TYPE, OBJECT_NAME, ACTION, STATUS, DETAILS)
        VALUES (7, 'TABLE', '_TEST_RESULTS_TRKFC_TRSTN', 'DROP', 'SKIPPED', 'Test table did not exist');
END;

-- =============================================================================
-- DISPLAY ROLLBACK RESULTS
-- =============================================================================
SELECT '================================================================================' AS "=";
SELECT '                         ROLLBACK COMPLETE                                     ' AS "STATUS";
SELECT '================================================================================' AS "=";

-- Summary
SELECT 
    COUNT(CASE WHEN STATUS = 'SUCCESS' THEN 1 END) AS "SUCCESSFUL",
    COUNT(CASE WHEN STATUS = 'SKIPPED' THEN 1 END) AS "SKIPPED",
    COUNT(CASE WHEN STATUS = 'ERROR' THEN 1 END) AS "ERRORS",
    COUNT(*) AS "TOTAL_STEPS",
    CASE 
        WHEN COUNT(CASE WHEN STATUS = 'ERROR' THEN 1 END) > 0 
            THEN '⚠️ ROLLBACK COMPLETED WITH ERRORS - Review details'
        ELSE '✅ ROLLBACK COMPLETED SUCCESSFULLY'
    END AS "RESULT"
FROM _ROLLBACK_LOG;

-- Detailed Log
SELECT 
    STEP_ORDER AS "#",
    OBJECT_TYPE AS "TYPE",
    OBJECT_NAME AS "OBJECT",
    ACTION,
    STATUS,
    DETAILS
FROM _ROLLBACK_LOG
ORDER BY STEP_ORDER;

-- =============================================================================
-- POST-ROLLBACK VERIFICATION
-- =============================================================================
SELECT '================================================================================' AS "=";
SELECT '                    POST-ROLLBACK VERIFICATION                                 ' AS "SECTION";
SELECT '================================================================================' AS "=";

-- Check what objects remain
EXECUTE IMMEDIATE 'SHOW TASKS LIKE ''%TRKFC_TRSTN%'' IN SCHEMA ' || $V_DATABASE || '.' || $V_SCHEMA;
EXECUTE IMMEDIATE 'SHOW STREAMS LIKE ''%TRKFC_TRSTN%'' IN SCHEMA ' || $V_DATABASE || '.' || $V_SCHEMA;
EXECUTE IMMEDIATE 'SHOW PROCEDURES LIKE ''%TRKFC_TRSTN%'' IN SCHEMA ' || $V_DATABASE || '.' || $V_SCHEMA;
EXECUTE IMMEDIATE 'SHOW TABLES LIKE ''%TRKFC_TRSTN%'' IN SCHEMA ' || $V_DATABASE || '.' || $V_SCHEMA;

-- Cleanup
DROP TABLE IF EXISTS _ROLLBACK_LOG;

/*
================================================================================
ROLLBACK COMPLETE
================================================================================

WHAT WAS REMOVED:
-----------------
✓ Task: TASK_PROCESS_TRKFC_TRSTN_CDC (suspended and dropped)
✓ Procedure: SP_PROCESS_TRKFC_TRSTN_CDC (dropped)
✓ Stream: TRKFC_TRSTN_BASE_HIST_STREAM (dropped)
✓ Test Table: _TEST_RESULTS_TRKFC_TRSTN (dropped if existed)

WHAT WAS PRESERVED:
-------------------
✓ Source Table: TRKFC_TRSTN_BASE (never modified)
✓ V1 Table: TRKFC_TRSTN_V1 (based on PRESERVE_V1_DATA setting)
✓ Change Tracking: Left enabled on source (harmless)

TO REDEPLOY:
------------
1. Fix any issues that caused the failure
2. Run: TRKFC_TRSTN_PRE_DEPLOYMENT_VALIDATION.sql
3. Run: TRKFC_TRSTN_DEPLOY_PARAMETERIZED.sql

================================================================================
*/
