/*
================================================================================
CDC DATA PRESERVATION - PRODUCTION ROLLBACK SCRIPT
================================================================================
Project      : Snowflake CDC Data Preservation Strategy
Version      : 1.0
Created Date : February 23, 2026
Total Objects: 21 Tables, 21 Streams, 21 Stored Procedures, 21 Tasks

ROLLBACK MODES:
  - FULL       : Removes ALL objects (Tasks, SPs, Streams, Target Tables, Change Tracking)
  - SOFT       : Removes Tasks, SPs, Streams only (preserves Target Tables with data)
  - TASK_ONLY  : Suspends and removes Tasks only (preserves SPs, Streams, Tables)

IMPORTANT: 
  - Execute with appropriate role (D-SNW-DEVBI1-ETL or ACCOUNTADMIN)
  - Review before execution in production
  - Target tables contain preserved data - FULL mode will DELETE ALL DATA
================================================================================
*/

-- =============================================================================
-- CONFIGURATION: SET ROLLBACK MODE BEFORE EXECUTION
-- =============================================================================
SET ROLLBACK_MODE = 'SOFT';  -- Options: 'FULL', 'SOFT', 'TASK_ONLY'
SET DRY_RUN = FALSE;         -- Set TRUE to preview without executing

-- =============================================================================
-- STEP 1: CREATE ROLLBACK STORED PROCEDURE
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_RAW.SADB.SP_CDC_ROLLBACK(
    P_ROLLBACK_MODE VARCHAR DEFAULT 'SOFT',
    P_DRY_RUN BOOLEAN DEFAULT FALSE,
    P_TABLE_FILTER VARCHAR DEFAULT 'ALL'
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_result ARRAY DEFAULT ARRAY_CONSTRUCT();
    v_start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_sql VARCHAR;
    v_task_count NUMBER DEFAULT 0;
    v_sp_count NUMBER DEFAULT 0;
    v_stream_count NUMBER DEFAULT 0;
    v_table_count NUMBER DEFAULT 0;
    v_ct_count NUMBER DEFAULT 0;
    v_error_count NUMBER DEFAULT 0;
    v_mode VARCHAR DEFAULT UPPER(P_ROLLBACK_MODE);
    
    -- Cursor for all CDC objects
    c_objects CURSOR FOR
        SELECT * FROM (
            VALUES 
            ('OPTRN', 'D_RAW.SADB.OPTRN_BASE', 'D_BRONZE.SADB.OPTRN', 'D_RAW.SADB.OPTRN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_OPTRN', 'D_RAW.SADB.TASK_PROCESS_OPTRN'),
            ('OPTRN_LEG', 'D_RAW.SADB.OPTRN_LEG_BASE', 'D_BRONZE.SADB.OPTRN_LEG', 'D_RAW.SADB.OPTRN_LEG_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_OPTRN_LEG', 'D_RAW.SADB.TASK_PROCESS_OPTRN_LEG'),
            ('OPTRN_EVENT', 'D_RAW.SADB.OPTRN_EVENT_BASE', 'D_BRONZE.SADB.OPTRN_EVENT', 'D_RAW.SADB.OPTRN_EVENT_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_OPTRN_EVENT', 'D_RAW.SADB.TASK_PROCESS_OPTRN_EVENT'),
            ('TRAIN_PLAN', 'D_RAW.SADB.TRAIN_PLAN_BASE', 'D_BRONZE.SADB.TRAIN_PLAN', 'D_RAW.SADB.TRAIN_PLAN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRAIN_PLAN', 'D_RAW.SADB.TASK_PROCESS_TRAIN_PLAN'),
            ('TRAIN_PLAN_LEG', 'D_RAW.SADB.TRAIN_PLAN_LEG_BASE', 'D_BRONZE.SADB.TRAIN_PLAN_LEG', 'D_RAW.SADB.TRAIN_PLAN_LEG_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_LEG', 'D_RAW.SADB.TASK_PROCESS_TRAIN_PLAN_LEG'),
            ('TRAIN_PLAN_EVENT', 'D_RAW.SADB.TRAIN_PLAN_EVENT_BASE', 'D_BRONZE.SADB.TRAIN_PLAN_EVENT', 'D_RAW.SADB.TRAIN_PLAN_EVENT_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_EVENT', 'D_RAW.SADB.TASK_PROCESS_TRAIN_PLAN_EVENT'),
            ('LCMTV_MVMNT_EVENT', 'D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE', 'D_BRONZE.SADB.LCMTV_MVMNT_EVENT', 'D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_LCMTV_MVMNT_EVENT', 'D_RAW.SADB.TASK_PROCESS_LCMTV_MVMNT_EVENT'),
            ('EQPMV_RFEQP_MVMNT_EVENT', 'D_RAW.SADB.EQPMV_RFEQP_MVMNT_EVENT_BASE', 'D_BRONZE.SADB.EQPMV_RFEQP_MVMNT_EVENT', 'D_RAW.SADB.EQPMV_RFEQP_MVMNT_EVENT_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_EQPMV_RFEQP_MVMNT_EVENT', 'D_RAW.SADB.TASK_PROCESS_EQPMV_RFEQP_MVMNT_EVENT'),
            ('EQPMV_EQPMT_EVENT_TYPE', 'D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE', 'D_BRONZE.SADB.EQPMV_EQPMT_EVENT_TYPE', 'D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_EQPMV_EQPMT_EVENT_TYPE', 'D_RAW.SADB.TASK_PROCESS_EQPMV_EQPMT_EVENT_TYPE'),
            ('TRAIN_CNST_SMRY', 'D_RAW.SADB.TRAIN_CNST_SMRY_BASE', 'D_BRONZE.SADB.TRAIN_CNST_SMRY', 'D_RAW.SADB.TRAIN_CNST_SMRY_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRAIN_CNST_SMRY', 'D_RAW.SADB.TASK_PROCESS_TRAIN_CNST_SMRY'),
            ('TRAIN_CNST_DTL_RAIL_EQPT', 'D_RAW.SADB.TRAIN_CNST_DTL_RAIL_EQPT_BASE', 'D_BRONZE.SADB.TRAIN_CNST_DTL_RAIL_EQPT', 'D_RAW.SADB.TRAIN_CNST_DTL_RAIL_EQPT_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRAIN_CNST_DTL_RAIL_EQPT', 'D_RAW.SADB.TASK_PROCESS_TRAIN_CNST_DTL_RAIL_EQPT'),
            ('TRKFC_TRSTN', 'D_RAW.SADB.TRKFC_TRSTN_BASE', 'D_BRONZE.SADB.TRKFC_TRSTN', 'D_RAW.SADB.TRKFC_TRSTN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRKFC_TRSTN', 'D_RAW.SADB.TASK_PROCESS_TRKFC_TRSTN'),
            ('EQPMNT_AAR_BASE', 'D_RAW.SADB.EQPMNT_AAR_BASE_BASE', 'D_BRONZE.SADB.EQPMNT_AAR_BASE', 'D_RAW.SADB.EQPMNT_AAR_BASE_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_EQPMNT_AAR_BASE', 'D_RAW.SADB.TASK_PROCESS_EQPMNT_AAR_BASE'),
            ('STNWYB_MSG_DN', 'D_RAW.SADB.STNWYB_MSG_DN_BASE', 'D_BRONZE.SADB.STNWYB_MSG_DN', 'D_RAW.SADB.STNWYB_MSG_DN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_STNWYB_MSG_DN', 'D_RAW.SADB.TASK_PROCESS_STNWYB_MSG_DN'),
            ('LCMTV_EMIS', 'D_RAW.SADB.LCMTV_EMIS_BASE', 'D_BRONZE.SADB.LCMTV_EMIS', 'D_RAW.SADB.LCMTV_EMIS_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_LCMTV_EMIS', 'D_RAW.SADB.TASK_PROCESS_LCMTV_EMIS'),
            ('TRKFCG_FIXED_PLANT_ASSET', 'D_RAW.SADB.TRKFCG_FIXED_PLANT_ASSET_BASE', 'D_BRONZE.SADB.TRKFCG_FIXED_PLANT_ASSET', 'D_RAW.SADB.TRKFCG_FIXED_PLANT_ASSET_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRKFCG_FIXED_PLANT_ASSET', 'D_RAW.SADB.TASK_PROCESS_TRKFCG_FIXED_PLANT_ASSET'),
            ('TRKFCG_FXPLA_TRACK_LCTN_DN', 'D_RAW.SADB.TRKFCG_FXPLA_TRACK_LCTN_DN_BASE', 'D_BRONZE.SADB.TRKFCG_FXPLA_TRACK_LCTN_DN', 'D_RAW.SADB.TRKFCG_FXPLA_TRACK_LCTN_DN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRKFCG_FXPLA_TRACK_LCTN_DN', 'D_RAW.SADB.TASK_PROCESS_TRKFCG_FXPLA_TRACK_LCTN_DN'),
            ('TRKFCG_TRACK_SGMNT_DN', 'D_RAW.SADB.TRKFCG_TRACK_SGMNT_DN_BASE', 'D_BRONZE.SADB.TRKFCG_TRACK_SGMNT_DN', 'D_RAW.SADB.TRKFCG_TRACK_SGMNT_DN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRKFCG_TRACK_SGMNT_DN', 'D_RAW.SADB.TASK_PROCESS_TRKFCG_TRACK_SGMNT_DN'),
            ('TRKFCG_SBDVSN', 'D_RAW.SADB.TRKFCG_SBDVSN_BASE', 'D_BRONZE.SADB.TRKFCG_SBDVSN', 'D_RAW.SADB.TRKFCG_SBDVSN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRKFCG_SBDVSN', 'D_RAW.SADB.TASK_PROCESS_TRKFCG_SBDVSN'),
            ('TRKFCG_SRVC_AREA', 'D_RAW.SADB.TRKFCG_SRVC_AREA_BASE', 'D_BRONZE.SADB.TRKFCG_SRVC_AREA', 'D_RAW.SADB.TRKFCG_SRVC_AREA_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRKFCG_SRVC_AREA', 'D_RAW.SADB.TASK_PROCESS_TRKFCG_SRVC_AREA'),
            ('CTNAPP_CTNG_LINE_DN', 'D_RAW.SADB.CTNAPP_CTNG_LINE_DN_BASE', 'D_BRONZE.SADB.CTNAPP_CTNG_LINE_DN', 'D_RAW.SADB.CTNAPP_CTNG_LINE_DN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_CTNAPP_CTNG_LINE_DN', 'D_RAW.SADB.TASK_PROCESS_CTNAPP_CTNG_LINE_DN')
        ) AS t(TABLE_NAME, SOURCE_TABLE, TARGET_TABLE, STREAM_NAME, SP_NAME, TASK_NAME)
        WHERE (P_TABLE_FILTER = 'ALL' OR TABLE_NAME = P_TABLE_FILTER);

    v_table_name VARCHAR;
    v_source_table VARCHAR;
    v_target_table VARCHAR;
    v_stream_name VARCHAR;
    v_sp_name VARCHAR;
    v_task_name VARCHAR;

BEGIN
    -- Validate rollback mode
    IF (v_mode NOT IN ('FULL', 'SOFT', 'TASK_ONLY')) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'ERROR',
            'message', 'Invalid ROLLBACK_MODE. Use FULL, SOFT, or TASK_ONLY',
            'timestamp', CURRENT_TIMESTAMP()
        );
    END IF;

    -- Log start
    v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
        'action', 'ROLLBACK_START',
        'mode', v_mode,
        'dry_run', P_DRY_RUN,
        'filter', P_TABLE_FILTER,
        'timestamp', v_start_time
    ));

    -- Process each table
    FOR rec IN c_objects DO
        v_table_name := rec.TABLE_NAME;
        v_source_table := rec.SOURCE_TABLE;
        v_target_table := rec.TARGET_TABLE;
        v_stream_name := rec.STREAM_NAME;
        v_sp_name := rec.SP_NAME;
        v_task_name := rec.TASK_NAME;

        -- =====================================================================
        -- STEP 1: SUSPEND AND DROP TASK (All modes)
        -- =====================================================================
        BEGIN
            IF (NOT P_DRY_RUN) THEN
                EXECUTE IMMEDIATE 'ALTER TASK IF EXISTS ' || v_task_name || ' SUSPEND';
                EXECUTE IMMEDIATE 'DROP TASK IF EXISTS ' || v_task_name;
            END IF;
            v_task_count := v_task_count + 1;
            v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                'action', 'DROP_TASK',
                'object', v_task_name,
                'table', v_table_name,
                'status', 'SUCCESS',
                'dry_run', P_DRY_RUN
            ));
        EXCEPTION
            WHEN OTHER THEN
                v_error_count := v_error_count + 1;
                v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                    'action', 'DROP_TASK',
                    'object', v_task_name,
                    'table', v_table_name,
                    'status', 'ERROR',
                    'error', SQLERRM
                ));
        END;

        -- =====================================================================
        -- STEP 2: DROP STORED PROCEDURE (SOFT and FULL modes)
        -- =====================================================================
        IF (v_mode IN ('SOFT', 'FULL')) THEN
            BEGIN
                IF (NOT P_DRY_RUN) THEN
                    EXECUTE IMMEDIATE 'DROP PROCEDURE IF EXISTS ' || v_sp_name || '()';
                END IF;
                v_sp_count := v_sp_count + 1;
                v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                    'action', 'DROP_PROCEDURE',
                    'object', v_sp_name,
                    'table', v_table_name,
                    'status', 'SUCCESS',
                    'dry_run', P_DRY_RUN
                ));
            EXCEPTION
                WHEN OTHER THEN
                    v_error_count := v_error_count + 1;
                    v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                        'action', 'DROP_PROCEDURE',
                        'object', v_sp_name,
                        'table', v_table_name,
                        'status', 'ERROR',
                        'error', SQLERRM
                    ));
            END;

            -- =================================================================
            -- STEP 3: DROP STREAM (SOFT and FULL modes)
            -- =================================================================
            BEGIN
                IF (NOT P_DRY_RUN) THEN
                    EXECUTE IMMEDIATE 'DROP STREAM IF EXISTS ' || v_stream_name;
                END IF;
                v_stream_count := v_stream_count + 1;
                v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                    'action', 'DROP_STREAM',
                    'object', v_stream_name,
                    'table', v_table_name,
                    'status', 'SUCCESS',
                    'dry_run', P_DRY_RUN
                ));
            EXCEPTION
                WHEN OTHER THEN
                    v_error_count := v_error_count + 1;
                    v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                        'action', 'DROP_STREAM',
                        'object', v_stream_name,
                        'table', v_table_name,
                        'status', 'ERROR',
                        'error', SQLERRM
                    ));
            END;
        END IF;

        -- =====================================================================
        -- STEP 4: DROP TARGET TABLE (FULL mode only)
        -- =====================================================================
        IF (v_mode = 'FULL') THEN
            BEGIN
                IF (NOT P_DRY_RUN) THEN
                    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS ' || v_target_table;
                END IF;
                v_table_count := v_table_count + 1;
                v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                    'action', 'DROP_TABLE',
                    'object', v_target_table,
                    'table', v_table_name,
                    'status', 'SUCCESS',
                    'dry_run', P_DRY_RUN
                ));
            EXCEPTION
                WHEN OTHER THEN
                    v_error_count := v_error_count + 1;
                    v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                        'action', 'DROP_TABLE',
                        'object', v_target_table,
                        'table', v_table_name,
                        'status', 'ERROR',
                        'error', SQLERRM
                    ));
            END;

            -- =================================================================
            -- STEP 5: DISABLE CHANGE TRACKING (FULL mode only)
            -- =================================================================
            BEGIN
                IF (NOT P_DRY_RUN) THEN
                    EXECUTE IMMEDIATE 'ALTER TABLE IF EXISTS ' || v_source_table || ' SET CHANGE_TRACKING = FALSE';
                END IF;
                v_ct_count := v_ct_count + 1;
                v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                    'action', 'DISABLE_CHANGE_TRACKING',
                    'object', v_source_table,
                    'table', v_table_name,
                    'status', 'SUCCESS',
                    'dry_run', P_DRY_RUN
                ));
            EXCEPTION
                WHEN OTHER THEN
                    v_error_count := v_error_count + 1;
                    v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                        'action', 'DISABLE_CHANGE_TRACKING',
                        'object', v_source_table,
                        'table', v_table_name,
                        'status', 'ERROR',
                        'error', SQLERRM
                    ));
            END;
        END IF;
    END FOR;

    -- Log completion
    v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
        'action', 'ROLLBACK_COMPLETE',
        'mode', v_mode,
        'dry_run', P_DRY_RUN,
        'filter', P_TABLE_FILTER,
        'tasks_dropped', v_task_count,
        'procedures_dropped', v_sp_count,
        'streams_dropped', v_stream_count,
        'tables_dropped', v_table_count,
        'change_tracking_disabled', v_ct_count,
        'errors', v_error_count,
        'start_time', v_start_time,
        'end_time', CURRENT_TIMESTAMP(),
        'duration_seconds', DATEDIFF('second', v_start_time, CURRENT_TIMESTAMP())
    ));

    RETURN OBJECT_CONSTRUCT(
        'status', IFF(v_error_count = 0, 'SUCCESS', 'COMPLETED_WITH_ERRORS'),
        'mode', v_mode,
        'dry_run', P_DRY_RUN,
        'summary', OBJECT_CONSTRUCT(
            'tasks_dropped', v_task_count,
            'procedures_dropped', v_sp_count,
            'streams_dropped', v_stream_count,
            'tables_dropped', v_table_count,
            'change_tracking_disabled', v_ct_count,
            'errors', v_error_count
        ),
        'details', v_result
    );

EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'FATAL_ERROR',
            'error', SQLERRM,
            'timestamp', CURRENT_TIMESTAMP()
        );
END;
$$;

-- =============================================================================
-- STEP 2: GRANT EXECUTE ON ROLLBACK PROCEDURE
-- =============================================================================
GRANT USAGE ON PROCEDURE D_RAW.SADB.SP_CDC_ROLLBACK(VARCHAR, BOOLEAN, VARCHAR) TO ROLE "D-SNW-DEVBI1-ETL";

-- =============================================================================
-- USAGE EXAMPLES
-- =============================================================================

/*
================================================================================
EXAMPLE 1: DRY RUN - Preview what will be rolled back (RECOMMENDED FIRST STEP)
================================================================================
-- Preview SOFT rollback for all tables
CALL D_RAW.SADB.SP_CDC_ROLLBACK('SOFT', TRUE, 'ALL');

-- Preview FULL rollback for all tables
CALL D_RAW.SADB.SP_CDC_ROLLBACK('FULL', TRUE, 'ALL');

-- Preview rollback for single table
CALL D_RAW.SADB.SP_CDC_ROLLBACK('SOFT', TRUE, 'OPTRN');

================================================================================
EXAMPLE 2: TASK_ONLY MODE - Suspend automation only (safest option)
================================================================================
-- Stop all tasks but preserve everything else
CALL D_RAW.SADB.SP_CDC_ROLLBACK('TASK_ONLY', FALSE, 'ALL');

-- Stop task for single table
CALL D_RAW.SADB.SP_CDC_ROLLBACK('TASK_ONLY', FALSE, 'OPTRN_EVENT');

================================================================================
EXAMPLE 3: SOFT MODE - Remove automation but preserve data
================================================================================
-- Remove Tasks, SPs, Streams - keep target tables with data
CALL D_RAW.SADB.SP_CDC_ROLLBACK('SOFT', FALSE, 'ALL');

-- Soft rollback for single table
CALL D_RAW.SADB.SP_CDC_ROLLBACK('SOFT', FALSE, 'TRAIN_PLAN');

================================================================================
EXAMPLE 4: FULL MODE - Complete removal (CAUTION: DATA LOSS)
================================================================================
-- FULL rollback - removes ALL objects including target tables
-- WARNING: This will DELETE ALL PRESERVED DATA
CALL D_RAW.SADB.SP_CDC_ROLLBACK('FULL', FALSE, 'ALL');

-- FULL rollback for single table
CALL D_RAW.SADB.SP_CDC_ROLLBACK('FULL', FALSE, 'LCMTV_MVMNT_EVENT');
*/

-- =============================================================================
-- QUICK ROLLBACK COMMANDS (Alternative to Stored Procedure)
-- =============================================================================

/*
-- TASK_ONLY: Suspend all tasks immediately
ALTER TASK D_RAW.SADB.TASK_PROCESS_OPTRN SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_OPTRN_LEG SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_OPTRN_EVENT SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_TRAIN_PLAN SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_TRAIN_PLAN_LEG SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_TRAIN_PLAN_EVENT SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_LCMTV_MVMNT_EVENT SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_EQPMV_RFEQP_MVMNT_EVENT SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_EQPMV_EQPMT_EVENT_TYPE SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_TRAIN_CNST_SMRY SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_TRAIN_CNST_DTL_RAIL_EQPT SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_TRKFC_TRSTN SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_EQPMNT_AAR_BASE SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_STNWYB_MSG_DN SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_LCMTV_EMIS SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_TRKFCG_FIXED_PLANT_ASSET SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_TRKFCG_FXPLA_TRACK_LCTN_DN SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_TRKFCG_TRACK_SGMNT_DN SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_TRKFCG_SBDVSN SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_TRKFCG_SRVC_AREA SUSPEND;
ALTER TASK D_RAW.SADB.TASK_PROCESS_CTNAPP_CTNG_LINE_DN SUSPEND;
*/

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================

/*
-- Check task status after rollback
SELECT 
    NAME,
    STATE,
    SCHEDULE,
    LAST_COMMITTED_ON,
    LAST_SUSPENDED_ON
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME LIKE 'TASK_PROCESS_%'
AND DATABASE_NAME = 'D_RAW'
ORDER BY NAME;

-- Check remaining streams
SHOW STREAMS LIKE '%_HIST_STREAM' IN SCHEMA D_RAW.SADB;

-- Check remaining procedures
SHOW PROCEDURES LIKE 'SP_PROCESS_%' IN SCHEMA D_RAW.SADB;

-- Check target tables (should exist if SOFT mode used)
SHOW TABLES LIKE '%' IN SCHEMA D_BRONZE.SADB;
*/

-- =============================================================================
-- END OF ROLLBACK SCRIPT
-- =============================================================================
