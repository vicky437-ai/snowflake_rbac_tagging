/*
================================================================================
CDC DATA PRESERVATION - PRE-DEPLOYMENT VALIDATION SCRIPT
================================================================================
Purpose      : Validate all CDC objects exist and are properly configured
               before executing rollback or any maintenance operations
Version      : 1.0
Created Date : February 23, 2026
Total Objects: 21 Tables × 5 Object Types = 105 Objects to Validate

VALIDATION CATEGORIES:
  1. Tasks (21)           - Verify existence and state
  2. Stored Procedures (21) - Verify existence
  3. Streams (21)         - Verify existence and state
  4. Target Tables (21)   - Verify existence and row counts
  5. Source Tables (21)   - Verify change tracking enabled
================================================================================
*/

-- =============================================================================
-- STEP 1: CREATE PRE-DEPLOYMENT VALIDATION STORED PROCEDURE
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_RAW.SADB.SP_CDC_PREDEPLOYMENT_VALIDATION(
    P_VALIDATION_TYPE VARCHAR DEFAULT 'ALL',
    P_TABLE_FILTER VARCHAR DEFAULT 'ALL'
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_result ARRAY DEFAULT ARRAY_CONSTRUCT();
    v_summary OBJECT;
    v_start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    
    -- Counters
    v_tasks_found NUMBER DEFAULT 0;
    v_tasks_running NUMBER DEFAULT 0;
    v_tasks_suspended NUMBER DEFAULT 0;
    v_tasks_missing NUMBER DEFAULT 0;
    
    v_sps_found NUMBER DEFAULT 0;
    v_sps_missing NUMBER DEFAULT 0;
    
    v_streams_found NUMBER DEFAULT 0;
    v_streams_stale NUMBER DEFAULT 0;
    v_streams_missing NUMBER DEFAULT 0;
    
    v_target_tables_found NUMBER DEFAULT 0;
    v_target_tables_missing NUMBER DEFAULT 0;
    v_target_total_rows NUMBER DEFAULT 0;
    
    v_source_ct_enabled NUMBER DEFAULT 0;
    v_source_ct_disabled NUMBER DEFAULT 0;
    
    v_total_checks NUMBER DEFAULT 0;
    v_total_passed NUMBER DEFAULT 0;
    v_total_failed NUMBER DEFAULT 0;
    v_total_warnings NUMBER DEFAULT 0;
    
    v_sql VARCHAR;
    v_count NUMBER;
    v_state VARCHAR;
    v_row_count NUMBER;
    
    -- Object configuration cursor
    c_objects CURSOR FOR
        SELECT * FROM (
            VALUES 
            (1, 'OPTRN', 'D_RAW.SADB.OPTRN_BASE', 'D_BRONZE.SADB.OPTRN', 'D_RAW.SADB.OPTRN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_OPTRN', 'D_RAW.SADB.TASK_PROCESS_OPTRN', 'Single'),
            (2, 'OPTRN_LEG', 'D_RAW.SADB.OPTRN_LEG_BASE', 'D_BRONZE.SADB.OPTRN_LEG', 'D_RAW.SADB.OPTRN_LEG_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_OPTRN_LEG', 'D_RAW.SADB.TASK_PROCESS_OPTRN_LEG', 'Single'),
            (3, 'OPTRN_EVENT', 'D_RAW.SADB.OPTRN_EVENT_BASE', 'D_BRONZE.SADB.OPTRN_EVENT', 'D_RAW.SADB.OPTRN_EVENT_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_OPTRN_EVENT', 'D_RAW.SADB.TASK_PROCESS_OPTRN_EVENT', 'Single'),
            (4, 'TRAIN_PLAN', 'D_RAW.SADB.TRAIN_PLAN_BASE', 'D_BRONZE.SADB.TRAIN_PLAN', 'D_RAW.SADB.TRAIN_PLAN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRAIN_PLAN', 'D_RAW.SADB.TASK_PROCESS_TRAIN_PLAN', 'Single'),
            (5, 'TRAIN_PLAN_LEG', 'D_RAW.SADB.TRAIN_PLAN_LEG_BASE', 'D_BRONZE.SADB.TRAIN_PLAN_LEG', 'D_RAW.SADB.TRAIN_PLAN_LEG_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_LEG', 'D_RAW.SADB.TASK_PROCESS_TRAIN_PLAN_LEG', 'Single'),
            (6, 'TRAIN_PLAN_EVENT', 'D_RAW.SADB.TRAIN_PLAN_EVENT_BASE', 'D_BRONZE.SADB.TRAIN_PLAN_EVENT', 'D_RAW.SADB.TRAIN_PLAN_EVENT_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_EVENT', 'D_RAW.SADB.TASK_PROCESS_TRAIN_PLAN_EVENT', 'Single'),
            (7, 'LCMTV_MVMNT_EVENT', 'D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE', 'D_BRONZE.SADB.LCMTV_MVMNT_EVENT', 'D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_LCMTV_MVMNT_EVENT', 'D_RAW.SADB.TASK_PROCESS_LCMTV_MVMNT_EVENT', 'Single'),
            (8, 'EQPMV_RFEQP_MVMNT_EVENT', 'D_RAW.SADB.EQPMV_RFEQP_MVMNT_EVENT_BASE', 'D_BRONZE.SADB.EQPMV_RFEQP_MVMNT_EVENT', 'D_RAW.SADB.EQPMV_RFEQP_MVMNT_EVENT_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_EQPMV_RFEQP_MVMNT_EVENT', 'D_RAW.SADB.TASK_PROCESS_EQPMV_RFEQP_MVMNT_EVENT', 'Single'),
            (9, 'EQPMV_EQPMT_EVENT_TYPE', 'D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE', 'D_BRONZE.SADB.EQPMV_EQPMT_EVENT_TYPE', 'D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_EQPMV_EQPMT_EVENT_TYPE', 'D_RAW.SADB.TASK_PROCESS_EQPMV_EQPMT_EVENT_TYPE', 'Single'),
            (10, 'TRAIN_CNST_SMRY', 'D_RAW.SADB.TRAIN_CNST_SMRY_BASE', 'D_BRONZE.SADB.TRAIN_CNST_SMRY', 'D_RAW.SADB.TRAIN_CNST_SMRY_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRAIN_CNST_SMRY', 'D_RAW.SADB.TASK_PROCESS_TRAIN_CNST_SMRY', 'Composite(2)'),
            (11, 'TRAIN_CNST_DTL_RAIL_EQPT', 'D_RAW.SADB.TRAIN_CNST_DTL_RAIL_EQPT_BASE', 'D_BRONZE.SADB.TRAIN_CNST_DTL_RAIL_EQPT', 'D_RAW.SADB.TRAIN_CNST_DTL_RAIL_EQPT_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRAIN_CNST_DTL_RAIL_EQPT', 'D_RAW.SADB.TASK_PROCESS_TRAIN_CNST_DTL_RAIL_EQPT', 'Composite(3)'),
            (12, 'TRKFC_TRSTN', 'D_RAW.SADB.TRKFC_TRSTN_BASE', 'D_BRONZE.SADB.TRKFC_TRSTN', 'D_RAW.SADB.TRKFC_TRSTN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRKFC_TRSTN', 'D_RAW.SADB.TASK_PROCESS_TRKFC_TRSTN', 'Composite(2)'),
            (13, 'EQPMNT_AAR_BASE', 'D_RAW.SADB.EQPMNT_AAR_BASE_BASE', 'D_BRONZE.SADB.EQPMNT_AAR_BASE', 'D_RAW.SADB.EQPMNT_AAR_BASE_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_EQPMNT_AAR_BASE', 'D_RAW.SADB.TASK_PROCESS_EQPMNT_AAR_BASE', 'Single'),
            (14, 'STNWYB_MSG_DN', 'D_RAW.SADB.STNWYB_MSG_DN_BASE', 'D_BRONZE.SADB.STNWYB_MSG_DN', 'D_RAW.SADB.STNWYB_MSG_DN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_STNWYB_MSG_DN', 'D_RAW.SADB.TASK_PROCESS_STNWYB_MSG_DN', 'Single'),
            (15, 'LCMTV_EMIS', 'D_RAW.SADB.LCMTV_EMIS_BASE', 'D_BRONZE.SADB.LCMTV_EMIS', 'D_RAW.SADB.LCMTV_EMIS_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_LCMTV_EMIS', 'D_RAW.SADB.TASK_PROCESS_LCMTV_EMIS', 'Composite(2)'),
            (16, 'TRKFCG_FIXED_PLANT_ASSET', 'D_RAW.SADB.TRKFCG_FIXED_PLANT_ASSET_BASE', 'D_BRONZE.SADB.TRKFCG_FIXED_PLANT_ASSET', 'D_RAW.SADB.TRKFCG_FIXED_PLANT_ASSET_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRKFCG_FIXED_PLANT_ASSET', 'D_RAW.SADB.TASK_PROCESS_TRKFCG_FIXED_PLANT_ASSET', 'Single'),
            (17, 'TRKFCG_FXPLA_TRACK_LCTN_DN', 'D_RAW.SADB.TRKFCG_FXPLA_TRACK_LCTN_DN_BASE', 'D_BRONZE.SADB.TRKFCG_FXPLA_TRACK_LCTN_DN', 'D_RAW.SADB.TRKFCG_FXPLA_TRACK_LCTN_DN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRKFCG_FXPLA_TRACK_LCTN_DN', 'D_RAW.SADB.TASK_PROCESS_TRKFCG_FXPLA_TRACK_LCTN_DN', 'Single'),
            (18, 'TRKFCG_TRACK_SGMNT_DN', 'D_RAW.SADB.TRKFCG_TRACK_SGMNT_DN_BASE', 'D_BRONZE.SADB.TRKFCG_TRACK_SGMNT_DN', 'D_RAW.SADB.TRKFCG_TRACK_SGMNT_DN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRKFCG_TRACK_SGMNT_DN', 'D_RAW.SADB.TASK_PROCESS_TRKFCG_TRACK_SGMNT_DN', 'Single'),
            (19, 'TRKFCG_SBDVSN', 'D_RAW.SADB.TRKFCG_SBDVSN_BASE', 'D_BRONZE.SADB.TRKFCG_SBDVSN', 'D_RAW.SADB.TRKFCG_SBDVSN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRKFCG_SBDVSN', 'D_RAW.SADB.TASK_PROCESS_TRKFCG_SBDVSN', 'Single'),
            (20, 'TRKFCG_SRVC_AREA', 'D_RAW.SADB.TRKFCG_SRVC_AREA_BASE', 'D_BRONZE.SADB.TRKFCG_SRVC_AREA', 'D_RAW.SADB.TRKFCG_SRVC_AREA_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_TRKFCG_SRVC_AREA', 'D_RAW.SADB.TASK_PROCESS_TRKFCG_SRVC_AREA', 'Single'),
            (21, 'CTNAPP_CTNG_LINE_DN', 'D_RAW.SADB.CTNAPP_CTNG_LINE_DN_BASE', 'D_BRONZE.SADB.CTNAPP_CTNG_LINE_DN', 'D_RAW.SADB.CTNAPP_CTNG_LINE_DN_BASE_HIST_STREAM', 'D_RAW.SADB.SP_PROCESS_CTNAPP_CTNG_LINE_DN', 'D_RAW.SADB.TASK_PROCESS_CTNAPP_CTNG_LINE_DN', 'Single')
        ) AS t(SEQ, TABLE_NAME, SOURCE_TABLE, TARGET_TABLE, STREAM_NAME, SP_NAME, TASK_NAME, PK_TYPE)
        WHERE (P_TABLE_FILTER = 'ALL' OR TABLE_NAME = P_TABLE_FILTER)
        ORDER BY SEQ;

    v_seq NUMBER;
    v_table_name VARCHAR;
    v_source_table VARCHAR;
    v_target_table VARCHAR;
    v_stream_name VARCHAR;
    v_sp_name VARCHAR;
    v_task_name VARCHAR;
    v_pk_type VARCHAR;
    v_check_status VARCHAR;
    v_check_detail VARCHAR;

BEGIN
    -- =========================================================================
    -- VALIDATION HEADER
    -- =========================================================================
    v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
        'section', 'VALIDATION_START',
        'validation_type', P_VALIDATION_TYPE,
        'table_filter', P_TABLE_FILTER,
        'timestamp', v_start_time,
        'executed_by', CURRENT_USER(),
        'role', CURRENT_ROLE()
    ));

    -- =========================================================================
    -- PROCESS EACH TABLE CONFIGURATION
    -- =========================================================================
    FOR rec IN c_objects DO
        v_seq := rec.SEQ;
        v_table_name := rec.TABLE_NAME;
        v_source_table := rec.SOURCE_TABLE;
        v_target_table := rec.TARGET_TABLE;
        v_stream_name := rec.STREAM_NAME;
        v_sp_name := rec.SP_NAME;
        v_task_name := rec.TASK_NAME;
        v_pk_type := rec.PK_TYPE;

        -- =====================================================================
        -- CHECK 1: TASK EXISTS AND STATE
        -- =====================================================================
        IF (P_VALIDATION_TYPE IN ('ALL', 'TASKS')) THEN
            v_total_checks := v_total_checks + 1;
            BEGIN
                SELECT COUNT(*), MAX(STATE) INTO v_count, v_state
                FROM D_RAW.INFORMATION_SCHEMA.TASKS
                WHERE TASK_SCHEMA = 'SADB' 
                AND TASK_NAME = SPLIT_PART(v_task_name, '.', 3);
                
                IF (v_count > 0) THEN
                    v_tasks_found := v_tasks_found + 1;
                    IF (v_state = 'started') THEN
                        v_tasks_running := v_tasks_running + 1;
                        v_check_status := 'PASS';
                        v_check_detail := 'Task exists and is RUNNING';
                        v_total_passed := v_total_passed + 1;
                    ELSE
                        v_tasks_suspended := v_tasks_suspended + 1;
                        v_check_status := 'WARNING';
                        v_check_detail := 'Task exists but is SUSPENDED';
                        v_total_warnings := v_total_warnings + 1;
                    END IF;
                ELSE
                    v_tasks_missing := v_tasks_missing + 1;
                    v_check_status := 'FAIL';
                    v_check_detail := 'Task NOT FOUND';
                    v_total_failed := v_total_failed + 1;
                END IF;
                
                v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                    'seq', v_seq,
                    'table', v_table_name,
                    'check_type', 'TASK',
                    'object', v_task_name,
                    'status', v_check_status,
                    'state', v_state,
                    'detail', v_check_detail
                ));
            EXCEPTION
                WHEN OTHER THEN
                    v_tasks_missing := v_tasks_missing + 1;
                    v_total_failed := v_total_failed + 1;
                    v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                        'seq', v_seq,
                        'table', v_table_name,
                        'check_type', 'TASK',
                        'object', v_task_name,
                        'status', 'ERROR',
                        'detail', SQLERRM
                    ));
            END;
        END IF;

        -- =====================================================================
        -- CHECK 2: STORED PROCEDURE EXISTS
        -- =====================================================================
        IF (P_VALIDATION_TYPE IN ('ALL', 'PROCEDURES')) THEN
            v_total_checks := v_total_checks + 1;
            BEGIN
                SELECT COUNT(*) INTO v_count
                FROM D_RAW.INFORMATION_SCHEMA.PROCEDURES
                WHERE PROCEDURE_SCHEMA = 'SADB' 
                AND PROCEDURE_NAME = SPLIT_PART(v_sp_name, '.', 3);
                
                IF (v_count > 0) THEN
                    v_sps_found := v_sps_found + 1;
                    v_check_status := 'PASS';
                    v_check_detail := 'Stored Procedure exists';
                    v_total_passed := v_total_passed + 1;
                ELSE
                    v_sps_missing := v_sps_missing + 1;
                    v_check_status := 'FAIL';
                    v_check_detail := 'Stored Procedure NOT FOUND';
                    v_total_failed := v_total_failed + 1;
                END IF;
                
                v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                    'seq', v_seq,
                    'table', v_table_name,
                    'check_type', 'PROCEDURE',
                    'object', v_sp_name,
                    'status', v_check_status,
                    'detail', v_check_detail
                ));
            EXCEPTION
                WHEN OTHER THEN
                    v_sps_missing := v_sps_missing + 1;
                    v_total_failed := v_total_failed + 1;
                    v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                        'seq', v_seq,
                        'table', v_table_name,
                        'check_type', 'PROCEDURE',
                        'object', v_sp_name,
                        'status', 'ERROR',
                        'detail', SQLERRM
                    ));
            END;
        END IF;

        -- =====================================================================
        -- CHECK 3: STREAM EXISTS AND STATE
        -- =====================================================================
        IF (P_VALIDATION_TYPE IN ('ALL', 'STREAMS')) THEN
            v_total_checks := v_total_checks + 1;
            BEGIN
                SELECT COUNT(*), MAX(STALE) INTO v_count, v_state
                FROM D_RAW.INFORMATION_SCHEMA.STREAMS
                WHERE STREAM_SCHEMA = 'SADB' 
                AND STREAM_NAME = SPLIT_PART(v_stream_name, '.', 3);
                
                IF (v_count > 0) THEN
                    v_streams_found := v_streams_found + 1;
                    IF (v_state = 'false' OR v_state IS NULL) THEN
                        v_check_status := 'PASS';
                        v_check_detail := 'Stream exists and is ACTIVE';
                        v_total_passed := v_total_passed + 1;
                    ELSE
                        v_streams_stale := v_streams_stale + 1;
                        v_check_status := 'WARNING';
                        v_check_detail := 'Stream exists but is STALE';
                        v_total_warnings := v_total_warnings + 1;
                    END IF;
                ELSE
                    v_streams_missing := v_streams_missing + 1;
                    v_check_status := 'FAIL';
                    v_check_detail := 'Stream NOT FOUND';
                    v_total_failed := v_total_failed + 1;
                END IF;
                
                v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                    'seq', v_seq,
                    'table', v_table_name,
                    'check_type', 'STREAM',
                    'object', v_stream_name,
                    'status', v_check_status,
                    'stale', v_state,
                    'detail', v_check_detail
                ));
            EXCEPTION
                WHEN OTHER THEN
                    v_streams_missing := v_streams_missing + 1;
                    v_total_failed := v_total_failed + 1;
                    v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                        'seq', v_seq,
                        'table', v_table_name,
                        'check_type', 'STREAM',
                        'object', v_stream_name,
                        'status', 'ERROR',
                        'detail', SQLERRM
                    ));
            END;
        END IF;

        -- =====================================================================
        -- CHECK 4: TARGET TABLE EXISTS AND ROW COUNT
        -- =====================================================================
        IF (P_VALIDATION_TYPE IN ('ALL', 'TABLES')) THEN
            v_total_checks := v_total_checks + 1;
            BEGIN
                EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_target_table INTO v_row_count;
                
                v_target_tables_found := v_target_tables_found + 1;
                v_target_total_rows := v_target_total_rows + v_row_count;
                v_check_status := 'PASS';
                v_check_detail := 'Target table exists with ' || v_row_count || ' rows';
                v_total_passed := v_total_passed + 1;
                
                v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                    'seq', v_seq,
                    'table', v_table_name,
                    'check_type', 'TARGET_TABLE',
                    'object', v_target_table,
                    'status', v_check_status,
                    'row_count', v_row_count,
                    'pk_type', v_pk_type,
                    'detail', v_check_detail
                ));
            EXCEPTION
                WHEN OTHER THEN
                    v_target_tables_missing := v_target_tables_missing + 1;
                    v_total_failed := v_total_failed + 1;
                    v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                        'seq', v_seq,
                        'table', v_table_name,
                        'check_type', 'TARGET_TABLE',
                        'object', v_target_table,
                        'status', 'FAIL',
                        'detail', 'Target table NOT FOUND or inaccessible: ' || SQLERRM
                    ));
            END;
        END IF;

        -- =====================================================================
        -- CHECK 5: SOURCE TABLE CHANGE TRACKING
        -- =====================================================================
        IF (P_VALIDATION_TYPE IN ('ALL', 'SOURCE')) THEN
            v_total_checks := v_total_checks + 1;
            BEGIN
                SELECT COUNT(*) INTO v_count
                FROM D_RAW.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'SADB' 
                AND TABLE_NAME = SPLIT_PART(v_source_table, '.', 3)
                AND CHANGE_TRACKING = 'ON';
                
                IF (v_count > 0) THEN
                    v_source_ct_enabled := v_source_ct_enabled + 1;
                    v_check_status := 'PASS';
                    v_check_detail := 'Change tracking is ENABLED';
                    v_total_passed := v_total_passed + 1;
                ELSE
                    v_source_ct_disabled := v_source_ct_disabled + 1;
                    v_check_status := 'WARNING';
                    v_check_detail := 'Change tracking is DISABLED or table not found';
                    v_total_warnings := v_total_warnings + 1;
                END IF;
                
                v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                    'seq', v_seq,
                    'table', v_table_name,
                    'check_type', 'SOURCE_CHANGE_TRACKING',
                    'object', v_source_table,
                    'status', v_check_status,
                    'detail', v_check_detail
                ));
            EXCEPTION
                WHEN OTHER THEN
                    v_source_ct_disabled := v_source_ct_disabled + 1;
                    v_total_warnings := v_total_warnings + 1;
                    v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
                        'seq', v_seq,
                        'table', v_table_name,
                        'check_type', 'SOURCE_CHANGE_TRACKING',
                        'object', v_source_table,
                        'status', 'WARNING',
                        'detail', 'Unable to verify: ' || SQLERRM
                    ));
            END;
        END IF;

    END FOR;

    -- =========================================================================
    -- BUILD SUMMARY
    -- =========================================================================
    v_summary := OBJECT_CONSTRUCT(
        'total_checks', v_total_checks,
        'passed', v_total_passed,
        'failed', v_total_failed,
        'warnings', v_total_warnings,
        'pass_rate', ROUND((v_total_passed / NULLIF(v_total_checks, 0)) * 100, 2),
        'tasks', OBJECT_CONSTRUCT(
            'found', v_tasks_found,
            'running', v_tasks_running,
            'suspended', v_tasks_suspended,
            'missing', v_tasks_missing
        ),
        'procedures', OBJECT_CONSTRUCT(
            'found', v_sps_found,
            'missing', v_sps_missing
        ),
        'streams', OBJECT_CONSTRUCT(
            'found', v_streams_found,
            'stale', v_streams_stale,
            'missing', v_streams_missing
        ),
        'target_tables', OBJECT_CONSTRUCT(
            'found', v_target_tables_found,
            'missing', v_target_tables_missing,
            'total_rows', v_target_total_rows
        ),
        'source_change_tracking', OBJECT_CONSTRUCT(
            'enabled', v_source_ct_enabled,
            'disabled', v_source_ct_disabled
        )
    );

    -- =========================================================================
    -- DETERMINE OVERALL STATUS
    -- =========================================================================
    LET v_overall_status VARCHAR := CASE
        WHEN v_total_failed > 0 THEN 'FAIL'
        WHEN v_total_warnings > 0 THEN 'PASS_WITH_WARNINGS'
        ELSE 'PASS'
    END;

    LET v_ready_for_rollback BOOLEAN := (v_total_failed = 0);

    -- Add completion entry
    v_result := ARRAY_APPEND(v_result, OBJECT_CONSTRUCT(
        'section', 'VALIDATION_COMPLETE',
        'overall_status', v_overall_status,
        'ready_for_rollback', v_ready_for_rollback,
        'duration_seconds', DATEDIFF('second', v_start_time, CURRENT_TIMESTAMP()),
        'timestamp', CURRENT_TIMESTAMP()
    ));

    RETURN OBJECT_CONSTRUCT(
        'status', v_overall_status,
        'ready_for_rollback', v_ready_for_rollback,
        'summary', v_summary,
        'recommendation', CASE
            WHEN v_total_failed > 0 THEN 'DO NOT proceed with rollback. Fix failed checks first.'
            WHEN v_total_warnings > 0 THEN 'Review warnings before proceeding with rollback.'
            ELSE 'All checks passed. Safe to proceed with rollback.'
        END,
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
-- STEP 2: GRANT EXECUTE PERMISSION
-- =============================================================================
GRANT USAGE ON PROCEDURE D_RAW.SADB.SP_CDC_PREDEPLOYMENT_VALIDATION(VARCHAR, VARCHAR) TO ROLE "D-SNW-DEVBI1-ETL";

-- =============================================================================
-- USAGE EXAMPLES
-- =============================================================================

/*
================================================================================
EXAMPLE 1: FULL VALIDATION (Recommended before any rollback)
================================================================================
CALL D_RAW.SADB.SP_CDC_PREDEPLOYMENT_VALIDATION('ALL', 'ALL');

================================================================================
EXAMPLE 2: VALIDATE SPECIFIC OBJECT TYPES
================================================================================
-- Check only tasks
CALL D_RAW.SADB.SP_CDC_PREDEPLOYMENT_VALIDATION('TASKS', 'ALL');

-- Check only stored procedures
CALL D_RAW.SADB.SP_CDC_PREDEPLOYMENT_VALIDATION('PROCEDURES', 'ALL');

-- Check only streams
CALL D_RAW.SADB.SP_CDC_PREDEPLOYMENT_VALIDATION('STREAMS', 'ALL');

-- Check only target tables
CALL D_RAW.SADB.SP_CDC_PREDEPLOYMENT_VALIDATION('TABLES', 'ALL');

-- Check only source change tracking
CALL D_RAW.SADB.SP_CDC_PREDEPLOYMENT_VALIDATION('SOURCE', 'ALL');

================================================================================
EXAMPLE 3: VALIDATE SINGLE TABLE
================================================================================
CALL D_RAW.SADB.SP_CDC_PREDEPLOYMENT_VALIDATION('ALL', 'OPTRN');
CALL D_RAW.SADB.SP_CDC_PREDEPLOYMENT_VALIDATION('ALL', 'TRAIN_PLAN');
CALL D_RAW.SADB.SP_CDC_PREDEPLOYMENT_VALIDATION('ALL', 'LCMTV_MVMNT_EVENT');
*/

-- =============================================================================
-- QUICK MANUAL VALIDATION QUERIES
-- =============================================================================

-- Check all tasks status
SELECT 
    TASK_NAME,
    STATE,
    SCHEDULE,
    CREATED,
    LAST_COMMITTED_ON
FROM D_RAW.INFORMATION_SCHEMA.TASKS
WHERE TASK_SCHEMA = 'SADB'
AND TASK_NAME LIKE 'TASK_PROCESS_%'
ORDER BY TASK_NAME;

-- Check all streams status
SELECT 
    STREAM_NAME,
    TABLE_NAME AS SOURCE_TABLE,
    STALE,
    CREATED
FROM D_RAW.INFORMATION_SCHEMA.STREAMS
WHERE STREAM_SCHEMA = 'SADB'
AND STREAM_NAME LIKE '%_HIST_STREAM'
ORDER BY STREAM_NAME;

-- Check all stored procedures
SELECT 
    PROCEDURE_NAME,
    ARGUMENT_SIGNATURE,
    CREATED
FROM D_RAW.INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA = 'SADB'
AND PROCEDURE_NAME LIKE 'SP_PROCESS_%'
ORDER BY PROCEDURE_NAME;

-- Check target tables and row counts
SELECT 
    TABLE_NAME,
    ROW_COUNT,
    CREATED,
    LAST_ALTERED
FROM D_BRONZE.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SADB'
AND TABLE_NAME IN (
    'OPTRN', 'OPTRN_LEG', 'OPTRN_EVENT', 'TRAIN_PLAN', 'TRAIN_PLAN_LEG',
    'TRAIN_PLAN_EVENT', 'LCMTV_MVMNT_EVENT', 'EQPMV_RFEQP_MVMNT_EVENT',
    'EQPMV_EQPMT_EVENT_TYPE', 'TRAIN_CNST_SMRY', 'TRAIN_CNST_DTL_RAIL_EQPT',
    'TRKFC_TRSTN', 'EQPMNT_AAR_BASE', 'STNWYB_MSG_DN', 'LCMTV_EMIS',
    'TRKFCG_FIXED_PLANT_ASSET', 'TRKFCG_FXPLA_TRACK_LCTN_DN',
    'TRKFCG_TRACK_SGMNT_DN', 'TRKFCG_SBDVSN', 'TRKFCG_SRVC_AREA',
    'CTNAPP_CTNG_LINE_DN'
)
ORDER BY TABLE_NAME;

-- =============================================================================
-- END OF PRE-DEPLOYMENT VALIDATION SCRIPT
-- =============================================================================
