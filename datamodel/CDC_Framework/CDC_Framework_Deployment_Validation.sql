/*
================================================================================
CDC DATA PRESERVATION FRAMEWORK - DEPLOYMENT VALIDATION SCRIPT
================================================================================
Purpose:     Validate all 10 stored procedures exist and are executable
Environment: Target Snowflake account after framework deployment
Usage:       Execute this script after running CDC_Data_Preservation_Framework.sql
================================================================================
*/

-- ============================================================================
-- SECTION 1: ENVIRONMENT SETUP
-- ============================================================================
USE ROLE ACCOUNTADMIN;  -- Or your deployment role
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- SECTION 2: CREATE VALIDATION PROCEDURE
-- ============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.MONITORING.SP_VALIDATE_DEPLOYMENT()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_results ARRAY := ARRAY_CONSTRUCT();
    v_errors ARRAY := ARRAY_CONSTRUCT();
    v_warnings ARRAY := ARRAY_CONSTRUCT();
    v_total_checks NUMBER := 0;
    v_passed_checks NUMBER := 0;
    v_failed_checks NUMBER := 0;
    v_proc_count NUMBER := 0;
    v_table_count NUMBER := 0;
    v_view_count NUMBER := 0;
    v_test_result VARCHAR;
BEGIN
    -- ========================================================================
    -- CHECK 1: Verify Database Exists
    -- ========================================================================
    v_total_checks := v_total_checks + 1;
    BEGIN
        SELECT COUNT(*) INTO v_proc_count FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME = 'CDC_PRESERVATION';
        IF (v_proc_count > 0) THEN
            v_passed_checks := v_passed_checks + 1;
            v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'Database CDC_PRESERVATION', 'status', 'PASSED'));
        ELSE
            v_failed_checks := v_failed_checks + 1;
            v_errors := ARRAY_APPEND(v_errors, 'Database CDC_PRESERVATION does not exist');
            v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'Database CDC_PRESERVATION', 'status', 'FAILED'));
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            v_failed_checks := v_failed_checks + 1;
            v_errors := ARRAY_APPEND(v_errors, 'Cannot access database: ' || SQLERRM);
    END;

    -- ========================================================================
    -- CHECK 2: Verify Schemas Exist
    -- ========================================================================
    FOR schema_name IN (SELECT VALUE::VARCHAR AS NAME FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT('CONFIG', 'PROCESSING', 'MONITORING')))) DO
        v_total_checks := v_total_checks + 1;
        BEGIN
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM CDC_PRESERVATION.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ''' || schema_name.NAME || '''';
            LET rs RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
            LET cur CURSOR FOR rs;
            LET cnt NUMBER;
            OPEN cur; FETCH cur INTO cnt; CLOSE cur;
            
            IF (cnt > 0) THEN
                v_passed_checks := v_passed_checks + 1;
                v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'Schema ' || schema_name.NAME, 'status', 'PASSED'));
            ELSE
                v_failed_checks := v_failed_checks + 1;
                v_errors := ARRAY_APPEND(v_errors, 'Schema ' || schema_name.NAME || ' does not exist');
                v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'Schema ' || schema_name.NAME, 'status', 'FAILED'));
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_failed_checks := v_failed_checks + 1;
                v_errors := ARRAY_APPEND(v_errors, 'Schema check failed: ' || SQLERRM);
        END;
    END FOR;

    -- ========================================================================
    -- CHECK 3: Verify Configuration Tables Exist
    -- ========================================================================
    FOR tbl IN (SELECT VALUE::VARCHAR AS NAME FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT('TABLE_CONFIG')))) DO
        v_total_checks := v_total_checks + 1;
        BEGIN
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM CDC_PRESERVATION.CONFIG.' || tbl.NAME || ' WHERE 1=0';
            v_passed_checks := v_passed_checks + 1;
            v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'Table CONFIG.' || tbl.NAME, 'status', 'PASSED'));
        EXCEPTION
            WHEN OTHER THEN
                v_failed_checks := v_failed_checks + 1;
                v_errors := ARRAY_APPEND(v_errors, 'Table CONFIG.' || tbl.NAME || ' does not exist or not accessible');
                v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'Table CONFIG.' || tbl.NAME, 'status', 'FAILED'));
        END;
    END FOR;

    -- ========================================================================
    -- CHECK 4: Verify Monitoring Tables Exist
    -- ========================================================================
    FOR tbl IN (SELECT VALUE::VARCHAR AS NAME FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT('PROCESSING_LOG', 'STREAM_STATUS')))) DO
        v_total_checks := v_total_checks + 1;
        BEGIN
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM CDC_PRESERVATION.MONITORING.' || tbl.NAME || ' WHERE 1=0';
            v_passed_checks := v_passed_checks + 1;
            v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'Table MONITORING.' || tbl.NAME, 'status', 'PASSED'));
        EXCEPTION
            WHEN OTHER THEN
                v_failed_checks := v_failed_checks + 1;
                v_errors := ARRAY_APPEND(v_errors, 'Table MONITORING.' || tbl.NAME || ' does not exist');
                v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'Table MONITORING.' || tbl.NAME, 'status', 'FAILED'));
        END;
    END FOR;

    -- ========================================================================
    -- CHECK 5: Verify All 10 Stored Procedures Exist
    -- ========================================================================
    FOR proc IN (SELECT VALUE::VARCHAR AS NAME FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT(
        'SP_PROCESS_CDC_GENERIC',
        'SP_CREATE_TARGET_TABLE',
        'SP_CREATE_STREAM',
        'SP_CREATE_TASK',
        'SP_RESUME_TASK',
        'SP_SUSPEND_TASK',
        'SP_RESUME_ALL_TASKS',
        'SP_SUSPEND_ALL_TASKS',
        'SP_SETUP_PIPELINE',
        'SP_SETUP_ALL_PIPELINES'
    )))) DO
        v_total_checks := v_total_checks + 1;
        BEGIN
            EXECUTE IMMEDIATE 'DESCRIBE PROCEDURE CDC_PRESERVATION.PROCESSING.' || proc.NAME || '(NUMBER)';
            v_passed_checks := v_passed_checks + 1;
            v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'Procedure ' || proc.NAME, 'status', 'PASSED', 'signature', 'NUMBER'));
        EXCEPTION
            WHEN OTHER THEN
                -- Try without parameters (for _ALL procedures)
                BEGIN
                    EXECUTE IMMEDIATE 'DESCRIBE PROCEDURE CDC_PRESERVATION.PROCESSING.' || proc.NAME || '()';
                    v_passed_checks := v_passed_checks + 1;
                    v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'Procedure ' || proc.NAME, 'status', 'PASSED', 'signature', 'NONE'));
                EXCEPTION
                    WHEN OTHER THEN
                        -- Try with BOOLEAN parameter
                        BEGIN
                            EXECUTE IMMEDIATE 'DESCRIBE PROCEDURE CDC_PRESERVATION.PROCESSING.' || proc.NAME || '(BOOLEAN)';
                            v_passed_checks := v_passed_checks + 1;
                            v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'Procedure ' || proc.NAME, 'status', 'PASSED', 'signature', 'BOOLEAN'));
                        EXCEPTION
                            WHEN OTHER THEN
                                -- Try with NUMBER, BOOLEAN
                                BEGIN
                                    EXECUTE IMMEDIATE 'DESCRIBE PROCEDURE CDC_PRESERVATION.PROCESSING.' || proc.NAME || '(NUMBER, BOOLEAN)';
                                    v_passed_checks := v_passed_checks + 1;
                                    v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'Procedure ' || proc.NAME, 'status', 'PASSED', 'signature', 'NUMBER, BOOLEAN'));
                                EXCEPTION
                                    WHEN OTHER THEN
                                        v_failed_checks := v_failed_checks + 1;
                                        v_errors := ARRAY_APPEND(v_errors, 'Procedure ' || proc.NAME || ' does not exist or has unexpected signature');
                                        v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'Procedure ' || proc.NAME, 'status', 'FAILED'));
                                END;
                        END;
                END;
        END;
    END FOR;

    -- ========================================================================
    -- CHECK 6: Verify Monitoring Views Exist (if created)
    -- ========================================================================
    FOR vw IN (SELECT VALUE::VARCHAR AS NAME FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT(
        'V_PIPELINE_STATUS',
        'V_PROCESSING_STATS', 
        'V_RECENT_ERRORS'
    )))) DO
        v_total_checks := v_total_checks + 1;
        BEGIN
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM CDC_PRESERVATION.MONITORING.' || vw.NAME || ' WHERE 1=0';
            v_passed_checks := v_passed_checks + 1;
            v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'View MONITORING.' || vw.NAME, 'status', 'PASSED'));
        EXCEPTION
            WHEN OTHER THEN
                v_warnings := ARRAY_APPEND(v_warnings, 'View ' || vw.NAME || ' not found (optional)');
                v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('check', 'View MONITORING.' || vw.NAME, 'status', 'WARNING', 'note', 'Optional view'));
        END;
    END FOR;

    -- ========================================================================
    -- RETURN FINAL RESULTS
    -- ========================================================================
    RETURN OBJECT_CONSTRUCT(
        'validation_timestamp', CURRENT_TIMESTAMP(),
        'summary', OBJECT_CONSTRUCT(
            'total_checks', v_total_checks,
            'passed', v_passed_checks,
            'failed', v_failed_checks,
            'pass_rate', ROUND(100.0 * v_passed_checks / v_total_checks, 2) || '%',
            'status', IFF(v_failed_checks = 0, 'DEPLOYMENT VALID ✓', 'DEPLOYMENT ISSUES FOUND ✗')
        ),
        'details', v_results,
        'errors', v_errors,
        'warnings', v_warnings
    );
END;
$$;

-- ============================================================================
-- SECTION 3: RUN VALIDATION
-- ============================================================================
CALL CDC_PRESERVATION.MONITORING.SP_VALIDATE_DEPLOYMENT();

-- ============================================================================
-- SECTION 4: ALTERNATIVE QUICK VALIDATION (Without Creating SP)
-- ============================================================================

-- Quick Check: Count all procedures in PROCESSING schema
SELECT 'PROCEDURE COUNT' AS CHECK_TYPE, COUNT(*) AS COUNT_FOUND, 10 AS EXPECTED,
       IFF(COUNT(*) >= 10, '✓ PASSED', '✗ FAILED') AS STATUS
FROM CDC_PRESERVATION.INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA = 'PROCESSING';

-- Quick Check: List all 10 required procedures
WITH REQUIRED_PROCS AS (
    SELECT VALUE::VARCHAR AS PROC_NAME
    FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT(
        'SP_PROCESS_CDC_GENERIC',
        'SP_CREATE_TARGET_TABLE',
        'SP_CREATE_STREAM',
        'SP_CREATE_TASK',
        'SP_RESUME_TASK',
        'SP_SUSPEND_TASK',
        'SP_RESUME_ALL_TASKS',
        'SP_SUSPEND_ALL_TASKS',
        'SP_SETUP_PIPELINE',
        'SP_SETUP_ALL_PIPELINES'
    )))
),
EXISTING_PROCS AS (
    SELECT DISTINCT PROCEDURE_NAME AS PROC_NAME
    FROM CDC_PRESERVATION.INFORMATION_SCHEMA.PROCEDURES
    WHERE PROCEDURE_SCHEMA = 'PROCESSING'
)
SELECT 
    r.PROC_NAME,
    IFF(e.PROC_NAME IS NOT NULL, '✓ EXISTS', '✗ MISSING') AS STATUS
FROM REQUIRED_PROCS r
LEFT JOIN EXISTING_PROCS e ON r.PROC_NAME = e.PROC_NAME
ORDER BY r.PROC_NAME;

-- Quick Check: Verify tables exist
SELECT 'CONFIG.TABLE_CONFIG' AS OBJECT, 
       IFF(COUNT(*) >= 0, '✓ EXISTS', '✗ MISSING') AS STATUS
FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE 1=0
UNION ALL
SELECT 'MONITORING.PROCESSING_LOG', 
       IFF(COUNT(*) >= 0, '✓ EXISTS', '✗ MISSING')
FROM CDC_PRESERVATION.MONITORING.PROCESSING_LOG WHERE 1=0
UNION ALL
SELECT 'MONITORING.STREAM_STATUS',
       IFF(COUNT(*) >= 0, '✓ EXISTS', '✗ MISSING')
FROM CDC_PRESERVATION.MONITORING.STREAM_STATUS WHERE 1=0;

-- ============================================================================
-- SECTION 5: EXECUTION TEST (Optional - Tests SP can run)
-- ============================================================================

-- Test SP_PROCESS_CDC_GENERIC with invalid config (should return error gracefully)
-- This tests the procedure is callable without affecting real data
SELECT 'SP_PROCESS_CDC_GENERIC Execution Test' AS TEST;
CALL CDC_PRESERVATION.PROCESSING.SP_PROCESS_CDC_GENERIC(-999);
-- Expected: Returns error message (no config found) - proves SP is executable

-- Test SP_RESUME_ALL_TASKS (dry run - no active configs)
SELECT 'SP_RESUME_ALL_TASKS Execution Test' AS TEST;
-- Only run if you want to test: CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_ALL_TASKS();

-- ============================================================================
-- SECTION 6: FULL DEPLOYMENT REPORT
-- ============================================================================
CREATE OR REPLACE TEMPORARY TABLE _DEPLOYMENT_VALIDATION_REPORT AS
WITH proc_check AS (
    SELECT 
        p.VALUE::VARCHAR AS REQUIRED_PROC,
        IFF(e.PROCEDURE_NAME IS NOT NULL, 'EXISTS', 'MISSING') AS STATUS
    FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT(
        'SP_PROCESS_CDC_GENERIC', 'SP_CREATE_TARGET_TABLE', 'SP_CREATE_STREAM',
        'SP_CREATE_TASK', 'SP_RESUME_TASK', 'SP_SUSPEND_TASK',
        'SP_RESUME_ALL_TASKS', 'SP_SUSPEND_ALL_TASKS', 
        'SP_SETUP_PIPELINE', 'SP_SETUP_ALL_PIPELINES'
    ))) p
    LEFT JOIN CDC_PRESERVATION.INFORMATION_SCHEMA.PROCEDURES e 
        ON p.VALUE::VARCHAR = e.PROCEDURE_NAME AND e.PROCEDURE_SCHEMA = 'PROCESSING'
)
SELECT 
    'STORED PROCEDURE' AS OBJECT_TYPE,
    REQUIRED_PROC AS OBJECT_NAME,
    STATUS,
    IFF(STATUS = 'EXISTS', '✓', '✗') AS INDICATOR
FROM proc_check;

-- Display final report
SELECT * FROM _DEPLOYMENT_VALIDATION_REPORT ORDER BY OBJECT_NAME;

-- Summary
SELECT 
    COUNT(*) AS TOTAL_PROCEDURES,
    SUM(IFF(STATUS = 'EXISTS', 1, 0)) AS FOUND,
    SUM(IFF(STATUS = 'MISSING', 1, 0)) AS MISSING,
    IFF(SUM(IFF(STATUS = 'MISSING', 1, 0)) = 0, 
        '✓ ALL 10 PROCEDURES DEPLOYED SUCCESSFULLY', 
        '✗ MISSING PROCEDURES - CHECK DEPLOYMENT') AS VALIDATION_RESULT
FROM _DEPLOYMENT_VALIDATION_REPORT;

-- ============================================================================
-- SECTION 7: PERMISSION VALIDATION
-- ============================================================================
-- Check if current role can execute procedures
SELECT 
    CURRENT_ROLE() AS CURRENT_ROLE,
    CURRENT_USER() AS CURRENT_USER,
    CURRENT_WAREHOUSE() AS CURRENT_WAREHOUSE,
    CURRENT_DATABASE() AS CURRENT_DATABASE;

-- Check grants on procedures
SHOW GRANTS ON SCHEMA CDC_PRESERVATION.PROCESSING;

-- ============================================================================
-- END OF VALIDATION SCRIPT
-- ============================================================================
/*
EXPECTED RESULTS:
================================================================================
1. SP_VALIDATE_DEPLOYMENT() returns JSON with:
   - summary.status = "DEPLOYMENT VALID ✓"
   - summary.failed = 0
   - summary.pass_rate = "100%"

2. Quick validation shows all 10 procedures with "✓ EXISTS"

3. All 3 tables (TABLE_CONFIG, PROCESSING_LOG, STREAM_STATUS) exist

4. SP_PROCESS_CDC_GENERIC(-999) returns error message (proves it's callable)

TROUBLESHOOTING:
================================================================================
If procedures are missing:
1. Re-run CDC_Data_Preservation_Framework.sql
2. Check for SQL errors during deployment
3. Verify role has CREATE PROCEDURE privilege

If tables are missing:
1. Re-run Section 3.2 from Production Runbook
2. Verify role has CREATE TABLE privilege
================================================================================
*/
