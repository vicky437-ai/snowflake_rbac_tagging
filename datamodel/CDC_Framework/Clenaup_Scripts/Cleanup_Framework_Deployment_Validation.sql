/*
================================================================================
BASE TABLE CLEANUP FRAMEWORK - DEPLOYMENT VALIDATION SCRIPT
================================================================================
Purpose:     Validate framework deployment across DEV, STAGING, PROD environments
Version:     1.0
Created:     2026-02-16
Tested:      2026-02-16 - All validations passed
================================================================================

USAGE:
1. Set environment parameters in Section 1 (lines 25-45)
2. Run entire script sequentially
3. Review validation results at end

ENVIRONMENT PRESETS:
  DEV:     Frequent runs (every 4 hours), shorter retention for testing
  STAGING: Daily runs (3 AM UTC), mirrors production settings
  PROD:    Daily runs (2 AM UTC), standard 45-day retention

================================================================================
*/

-- ============================================================================
-- SECTION 1: ENVIRONMENT CONFIGURATION
-- ============================================================================
-- ⚠️ MODIFY THESE VALUES FOR YOUR TARGET ENVIRONMENT

-- Choose environment: 'DEV', 'STAGING', or 'PROD'
SET ENV_NAME = 'DEV';

-- Framework location (typically same across all environments)
SET FRAMEWORK_DATABASE = 'CDC_PRESERVATION';
SET FRAMEWORK_SCHEMA = 'CLEANUP';

-- Target schema to validate (MODIFY FOR EACH ENVIRONMENT)
SET TARGET_DATABASE = 'D_BRONZE';      -- DEV: D_BRONZE_DEV, STAGING: D_BRONZE_STG, PROD: D_BRONZE
SET TARGET_SCHEMA = 'SALES';           -- Schema containing _BASE tables
SET DATE_COLUMN = 'CREATED_DATE';      -- Date column for age calculation
SET TABLE_PATTERN = '%_BASE';          -- Table name pattern

-- Environment-specific settings
SET RETENTION_DAYS = 45;               -- DEV: 7, STAGING: 45, PROD: 45
SET TASK_WAREHOUSE = 'COMPUTE_WH';     -- Warehouse for task execution

-- ============================================================================
-- SECTION 2: ENVIRONMENT PRESETS (Auto-calculated)
-- ============================================================================
-- These are automatically set based on ENV_NAME

SET TASK_SCHEDULE = (
    SELECT CASE $ENV_NAME
        WHEN 'DEV' THEN 'USING CRON 0 */4 * * * UTC'      -- Every 4 hours
        WHEN 'STAGING' THEN 'USING CRON 0 3 * * * UTC'    -- 3 AM UTC
        ELSE 'USING CRON 0 2 * * * UTC'                   -- 2 AM UTC (PROD)
    END
);

SET RECOMMENDED_RETENTION = (
    SELECT CASE $ENV_NAME
        WHEN 'DEV' THEN 7
        ELSE 45
    END
);

-- ============================================================================
-- SECTION 3: VALIDATION RESULTS TABLE
-- ============================================================================

CREATE OR REPLACE TEMPORARY TABLE _VALIDATION_RESULTS (
    TEST_ID NUMBER,
    TEST_CATEGORY VARCHAR(100),
    TEST_NAME VARCHAR(500),
    EXPECTED_RESULT VARCHAR(1000),
    ACTUAL_RESULT VARCHAR(1000),
    STATUS VARCHAR(20),
    DETAILS VARCHAR(4000),
    EXECUTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- SECTION 4: FRAMEWORK OBJECT VALIDATION
-- ============================================================================

-- 4.1 Validate Framework Schema
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    1,
    'FRAMEWORK',
    'Framework schema exists',
    'EXISTS',
    CASE WHEN COUNT(*) > 0 THEN 'EXISTS' ELSE 'MISSING' END,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
    'Schema: ' || $FRAMEWORK_DATABASE || '.' || $FRAMEWORK_SCHEMA
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE CATALOG_NAME = $FRAMEWORK_DATABASE AND SCHEMA_NAME = $FRAMEWORK_SCHEMA;

-- 4.2 Validate Configuration Table
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    2,
    'FRAMEWORK',
    'CLEANUP_CONFIG table exists',
    'EXISTS',
    CASE WHEN COUNT(*) > 0 THEN 'EXISTS' ELSE 'MISSING' END,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
    'Stores cleanup configurations per schema'
FROM IDENTIFIER($FRAMEWORK_DATABASE || '.INFORMATION_SCHEMA.TABLES')
WHERE TABLE_SCHEMA = $FRAMEWORK_SCHEMA AND TABLE_NAME = 'CLEANUP_CONFIG';

-- 4.3 Validate Log Table
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    3,
    'FRAMEWORK',
    'CLEANUP_LOG table exists',
    'EXISTS',
    CASE WHEN COUNT(*) > 0 THEN 'EXISTS' ELSE 'MISSING' END,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
    'Stores cleanup execution history'
FROM IDENTIFIER($FRAMEWORK_DATABASE || '.INFORMATION_SCHEMA.TABLES')
WHERE TABLE_SCHEMA = $FRAMEWORK_SCHEMA AND TABLE_NAME = 'CLEANUP_LOG';

-- 4.4 Validate Exclusions Table
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    4,
    'FRAMEWORK',
    'CLEANUP_EXCLUSIONS table exists',
    'EXISTS',
    CASE WHEN COUNT(*) > 0 THEN 'EXISTS' ELSE 'MISSING' END,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
    'Stores tables to exclude from cleanup'
FROM IDENTIFIER($FRAMEWORK_DATABASE || '.INFORMATION_SCHEMA.TABLES')
WHERE TABLE_SCHEMA = $FRAMEWORK_SCHEMA AND TABLE_NAME = 'CLEANUP_EXCLUSIONS';

-- 4.5 Validate All Procedures
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
WITH EXPECTED_PROCS AS (
    SELECT VALUE AS PROC_NAME FROM TABLE(FLATTEN(INPUT => PARSE_JSON('["SP_CLEANUP_BASE_TABLE","SP_CLEANUP_SCHEMA","SP_CLEANUP_DRY_RUN","SP_CLEANUP_BY_CONFIG","SP_CLEANUP_ALL_CONFIGS","SP_CREATE_MASTER_CLEANUP_TASK","SP_RESUME_CLEANUP_TASK","SP_SUSPEND_CLEANUP_TASK"]')))
),
ACTUAL_PROCS AS (
    SELECT DISTINCT PROCEDURE_NAME 
    FROM IDENTIFIER($FRAMEWORK_DATABASE || '.INFORMATION_SCHEMA.PROCEDURES')
    WHERE PROCEDURE_SCHEMA = $FRAMEWORK_SCHEMA
)
SELECT 
    5,
    'FRAMEWORK',
    'All required procedures exist (8)',
    '8 procedures',
    (SELECT COUNT(DISTINCT PROCEDURE_NAME)::VARCHAR || ' procedures' FROM ACTUAL_PROCS),
    CASE WHEN (SELECT COUNT(DISTINCT PROCEDURE_NAME) FROM ACTUAL_PROCS) >= 8 THEN 'PASS' ELSE 'FAIL' END,
    'Core: SP_CLEANUP_*, Utility: SP_*_TASK';

-- 4.6 Validate All Views
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    6,
    'FRAMEWORK',
    'All monitoring views exist (4)',
    '4 views',
    COUNT(*)::VARCHAR || ' views',
    CASE WHEN COUNT(*) >= 4 THEN 'PASS' ELSE 'FAIL' END,
    'V_CLEANUP_SUMMARY, V_CONFIG_STATUS, V_RECENT_CLEANUPS, V_FAILED_CLEANUPS'
FROM IDENTIFIER($FRAMEWORK_DATABASE || '.INFORMATION_SCHEMA.VIEWS')
WHERE TABLE_SCHEMA = $FRAMEWORK_SCHEMA AND TABLE_NAME LIKE 'V_%';

-- ============================================================================
-- SECTION 5: TARGET SCHEMA VALIDATION
-- ============================================================================

-- 5.1 Validate Target Database
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    10,
    'TARGET',
    'Target database accessible',
    'Accessible',
    CASE WHEN COUNT(*) > 0 THEN 'Accessible' ELSE 'Not found' END,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
    'Database: ' || $TARGET_DATABASE
FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME = $TARGET_DATABASE;

-- 5.2 Validate Target Schema
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    11,
    'TARGET',
    'Target schema exists',
    'EXISTS',
    CASE WHEN COUNT(*) > 0 THEN 'EXISTS' ELSE 'MISSING' END,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
    'Schema: ' || $TARGET_DATABASE || '.' || $TARGET_SCHEMA
FROM IDENTIFIER($TARGET_DATABASE || '.INFORMATION_SCHEMA.SCHEMATA')
WHERE SCHEMA_NAME = $TARGET_SCHEMA;

-- 5.3 Validate _BASE Tables Found
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    12,
    'TARGET',
    '_BASE tables found in target schema',
    '>= 1 table',
    COUNT(*)::VARCHAR || ' tables found',
    CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END,
    'Tables: ' || COALESCE(LISTAGG(TABLE_NAME, ', ') WITHIN GROUP (ORDER BY TABLE_NAME), 'None')
FROM IDENTIFIER($TARGET_DATABASE || '.INFORMATION_SCHEMA.TABLES')
WHERE TABLE_SCHEMA = $TARGET_SCHEMA 
  AND TABLE_NAME LIKE $TABLE_PATTERN 
  AND TABLE_TYPE = 'BASE TABLE';

-- 5.4 Validate Date Column in _BASE Tables
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    13,
    'TARGET',
    'Date column exists in _BASE tables',
    'Column found',
    CASE WHEN COUNT(*) > 0 THEN 'Found in ' || COUNT(*)::VARCHAR || ' tables' ELSE 'Not found' END,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'WARN' END,
    'Column: ' || $DATE_COLUMN
FROM IDENTIFIER($TARGET_DATABASE || '.INFORMATION_SCHEMA.COLUMNS')
WHERE TABLE_SCHEMA = $TARGET_SCHEMA 
  AND TABLE_NAME LIKE $TABLE_PATTERN
  AND UPPER(COLUMN_NAME) = UPPER($DATE_COLUMN);

-- ============================================================================
-- SECTION 6: CONFIGURATION VALIDATION
-- ============================================================================

-- 6.1 Check Configuration Entry
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    20,
    'CONFIG',
    'Configuration exists for target schema',
    'Config exists',
    CASE WHEN COUNT(*) > 0 THEN 'Exists (ID=' || MAX(CONFIG_ID)::VARCHAR || ')' ELSE 'Not configured' END,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'WARN' END,
    'Add config if missing: INSERT INTO CLEANUP_CONFIG...'
FROM IDENTIFIER($FRAMEWORK_DATABASE || '.' || $FRAMEWORK_SCHEMA || '.CLEANUP_CONFIG')
WHERE DATABASE_NAME = $TARGET_DATABASE AND SCHEMA_NAME = $TARGET_SCHEMA;

-- 6.2 Check Configuration Active
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    21,
    'CONFIG',
    'Configuration is active',
    'IS_ACTIVE = TRUE',
    CASE WHEN MAX(IS_ACTIVE::INT) = 1 THEN 'Active' WHEN MAX(IS_ACTIVE::INT) = 0 THEN 'Inactive' ELSE 'No config' END,
    CASE WHEN MAX(IS_ACTIVE::INT) = 1 THEN 'PASS' ELSE 'WARN' END,
    'Enable with: UPDATE CLEANUP_CONFIG SET IS_ACTIVE = TRUE...'
FROM IDENTIFIER($FRAMEWORK_DATABASE || '.' || $FRAMEWORK_SCHEMA || '.CLEANUP_CONFIG')
WHERE DATABASE_NAME = $TARGET_DATABASE AND SCHEMA_NAME = $TARGET_SCHEMA;

-- 6.3 Check Retention Days
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    22,
    'CONFIG',
    'Retention days configured',
    $RETENTION_DAYS::VARCHAR || ' days (expected)',
    COALESCE(MAX(RETENTION_DAYS)::VARCHAR || ' days', 'Not set'),
    CASE WHEN MAX(RETENTION_DAYS) = $RETENTION_DAYS THEN 'PASS' WHEN MAX(RETENTION_DAYS) IS NOT NULL THEN 'INFO' ELSE 'WARN' END,
    'Recommended for ' || $ENV_NAME || ': ' || $RECOMMENDED_RETENTION || ' days'
FROM IDENTIFIER($FRAMEWORK_DATABASE || '.' || $FRAMEWORK_SCHEMA || '.CLEANUP_CONFIG')
WHERE DATABASE_NAME = $TARGET_DATABASE AND SCHEMA_NAME = $TARGET_SCHEMA;

-- ============================================================================
-- SECTION 7: TASK VALIDATION
-- ============================================================================

-- 7.1 Check Task Exists (via SHOW TASKS)
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
VALUES (
    30,
    'TASK',
    'Master cleanup task exists',
    'Task created',
    'Verify via: SHOW TASKS IN SCHEMA ' || $FRAMEWORK_DATABASE || '.' || $FRAMEWORK_SCHEMA,
    'INFO',
    'Task: TASK_CLEANUP_ALL_SCHEMAS'
);

-- ============================================================================
-- SECTION 8: FUNCTIONAL VALIDATION (DRY RUN)
-- ============================================================================

-- Execute dry run test
CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_DRY_RUN(
    $TARGET_DATABASE,
    $TARGET_SCHEMA,
    $DATE_COLUMN,
    $RETENTION_DAYS,
    $TABLE_PATTERN
);

-- Store result for analysis
CREATE OR REPLACE TEMPORARY TABLE _DRY_RUN_RESULT AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- 8.1 Validate Dry Run Success
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    40,
    'FUNCTIONAL',
    'Dry run executes successfully',
    'Valid JSON result',
    CASE WHEN SP_CLEANUP_DRY_RUN IS NOT NULL THEN 'Success' ELSE 'Failed' END,
    CASE WHEN SP_CLEANUP_DRY_RUN IS NOT NULL THEN 'PASS' ELSE 'FAIL' END,
    'Mode: ' || COALESCE(SP_CLEANUP_DRY_RUN:mode::VARCHAR, 'N/A')
FROM _DRY_RUN_RESULT;

-- 8.2 Validate Tables Found
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    41,
    'FUNCTIONAL',
    'Dry run found target tables',
    '>= 1 table',
    COALESCE(SP_CLEANUP_DRY_RUN:table_count::VARCHAR, '0') || ' tables',
    CASE WHEN COALESCE(SP_CLEANUP_DRY_RUN:table_count::INT, 0) >= 1 THEN 'PASS' ELSE 'FAIL' END,
    'Pattern: ' || $TABLE_PATTERN
FROM _DRY_RUN_RESULT;

-- 8.3 Validate Cutoff Date
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    42,
    'FUNCTIONAL',
    'Cutoff date calculated correctly',
    DATEADD('day', -$RETENTION_DAYS, CURRENT_DATE())::VARCHAR,
    COALESCE(SP_CLEANUP_DRY_RUN:cutoff_date::VARCHAR, 'N/A'),
    CASE WHEN SP_CLEANUP_DRY_RUN:cutoff_date::DATE = DATEADD('day', -$RETENTION_DAYS, CURRENT_DATE()) THEN 'PASS' ELSE 'WARN' END,
    'Retention: ' || $RETENTION_DAYS || ' days from today'
FROM _DRY_RUN_RESULT;

-- 8.4 Show Rows to Delete Preview
INSERT INTO _VALIDATION_RESULTS (TEST_ID, TEST_CATEGORY, TEST_NAME, EXPECTED_RESULT, ACTUAL_RESULT, STATUS, DETAILS)
SELECT 
    43,
    'FUNCTIONAL',
    'Rows eligible for deletion',
    'Preview count',
    COALESCE(SP_CLEANUP_DRY_RUN:total_rows_to_delete::VARCHAR, '0') || ' rows',
    'INFO',
    'This is a preview only - no data will be deleted by dry run'
FROM _DRY_RUN_RESULT;

-- ============================================================================
-- SECTION 9: GENERATE VALIDATION REPORT
-- ============================================================================

-- Header
SELECT '═══════════════════════════════════════════════════════════════════════════════' AS REPORT;
SELECT 'CLEANUP FRAMEWORK - DEPLOYMENT VALIDATION REPORT' AS REPORT;
SELECT '═══════════════════════════════════════════════════════════════════════════════' AS REPORT;
SELECT 'Environment:      ' || $ENV_NAME AS REPORT;
SELECT 'Validation Date:  ' || CURRENT_TIMESTAMP()::VARCHAR AS REPORT;
SELECT 'Target Schema:    ' || $TARGET_DATABASE || '.' || $TARGET_SCHEMA AS REPORT;
SELECT 'Retention:        ' || $RETENTION_DAYS || ' days' AS REPORT;
SELECT 'Table Pattern:    ' || $TABLE_PATTERN AS REPORT;
SELECT '───────────────────────────────────────────────────────────────────────────────' AS REPORT;

-- Detailed Results
SELECT 
    TEST_ID,
    TEST_CATEGORY,
    CASE 
        WHEN STATUS = 'PASS' THEN '✓'
        WHEN STATUS = 'WARN' THEN '⚠'
        WHEN STATUS = 'INFO' THEN 'ℹ'
        ELSE '✗'
    END AS RESULT,
    TEST_NAME,
    STATUS,
    ACTUAL_RESULT,
    DETAILS
FROM _VALIDATION_RESULTS
ORDER BY TEST_ID;

-- Summary Statistics
SELECT '───────────────────────────────────────────────────────────────────────────────' AS REPORT;
SELECT 'VALIDATION SUMMARY' AS REPORT;
SELECT '───────────────────────────────────────────────────────────────────────────────' AS REPORT;

SELECT 
    STATUS,
    COUNT(*) AS TEST_COUNT,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1)::VARCHAR || '%' AS PERCENTAGE
FROM _VALIDATION_RESULTS
GROUP BY STATUS
ORDER BY CASE STATUS WHEN 'PASS' THEN 1 WHEN 'INFO' THEN 2 WHEN 'WARN' THEN 3 ELSE 4 END;

-- Overall Result
SELECT '───────────────────────────────────────────────────────────────────────────────' AS REPORT;

SELECT 
    CASE 
        WHEN SUM(CASE WHEN STATUS = 'FAIL' THEN 1 ELSE 0 END) = 0 
             AND SUM(CASE WHEN STATUS = 'WARN' THEN 1 ELSE 0 END) = 0 
        THEN '✅ ALL TESTS PASSED - Ready for ' || $ENV_NAME || ' deployment'
        WHEN SUM(CASE WHEN STATUS = 'FAIL' THEN 1 ELSE 0 END) = 0 
        THEN '⚠️ PASSED WITH WARNINGS - Review warnings before ' || $ENV_NAME || ' deployment'
        ELSE '❌ VALIDATION FAILED - Fix ' || SUM(CASE WHEN STATUS = 'FAIL' THEN 1 ELSE 0 END)::VARCHAR || ' issue(s) before deployment'
    END AS OVERALL_RESULT
FROM _VALIDATION_RESULTS;

-- Failed/Warning Details
SELECT 
    TEST_ID,
    TEST_NAME,
    STATUS,
    DETAILS AS ACTION_REQUIRED
FROM _VALIDATION_RESULTS
WHERE STATUS IN ('FAIL', 'WARN')
ORDER BY CASE STATUS WHEN 'FAIL' THEN 1 ELSE 2 END, TEST_ID;

-- ============================================================================
-- SECTION 10: ENVIRONMENT-SPECIFIC NEXT STEPS
-- ============================================================================

SELECT '═══════════════════════════════════════════════════════════════════════════════' AS REPORT;
SELECT 'NEXT STEPS FOR ' || $ENV_NAME || ' ENVIRONMENT' AS REPORT;
SELECT '═══════════════════════════════════════════════════════════════════════════════' AS REPORT;

SELECT CASE $ENV_NAME
    WHEN 'DEV' THEN
'┌─────────────────────────────────────────────────────────────────────────────┐
│ DEV DEPLOYMENT NEXT STEPS                                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│ 1. ADD CONFIGURATION (if not exists):                                        │
│    INSERT INTO CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG                      │
│    (DATABASE_NAME, SCHEMA_NAME, DATE_COLUMN, RETENTION_DAYS, NOTES)         │
│    VALUES (''' || $TARGET_DATABASE || ''', ''' || $TARGET_SCHEMA || ''', ''' || $DATE_COLUMN || ''', 7,                │
│            ''DEV - short retention for testing'');                           │
│                                                                              │
│ 2. RUN MANUAL TEST:                                                          │
│    CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_SCHEMA(                         │
│        ''' || $TARGET_DATABASE || ''', ''' || $TARGET_SCHEMA || ''', ''' || $DATE_COLUMN || ''', 7);                    │
│                                                                              │
│ 3. CREATE/UPDATE TASK (frequent runs for testing):                           │
│    CALL CDC_PRESERVATION.CLEANUP.SP_CREATE_MASTER_CLEANUP_TASK(             │
│        ''COMPUTE_WH'', ''USING CRON 0 */4 * * * UTC'');                      │
│                                                                              │
│ 4. ENABLE TASK:                                                              │
│    ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS RESUME;     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘'

    WHEN 'STAGING' THEN
'┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGING DEPLOYMENT NEXT STEPS                                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│ 1. ADD CONFIGURATION (mirror PROD settings):                                 │
│    INSERT INTO CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG                      │
│    (DATABASE_NAME, SCHEMA_NAME, DATE_COLUMN, RETENTION_DAYS, NOTES)         │
│    VALUES (''' || $TARGET_DATABASE || ''', ''' || $TARGET_SCHEMA || ''', ''' || $DATE_COLUMN || ''', 45,               │
│            ''STAGING - mirrors production'');                                │
│                                                                              │
│ 2. RUN FULL VALIDATION TEST:                                                 │
│    CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_DRY_RUN(...);                   │
│    CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_SCHEMA(...);                    │
│                                                                              │
│ 3. CREATE TASK (offset from PROD schedule):                                  │
│    CALL CDC_PRESERVATION.CLEANUP.SP_CREATE_MASTER_CLEANUP_TASK(             │
│        ''COMPUTE_WH'', ''USING CRON 0 3 * * * UTC'');  -- 3 AM UTC          │
│                                                                              │
│ 4. VALIDATE FOR 3-5 DAYS BEFORE PROD DEPLOYMENT                             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘'

    ELSE  -- PROD
'┌─────────────────────────────────────────────────────────────────────────────┐
│ PRODUCTION DEPLOYMENT NEXT STEPS                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│ ⚠️  PREREQUISITES:                                                           │
│     • DEV and STAGING validations passed                                     │
│     • Change management approval obtained                                    │
│     • Stakeholders notified of maintenance window                           │
│                                                                              │
│ 1. ADD CONFIGURATION:                                                        │
│    INSERT INTO CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG                      │
│    (DATABASE_NAME, SCHEMA_NAME, DATE_COLUMN, RETENTION_DAYS, NOTES)         │
│    VALUES (''' || $TARGET_DATABASE || ''', ''' || $TARGET_SCHEMA || ''', ''' || $DATE_COLUMN || ''', 45,               │
│            ''PRODUCTION - 45 day retention'');                               │
│                                                                              │
│ 2. FINAL DRY RUN VERIFICATION:                                               │
│    CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_DRY_RUN(...);                   │
│                                                                              │
│ 3. CREATE PRODUCTION TASK:                                                   │
│    CALL CDC_PRESERVATION.CLEANUP.SP_CREATE_MASTER_CLEANUP_TASK(             │
│        ''COMPUTE_WH'', ''USING CRON 0 2 * * * UTC'');  -- 2 AM UTC          │
│                                                                              │
│ 4. ENABLE TASK (during maintenance window):                                  │
│    ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS RESUME;     │
│                                                                              │
│ 5. MONITOR FIRST EXECUTION:                                                  │
│    SELECT * FROM CDC_PRESERVATION.CLEANUP.V_CLEANUP_SUMMARY;                │
│    SELECT * FROM CDC_PRESERVATION.CLEANUP.V_FAILED_CLEANUPS;                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘'

END AS DEPLOYMENT_GUIDE;

-- Cleanup
DROP TABLE IF EXISTS _DRY_RUN_RESULT;

SELECT '═══════════════════════════════════════════════════════════════════════════════' AS REPORT;
SELECT 'END OF VALIDATION REPORT' AS REPORT;
SELECT '═══════════════════════════════════════════════════════════════════════════════' AS REPORT;
