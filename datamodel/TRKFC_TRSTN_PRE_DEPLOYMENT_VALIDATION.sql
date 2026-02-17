/*
================================================================================
TRKFC_TRSTN CDC - PRE-DEPLOYMENT VALIDATION SCRIPT
================================================================================
Purpose      : Validates environment configuration before CDC deployment
Version      : 1.0.1 (Production Ready)
Usage        : Run this script BEFORE deploying TRKFC_TRSTN_DEPLOY_PARAMETERIZED.sql
================================================================================

INSTRUCTIONS:
  1. Set the configuration variables below to match your target environment
  2. Run this entire script
  3. Review validation results:
     - PASS  = Ready for deployment
     - WARN  = Review before proceeding (existing objects will be replaced)
     - FAIL  = Must fix before deployment
  4. Address any FAIL items before running the deployment script
================================================================================
*/

-- =============================================================================
-- CONFIGURATION VARIABLES - MODIFY FOR YOUR ENVIRONMENT
-- =============================================================================
SET V_DATABASE = 'D_BRONZE';           -- Target database
SET V_SCHEMA = 'SADB';                 -- Target schema  
SET V_WAREHOUSE = 'INFA_INGEST_WH';    -- Warehouse for task execution (customer's warehouse)

-- =============================================================================
-- CREATE VALIDATION RESULTS TABLE
-- =============================================================================
CREATE OR REPLACE TEMPORARY TABLE _PRE_DEPLOY_VALIDATION (
    CHECK_ORDER NUMBER,
    CHECK_CATEGORY VARCHAR(50),
    CHECK_NAME VARCHAR(100),
    STATUS VARCHAR(10),
    DETAILS VARCHAR(500),
    RECOMMENDATION VARCHAR(500)
);

-- =============================================================================
-- VALIDATION 1: DATABASE EXISTS
-- =============================================================================
INSERT INTO _PRE_DEPLOY_VALIDATION (CHECK_ORDER, CHECK_CATEGORY, CHECK_NAME, STATUS, DETAILS, RECOMMENDATION)
WITH db_check AS (
    SELECT COUNT(*) AS cnt FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES 
    WHERE DATABASE_NAME = $V_DATABASE AND DELETED IS NULL
)
SELECT 1, 'INFRASTRUCTURE', 'Database Exists',
    CASE WHEN cnt > 0 THEN 'PASS' ELSE 'FAIL' END,
    CASE WHEN cnt > 0 THEN 'Database ' || $V_DATABASE || ' exists' 
         ELSE 'Database ' || $V_DATABASE || ' NOT FOUND' END,
    CASE WHEN cnt > 0 THEN 'No action required'
         ELSE 'Create database: CREATE DATABASE ' || $V_DATABASE END
FROM db_check;

-- =============================================================================
-- VALIDATION 2: SCHEMA EXISTS  
-- =============================================================================
EXECUTE IMMEDIATE '
INSERT INTO _PRE_DEPLOY_VALIDATION (CHECK_ORDER, CHECK_CATEGORY, CHECK_NAME, STATUS, DETAILS, RECOMMENDATION)
SELECT 2, ''INFRASTRUCTURE'', ''Schema Exists'',
    CASE WHEN COUNT(*) > 0 THEN ''PASS'' ELSE ''FAIL'' END,
    CASE WHEN COUNT(*) > 0 THEN ''Schema ' || $V_DATABASE || '.' || $V_SCHEMA || ' exists''
         ELSE ''Schema ' || $V_DATABASE || '.' || $V_SCHEMA || ' NOT FOUND'' END,
    CASE WHEN COUNT(*) > 0 THEN ''No action required''
         ELSE ''Create schema: CREATE SCHEMA ' || $V_DATABASE || '.' || $V_SCHEMA || ''' END
FROM ' || $V_DATABASE || '.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ''' || $V_SCHEMA || '''';

-- =============================================================================
-- VALIDATION 3: WAREHOUSE EXISTS (Using result set parsing)
-- =============================================================================
DECLARE
    v_wh_exists BOOLEAN := FALSE;
    v_wh_name VARCHAR := '';
BEGIN
    v_wh_name := $V_WAREHOUSE;
    
    -- Create temp table to hold SHOW results
    CREATE OR REPLACE TEMPORARY TABLE _WH_CHECK AS
    SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE 1=0;
    
    EXECUTE IMMEDIATE 'SHOW WAREHOUSES LIKE ''' || v_wh_name || '''';
    
    CREATE OR REPLACE TEMPORARY TABLE _WH_CHECK AS
    SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    
    SELECT COUNT(*) > 0 INTO v_wh_exists FROM _WH_CHECK;
    
    INSERT INTO _PRE_DEPLOY_VALIDATION (CHECK_ORDER, CHECK_CATEGORY, CHECK_NAME, STATUS, DETAILS, RECOMMENDATION)
    SELECT 3, 'INFRASTRUCTURE', 'Warehouse Exists',
        CASE WHEN :v_wh_exists THEN 'PASS' ELSE 'FAIL' END,
        CASE WHEN :v_wh_exists THEN 'Warehouse ' || :v_wh_name || ' exists'
             ELSE 'Warehouse ' || :v_wh_name || ' NOT FOUND' END,
        CASE WHEN :v_wh_exists THEN 'No action required'
             ELSE 'Create warehouse or change V_WAREHOUSE to: COMPUTE_WH' END;
    
    DROP TABLE IF EXISTS _WH_CHECK;
END;

-- =============================================================================
-- VALIDATION 4: SOURCE TABLE EXISTS (CRITICAL)
-- =============================================================================
EXECUTE IMMEDIATE '
INSERT INTO _PRE_DEPLOY_VALIDATION (CHECK_ORDER, CHECK_CATEGORY, CHECK_NAME, STATUS, DETAILS, RECOMMENDATION)
SELECT 4, ''SOURCE_DATA'', ''Source Table Exists (CRITICAL)'',
    CASE WHEN COUNT(*) > 0 THEN ''PASS'' ELSE ''FAIL'' END,
    CASE WHEN COUNT(*) > 0 THEN ''Source table TRKFC_TRSTN_BASE exists''
         ELSE ''CRITICAL: Source table TRKFC_TRSTN_BASE NOT FOUND'' END,
    CASE WHEN COUNT(*) > 0 THEN ''No action required''
         ELSE ''STOP: Source table must exist. Verify IDMC replication is complete.'' END
FROM ' || $V_DATABASE || '.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = ''' || $V_SCHEMA || ''' AND TABLE_NAME = ''TRKFC_TRSTN_BASE'' AND TABLE_TYPE = ''BASE TABLE''';

-- =============================================================================
-- VALIDATION 5: SOURCE TABLE HAS DATA
-- =============================================================================
BEGIN
    EXECUTE IMMEDIATE '
    INSERT INTO _PRE_DEPLOY_VALIDATION (CHECK_ORDER, CHECK_CATEGORY, CHECK_NAME, STATUS, DETAILS, RECOMMENDATION)
    SELECT 5, ''SOURCE_DATA'', ''Source Table Has Data'',
        CASE WHEN COUNT(*) > 0 THEN ''PASS'' ELSE ''WARN'' END,
        ''Source table has '' || COUNT(*) || '' rows'',
        CASE WHEN COUNT(*) > 0 THEN ''Initial load will process all existing rows''
             ELSE ''Source empty - verify IDMC replication before deployment'' END
    FROM ' || $V_DATABASE || '.' || $V_SCHEMA || '.TRKFC_TRSTN_BASE';
EXCEPTION
    WHEN OTHER THEN
        INSERT INTO _PRE_DEPLOY_VALIDATION VALUES 
        (5, 'SOURCE_DATA', 'Source Table Has Data', 'SKIP', 
         'Cannot check - source table does not exist', 'Fix source table issue first');
END;

-- =============================================================================
-- VALIDATION 6: SOURCE TABLE SCHEMA (40 COLUMNS EXPECTED)
-- =============================================================================
EXECUTE IMMEDIATE '
INSERT INTO _PRE_DEPLOY_VALIDATION (CHECK_ORDER, CHECK_CATEGORY, CHECK_NAME, STATUS, DETAILS, RECOMMENDATION)
SELECT 6, ''SOURCE_DATA'', ''Source Schema Valid (40 columns)'',
    CASE WHEN COUNT(*) = 40 THEN ''PASS'' WHEN COUNT(*) > 0 THEN ''WARN'' ELSE ''FAIL'' END,
    ''Found '' || COUNT(*) || '' columns (expected 40)'',
    CASE WHEN COUNT(*) = 40 THEN ''Schema matches expected structure''
         WHEN COUNT(*) > 0 THEN ''Schema differs - review changes before deployment''
         ELSE ''Source table not found'' END
FROM ' || $V_DATABASE || '.INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = ''' || $V_SCHEMA || ''' AND TABLE_NAME = ''TRKFC_TRSTN_BASE''';

-- =============================================================================
-- VALIDATION 7: PRIMARY KEY COLUMNS EXIST
-- =============================================================================
EXECUTE IMMEDIATE '
INSERT INTO _PRE_DEPLOY_VALIDATION (CHECK_ORDER, CHECK_CATEGORY, CHECK_NAME, STATUS, DETAILS, RECOMMENDATION)
SELECT 7, ''SOURCE_DATA'', ''Primary Key Columns (SCAC_CD, FSAC_CD)'',
    CASE WHEN COUNT(*) = 2 THEN ''PASS'' ELSE ''FAIL'' END,
    ''Found '' || COUNT(*) || '' of 2 required PK columns'',
    CASE WHEN COUNT(*) = 2 THEN ''Composite primary key columns exist''
         ELSE ''CRITICAL: Missing PK columns - CDC MERGE will fail'' END
FROM ' || $V_DATABASE || '.INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = ''' || $V_SCHEMA || ''' AND TABLE_NAME = ''TRKFC_TRSTN_BASE''
  AND COLUMN_NAME IN (''SCAC_CD'', ''FSAC_CD'')';

-- =============================================================================
-- VALIDATION 8: TARGET V1 TABLE STATUS
-- =============================================================================
EXECUTE IMMEDIATE '
INSERT INTO _PRE_DEPLOY_VALIDATION (CHECK_ORDER, CHECK_CATEGORY, CHECK_NAME, STATUS, DETAILS, RECOMMENDATION)
SELECT 8, ''TARGET_OBJECTS'', ''V1 Table Status'',
    CASE WHEN COUNT(*) = 0 THEN ''PASS'' ELSE ''WARN'' END,
    CASE WHEN COUNT(*) = 0 THEN ''V1 does not exist - fresh deployment''
         ELSE ''V1 EXISTS with data - will be preserved (CREATE IF NOT EXISTS)'' END,
    CASE WHEN COUNT(*) = 0 THEN ''Table will be created''
         ELSE ''Existing data preserved - to reset, DROP table first'' END
FROM ' || $V_DATABASE || '.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = ''' || $V_SCHEMA || ''' AND TABLE_NAME = ''TRKFC_TRSTN_V1''';

-- =============================================================================
-- VALIDATION 9: STREAM STATUS
-- =============================================================================
EXECUTE IMMEDIATE '
INSERT INTO _PRE_DEPLOY_VALIDATION (CHECK_ORDER, CHECK_CATEGORY, CHECK_NAME, STATUS, DETAILS, RECOMMENDATION)
SELECT 9, ''TARGET_OBJECTS'', ''Stream Status'',
    CASE WHEN COUNT(*) = 0 THEN ''PASS'' ELSE ''WARN'' END,
    CASE WHEN COUNT(*) = 0 THEN ''Stream does not exist - fresh deployment''
         ELSE ''Stream EXISTS - will be REPLACED (CREATE OR REPLACE)'' END,
    CASE WHEN COUNT(*) = 0 THEN ''Stream created with SHOW_INITIAL_ROWS=TRUE''
         ELSE ''New stream will capture all existing source data'' END
FROM ' || $V_DATABASE || '.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = ''' || $V_SCHEMA || ''' AND TABLE_TYPE = ''STREAM'' 
  AND TABLE_NAME = ''TRKFC_TRSTN_BASE_HIST_STREAM''';

-- =============================================================================
-- VALIDATION 10: PROCEDURE STATUS
-- =============================================================================
EXECUTE IMMEDIATE '
INSERT INTO _PRE_DEPLOY_VALIDATION (CHECK_ORDER, CHECK_CATEGORY, CHECK_NAME, STATUS, DETAILS, RECOMMENDATION)
SELECT 10, ''TARGET_OBJECTS'', ''Procedure Status'',
    CASE WHEN COUNT(*) = 0 THEN ''PASS'' ELSE ''WARN'' END,
    CASE WHEN COUNT(*) = 0 THEN ''Procedure does not exist - fresh deployment''
         ELSE ''Procedure EXISTS - will be REPLACED'' END,
    CASE WHEN COUNT(*) = 0 THEN ''Procedure will be created''
         ELSE ''Existing procedure replaced with new version'' END
FROM ' || $V_DATABASE || '.INFORMATION_SCHEMA.PROCEDURES 
WHERE PROCEDURE_SCHEMA = ''' || $V_SCHEMA || ''' AND PROCEDURE_NAME = ''SP_PROCESS_TRKFC_TRSTN_CDC''';

-- =============================================================================
-- VALIDATION 11: CURRENT ROLE
-- =============================================================================
INSERT INTO _PRE_DEPLOY_VALIDATION (CHECK_ORDER, CHECK_CATEGORY, CHECK_NAME, STATUS, DETAILS, RECOMMENDATION)
SELECT 11, 'PERMISSIONS', 'Current Role',
    CASE WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'SECURITYADMIN') THEN 'PASS' ELSE 'WARN' END,
    'Current role: ' || CURRENT_ROLE(),
    CASE WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'SECURITYADMIN') 
         THEN 'Admin role has all required privileges'
         ELSE 'Verify: CREATE TABLE/STREAM/PROCEDURE/TASK, EXECUTE TASK' END;

-- =============================================================================
-- VALIDATION 12: WAREHOUSE SELECTED FOR SESSION
-- =============================================================================
INSERT INTO _PRE_DEPLOY_VALIDATION (CHECK_ORDER, CHECK_CATEGORY, CHECK_NAME, STATUS, DETAILS, RECOMMENDATION)
SELECT 12, 'PERMISSIONS', 'Session Warehouse',
    CASE WHEN CURRENT_WAREHOUSE() IS NOT NULL THEN 'PASS' ELSE 'FAIL' END,
    CASE WHEN CURRENT_WAREHOUSE() IS NOT NULL 
         THEN 'Session warehouse: ' || CURRENT_WAREHOUSE()
         ELSE 'No warehouse selected for session' END,
    CASE WHEN CURRENT_WAREHOUSE() IS NOT NULL 
         THEN 'Warehouse available for deployment commands'
         ELSE 'Run: USE WAREHOUSE <warehouse_name>' END;

-- =============================================================================
-- DISPLAY VALIDATION RESULTS
-- =============================================================================

-- Header
SELECT '================================================================================' AS "=";
SELECT '             TRKFC_TRSTN CDC - PRE-DEPLOYMENT VALIDATION RESULTS              ' AS "REPORT";
SELECT '================================================================================' AS "=";

-- Summary
SELECT 
    COUNT(CASE WHEN STATUS = 'PASS' THEN 1 END) AS "PASSED",
    COUNT(CASE WHEN STATUS = 'WARN' THEN 1 END) AS "WARNINGS", 
    COUNT(CASE WHEN STATUS = 'FAIL' THEN 1 END) AS "FAILED",
    COUNT(CASE WHEN STATUS = 'SKIP' THEN 1 END) AS "SKIPPED",
    COUNT(*) AS "TOTAL",
    CASE 
        WHEN COUNT(CASE WHEN STATUS = 'FAIL' THEN 1 END) > 0 
            THEN '❌ DEPLOYMENT BLOCKED - Fix FAIL items'
        WHEN COUNT(CASE WHEN STATUS = 'WARN' THEN 1 END) > 0 
            THEN '⚠️ OK WITH WARNINGS - Review before proceeding'
        ELSE '✅ DEPLOYMENT READY - All checks passed'
    END AS "DEPLOYMENT_STATUS"
FROM _PRE_DEPLOY_VALIDATION;

-- Detailed Results
SELECT 
    CHECK_ORDER AS "#",
    CHECK_CATEGORY AS "CATEGORY",
    CHECK_NAME AS "CHECK",
    STATUS,
    DETAILS
FROM _PRE_DEPLOY_VALIDATION
ORDER BY 
    CASE STATUS WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 WHEN 'SKIP' THEN 3 ELSE 4 END,
    CHECK_ORDER;

-- Action Items
SELECT '================================================================================' AS "=";
SELECT '                           ACTION ITEMS                                        ' AS "SECTION";
SELECT '================================================================================' AS "=";

SELECT STATUS, CHECK_NAME AS "ITEM", RECOMMENDATION AS "ACTION_REQUIRED"
FROM _PRE_DEPLOY_VALIDATION
WHERE STATUS IN ('FAIL', 'WARN')
ORDER BY CASE STATUS WHEN 'FAIL' THEN 1 ELSE 2 END, CHECK_ORDER;

-- Configuration
SELECT '================================================================================' AS "=";
SELECT '                      CONFIGURATION SUMMARY                                    ' AS "SECTION";
SELECT '================================================================================' AS "=";

SELECT 
    $V_DATABASE AS "DATABASE",
    $V_SCHEMA AS "SCHEMA",
    $V_WAREHOUSE AS "TASK_WAREHOUSE",
    'TRKFC_TRSTN_BASE' AS "SOURCE_TABLE",
    'TRKFC_TRSTN_V1' AS "TARGET_TABLE",
    'SP_PROCESS_TRKFC_TRSTN_CDC' AS "PROCEDURE",
    'TASK_PROCESS_TRKFC_TRSTN_CDC' AS "TASK";

-- Cleanup
DROP TABLE IF EXISTS _PRE_DEPLOY_VALIDATION;

/*
================================================================================
NEXT STEPS BASED ON RESULTS
================================================================================

✅ ALL PASSED:
   Run: TRKFC_TRSTN_DEPLOY_PARAMETERIZED.sql

❌ ANY FAILED:
   1. Review FAIL items and recommendations
   2. Fix issues (create missing objects, grant permissions, etc.)
   3. Re-run this validation script
   4. Once all PASS, run deployment script

⚠️ WARNINGS ONLY:
   - WARN on V1/Stream/Procedure = existing objects will be replaced
   - This is expected for re-deployments
   - Review to confirm this is acceptable
   - Proceed with deployment

================================================================================
AVAILABLE WAREHOUSES (for reference):
================================================================================
Run: SHOW WAREHOUSES;
================================================================================
*/
