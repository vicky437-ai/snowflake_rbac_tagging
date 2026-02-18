/*
================================================================================
DATABASE LAYER REORGANIZATION - PRODUCTION DEPLOYMENT SCRIPT
================================================================================
Version      : 1.0.0
Purpose      : Migrate from single D_BRONZE to D_RAW + D_BRONZE architecture
Author       : Data Engineering Team
Est. Duration: 30-45 minutes (depending on data volume)

ARCHITECTURE CHANGE:
  BEFORE: D_BRONZE (raw + preservation)
  AFTER:  D_RAW (IDMC raw data) + D_BRONZE (data preservation)

================================================================================
EXECUTION INSTRUCTIONS:
1. Run Phase 1 (Assessment) - Review output before proceeding
2. Run Phase 2 (Backup) - Save grant scripts
3. Run Phase 3 (Migration) - Execute during maintenance window
4. Run Phase 4 (Validation) - Verify all objects
5. Run Phase 5 (Cleanup) - Remove duplicates after validation
================================================================================
*/

-- =============================================================================
-- CONFIGURATION
-- =============================================================================
SET V_SOURCE_DB = 'D_BRONZE';           -- Current database
SET V_RAW_DB = 'D_RAW';                 -- New raw layer database
SET V_BRONZE_DB = 'D_BRONZE';           -- Preservation layer (same name)
SET V_WAREHOUSE = 'COMPUTE_WH';         -- Execution warehouse

USE WAREHOUSE IDENTIFIER($V_WAREHOUSE);

-- =============================================================================
-- PHASE 1: PRE-MIGRATION ASSESSMENT (Duration: 5 minutes)
-- =============================================================================
SELECT '========== PHASE 1: PRE-MIGRATION ASSESSMENT ==========' AS PHASE;

-- 1.1 Current database inventory
CREATE OR REPLACE TEMPORARY TABLE _MIGRATION_INVENTORY AS
SELECT 
    'DATABASE' AS OBJECT_LEVEL,
    DATABASE_NAME AS OBJECT_NAME,
    NULL AS PARENT_OBJECT,
    NULL AS ROW_COUNT,
    CREATED AS CREATED_DATE
FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES 
WHERE DATABASE_NAME = 'D_BRONZE' AND DELETED IS NULL
UNION ALL
SELECT 
    'SCHEMA',
    SCHEMA_NAME,
    CATALOG_NAME,
    NULL,
    CREATED
FROM D_BRONZE.INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
UNION ALL
SELECT 
    CASE 
        WHEN TABLE_NAME LIKE '%_BASE' THEN 'RAW_TABLE'
        WHEN TABLE_NAME LIKE '%_V1' THEN 'PRESERVATION_TABLE'
        ELSE 'OTHER_TABLE'
    END,
    TABLE_SCHEMA || '.' || TABLE_NAME,
    TABLE_SCHEMA,
    ROW_COUNT,
    CREATED
FROM D_BRONZE.INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA');

SELECT * FROM _MIGRATION_INVENTORY ORDER BY OBJECT_LEVEL, OBJECT_NAME;

-- 1.2 Summary
SELECT 
    OBJECT_LEVEL,
    COUNT(*) AS COUNT,
    SUM(NVL(ROW_COUNT, 0)) AS TOTAL_ROWS
FROM _MIGRATION_INVENTORY
GROUP BY OBJECT_LEVEL
ORDER BY OBJECT_LEVEL;

-- 1.3 Identify tables to move
SELECT '--- Tables staying in D_BRONZE (Preservation) ---' AS CATEGORY;
SELECT OBJECT_NAME, ROW_COUNT FROM _MIGRATION_INVENTORY 
WHERE OBJECT_LEVEL = 'PRESERVATION_TABLE';

SELECT '--- Tables moving to D_RAW (Raw Ingestion) ---' AS CATEGORY;
SELECT OBJECT_NAME, ROW_COUNT FROM _MIGRATION_INVENTORY 
WHERE OBJECT_LEVEL = 'RAW_TABLE';

SELECT '--- Other tables (need manual classification) ---' AS CATEGORY;
SELECT OBJECT_NAME, ROW_COUNT FROM _MIGRATION_INVENTORY 
WHERE OBJECT_LEVEL = 'OTHER_TABLE';

-- 1.4 Check for streams, tasks, procedures
SELECT '--- Streams (will need recreation) ---' AS CATEGORY;
SHOW STREAMS IN DATABASE D_BRONZE;

SELECT '--- Tasks (will need updating) ---' AS CATEGORY;
SHOW TASKS IN DATABASE D_BRONZE;

SELECT '--- Procedures (will need updating) ---' AS CATEGORY;
SHOW PROCEDURES IN DATABASE D_BRONZE;

/*
================================================================================
PHASE 1 CHECKPOINT
================================================================================
Review the output above:
[ ] Verified all schemas listed
[ ] Identified which tables go to D_RAW (BASE tables)
[ ] Identified which tables stay in D_BRONZE (V1 tables)
[ ] Noted any streams/tasks/procedures to update
[ ] No unexpected objects found

PROCEED TO PHASE 2 ONLY IF ALL CHECKS PASS
================================================================================
*/

-- =============================================================================
-- PHASE 2: BACKUP GRANTS (Duration: 5 minutes)
-- =============================================================================
SELECT '========== PHASE 2: BACKUP GRANTS ==========' AS PHASE;

-- 2.1 Capture database-level grants
CREATE OR REPLACE TEMPORARY TABLE _GRANT_BACKUP AS
SELECT 
    CREATED_ON,
    PRIVILEGE,
    GRANTED_ON,
    NAME AS OBJECT_NAME,
    GRANTEE_NAME,
    GRANT_OPTION,
    -- Generate re-grant script for D_RAW
    'GRANT ' || PRIVILEGE || ' ON DATABASE D_RAW TO ROLE ' || GRANTEE_NAME || 
    CASE WHEN GRANT_OPTION = 'true' THEN ' WITH GRANT OPTION' ELSE '' END || ';' AS GRANT_SCRIPT_D_RAW,
    -- Generate re-grant script for D_BRONZE (in case we need it)
    'GRANT ' || PRIVILEGE || ' ON DATABASE D_BRONZE TO ROLE ' || GRANTEE_NAME || 
    CASE WHEN GRANT_OPTION = 'true' THEN ' WITH GRANT OPTION' ELSE '' END || ';' AS GRANT_SCRIPT_D_BRONZE
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- 2.2 Get schema-level grants
SHOW GRANTS ON SCHEMA D_BRONZE.SADB;
INSERT INTO _GRANT_BACKUP 
SELECT 
    CREATED_ON, PRIVILEGE, GRANTED_ON, NAME, GRANTEE_NAME, GRANT_OPTION,
    'GRANT ' || PRIVILEGE || ' ON SCHEMA D_RAW.SADB TO ROLE ' || GRANTEE_NAME || ';',
    'GRANT ' || PRIVILEGE || ' ON SCHEMA D_BRONZE.SADB TO ROLE ' || GRANTEE_NAME || ';'
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE PRIVILEGE != 'OWNERSHIP';

-- 2.3 Display grant backup
SELECT '--- Grant Scripts to Re-Apply on D_RAW ---' AS INFO;
SELECT GRANT_SCRIPT_D_RAW FROM _GRANT_BACKUP WHERE GRANT_SCRIPT_D_RAW IS NOT NULL;

/*
================================================================================
PHASE 2 CHECKPOINT
================================================================================
[ ] Saved grant scripts for D_RAW
[ ] Documented all roles with access
[ ] Ready to proceed with migration

COPY THE GRANT SCRIPTS ABOVE AND SAVE THEM BEFORE PROCEEDING
================================================================================
*/

-- =============================================================================
-- PHASE 3: MIGRATION EXECUTION (Duration: 15-20 minutes)
-- =============================================================================
SELECT '========== PHASE 3: MIGRATION EXECUTION ==========' AS PHASE;

/*
MIGRATION STRATEGY:
1. Clone D_BRONZE to D_RAW (instant, zero-copy)
2. In D_RAW: Drop preservation tables (_V1)
3. In D_BRONZE: Drop raw tables (_BASE) 
4. Apply grants to D_RAW
5. Recreate CDC infrastructure pointing to D_RAW source

This approach:
- Minimizes data movement
- Provides instant clone
- Allows parallel validation
- Easy rollback (just drop D_RAW)
*/

-- 3.1 Suspend any active tasks first
SELECT '--- Step 3.1: Suspending active tasks ---' AS STEP;
-- ALTER TASK D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC SUSPEND;

-- 3.2 Create D_RAW as clone of D_BRONZE
SELECT '--- Step 3.2: Creating D_RAW as clone ---' AS STEP;

-- Check if D_RAW already exists
SHOW DATABASES LIKE 'D_RAW';

-- Create the clone (THIS IS THE MAIN MIGRATION COMMAND)
-- UNCOMMENT TO EXECUTE:
-- CREATE DATABASE D_RAW CLONE D_BRONZE;

SELECT 'EXECUTE: CREATE DATABASE D_RAW CLONE D_BRONZE;' AS COMMAND_TO_RUN;

-- 3.3 Verify clone was successful
-- SHOW DATABASES LIKE 'D_RAW';
-- SELECT COUNT(*) FROM D_RAW.INFORMATION_SCHEMA.TABLES;

-- 3.4 Apply grants to D_RAW
SELECT '--- Step 3.4: Apply grants to D_RAW ---' AS STEP;
-- Run the grant scripts captured in Phase 2

/*
EXAMPLE GRANT COMMANDS (customize based on your environment):

GRANT USAGE ON DATABASE D_RAW TO ROLE DATA_ENGINEER;
GRANT USAGE ON DATABASE D_RAW TO ROLE IDMC_ROLE;
GRANT ALL ON DATABASE D_RAW TO ROLE DATA_ADMIN;

GRANT USAGE ON SCHEMA D_RAW.SADB TO ROLE DATA_ENGINEER;
GRANT ALL ON SCHEMA D_RAW.SADB TO ROLE IDMC_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA D_RAW.SADB TO ROLE IDMC_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA D_RAW.SADB TO ROLE DATA_READER;

-- Future grants (important for new tables created by IDMC)
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA D_RAW.SADB TO ROLE IDMC_ROLE;
*/

-- =============================================================================
-- PHASE 4: REORGANIZE TABLES (Duration: 5-10 minutes)
-- =============================================================================
SELECT '========== PHASE 4: REORGANIZE TABLES ==========' AS PHASE;

-- 4.1 In D_RAW: Drop preservation tables (they belong in D_BRONZE)
SELECT '--- Step 4.1: Remove V1 tables from D_RAW ---' AS STEP;

/*
EXECUTE THESE COMMANDS AFTER CLONE:

-- Drop preservation tables from D_RAW (keep only raw/BASE tables)
DROP TABLE IF EXISTS D_RAW.SADB.TRKFC_TRSTN_V1;
DROP TABLE IF EXISTS D_RAW.SADB.TRKFC_TRSTN;  -- If this is also a V1-type table

-- Repeat for all other V1/preservation tables
*/

-- 4.2 In D_BRONZE: Drop raw tables (they now live in D_RAW)
SELECT '--- Step 4.2: Remove BASE tables from D_BRONZE ---' AS STEP;

/*
EXECUTE THESE COMMANDS:

-- Keep change tracking enabled on source for CDC reference
-- But the actual raw data now lives in D_RAW

-- Option A: Drop BASE tables from D_BRONZE entirely
DROP TABLE IF EXISTS D_BRONZE.SADB.TRKFC_TRSTN_BASE;

-- Option B (Recommended): Create VIEW in D_BRONZE pointing to D_RAW
-- This maintains backward compatibility for any queries expecting D_BRONZE.SADB.TRKFC_TRSTN_BASE
CREATE OR REPLACE VIEW D_BRONZE.SADB.TRKFC_TRSTN_BASE AS
SELECT * FROM D_RAW.SADB.TRKFC_TRSTN_BASE;
*/

-- =============================================================================
-- PHASE 5: UPDATE CDC INFRASTRUCTURE (Duration: 10 minutes)
-- =============================================================================
SELECT '========== PHASE 5: UPDATE CDC INFRASTRUCTURE ==========' AS PHASE;

-- 5.1 The CDC stream needs to point to D_RAW source table
SELECT '--- Step 5.1: Recreate stream on D_RAW source ---' AS STEP;

/*
-- Drop old stream (if exists)
DROP STREAM IF EXISTS D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM;

-- Create new stream pointing to D_RAW
CREATE OR REPLACE STREAM D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM
ON TABLE D_RAW.SADB.TRKFC_TRSTN_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream capturing changes from D_RAW source to D_BRONZE preservation';

-- Enable change tracking on D_RAW source table
ALTER TABLE D_RAW.SADB.TRKFC_TRSTN_BASE
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 14;
*/

-- 5.2 Update the stored procedure (if database references are hardcoded)
SELECT '--- Step 5.2: Update procedure references ---' AS STEP;

/*
The existing SP_PROCESS_TRKFC_TRSTN_CDC procedure has hardcoded D_BRONZE references.
After migration:
  - Source table: D_RAW.SADB.TRKFC_TRSTN_BASE
  - Target table: D_BRONZE.SADB.TRKFC_TRSTN_V1
  - Stream: D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM (on D_RAW source)

The procedure needs to be recreated with updated references.
See: DB_MIGRATION_UPDATED_PROCEDURE.sql
*/

-- 5.3 Update and restart task
SELECT '--- Step 5.3: Update task ---' AS STEP;

/*
-- Recreate task with updated stream reference
CREATE OR REPLACE TASK D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
WHEN
    SYSTEM$STREAM_HAS_DATA('D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM')
AS
    CALL D_BRONZE.SADB.SP_PROCESS_TRKFC_TRSTN_CDC();

-- Resume task
ALTER TASK D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC RESUME;
*/

-- =============================================================================
-- PHASE 6: VALIDATION (Duration: 5 minutes)
-- =============================================================================
SELECT '========== PHASE 6: VALIDATION ==========' AS PHASE;

-- 6.1 Verify D_RAW structure
SELECT '--- Validation 6.1: D_RAW contains raw tables ---' AS CHECK_NAME;
/*
SELECT TABLE_NAME, ROW_COUNT 
FROM D_RAW.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'SADB' AND TABLE_TYPE = 'BASE TABLE';
-- Expected: TRKFC_TRSTN_BASE and other _BASE tables
*/

-- 6.2 Verify D_BRONZE structure  
SELECT '--- Validation 6.2: D_BRONZE contains preservation tables ---' AS CHECK_NAME;
/*
SELECT TABLE_NAME, ROW_COUNT 
FROM D_BRONZE.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'SADB' AND TABLE_TYPE = 'BASE TABLE';
-- Expected: TRKFC_TRSTN_V1 and other _V1 tables
*/

-- 6.3 Verify stream is not stale
SELECT '--- Validation 6.3: Stream health ---' AS CHECK_NAME;
/*
SHOW STREAMS LIKE 'TRKFC_TRSTN_BASE_HIST_STREAM' IN SCHEMA D_BRONZE.SADB;
-- Expected: stale = false
*/

-- 6.4 Verify data consistency
SELECT '--- Validation 6.4: Data consistency ---' AS CHECK_NAME;
/*
SELECT 
    'D_RAW Source' AS LOCATION,
    COUNT(*) AS ROW_COUNT
FROM D_RAW.SADB.TRKFC_TRSTN_BASE
UNION ALL
SELECT 
    'D_BRONZE V1 (Active)',
    COUNT(*)
FROM D_BRONZE.SADB.TRKFC_TRSTN_V1
WHERE IS_DELETED = FALSE;
-- Row counts should match (or be close)
*/

-- 6.5 Test CDC processing
SELECT '--- Validation 6.5: Test CDC processing ---' AS CHECK_NAME;
/*
-- Make a test change in D_RAW
UPDATE D_RAW.SADB.TRKFC_TRSTN_BASE
SET VRSN_NBR = VRSN_NBR + 1
WHERE SCAC_CD = (SELECT MIN(SCAC_CD) FROM D_RAW.SADB.TRKFC_TRSTN_BASE);

-- Process CDC
CALL D_BRONZE.SADB.SP_PROCESS_TRKFC_TRSTN_CDC();

-- Verify change captured in V1
SELECT CDC_OPERATION, CDC_TIMESTAMP, VRSN_NBR 
FROM D_BRONZE.SADB.TRKFC_TRSTN_V1 
ORDER BY CDC_TIMESTAMP DESC LIMIT 5;
*/

-- =============================================================================
-- PHASE 7: IDMC CONFIGURATION UPDATE
-- =============================================================================
SELECT '========== PHASE 7: IDMC UPDATE ==========' AS PHASE;

/*
IDMC CONNECTION CHANGES REQUIRED:
================================

BEFORE:
  Connection Target: D_BRONZE.SADB
  Tables: TRKFC_TRSTN_BASE, etc.

AFTER:
  Connection Target: D_RAW.SADB
  Tables: TRKFC_TRSTN_BASE, etc. (same table names, different database)

STEPS:
1. In IDMC, update the Snowflake connection to point to D_RAW
2. Or create a new connection for D_RAW
3. Update all mappings to use the new connection
4. Test with a small data load
5. Resume full replication

NOTE: Table names remain the same, only database changes from D_BRONZE to D_RAW
*/

-- =============================================================================
-- ROLLBACK PROCEDURE
-- =============================================================================
SELECT '========== ROLLBACK PROCEDURE ==========' AS INFO;

/*
IF MIGRATION FAILS OR NEEDS TO BE REVERSED:

-- Step 1: Suspend CDC task
ALTER TASK D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC SUSPEND;

-- Step 2: Drop D_RAW database
DROP DATABASE IF EXISTS D_RAW;

-- Step 3: Restore D_BRONZE to original state
-- If we created views in D_BRONZE pointing to D_RAW, drop them
DROP VIEW IF EXISTS D_BRONZE.SADB.TRKFC_TRSTN_BASE;

-- Step 4: Recreate original objects using Time Travel (if needed)
-- CREATE TABLE D_BRONZE.SADB.TRKFC_TRSTN_BASE 
-- CLONE D_BRONZE.SADB.TRKFC_TRSTN_BASE AT (TIMESTAMP => '<before_migration_timestamp>');

-- Step 5: Restore original CDC infrastructure
-- Recreate stream pointing to D_BRONZE.SADB.TRKFC_TRSTN_BASE

-- Step 6: Update IDMC back to D_BRONZE

-- Step 7: Resume task
ALTER TASK D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC RESUME;
*/

SELECT 'MIGRATION SCRIPT COMPLETE - Review and execute each phase carefully' AS STATUS;
