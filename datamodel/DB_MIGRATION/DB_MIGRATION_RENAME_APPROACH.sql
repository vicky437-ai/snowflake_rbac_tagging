/*
================================================================================
DATABASE LAYER REORGANIZATION - RENAME APPROACH
================================================================================
Version      : 1.0.0
Scenario     : Early development - minimal objects, no V1 tables yet
Duration     : 10-15 minutes

APPROACH: Rename D_BRONZE → D_RAW, then create new D_BRONZE

BEFORE:
  D_BRONZE (contains IDMC raw tables)
    └── SADB
        └── TRKFC_TRSTN_BASE (IDMC)

AFTER:
  D_RAW (renamed from D_BRONZE - IDMC raw data)
    └── SADB
        └── TRKFC_TRSTN_BASE
  
  D_BRONZE (new - for data preservation)
    └── SADB
        └── (future V1 tables)

================================================================================
WHY RENAME FOR EARLY DEVELOPMENT?
================================================================================
✅ Single command - simple and fast
✅ All grants follow automatically (no re-granting)
✅ All objects stay intact (tables, streams, procedures)
✅ No data movement or duplication
✅ IDMC just needs connection database name update
================================================================================
*/

-- =============================================================================
-- PRE-MIGRATION CHECKLIST
-- =============================================================================
/*
Before proceeding, verify:
[ ] You have ACCOUNTADMIN or sufficient privileges
[ ] No active queries/processes on D_BRONZE
[ ] IDMC jobs are paused
[ ] No V1/preservation tables exist yet
[ ] Stakeholders notified of brief downtime
*/

-- =============================================================================
-- PHASE 1: PRE-MIGRATION ASSESSMENT (2 minutes)
-- =============================================================================

-- 1.1 Set context
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- 1.2 Document current state
SELECT '=== PHASE 1: PRE-MIGRATION ASSESSMENT ===' AS PHASE;

-- List all objects in D_BRONZE
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE,
    ROW_COUNT
FROM D_BRONZE.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- 1.3 Document current grants (these will follow the rename)
SHOW GRANTS ON DATABASE D_BRONZE;
SHOW GRANTS ON SCHEMA D_BRONZE.SADB;

-- 1.4 Verify no V1 tables exist
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ SAFE TO PROCEED - No V1 tables found'
        ELSE '⚠️ WARNING: V1 tables exist - consider CLONE approach instead'
    END AS V1_CHECK
FROM D_BRONZE.INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE '%_V1' AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA');

-- 1.5 Verify D_RAW doesn't already exist
SHOW DATABASES LIKE 'D_RAW';

-- =============================================================================
-- PHASE 2: EXECUTE RENAME (1 minute)
-- =============================================================================

SELECT '=== PHASE 2: RENAME D_BRONZE TO D_RAW ===' AS PHASE;

-- 2.1 THE MAIN COMMAND - Rename D_BRONZE to D_RAW
ALTER DATABASE D_BRONZE RENAME TO D_RAW;

-- 2.2 Verify rename successful
SHOW DATABASES LIKE 'D_RAW';

-- 2.3 Verify all objects moved with the rename
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    ROW_COUNT
FROM D_RAW.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- 2.4 Verify grants followed (should be same as before)
SHOW GRANTS ON DATABASE D_RAW;

-- =============================================================================
-- PHASE 3: CREATE NEW D_BRONZE (2 minutes)
-- =============================================================================

SELECT '=== PHASE 3: CREATE NEW D_BRONZE ===' AS PHASE;

-- 3.1 Create new D_BRONZE database for data preservation layer
CREATE DATABASE D_BRONZE
COMMENT = 'Data Preservation Layer - Contains V1 tables with soft deletes';

-- 3.2 Create matching schemas (mirror D_RAW structure)
CREATE SCHEMA D_BRONZE.SADB
COMMENT = 'SADB schema - Data preservation tables';

-- Create other schemas as needed (based on your D_RAW schemas)
-- CREATE SCHEMA D_BRONZE.SALES;
-- CREATE SCHEMA D_BRONZE.PUBLIC;  -- Usually auto-created

-- 3.3 Verify D_BRONZE created
SHOW DATABASES LIKE 'D_BRONZE';
SHOW SCHEMAS IN DATABASE D_BRONZE;

-- =============================================================================
-- PHASE 4: APPLY GRANTS TO NEW D_BRONZE (3 minutes)
-- =============================================================================

SELECT '=== PHASE 4: GRANTS FOR NEW D_BRONZE ===' AS PHASE;

/*
Since D_BRONZE is NEW, you need to grant access.
Customize these based on your roles:
*/

-- 4.1 Database-level grants
GRANT USAGE ON DATABASE D_BRONZE TO ROLE SYSADMIN;
-- GRANT USAGE ON DATABASE D_BRONZE TO ROLE DATA_ENGINEER;
-- GRANT USAGE ON DATABASE D_BRONZE TO ROLE DATA_READER;

-- 4.2 Schema-level grants
GRANT USAGE ON SCHEMA D_BRONZE.SADB TO ROLE SYSADMIN;
GRANT ALL ON SCHEMA D_BRONZE.SADB TO ROLE SYSADMIN;
-- GRANT USAGE ON SCHEMA D_BRONZE.SADB TO ROLE DATA_ENGINEER;
-- GRANT ALL ON SCHEMA D_BRONZE.SADB TO ROLE DATA_ENGINEER;

-- 4.3 Future grants (for V1 tables that will be created)
GRANT SELECT ON FUTURE TABLES IN SCHEMA D_BRONZE.SADB TO ROLE SYSADMIN;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA D_BRONZE.SADB TO ROLE DATA_READER;
-- GRANT ALL ON FUTURE TABLES IN SCHEMA D_BRONZE.SADB TO ROLE DATA_ENGINEER;

-- =============================================================================
-- PHASE 5: SETUP CDC INFRASTRUCTURE (5 minutes)
-- =============================================================================

SELECT '=== PHASE 5: SETUP CDC INFRASTRUCTURE ===' AS PHASE;

-- 5.1 Enable change tracking on D_RAW source table
ALTER TABLE D_RAW.SADB.TRKFC_TRSTN_BASE
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 14;

-- 5.2 Create V1 preservation table in D_BRONZE
CREATE TABLE IF NOT EXISTS D_BRONZE.SADB.TRKFC_TRSTN_V1 (
    -- Primary Keys
    SCAC_CD VARCHAR(4) NOT NULL,
    FSAC_CD VARCHAR(4) NOT NULL,
    
    -- Business Columns (copy from source table structure)
    VRSN_NBR NUMBER(38,0),
    CREAT_TS TIMESTAMP_NTZ(9),
    CREAT_USER_ID VARCHAR(18),
    UPD_TS TIMESTAMP_NTZ(9),
    UPD_USER_ID VARCHAR(18),
    STRT_EFF_DT DATE,
    END_EFF_DT DATE,
    XFR_TYP VARCHAR(10),
    OPRATN_TYP VARCHAR(10),
    SEG_TYP_CD VARCHAR(1),
    HZMT_IN VARCHAR(1),
    PLLT_IN VARCHAR(1),
    CRTG_IN VARCHAR(1),
    TARP_IN VARCHAR(1),
    MAX_LEN_NBR NUMBER(38,0),
    MAX_WT_NBR NUMBER(38,0),
    XPDT_DLVR_DAY_CNT NUMBER(38,0),
    XPDT_PCKG_DAY_CNT NUMBER(38,0),
    ONLN_AVBL_IN VARCHAR(1),
    PPD_ALOW_IN VARCHAR(1),
    COL_ALOW_IN VARCHAR(1),
    TP_ALOW_IN VARCHAR(1),
    SHP_SITE_RGST_IN VARCHAR(1),
    CON_SITE_RGST_IN VARCHAR(1),
    MIN_PCKG_CHG_AMT NUMBER(11,2),
    MIN_DLVR_CHG_AMT NUMBER(11,2),
    DFLT_TRSTN_TYP VARCHAR(10),
    DFLT_MUL_STOP_IND VARCHAR(1),
    DFLT_INTR_IND VARCHAR(1),
    DFLT_OBSZ_IND VARCHAR(1),
    ASGN_SHP_SITE_IN VARCHAR(1),
    ASGN_CON_SITE_IN VARCHAR(1),
    SLCT_VIA_PTS_IN VARCHAR(1),
    WT_UPLD_ALWD_IN VARCHAR(1),
    
    -- CDC Metadata Columns
    IS_DELETED BOOLEAN DEFAULT FALSE,
    CDC_OPERATION VARCHAR(10),
    CDC_TIMESTAMP TIMESTAMP_NTZ(9),
    RECORD_CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    RECORD_UPDATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_LOAD_BATCH_ID VARCHAR(100),
    
    -- Primary Key Constraint
    PRIMARY KEY (SCAC_CD, FSAC_CD)
)
COMMENT = 'Data preservation table with soft deletes - Source: D_RAW.SADB.TRKFC_TRSTN_BASE';

-- 5.3 Create CDC stream (in D_BRONZE, pointing to D_RAW source)
CREATE OR REPLACE STREAM D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM
ON TABLE D_RAW.SADB.TRKFC_TRSTN_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC stream capturing changes from D_RAW source';

-- 5.4 Verify stream created and has initial data
SHOW STREAMS LIKE 'TRKFC_TRSTN_BASE_HIST_STREAM' IN SCHEMA D_BRONZE.SADB;
SELECT COUNT(*) AS INITIAL_ROWS FROM D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM;

-- =============================================================================
-- PHASE 6: VALIDATION (2 minutes)
-- =============================================================================

SELECT '=== PHASE 6: VALIDATION ===' AS PHASE;

-- 6.1 Final architecture check
SELECT 
    'D_RAW' AS DATABASE_NAME,
    TABLE_SCHEMA,
    TABLE_NAME,
    'RAW/SOURCE' AS LAYER
FROM D_RAW.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SADB' AND TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 
    'D_BRONZE',
    TABLE_SCHEMA,
    TABLE_NAME,
    'PRESERVATION'
FROM D_BRONZE.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SADB' AND TABLE_TYPE = 'BASE TABLE'
ORDER BY 1, 2, 3;

-- 6.2 Verify grants on both databases
SELECT '--- D_RAW Grants ---' AS CHECK_NAME;
SHOW GRANTS ON DATABASE D_RAW;

SELECT '--- D_BRONZE Grants ---' AS CHECK_NAME;
SHOW GRANTS ON DATABASE D_BRONZE;

-- 6.3 Verify stream health
SELECT '--- Stream Status ---' AS CHECK_NAME;
SHOW STREAMS IN SCHEMA D_BRONZE.SADB;

-- =============================================================================
-- PHASE 7: IDMC CONFIGURATION UPDATE
-- =============================================================================

SELECT '=== PHASE 7: IDMC UPDATE REQUIRED ===' AS PHASE;

/*
================================================================================
IDMC CONNECTION CHANGE REQUIRED:
================================================================================

BEFORE:
  Database: D_BRONZE
  Schema: SADB
  
AFTER:
  Database: D_RAW    ← CHANGE THIS
  Schema: SADB       ← No change

STEPS IN IDMC:
1. Edit Snowflake connection
2. Change database from "D_BRONZE" to "D_RAW"
3. Test connection
4. No mapping changes needed (table names same)
5. Resume IDMC jobs

================================================================================
*/

-- =============================================================================
-- ROLLBACK PROCEDURE (If needed)
-- =============================================================================

/*
================================================================================
ROLLBACK - If something goes wrong:
================================================================================

-- Step 1: Drop new D_BRONZE
DROP DATABASE IF EXISTS D_BRONZE;

-- Step 2: Rename D_RAW back to D_BRONZE
ALTER DATABASE D_RAW RENAME TO D_BRONZE;

-- Step 3: Update IDMC back to D_BRONZE

-- Step 4: Verify
SHOW DATABASES LIKE 'D_BRONZE';

================================================================================
*/

SELECT '=== MIGRATION COMPLETE ===' AS STATUS;

/*
================================================================================
SUMMARY
================================================================================

COMPLETED:
✅ Renamed D_BRONZE → D_RAW (all grants followed)
✅ Created new D_BRONZE database
✅ Created SADB schema in D_BRONZE
✅ Applied grants to new D_BRONZE
✅ Enabled change tracking on D_RAW source
✅ Created V1 preservation table in D_BRONZE
✅ Created CDC stream (cross-database)

PENDING (Customer Action):
⏳ Update IDMC connection to use D_RAW
⏳ Create CDC stored procedure
⏳ Create CDC task
⏳ Test end-to-end CDC flow

ARCHITECTURE:
  D_RAW (IDMC ingestion)           D_BRONZE (Preservation)
  └── SADB                         └── SADB
      └── TRKFC_TRSTN_BASE  ──────►    ├── Stream (on D_RAW)
          (source)                      └── TRKFC_TRSTN_V1
                                            (soft deletes)

================================================================================
*/
