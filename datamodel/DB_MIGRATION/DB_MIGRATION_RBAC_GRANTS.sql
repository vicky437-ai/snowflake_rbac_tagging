/*
================================================================================
RBAC IMPACT ANALYSIS & GRANT MANAGEMENT
================================================================================
Purpose: Document and manage RBAC changes for D_RAW + D_BRONZE migration
================================================================================

RBAC BEHAVIOR IN SNOWFLAKE:
===========================

1. DATABASE CLONE:
   - Grants on source database DO NOT transfer to clone
   - All grants must be re-applied to cloned database
   - Child objects (schemas, tables) also need grants

2. DATABASE RENAME:
   - Grants FOLLOW the renamed database automatically
   - No re-granting needed for renamed objects

3. STREAM ON EXTERNAL TABLE:
   - Stream in D_BRONZE can reference table in D_RAW
   - Requires: USAGE on D_RAW database and schema
   - Requires: SELECT on source table in D_RAW

================================================================================
*/

-- =============================================================================
-- PHASE 1: CAPTURE CURRENT GRANTS (Run BEFORE migration)
-- =============================================================================

-- 1.1 Database-level grants
SHOW GRANTS ON DATABASE D_BRONZE;

-- 1.2 Schema-level grants (repeat for each schema)
SHOW GRANTS ON SCHEMA D_BRONZE.SADB;
SHOW GRANTS ON SCHEMA D_BRONZE.SALES;
SHOW GRANTS ON SCHEMA D_BRONZE.PUBLIC;

-- 1.3 Table-level grants (sample)
SHOW GRANTS ON TABLE D_BRONZE.SADB.TRKFC_TRSTN_BASE;

-- =============================================================================
-- PHASE 2: REQUIRED GRANTS FOR NEW ARCHITECTURE
-- =============================================================================

/*
ROLE MATRIX:
============
+------------------+------------------+------------------+------------------+
| Role             | D_RAW            | D_BRONZE         | Cross-DB Stream  |
+------------------+------------------+------------------+------------------+
| ACCOUNTADMIN     | OWNERSHIP        | OWNERSHIP        | Full access      |
| SYSADMIN         | ALL              | ALL              | Full access      |
| DATA_ADMIN       | ALL              | ALL              | Full access      |
| IDMC_ROLE        | USAGE, INSERT,   | READ ONLY        | N/A              |
|                  | UPDATE, DELETE   | (V1 tables)      |                  |
| DATA_ENGINEER    | USAGE, SELECT    | ALL              | Full access      |
| DATA_READER      | SELECT           | SELECT           | N/A              |
| DATA_SCIENTIST   | SELECT           | SELECT           | N/A              |
+------------------+------------------+------------------+------------------+
*/

-- =============================================================================
-- PHASE 3: GRANT SCRIPTS FOR D_RAW (New Raw Layer)
-- =============================================================================

-- 3.1 Database-level grants
GRANT USAGE ON DATABASE D_RAW TO ROLE SYSADMIN;
GRANT USAGE ON DATABASE D_RAW TO ROLE DATA_ADMIN;
GRANT USAGE ON DATABASE D_RAW TO ROLE DATA_ENGINEER;
GRANT USAGE ON DATABASE D_RAW TO ROLE DATA_READER;
-- GRANT USAGE ON DATABASE D_RAW TO ROLE IDMC_ROLE;  -- Uncomment if IDMC uses a custom role

-- 3.2 Schema-level grants
GRANT USAGE ON SCHEMA D_RAW.SADB TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA D_RAW.SADB TO ROLE DATA_ADMIN;
GRANT USAGE ON SCHEMA D_RAW.SADB TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA D_RAW.SADB TO ROLE DATA_READER;
-- GRANT ALL ON SCHEMA D_RAW.SADB TO ROLE IDMC_ROLE;

-- 3.3 Table-level grants for existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA D_RAW.SADB TO ROLE DATA_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA D_RAW.SADB TO ROLE DATA_ENGINEER;
GRANT ALL ON ALL TABLES IN SCHEMA D_RAW.SADB TO ROLE DATA_ADMIN;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA D_RAW.SADB TO ROLE IDMC_ROLE;

-- 3.4 FUTURE grants (for tables IDMC will create)
GRANT SELECT ON FUTURE TABLES IN SCHEMA D_RAW.SADB TO ROLE DATA_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA D_RAW.SADB TO ROLE DATA_ENGINEER;
GRANT ALL ON FUTURE TABLES IN SCHEMA D_RAW.SADB TO ROLE DATA_ADMIN;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA D_RAW.SADB TO ROLE IDMC_ROLE;

-- =============================================================================
-- PHASE 4: VERIFY/UPDATE D_BRONZE GRANTS (Preservation Layer)
-- =============================================================================

-- 4.1 Existing grants should remain (D_BRONZE already exists)
-- Only need to ensure cross-database access works

-- 4.2 Grant access to D_RAW for stream functionality
-- The stream in D_BRONZE needs to read from D_RAW
GRANT USAGE ON DATABASE D_RAW TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA D_RAW.SADB TO ROLE ACCOUNTADMIN;
GRANT SELECT ON TABLE D_RAW.SADB.TRKFC_TRSTN_BASE TO ROLE ACCOUNTADMIN;

-- 4.3 Stream-specific grants
-- The role executing the CDC task needs:
-- - SELECT on D_RAW source table
-- - INSERT, UPDATE on D_BRONZE target table

-- =============================================================================
-- PHASE 5: CROSS-DATABASE STREAM REQUIREMENTS
-- =============================================================================

/*
CRITICAL REQUIREMENT:
====================
When a STREAM in D_BRONZE references a TABLE in D_RAW, the executing role needs:

1. On D_RAW (source database):
   - USAGE on database
   - USAGE on schema
   - SELECT on source table
   - Table must have CHANGE_TRACKING = TRUE

2. On D_BRONZE (stream database):
   - USAGE on database
   - USAGE on schema
   - SELECT on stream
   - INSERT/UPDATE on target table

EXAMPLE for CDC processing role:
*/

-- Create dedicated CDC role (recommended)
-- CREATE ROLE IF NOT EXISTS CDC_PROCESSOR_ROLE;

-- Grant D_RAW access
-- GRANT USAGE ON DATABASE D_RAW TO ROLE CDC_PROCESSOR_ROLE;
-- GRANT USAGE ON SCHEMA D_RAW.SADB TO ROLE CDC_PROCESSOR_ROLE;
-- GRANT SELECT ON TABLE D_RAW.SADB.TRKFC_TRSTN_BASE TO ROLE CDC_PROCESSOR_ROLE;

-- Grant D_BRONZE access
-- GRANT USAGE ON DATABASE D_BRONZE TO ROLE CDC_PROCESSOR_ROLE;
-- GRANT USAGE ON SCHEMA D_BRONZE.SADB TO ROLE CDC_PROCESSOR_ROLE;
-- GRANT SELECT ON STREAM D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM TO ROLE CDC_PROCESSOR_ROLE;
-- GRANT INSERT, UPDATE ON TABLE D_BRONZE.SADB.TRKFC_TRSTN_V1 TO ROLE CDC_PROCESSOR_ROLE;

-- Grant procedure execution
-- GRANT USAGE ON PROCEDURE D_BRONZE.SADB.SP_PROCESS_TRKFC_TRSTN_CDC() TO ROLE CDC_PROCESSOR_ROLE;

-- Grant warehouse for task execution
-- GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE CDC_PROCESSOR_ROLE;

-- =============================================================================
-- PHASE 6: RBAC VALIDATION QUERIES
-- =============================================================================

-- 6.1 Verify grants on D_RAW
SELECT 'D_RAW Grants' AS CHECK_NAME;
SHOW GRANTS ON DATABASE D_RAW;

-- 6.2 Verify grants on D_RAW.SADB schema
SHOW GRANTS ON SCHEMA D_RAW.SADB;

-- 6.3 Verify grants on source table
SHOW GRANTS ON TABLE D_RAW.SADB.TRKFC_TRSTN_BASE;

-- 6.4 Verify grants on stream (cross-DB)
SHOW GRANTS ON STREAM D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM;

-- 6.5 Verify grants on target table
SHOW GRANTS ON TABLE D_BRONZE.SADB.TRKFC_TRSTN_V1;

-- 6.6 Test cross-database access
-- Switch to a non-admin role and verify access
-- USE ROLE DATA_ENGINEER;
-- SELECT COUNT(*) FROM D_RAW.SADB.TRKFC_TRSTN_BASE;
-- SELECT COUNT(*) FROM D_BRONZE.SADB.TRKFC_TRSTN_V1;

-- =============================================================================
-- PHASE 7: IMPACT SUMMARY
-- =============================================================================

/*
RBAC IMPACT SUMMARY:
====================

1. NEW GRANTS REQUIRED:
   - All existing roles need USAGE on D_RAW database
   - Schema and table grants need replication on D_RAW
   - FUTURE grants ensure new IDMC tables get proper access

2. EXISTING GRANTS (D_BRONZE):
   - Remain intact (no changes needed)
   - D_BRONZE ownership unchanged

3. CROSS-DATABASE CONSIDERATIONS:
   - Stream in D_BRONZE accessing D_RAW table works
   - Executing role needs grants on BOTH databases
   - Task warehouse needs USAGE grants

4. IDMC CHANGES:
   - Connection target changes from D_BRONZE to D_RAW
   - Same table names, different database
   - May need to update IDMC connection credentials/role

5. APPLICATION IMPACT:
   - Apps reading from D_BRONZE V1 tables: NO CHANGE
   - Apps reading from BASE tables: UPDATE to D_RAW reference
   - OR: Create views in D_BRONZE pointing to D_RAW tables

================================================================================
*/

-- =============================================================================
-- BACKWARD COMPATIBILITY VIEWS (Optional)
-- =============================================================================

/*
If applications currently read from D_BRONZE.SADB.TRKFC_TRSTN_BASE,
create a view to maintain backward compatibility:

CREATE OR REPLACE VIEW D_BRONZE.SADB.TRKFC_TRSTN_BASE_VIEW AS
SELECT * FROM D_RAW.SADB.TRKFC_TRSTN_BASE;

GRANT SELECT ON VIEW D_BRONZE.SADB.TRKFC_TRSTN_BASE_VIEW TO ROLE DATA_READER;

This allows gradual migration of downstream applications.
*/
