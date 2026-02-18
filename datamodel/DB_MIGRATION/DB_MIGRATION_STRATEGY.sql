/*
================================================================================
DATABASE LAYER REORGANIZATION - MIGRATION STRATEGY & IMPLEMENTATION
================================================================================
Customer Requirement: Separate Raw Ingestion (D_RAW) from Data Preservation (D_BRONZE)

CURRENT STATE:
  D_BRONZE (Database)
    └── SADB (Schema)
        ├── TRKFC_TRSTN_BASE     ← IDMC ingestion (raw data)
        ├── TRKFC_TRSTN_V1       ← Data preservation table
        └── Other tables...
    └── SALES (Schema)
    └── PUBLIC (Schema)

TARGET STATE:
  D_RAW (Database)              ← NEW: Raw data layer (IDMC ingestion)
    └── SADB (Schema)
        ├── TRKFC_TRSTN_BASE    ← Moved here
        └── Other _BASE tables...
    └── SALES (Schema)
    └── PUBLIC (Schema)

  D_BRONZE (Database)           ← Data Preservation layer
    └── SADB (Schema)
        ├── TRKFC_TRSTN_V1      ← Stays here
        └── Other _V1 tables...
    └── SALES (Schema)
    └── PUBLIC (Schema)

================================================================================
APPROACH ANALYSIS
================================================================================

OPTION A: Rename D_BRONZE to D_RAW, Create new D_BRONZE
---------------------------------------------------------
Pros:
  ✓ Minimal data movement (only V1 tables need to move)
  ✓ IDMC connections may only need database name change
  ✓ All existing grants automatically transfer with rename
  ✓ Lower risk for raw data (stays in place)

Cons:
  ✗ V1 tables need to be moved/recreated
  ✗ Any hardcoded references to D_BRONZE break
  ✗ Streams/Tasks referencing D_BRONZE need updating

OPTION B: Create D_RAW, Move BASE tables, Keep D_BRONZE
---------------------------------------------------------
Pros:
  ✓ D_BRONZE references remain valid
  ✓ V1 tables stay in place
  ✓ Cleaner separation

Cons:
  ✗ More data movement (all BASE tables)
  ✗ IDMC needs full reconfiguration
  ✗ Complex grant management

OPTION C (RECOMMENDED): Clone + Swap Strategy
---------------------------------------------------------
1. Clone D_BRONZE to D_RAW (instant, zero-copy)
2. Drop V1 tables from D_RAW (keep only BASE tables)
3. Drop BASE tables from D_BRONZE (keep only V1 tables)
4. Update IDMC to point to D_RAW
5. Update CDC streams/procedures

Pros:
  ✓ Zero-copy cloning (instant)
  ✓ Minimal downtime
  ✓ Both databases maintain same structure initially
  ✓ Gradual cleanup possible
  ✓ Easy rollback

================================================================================
RECOMMENDED APPROACH: OPTION C - Clone + Reorganize
================================================================================
*/

-- =============================================================================
-- PHASE 1: PRE-MIGRATION ASSESSMENT
-- =============================================================================

-- 1.1 Document current state
SELECT '=== PRE-MIGRATION ASSESSMENT ===' AS PHASE;

-- 1.2 List all objects in D_BRONZE
SELECT 'SCHEMAS' AS TYPE, SCHEMA_NAME AS NAME, NULL AS ROW_COUNT 
FROM D_BRONZE.INFORMATION_SCHEMA.SCHEMATA 
WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
UNION ALL
SELECT 'TABLE', TABLE_SCHEMA || '.' || TABLE_NAME, ROW_COUNT::VARCHAR
FROM D_BRONZE.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
ORDER BY TYPE, NAME;

-- 1.3 Document existing grants (CRITICAL for RBAC)
SHOW GRANTS ON DATABASE D_BRONZE;
SHOW GRANTS ON SCHEMA D_BRONZE.SADB;
SHOW GRANTS ON SCHEMA D_BRONZE.SALES;

-- 1.4 Check for dependent objects
SHOW STREAMS IN DATABASE D_BRONZE;
SHOW TASKS IN DATABASE D_BRONZE;
SHOW PROCEDURES IN DATABASE D_BRONZE;

/*
================================================================================
RBAC IMPACT ANALYSIS
================================================================================

When you CLONE a database:
  - Grants on the SOURCE database do NOT transfer to the clone
  - Grants must be explicitly re-granted on the new database
  - Child object grants (schemas, tables) also need re-granting

When you RENAME a database:
  - Grants FOLLOW the renamed object automatically
  - No re-granting needed
  - All references update automatically

RECOMMENDED: Use CLONE for D_RAW, then re-apply grants
================================================================================
*/

-- =============================================================================
-- PHASE 2: BACKUP & GRANT DOCUMENTATION
-- =============================================================================

-- 2.1 Create grant backup script
-- This captures all grants that need to be reapplied

-- Database-level grants
SELECT 
    'GRANT ' || PRIVILEGE || ' ON DATABASE D_RAW TO ROLE ' || GRANTEE_NAME || 
    CASE WHEN GRANT_OPTION = 'true' THEN ' WITH GRANT OPTION' ELSE '' END || ';' 
    AS GRANT_SCRIPT
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE PRIVILEGE != 'OWNERSHIP';

/*
================================================================================
PHASE 3: MIGRATION EXECUTION SCRIPT
================================================================================
IMPORTANT: Run each step separately and verify before proceeding
================================================================================
*/
