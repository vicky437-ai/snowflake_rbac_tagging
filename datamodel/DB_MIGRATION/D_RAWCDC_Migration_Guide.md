# D_BRONZE to D_RAWCDC Schema Migration Guide

## Overview

This guide provides complete scripts for migrating SADB and AWS schemas from D_BRONZE to a new D_RAWCDC database.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MIGRATION OVERVIEW                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   BEFORE                                    AFTER                            │
│   ┌─────────────────────────┐              ┌─────────────────────────┐      │
│   │       D_BRONZE          │              │       D_RAWCDC          │      │
│   │  ┌───────┐ ┌───────┐   │              │  ┌───────┐ ┌───────┐   │      │
│   │  │ SADB  │ │  AWS  │   │   ────►      │  │ SADB  │ │  AWS  │   │      │
│   │  └───────┘ └───────┘   │   CLONE      │  └───────┘ └───────┘   │      │
│   │  ┌───────┐ ┌───────┐   │              └─────────────────────────┘      │
│   │  │SCHEMA3│ │SCHEMA4│   │                                                │
│   │  └───────┘ └───────┘   │              ┌─────────────────────────┐      │
│   │  ┌───────┐ ┌───────┐   │              │       D_BRONZE          │      │
│   │  │SCHEMA5│ │SCHEMA6│   │              │  ┌───────┐ ┌───────┐   │      │
│   │  └───────┘ └───────┘   │              │  │SCHEMA3│ │SCHEMA4│   │      │
│   │  ┌───────┐ ┌───────┐   │              │  └───────┘ └───────┘   │      │
│   │  │SCHEMA7│ │SCHEMA8│   │              │  ┌───────┐ ┌───────┐   │      │
│   │  └───────┘ └───────┘   │              │  │SCHEMA5│ │SCHEMA6│   │      │
│   └─────────────────────────┘              │  └───────┘ └───────┘   │      │
│                                            │  ┌───────┐ ┌───────┐   │      │
│                                            │  │SCHEMA7│ │SCHEMA8│   │      │
│                                            │  └───────┘ └───────┘   │      │
│                                            └─────────────────────────┘      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Table of Contents

1. [Phase 1: Pre-Migration Assessment](#phase-1-pre-migration-assessment)
2. [Phase 2: Pre-Migration Preparation](#phase-2-pre-migration-preparation)
3. [Phase 3: Execute Migration](#phase-3-execute-migration)
4. [Phase 4: Update Dependencies](#phase-4-update-dependencies)
5. [Phase 5: RBAC Configuration](#phase-5-rbac-configuration)
6. [Phase 6: Post-Migration Validation](#phase-6-post-migration-validation)
7. [Phase 7: Cleanup Original Schemas](#phase-7-cleanup-original-schemas)
8. [Phase 8: Rollback Procedures](#phase-8-rollback-procedures)
9. [Appendix: Informatica IDMC Reconfiguration](#appendix-informatica-idmc-reconfiguration)

---

## Phase 1: Pre-Migration Assessment

### Script 1.1: Inventory Objects in Target Schemas

```sql
-- ============================================================================
-- SCRIPT 1.1: INVENTORY ASSESSMENT
-- Run this FIRST to understand what will be migrated
-- ============================================================================

USE ROLE ACCOUNTADMIN;  -- Or role with appropriate privileges
USE DATABASE D_BRONZE;

-- 1.1.1: Count objects by type in SADB and AWS schemas
SELECT 
    TABLE_SCHEMA,
    TABLE_TYPE,
    COUNT(*) AS OBJECT_COUNT
FROM D_BRONZE.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('SADB', 'AWS')
GROUP BY TABLE_SCHEMA, TABLE_TYPE
ORDER BY TABLE_SCHEMA, TABLE_TYPE;

-- 1.1.2: List all tables with row counts and sizes
SELECT 
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    t.TABLE_TYPE,
    t.ROW_COUNT,
    t.BYTES / (1024*1024*1024) AS SIZE_GB,
    t.CREATED,
    t.LAST_ALTERED
FROM D_BRONZE.INFORMATION_SCHEMA.TABLES t
WHERE t.TABLE_SCHEMA IN ('SADB', 'AWS')
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;

-- 1.1.3: List all views
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    VIEW_DEFINITION
FROM D_BRONZE.INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA IN ('SADB', 'AWS')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- 1.1.4: List all streams
SELECT 
    STREAM_SCHEMA AS SCHEMA_NAME,
    STREAM_NAME,
    TABLE_NAME AS SOURCE_TABLE,
    TYPE,
    STALE,
    MODE
FROM D_BRONZE.INFORMATION_SCHEMA.STREAMS
WHERE STREAM_SCHEMA IN ('SADB', 'AWS')
ORDER BY STREAM_SCHEMA, STREAM_NAME;

-- 1.1.5: List all stages
SELECT 
    STAGE_SCHEMA,
    STAGE_NAME,
    STAGE_TYPE,
    STAGE_URL
FROM D_BRONZE.INFORMATION_SCHEMA.STAGES
WHERE STAGE_SCHEMA IN ('SADB', 'AWS')
ORDER BY STAGE_SCHEMA, STAGE_NAME;

-- 1.1.6: List all procedures
SELECT 
    PROCEDURE_SCHEMA,
    PROCEDURE_NAME,
    ARGUMENT_SIGNATURE
FROM D_BRONZE.INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA IN ('SADB', 'AWS')
ORDER BY PROCEDURE_SCHEMA, PROCEDURE_NAME;

-- 1.1.7: List all functions
SELECT 
    FUNCTION_SCHEMA,
    FUNCTION_NAME,
    ARGUMENT_SIGNATURE
FROM D_BRONZE.INFORMATION_SCHEMA.FUNCTIONS
WHERE FUNCTION_SCHEMA IN ('SADB', 'AWS')
ORDER BY FUNCTION_SCHEMA, FUNCTION_NAME;

-- 1.1.8: List all sequences
SELECT 
    SEQUENCE_SCHEMA,
    SEQUENCE_NAME,
    CURRENT_VALUE
FROM D_BRONZE.INFORMATION_SCHEMA.SEQUENCES
WHERE SEQUENCE_SCHEMA IN ('SADB', 'AWS')
ORDER BY SEQUENCE_SCHEMA, SEQUENCE_NAME;

-- 1.1.9: List all file formats
SELECT 
    FILE_FORMAT_SCHEMA,
    FILE_FORMAT_NAME,
    FILE_FORMAT_TYPE
FROM D_BRONZE.INFORMATION_SCHEMA.FILE_FORMATS
WHERE FILE_FORMAT_SCHEMA IN ('SADB', 'AWS')
ORDER BY FILE_FORMAT_SCHEMA, FILE_FORMAT_NAME;

-- 1.1.10: List all tasks
SELECT 
    SCHEMA_NAME,
    NAME AS TASK_NAME,
    STATE,
    SCHEDULE,
    DEFINITION
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'D_BRONZE.SADB.%',
    RECURSIVE => TRUE
));

SHOW TASKS IN SCHEMA D_BRONZE.SADB;
SHOW TASKS IN SCHEMA D_BRONZE.AWS;
```

### Script 1.2: Identify Cross-Schema Dependencies

```sql
-- ============================================================================
-- SCRIPT 1.2: DEPENDENCY ANALYSIS
-- Identify objects that reference SADB or AWS schemas
-- CRITICAL: Save these results - you'll need them in Phase 4
-- ============================================================================

-- 1.2.1: Find views in OTHER schemas within D_BRONZE that reference SADB or AWS
SELECT 
    TABLE_CATALOG AS DATABASE_NAME,
    TABLE_SCHEMA AS VIEW_SCHEMA,
    TABLE_NAME AS VIEW_NAME,
    VIEW_DEFINITION
FROM D_BRONZE.INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA NOT IN ('SADB', 'AWS', 'INFORMATION_SCHEMA')
    AND (
        VIEW_DEFINITION ILIKE '%SADB.%' 
        OR VIEW_DEFINITION ILIKE '%AWS.%'
        OR VIEW_DEFINITION ILIKE '%D_BRONZE.SADB.%'
        OR VIEW_DEFINITION ILIKE '%D_BRONZE.AWS.%'
    )
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- 1.2.2: Find streams in OTHER schemas that reference SADB or AWS tables
SELECT 
    s.STREAM_CATALOG,
    s.STREAM_SCHEMA,
    s.STREAM_NAME,
    s.TABLE_CATALOG AS SOURCE_DB,
    s.TABLE_SCHEMA AS SOURCE_SCHEMA,
    s.TABLE_NAME AS SOURCE_TABLE,
    s.STALE
FROM D_BRONZE.INFORMATION_SCHEMA.STREAMS s
WHERE s.STREAM_SCHEMA NOT IN ('SADB', 'AWS', 'INFORMATION_SCHEMA')
    AND s.TABLE_SCHEMA IN ('SADB', 'AWS');

-- 1.2.3: Find tasks that reference SADB or AWS (check task definition)
SELECT 
    DATABASE_NAME,
    SCHEMA_NAME,
    NAME AS TASK_NAME,
    STATE,
    DEFINITION
FROM SNOWFLAKE.ACCOUNT_USAGE.TASKS
WHERE DATABASE_NAME = 'D_BRONZE'
    AND (
        DEFINITION ILIKE '%SADB.%' 
        OR DEFINITION ILIKE '%AWS.%'
        OR DEFINITION ILIKE '%D_BRONZE.SADB.%'
        OR DEFINITION ILIKE '%D_BRONZE.AWS.%'
    )
    AND DELETED IS NULL;

-- 1.2.4: Find procedures that reference SADB or AWS
SELECT 
    PROCEDURE_CATALOG,
    PROCEDURE_SCHEMA,
    PROCEDURE_NAME,
    PROCEDURE_DEFINITION
FROM D_BRONZE.INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA NOT IN ('SADB', 'AWS', 'INFORMATION_SCHEMA')
    AND (
        PROCEDURE_DEFINITION ILIKE '%SADB.%' 
        OR PROCEDURE_DEFINITION ILIKE '%AWS.%'
    );

-- 1.2.5: Check for dependencies from D_SILVER database
SELECT 
    TABLE_CATALOG AS DATABASE_NAME,
    TABLE_SCHEMA,
    TABLE_NAME AS VIEW_NAME,
    VIEW_DEFINITION
FROM D_SILVER.INFORMATION_SCHEMA.VIEWS
WHERE VIEW_DEFINITION ILIKE '%D_BRONZE.SADB.%'
   OR VIEW_DEFINITION ILIKE '%D_BRONZE.AWS.%';

-- 1.2.6: Check for dependencies from D_GOLD database
SELECT 
    TABLE_CATALOG AS DATABASE_NAME,
    TABLE_SCHEMA,
    TABLE_NAME AS VIEW_NAME,
    VIEW_DEFINITION
FROM D_GOLD.INFORMATION_SCHEMA.VIEWS
WHERE VIEW_DEFINITION ILIKE '%D_BRONZE.SADB.%'
   OR VIEW_DEFINITION ILIKE '%D_BRONZE.AWS.%';

-- 1.2.7: Find streams in D_SILVER that reference D_BRONZE.SADB or D_BRONZE.AWS
SELECT 
    s.STREAM_CATALOG,
    s.STREAM_SCHEMA,
    s.STREAM_NAME,
    s.TABLE_CATALOG AS SOURCE_DB,
    s.TABLE_SCHEMA AS SOURCE_SCHEMA,
    s.TABLE_NAME AS SOURCE_TABLE
FROM D_SILVER.INFORMATION_SCHEMA.STREAMS s
WHERE s.TABLE_CATALOG = 'D_BRONZE'
    AND s.TABLE_SCHEMA IN ('SADB', 'AWS');

-- 1.2.8: Find tasks in D_SILVER referencing migrated schemas
SELECT 
    DATABASE_NAME,
    SCHEMA_NAME,
    NAME AS TASK_NAME,
    STATE,
    DEFINITION
FROM SNOWFLAKE.ACCOUNT_USAGE.TASKS
WHERE DATABASE_NAME = 'D_SILVER'
    AND (
        DEFINITION ILIKE '%D_BRONZE.SADB.%' 
        OR DEFINITION ILIKE '%D_BRONZE.AWS.%'
    )
    AND DELETED IS NULL;

-- 1.2.9: Find tasks in D_GOLD referencing migrated schemas
SELECT 
    DATABASE_NAME,
    SCHEMA_NAME,
    NAME AS TASK_NAME,
    STATE,
    DEFINITION
FROM SNOWFLAKE.ACCOUNT_USAGE.TASKS
WHERE DATABASE_NAME = 'D_GOLD'
    AND (
        DEFINITION ILIKE '%D_BRONZE.SADB.%' 
        OR DEFINITION ILIKE '%D_BRONZE.AWS.%'
    )
    AND DELETED IS NULL;
```

### Script 1.3: Document Current Grants

```sql
-- ============================================================================
-- SCRIPT 1.3: RBAC DOCUMENTATION
-- Capture existing grants for recreation in D_RAWCDC
-- SAVE THIS OUTPUT - Required for Phase 5
-- ============================================================================

-- 1.3.1: Schema-level grants
SELECT 
    PRIVILEGE,
    GRANTED_ON,
    NAME AS OBJECT_NAME,
    GRANTEE_NAME,
    GRANT_OPTION,
    'GRANT ' || PRIVILEGE || ' ON SCHEMA D_RAWCDC.' || 
        SPLIT_PART(NAME, '.', 2) || ' TO ROLE ' || GRANTEE_NAME || 
        CASE WHEN GRANT_OPTION = 'true' THEN ' WITH GRANT OPTION' ELSE '' END || ';' 
        AS RECREATE_GRANT_SQL
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE GRANTED_ON = 'SCHEMA'
    AND NAME IN ('D_BRONZE.SADB', 'D_BRONZE.AWS')
    AND DELETED_ON IS NULL
ORDER BY NAME, GRANTEE_NAME, PRIVILEGE;

-- 1.3.2: Table-level grants
SELECT 
    PRIVILEGE,
    GRANTED_ON,
    TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || NAME AS FULL_NAME,
    GRANTEE_NAME,
    'GRANT ' || PRIVILEGE || ' ON ' || GRANTED_ON || ' D_RAWCDC.' || 
        TABLE_SCHEMA || '.' || NAME || ' TO ROLE ' || GRANTEE_NAME || ';' 
        AS RECREATE_GRANT_SQL
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE GRANTED_ON IN ('TABLE', 'VIEW')
    AND TABLE_CATALOG = 'D_BRONZE'
    AND TABLE_SCHEMA IN ('SADB', 'AWS')
    AND DELETED_ON IS NULL
ORDER BY TABLE_SCHEMA, NAME, GRANTEE_NAME;

-- 1.3.3: Future grants on schemas
SHOW FUTURE GRANTS IN SCHEMA D_BRONZE.SADB;
SHOW FUTURE GRANTS IN SCHEMA D_BRONZE.AWS;

-- 1.3.4: Generate future grant recreation scripts
SELECT 
    'GRANT ' || "privilege" || ' ON FUTURE ' || "grant_on" || 'S IN SCHEMA D_RAWCDC.SADB TO ROLE ' || "grantee_name" || ';' AS RECREATE_SQL
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-2)));  -- Adjust based on SHOW FUTURE GRANTS output

SELECT 
    'GRANT ' || "privilege" || ' ON FUTURE ' || "grant_on" || 'S IN SCHEMA D_RAWCDC.AWS TO ROLE ' || "grantee_name" || ';' AS RECREATE_SQL
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-1)));  -- Adjust based on SHOW FUTURE GRANTS output
```

---

## Phase 2: Pre-Migration Preparation

### Script 2.1: Create Target Database

```sql
-- ============================================================================
-- SCRIPT 2.1: CREATE TARGET DATABASE
-- ============================================================================

USE ROLE SYSADMIN;  -- Or appropriate admin role

-- 2.1.1: Create the new database
CREATE DATABASE IF NOT EXISTS D_RAWCDC
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Raw CDC landing zone for Informatica IDMC - isolated from Bronze layer';

-- 2.1.2: Verify creation
SHOW DATABASES LIKE 'D_RAWCDC';

-- 2.1.3: Grant usage to appropriate roles
USE ROLE SECURITYADMIN;
GRANT USAGE ON DATABASE D_RAWCDC TO ROLE SYSADMIN;
-- Add other roles as needed based on your RBAC model
```

### Script 2.2: Pause All Dependent Processing

```sql
-- ============================================================================
-- SCRIPT 2.2: PAUSE INFORMATICA AND DOWNSTREAM PROCESSING
-- Execute during maintenance window
-- ============================================================================

USE ROLE SYSADMIN;

-- ============================================================
-- STEP 1: Suspend tasks WITHIN the schemas being migrated
-- ============================================================

-- List tasks in SADB schema
SHOW TASKS IN SCHEMA D_BRONZE.SADB;

-- List tasks in AWS schema
SHOW TASKS IN SCHEMA D_BRONZE.AWS;

-- Suspend each task found (replace with actual task names from SHOW TASKS output)
-- Example:
-- ALTER TASK D_BRONZE.SADB.TASK_NAME_1 SUSPEND;
-- ALTER TASK D_BRONZE.SADB.TASK_NAME_2 SUSPEND;
-- ALTER TASK D_BRONZE.AWS.TASK_NAME_1 SUSPEND;

-- ============================================================
-- STEP 2: Suspend tasks that DEPEND ON the schemas being migrated
-- (Use results from Script 1.2.3)
-- ============================================================

-- Example pattern (modify based on your dependency analysis):
-- ALTER TASK D_BRONZE.OTHER_SCHEMA.TASK_READING_SADB SUSPEND;
-- ALTER TASK D_SILVER.TRANSFORM.TASK_READING_FROM_BRONZE SUSPEND;
-- ALTER TASK D_GOLD.AGGREGATE.TASK_READING_FROM_SILVER SUSPEND;

-- ============================================================
-- STEP 3: Verify all relevant tasks are suspended
-- ============================================================

-- Check task states in D_BRONZE
SELECT 
    DATABASE_NAME,
    SCHEMA_NAME,
    NAME,
    STATE
FROM SNOWFLAKE.ACCOUNT_USAGE.TASKS
WHERE DATABASE_NAME = 'D_BRONZE'
    AND STATE = 'started'
    AND DELETED IS NULL
    AND (
        SCHEMA_NAME IN ('SADB', 'AWS')
        OR DEFINITION ILIKE '%SADB.%'
        OR DEFINITION ILIKE '%AWS.%'
    );

-- ============================================================
-- STEP 4: Document current stream offsets (for validation later)
-- ============================================================

SELECT 
    STREAM_CATALOG,
    STREAM_SCHEMA,
    STREAM_NAME,
    TABLE_NAME,
    STALE,
    STALE_AFTER
FROM D_BRONZE.INFORMATION_SCHEMA.STREAMS
WHERE STREAM_SCHEMA IN ('SADB', 'AWS')
   OR (STREAM_SCHEMA NOT IN ('SADB', 'AWS') AND TABLE_SCHEMA IN ('SADB', 'AWS'));

-- ============================================================
-- STEP 5: MANUAL STEP - Notify Informatica team
-- ============================================================
/*
IMPORTANT: Before proceeding, ensure Informatica IDMC CDC jobs are PAUSED.

Contact your Informatica administrator to:
1. Pause all CDC jobs targeting D_BRONZE.SADB
2. Pause all CDC jobs targeting D_BRONZE.AWS
3. Confirm jobs are fully stopped (no in-flight data)
4. Document the current CDC checkpoint/position

Do NOT proceed until Informatica confirms jobs are stopped.
*/
```

### Script 2.3: Create Migration Tracking Table

```sql
-- ============================================================================
-- SCRIPT 2.3: CREATE MIGRATION TRACKING TABLE
-- Track migration progress and enable rollback if needed
-- ============================================================================

USE ROLE SYSADMIN;

CREATE SCHEMA IF NOT EXISTS D_RAWCDC.MIGRATION_TRACKING;

CREATE TABLE IF NOT EXISTS D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG (
    MIGRATION_ID VARCHAR(50) DEFAULT UUID_STRING(),
    MIGRATION_PHASE VARCHAR(50),
    STEP_NAME VARCHAR(100),
    SOURCE_DATABASE VARCHAR(100),
    SOURCE_SCHEMA VARCHAR(100),
    TARGET_DATABASE VARCHAR(100),
    TARGET_SCHEMA VARCHAR(100),
    OBJECT_TYPE VARCHAR(50),
    OBJECT_NAME VARCHAR(256),
    STATUS VARCHAR(20),  -- 'STARTED', 'COMPLETED', 'FAILED', 'ROLLED_BACK'
    ERROR_MESSAGE VARCHAR(5000),
    STARTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT TIMESTAMP_NTZ,
    EXECUTED_BY VARCHAR(100) DEFAULT CURRENT_USER()
);

-- Insert migration start record
INSERT INTO D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
    (MIGRATION_PHASE, STEP_NAME, SOURCE_DATABASE, TARGET_DATABASE, STATUS)
VALUES 
    ('PREPARATION', 'Migration Started', 'D_BRONZE', 'D_RAWCDC', 'STARTED');
```

---

## Phase 3: Execute Migration

### Script 3.1: Clone Schemas to New Database

```sql
-- ============================================================================
-- SCRIPT 3.1: CLONE SCHEMAS
-- This is the core migration step - instantaneous zero-copy clone
-- ============================================================================

USE ROLE SYSADMIN;

-- ============================================================
-- STEP 1: Clone SADB schema
-- ============================================================

-- Log start
INSERT INTO D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
    (MIGRATION_PHASE, STEP_NAME, SOURCE_DATABASE, SOURCE_SCHEMA, TARGET_DATABASE, TARGET_SCHEMA, STATUS)
VALUES 
    ('CLONE', 'Clone SADB Schema', 'D_BRONZE', 'SADB', 'D_RAWCDC', 'SADB', 'STARTED');

-- Execute clone
CREATE SCHEMA D_RAWCDC.SADB 
    CLONE D_BRONZE.SADB
    COMMENT = 'Cloned from D_BRONZE.SADB on ' || CURRENT_TIMESTAMP()::STRING;

-- Log completion
UPDATE D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
SET STATUS = 'COMPLETED', COMPLETED_AT = CURRENT_TIMESTAMP()
WHERE MIGRATION_PHASE = 'CLONE' 
    AND STEP_NAME = 'Clone SADB Schema' 
    AND STATUS = 'STARTED';

-- ============================================================
-- STEP 2: Clone AWS schema
-- ============================================================

-- Log start
INSERT INTO D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
    (MIGRATION_PHASE, STEP_NAME, SOURCE_DATABASE, SOURCE_SCHEMA, TARGET_DATABASE, TARGET_SCHEMA, STATUS)
VALUES 
    ('CLONE', 'Clone AWS Schema', 'D_BRONZE', 'AWS', 'D_RAWCDC', 'AWS', 'STARTED');

-- Execute clone
CREATE SCHEMA D_RAWCDC.AWS 
    CLONE D_BRONZE.AWS
    COMMENT = 'Cloned from D_BRONZE.AWS on ' || CURRENT_TIMESTAMP()::STRING;

-- Log completion
UPDATE D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
SET STATUS = 'COMPLETED', COMPLETED_AT = CURRENT_TIMESTAMP()
WHERE MIGRATION_PHASE = 'CLONE' 
    AND STEP_NAME = 'Clone AWS Schema' 
    AND STATUS = 'STARTED';

-- ============================================================
-- STEP 3: Verify clone completed
-- ============================================================

SELECT 
    CATALOG_NAME AS DATABASE_NAME,
    SCHEMA_NAME,
    CREATED,
    LAST_ALTERED,
    COMMENT
FROM D_RAWCDC.INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME IN ('SADB', 'AWS');
```

### Script 3.2: Verify Clone Data Integrity

```sql
-- ============================================================================
-- SCRIPT 3.2: DATA VALIDATION
-- Verify cloned data matches source
-- ============================================================================

USE ROLE SYSADMIN;

-- ============================================================
-- STEP 1: Compare object counts
-- ============================================================

-- Object count comparison
SELECT 
    'D_BRONZE' AS DATABASE_NAME,
    TABLE_SCHEMA,
    TABLE_TYPE,
    COUNT(*) AS OBJECT_COUNT
FROM D_BRONZE.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('SADB', 'AWS')
GROUP BY TABLE_SCHEMA, TABLE_TYPE

UNION ALL

SELECT 
    'D_RAWCDC' AS DATABASE_NAME,
    TABLE_SCHEMA,
    TABLE_TYPE,
    COUNT(*) AS OBJECT_COUNT
FROM D_RAWCDC.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('SADB', 'AWS')
GROUP BY TABLE_SCHEMA, TABLE_TYPE

ORDER BY TABLE_SCHEMA, TABLE_TYPE, DATABASE_NAME;

-- ============================================================
-- STEP 2: Generate and run row count validation queries
-- ============================================================

-- Generate validation queries for all BASE tables
SELECT 
    'SELECT ''' || TABLE_SCHEMA || '.' || TABLE_NAME || ''' AS TABLE_NAME, ' ||
    '(SELECT COUNT(*) FROM D_BRONZE.' || TABLE_SCHEMA || '."' || TABLE_NAME || '") AS BRONZE_COUNT, ' ||
    '(SELECT COUNT(*) FROM D_RAWCDC.' || TABLE_SCHEMA || '."' || TABLE_NAME || '") AS RAWCDC_COUNT, ' ||
    'CASE WHEN (SELECT COUNT(*) FROM D_BRONZE.' || TABLE_SCHEMA || '."' || TABLE_NAME || '") = ' ||
    '(SELECT COUNT(*) FROM D_RAWCDC.' || TABLE_SCHEMA || '."' || TABLE_NAME || '") ' ||
    'THEN ''MATCH'' ELSE ''MISMATCH'' END AS VALIDATION_STATUS;'
    AS VALIDATION_QUERY
FROM D_BRONZE.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('SADB', 'AWS')
    AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- ============================================================
-- STEP 3: Validate specific critical tables (customize as needed)
-- ============================================================

-- Example validation for a specific table
-- Replace with your actual table names
/*
SELECT 
    'SADB.CUSTOMERS_BASE' AS TABLE_NAME,
    (SELECT COUNT(*) FROM D_BRONZE.SADB.CUSTOMERS_BASE) AS BRONZE_COUNT,
    (SELECT COUNT(*) FROM D_RAWCDC.SADB.CUSTOMERS_BASE) AS RAWCDC_COUNT,
    (SELECT MAX(UPDATED_AT) FROM D_BRONZE.SADB.CUSTOMERS_BASE) AS BRONZE_MAX_TS,
    (SELECT MAX(UPDATED_AT) FROM D_RAWCDC.SADB.CUSTOMERS_BASE) AS RAWCDC_MAX_TS;
*/

-- ============================================================
-- STEP 4: Log validation results
-- ============================================================

INSERT INTO D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
    (MIGRATION_PHASE, STEP_NAME, STATUS)
VALUES 
    ('VALIDATION', 'Data Integrity Check', 'COMPLETED');
```

---

## Phase 4: Update Dependencies

### Script 4.1: Recreate Streams on New Location

```sql
-- ============================================================================
-- SCRIPT 4.1: RECREATE STREAMS
-- Streams referencing D_BRONZE.SADB/AWS must be recreated to point to D_RAWCDC
-- NOTE: Stream offsets will be reset - plan for data reconciliation
-- ============================================================================

USE ROLE SYSADMIN;

-- ============================================================
-- STEP 1: Document existing streams before recreation
-- (Use results from Script 1.2.2)
-- ============================================================

-- List all streams that need to be recreated
SELECT 
    s.STREAM_CATALOG AS STREAM_DB,
    s.STREAM_SCHEMA,
    s.STREAM_NAME,
    s.TABLE_CATALOG AS SOURCE_DB,
    s.TABLE_SCHEMA AS SOURCE_SCHEMA,
    s.TABLE_NAME AS SOURCE_TABLE,
    s.TYPE AS STREAM_TYPE,
    s.MODE AS STREAM_MODE,
    s.STALE,
    -- Generate recreation DDL
    'CREATE OR REPLACE STREAM ' || s.STREAM_CATALOG || '.' || s.STREAM_SCHEMA || '.' || s.STREAM_NAME ||
    ' ON TABLE D_RAWCDC.' || s.TABLE_SCHEMA || '.' || s.TABLE_NAME ||
    CASE WHEN s.MODE = 'APPEND_ONLY' THEN ' APPEND_ONLY = TRUE' ELSE '' END ||
    ';' AS RECREATION_DDL
FROM D_BRONZE.INFORMATION_SCHEMA.STREAMS s
WHERE s.TABLE_CATALOG = 'D_BRONZE'
    AND s.TABLE_SCHEMA IN ('SADB', 'AWS')
    AND s.STREAM_SCHEMA NOT IN ('SADB', 'AWS');  -- Streams in other schemas

-- ============================================================
-- STEP 2: Recreate streams in D_BRONZE that pointed to SADB/AWS
-- ============================================================

-- IMPORTANT: Customize these based on your actual stream definitions
-- Example pattern:

/*
-- Example: Stream in D_BRONZE.PROCESSING schema pointing to D_BRONZE.SADB table
CREATE OR REPLACE STREAM D_BRONZE.PROCESSING.CUSTOMERS_CDC_STREAM
    ON TABLE D_RAWCDC.SADB.CUSTOMERS_BASE
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Recreated to point to D_RAWCDC after migration';

-- Example: Stream in D_BRONZE.PROCESSING schema pointing to D_BRONZE.AWS table
CREATE OR REPLACE STREAM D_BRONZE.PROCESSING.ORDERS_CDC_STREAM
    ON TABLE D_RAWCDC.AWS.ORDERS_BASE
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Recreated to point to D_RAWCDC after migration';
*/

-- ============================================================
-- STEP 3: Recreate streams in D_SILVER that pointed to D_BRONZE.SADB/AWS
-- ============================================================

-- List streams in D_SILVER pointing to migrated schemas
SELECT 
    s.STREAM_CATALOG AS STREAM_DB,
    s.STREAM_SCHEMA,
    s.STREAM_NAME,
    s.TABLE_CATALOG AS SOURCE_DB,
    s.TABLE_SCHEMA AS SOURCE_SCHEMA,
    s.TABLE_NAME AS SOURCE_TABLE,
    'CREATE OR REPLACE STREAM D_SILVER.' || s.STREAM_SCHEMA || '.' || s.STREAM_NAME ||
    ' ON TABLE D_RAWCDC.' || s.TABLE_SCHEMA || '.' || s.TABLE_NAME ||
    CASE WHEN s.MODE = 'APPEND_ONLY' THEN ' APPEND_ONLY = TRUE' ELSE '' END ||
    ';' AS RECREATION_DDL
FROM D_SILVER.INFORMATION_SCHEMA.STREAMS s
WHERE s.TABLE_CATALOG = 'D_BRONZE'
    AND s.TABLE_SCHEMA IN ('SADB', 'AWS');

-- Execute the generated DDL statements (example pattern):
/*
CREATE OR REPLACE STREAM D_SILVER.TRANSFORM.SADB_CUSTOMERS_STREAM
    ON TABLE D_RAWCDC.SADB.CUSTOMERS_BASE;
*/

-- ============================================================
-- STEP 4: Log stream recreation
-- ============================================================

INSERT INTO D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
    (MIGRATION_PHASE, STEP_NAME, OBJECT_TYPE, STATUS)
VALUES 
    ('DEPENDENCY_UPDATE', 'Recreate Streams', 'STREAM', 'COMPLETED');
```

### Script 4.2: Update Views

```sql
-- ============================================================================
-- SCRIPT 4.2: UPDATE VIEWS
-- Views referencing D_BRONZE.SADB/AWS must be updated to reference D_RAWCDC
-- ============================================================================

USE ROLE SYSADMIN;

-- ============================================================
-- STEP 1: Generate view update DDL for D_BRONZE views
-- ============================================================

SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    VIEW_DEFINITION AS ORIGINAL_DEFINITION,
    REPLACE(
        REPLACE(VIEW_DEFINITION, 'D_BRONZE.SADB.', 'D_RAWCDC.SADB.'),
        'D_BRONZE.AWS.', 'D_RAWCDC.AWS.'
    ) AS UPDATED_DEFINITION,
    'CREATE OR REPLACE VIEW D_BRONZE.' || TABLE_SCHEMA || '.' || TABLE_NAME || ' AS ' ||
    REPLACE(
        REPLACE(VIEW_DEFINITION, 'D_BRONZE.SADB.', 'D_RAWCDC.SADB.'),
        'D_BRONZE.AWS.', 'D_RAWCDC.AWS.'
    ) || ';' AS UPDATE_DDL
FROM D_BRONZE.INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA NOT IN ('SADB', 'AWS', 'INFORMATION_SCHEMA')
    AND (
        VIEW_DEFINITION ILIKE '%D_BRONZE.SADB.%'
        OR VIEW_DEFINITION ILIKE '%D_BRONZE.AWS.%'
    )
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- ============================================================
-- STEP 2: Generate view update DDL for D_SILVER views
-- ============================================================

SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    VIEW_DEFINITION AS ORIGINAL_DEFINITION,
    REPLACE(
        REPLACE(VIEW_DEFINITION, 'D_BRONZE.SADB.', 'D_RAWCDC.SADB.'),
        'D_BRONZE.AWS.', 'D_RAWCDC.AWS.'
    ) AS UPDATED_DEFINITION,
    'CREATE OR REPLACE VIEW D_SILVER.' || TABLE_SCHEMA || '.' || TABLE_NAME || ' AS ' ||
    REPLACE(
        REPLACE(VIEW_DEFINITION, 'D_BRONZE.SADB.', 'D_RAWCDC.SADB.'),
        'D_BRONZE.AWS.', 'D_RAWCDC.AWS.'
    ) || ';' AS UPDATE_DDL
FROM D_SILVER.INFORMATION_SCHEMA.VIEWS
WHERE VIEW_DEFINITION ILIKE '%D_BRONZE.SADB.%'
   OR VIEW_DEFINITION ILIKE '%D_BRONZE.AWS.%'
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- ============================================================
-- STEP 3: Generate view update DDL for D_GOLD views
-- ============================================================

SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    VIEW_DEFINITION AS ORIGINAL_DEFINITION,
    REPLACE(
        REPLACE(VIEW_DEFINITION, 'D_BRONZE.SADB.', 'D_RAWCDC.SADB.'),
        'D_BRONZE.AWS.', 'D_RAWCDC.AWS.'
    ) AS UPDATED_DEFINITION,
    'CREATE OR REPLACE VIEW D_GOLD.' || TABLE_SCHEMA || '.' || TABLE_NAME || ' AS ' ||
    REPLACE(
        REPLACE(VIEW_DEFINITION, 'D_BRONZE.SADB.', 'D_RAWCDC.SADB.'),
        'D_BRONZE.AWS.', 'D_RAWCDC.AWS.'
    ) || ';' AS UPDATE_DDL
FROM D_GOLD.INFORMATION_SCHEMA.VIEWS
WHERE VIEW_DEFINITION ILIKE '%D_BRONZE.SADB.%'
   OR VIEW_DEFINITION ILIKE '%D_BRONZE.AWS.%'
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- ============================================================
-- STEP 4: Execute the generated DDL statements
-- Review and run each statement from the above queries
-- ============================================================

-- Example pattern (customize based on your actual views):
/*
CREATE OR REPLACE VIEW D_SILVER.TRANSFORM.V_CUSTOMERS AS
SELECT 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    CREATED_AT
FROM D_RAWCDC.SADB.CUSTOMERS_BASE  -- Changed from D_BRONZE.SADB
WHERE IS_ACTIVE = TRUE;
*/

-- ============================================================
-- STEP 5: Log view updates
-- ============================================================

INSERT INTO D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
    (MIGRATION_PHASE, STEP_NAME, OBJECT_TYPE, STATUS)
VALUES 
    ('DEPENDENCY_UPDATE', 'Update Views', 'VIEW', 'COMPLETED');
```

### Script 4.3: Update Tasks

```sql
-- ============================================================================
-- SCRIPT 4.3: UPDATE TASKS
-- Tasks with SQL referencing D_BRONZE.SADB/AWS must be recreated
-- ============================================================================

USE ROLE SYSADMIN;

-- ============================================================
-- STEP 1: List tasks that need updating (from dependency analysis)
-- ============================================================

SELECT 
    DATABASE_NAME,
    SCHEMA_NAME,
    NAME AS TASK_NAME,
    STATE,
    SCHEDULE,
    DEFINITION,
    REPLACE(
        REPLACE(DEFINITION, 'D_BRONZE.SADB.', 'D_RAWCDC.SADB.'),
        'D_BRONZE.AWS.', 'D_RAWCDC.AWS.'
    ) AS UPDATED_DEFINITION
FROM SNOWFLAKE.ACCOUNT_USAGE.TASKS
WHERE DELETED IS NULL
    AND (
        DEFINITION ILIKE '%D_BRONZE.SADB.%' 
        OR DEFINITION ILIKE '%D_BRONZE.AWS.%'
    );

-- ============================================================
-- STEP 2: Recreate tasks with updated references
-- IMPORTANT: Tasks must be suspended before modification
-- ============================================================

-- Example pattern for task recreation:
/*
-- First, suspend the task
ALTER TASK D_BRONZE.PROCESSING.LOAD_CUSTOMERS SUSPEND;

-- Drop the old task
DROP TASK D_BRONZE.PROCESSING.LOAD_CUSTOMERS;

-- Create with updated references
CREATE TASK D_BRONZE.PROCESSING.LOAD_CUSTOMERS
    WAREHOUSE = TRANSFORM_WH
    SCHEDULE = 'USING CRON 0 * * * * America/New_York'
    COMMENT = 'Updated to reference D_RAWCDC after migration'
AS
INSERT INTO D_BRONZE.PROCESSED.CUSTOMERS_HISTORY
SELECT 
    *,
    CURRENT_TIMESTAMP() AS LOAD_TIMESTAMP
FROM D_RAWCDC.SADB.CUSTOMERS_BASE  -- Changed from D_BRONZE.SADB
WHERE METADATA$ACTION = 'INSERT';
*/

-- ============================================================
-- STEP 3: Handle task dependencies (DAG chains)
-- ============================================================

-- List task dependencies
SELECT 
    DATABASE_NAME,
    SCHEMA_NAME,
    NAME AS TASK_NAME,
    PREDECESSORS
FROM SNOWFLAKE.ACCOUNT_USAGE.TASKS
WHERE DELETED IS NULL
    AND DATABASE_NAME IN ('D_BRONZE', 'D_SILVER', 'D_GOLD')
    AND PREDECESSORS IS NOT NULL;

-- When recreating tasks in a DAG:
-- 1. Suspend the root task first
-- 2. Recreate dependent tasks from leaves to root
-- 3. Resume root task last

-- ============================================================
-- STEP 4: Log task updates
-- ============================================================

INSERT INTO D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
    (MIGRATION_PHASE, STEP_NAME, OBJECT_TYPE, STATUS)
VALUES 
    ('DEPENDENCY_UPDATE', 'Update Tasks', 'TASK', 'COMPLETED');
```

### Script 4.4: Update Stored Procedures

```sql
-- ============================================================================
-- SCRIPT 4.4: UPDATE STORED PROCEDURES
-- Procedures referencing D_BRONZE.SADB/AWS must be updated
-- ============================================================================

USE ROLE SYSADMIN;

-- ============================================================
-- STEP 1: List procedures that need updating
-- ============================================================

SELECT 
    PROCEDURE_CATALOG,
    PROCEDURE_SCHEMA,
    PROCEDURE_NAME,
    ARGUMENT_SIGNATURE,
    PROCEDURE_DEFINITION
FROM D_BRONZE.INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA NOT IN ('SADB', 'AWS', 'INFORMATION_SCHEMA')
    AND (
        PROCEDURE_DEFINITION ILIKE '%D_BRONZE.SADB.%'
        OR PROCEDURE_DEFINITION ILIKE '%D_BRONZE.AWS.%'
    );

-- Repeat for D_SILVER and D_GOLD
SELECT 
    PROCEDURE_CATALOG,
    PROCEDURE_SCHEMA,
    PROCEDURE_NAME,
    ARGUMENT_SIGNATURE,
    PROCEDURE_DEFINITION
FROM D_SILVER.INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_DEFINITION ILIKE '%D_BRONZE.SADB.%'
   OR PROCEDURE_DEFINITION ILIKE '%D_BRONZE.AWS.%';

-- ============================================================
-- STEP 2: Recreate procedures with updated references
-- ============================================================

-- Example pattern:
/*
CREATE OR REPLACE PROCEDURE D_SILVER.TRANSFORM.SP_PROCESS_CUSTOMERS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO D_SILVER.CLEAN.CUSTOMERS
    SELECT 
        CUSTOMER_ID,
        UPPER(CUSTOMER_NAME) AS CUSTOMER_NAME,
        LOWER(EMAIL) AS EMAIL
    FROM D_RAWCDC.SADB.CUSTOMERS_BASE  -- Changed from D_BRONZE.SADB
    WHERE CUSTOMER_ID IS NOT NULL;
    
    RETURN 'SUCCESS';
END;
$$;
*/

-- ============================================================
-- STEP 3: Log procedure updates
-- ============================================================

INSERT INTO D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
    (MIGRATION_PHASE, STEP_NAME, OBJECT_TYPE, STATUS)
VALUES 
    ('DEPENDENCY_UPDATE', 'Update Procedures', 'PROCEDURE', 'COMPLETED');
```

---

## Phase 5: RBAC Configuration

### Script 5.1: Create Roles for D_RAWCDC

```sql
-- ============================================================================
-- SCRIPT 5.1: RBAC SETUP FOR D_RAWCDC
-- Create proper role hierarchy for the new database
-- ============================================================================

USE ROLE SECURITYADMIN;

-- ============================================================
-- STEP 1: Create functional roles for D_RAWCDC
-- ============================================================

-- Role for Informatica IDMC (owns D_RAWCDC, can DROP/CREATE objects)
CREATE ROLE IF NOT EXISTS INFORMATICA_CDC_ROLE
    COMMENT = 'Role for Informatica IDMC CDC operations on D_RAWCDC';

-- Role for reading from D_RAWCDC (used by Bronze processing)
CREATE ROLE IF NOT EXISTS D_RAWCDC_READ_ROLE
    COMMENT = 'Read-only access to D_RAWCDC for downstream processing';

-- Role for administering D_RAWCDC (not Informatica)
CREATE ROLE IF NOT EXISTS D_RAWCDC_ADMIN_ROLE
    COMMENT = 'Administrative access to D_RAWCDC (non-Informatica operations)';

-- ============================================================
-- STEP 2: Grant database-level permissions
-- ============================================================

-- Informatica owns the database for CDC operations
GRANT OWNERSHIP ON DATABASE D_RAWCDC TO ROLE INFORMATICA_CDC_ROLE REVOKE CURRENT GRANTS;

-- But we need to ensure other roles can still access
GRANT USAGE ON DATABASE D_RAWCDC TO ROLE D_RAWCDC_READ_ROLE;
GRANT USAGE ON DATABASE D_RAWCDC TO ROLE D_RAWCDC_ADMIN_ROLE;
GRANT USAGE ON DATABASE D_RAWCDC TO ROLE SYSADMIN;

-- ============================================================
-- STEP 3: Grant schema-level permissions
-- ============================================================

-- Informatica owns schemas (can DROP/CREATE)
GRANT OWNERSHIP ON SCHEMA D_RAWCDC.SADB TO ROLE INFORMATICA_CDC_ROLE REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA D_RAWCDC.AWS TO ROLE INFORMATICA_CDC_ROLE REVOKE CURRENT GRANTS;

-- Read role gets USAGE on schemas
GRANT USAGE ON SCHEMA D_RAWCDC.SADB TO ROLE D_RAWCDC_READ_ROLE;
GRANT USAGE ON SCHEMA D_RAWCDC.AWS TO ROLE D_RAWCDC_READ_ROLE;

-- Admin role gets USAGE (but not ownership)
GRANT USAGE ON SCHEMA D_RAWCDC.SADB TO ROLE D_RAWCDC_ADMIN_ROLE;
GRANT USAGE ON SCHEMA D_RAWCDC.AWS TO ROLE D_RAWCDC_ADMIN_ROLE;

-- Migration tracking schema stays with SYSADMIN
GRANT USAGE ON SCHEMA D_RAWCDC.MIGRATION_TRACKING TO ROLE D_RAWCDC_ADMIN_ROLE;

-- ============================================================
-- STEP 4: Grant object-level permissions
-- ============================================================

-- Informatica owns all objects (automatic via schema ownership)

-- Read role gets SELECT on all tables/views
GRANT SELECT ON ALL TABLES IN SCHEMA D_RAWCDC.SADB TO ROLE D_RAWCDC_READ_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA D_RAWCDC.AWS TO ROLE D_RAWCDC_READ_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA D_RAWCDC.SADB TO ROLE D_RAWCDC_READ_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA D_RAWCDC.AWS TO ROLE D_RAWCDC_READ_ROLE;

-- ============================================================
-- STEP 5: Set up FUTURE GRANTS
-- ============================================================

-- Future grants for Read role (important for Informatica redeployments!)
GRANT SELECT ON FUTURE TABLES IN SCHEMA D_RAWCDC.SADB TO ROLE D_RAWCDC_READ_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA D_RAWCDC.AWS TO ROLE D_RAWCDC_READ_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA D_RAWCDC.SADB TO ROLE D_RAWCDC_READ_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA D_RAWCDC.AWS TO ROLE D_RAWCDC_READ_ROLE;

-- ============================================================
-- STEP 6: Set up role hierarchy
-- ============================================================

-- Admin role inherits Read role
GRANT ROLE D_RAWCDC_READ_ROLE TO ROLE D_RAWCDC_ADMIN_ROLE;

-- SYSADMIN inherits Admin role
GRANT ROLE D_RAWCDC_ADMIN_ROLE TO ROLE SYSADMIN;

-- Grant Informatica role to service account (customize as needed)
-- GRANT ROLE INFORMATICA_CDC_ROLE TO USER INFORMATICA_SERVICE_USER;

-- ============================================================
-- STEP 7: Verify role hierarchy
-- ============================================================

SHOW GRANTS TO ROLE INFORMATICA_CDC_ROLE;
SHOW GRANTS TO ROLE D_RAWCDC_READ_ROLE;
SHOW GRANTS TO ROLE D_RAWCDC_ADMIN_ROLE;
SHOW GRANTS ON DATABASE D_RAWCDC;
SHOW GRANTS ON SCHEMA D_RAWCDC.SADB;
SHOW GRANTS ON SCHEMA D_RAWCDC.AWS;
```

### Script 5.2: Update Existing Role Grants

```sql
-- ============================================================================
-- SCRIPT 5.2: UPDATE EXISTING ROLES
-- Grant D_RAWCDC_READ_ROLE to roles that previously accessed D_BRONZE.SADB/AWS
-- ============================================================================

USE ROLE SECURITYADMIN;

-- ============================================================
-- STEP 1: Identify roles that had access to old schemas
-- (Use results from Script 1.3)
-- ============================================================

-- Example: If BRONZE_PROCESSOR_ROLE had SELECT on D_BRONZE.SADB
-- Grant the new read role to maintain access

GRANT ROLE D_RAWCDC_READ_ROLE TO ROLE BRONZE_PROCESSOR_ROLE;
GRANT ROLE D_RAWCDC_READ_ROLE TO ROLE SILVER_PROCESSOR_ROLE;
-- Add other roles as identified in Script 1.3

-- ============================================================
-- STEP 2: Ensure downstream processing roles can read D_RAWCDC
-- ============================================================

-- Grant to roles used by D_BRONZE processing tasks
-- Customize based on your role structure

-- Example grants:
-- GRANT ROLE D_RAWCDC_READ_ROLE TO ROLE ETL_ROLE;
-- GRANT ROLE D_RAWCDC_READ_ROLE TO ROLE DATA_ENGINEER_ROLE;
-- GRANT ROLE D_RAWCDC_READ_ROLE TO ROLE TRANSFORM_ROLE;

-- ============================================================
-- STEP 3: Log RBAC configuration
-- ============================================================

USE ROLE SYSADMIN;
INSERT INTO D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
    (MIGRATION_PHASE, STEP_NAME, STATUS)
VALUES 
    ('RBAC', 'Role Configuration Complete', 'COMPLETED');
```

---

## Phase 6: Post-Migration Validation

### Script 6.1: Comprehensive Validation

```sql
-- ============================================================================
-- SCRIPT 6.1: POST-MIGRATION VALIDATION
-- Comprehensive checks before resuming operations
-- ============================================================================

USE ROLE SYSADMIN;

-- ============================================================
-- CHECK 1: Object Count Validation
-- ============================================================

SELECT 
    'OBJECT_COUNT' AS CHECK_TYPE,
    DATABASE_NAME,
    TABLE_SCHEMA,
    TABLE_TYPE,
    COUNT(*) AS COUNT,
    CASE 
        WHEN DATABASE_NAME = 'D_BRONZE' THEN 'SOURCE'
        ELSE 'TARGET'
    END AS LOCATION
FROM (
    SELECT 'D_BRONZE' AS DATABASE_NAME, TABLE_SCHEMA, TABLE_TYPE
    FROM D_BRONZE.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA IN ('SADB', 'AWS')
    
    UNION ALL
    
    SELECT 'D_RAWCDC' AS DATABASE_NAME, TABLE_SCHEMA, TABLE_TYPE
    FROM D_RAWCDC.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA IN ('SADB', 'AWS')
)
GROUP BY DATABASE_NAME, TABLE_SCHEMA, TABLE_TYPE
ORDER BY TABLE_SCHEMA, TABLE_TYPE, DATABASE_NAME;

-- ============================================================
-- CHECK 2: Row Count Validation for Critical Tables
-- ============================================================

-- Generate row count comparison for all tables
WITH bronze_counts AS (
    SELECT 
        TABLE_SCHEMA,
        TABLE_NAME,
        ROW_COUNT AS BRONZE_ROW_COUNT
    FROM D_BRONZE.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA IN ('SADB', 'AWS')
        AND TABLE_TYPE = 'BASE TABLE'
),
rawcdc_counts AS (
    SELECT 
        TABLE_SCHEMA,
        TABLE_NAME,
        ROW_COUNT AS RAWCDC_ROW_COUNT
    FROM D_RAWCDC.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA IN ('SADB', 'AWS')
        AND TABLE_TYPE = 'BASE TABLE'
)
SELECT 
    b.TABLE_SCHEMA,
    b.TABLE_NAME,
    b.BRONZE_ROW_COUNT,
    r.RAWCDC_ROW_COUNT,
    CASE 
        WHEN b.BRONZE_ROW_COUNT = r.RAWCDC_ROW_COUNT THEN 'PASS'
        ELSE 'FAIL - INVESTIGATE'
    END AS VALIDATION_STATUS
FROM bronze_counts b
JOIN rawcdc_counts r 
    ON b.TABLE_SCHEMA = r.TABLE_SCHEMA 
    AND b.TABLE_NAME = r.TABLE_NAME
ORDER BY VALIDATION_STATUS DESC, b.TABLE_SCHEMA, b.TABLE_NAME;

-- ============================================================
-- CHECK 3: Stream Health Check
-- ============================================================

SELECT 
    'STREAM_HEALTH' AS CHECK_TYPE,
    STREAM_CATALOG AS DATABASE_NAME,
    STREAM_SCHEMA,
    STREAM_NAME,
    TABLE_CATALOG AS SOURCE_DB,
    TABLE_SCHEMA AS SOURCE_SCHEMA,
    TABLE_NAME AS SOURCE_TABLE,
    STALE,
    CASE 
        WHEN STALE = 'false' THEN 'HEALTHY'
        ELSE 'STALE - NEEDS ATTENTION'
    END AS STATUS
FROM D_BRONZE.INFORMATION_SCHEMA.STREAMS
WHERE TABLE_CATALOG = 'D_RAWCDC'
   OR TABLE_SCHEMA IN ('SADB', 'AWS');

-- Also check D_SILVER streams
SELECT 
    'STREAM_HEALTH' AS CHECK_TYPE,
    STREAM_CATALOG AS DATABASE_NAME,
    STREAM_SCHEMA,
    STREAM_NAME,
    TABLE_CATALOG AS SOURCE_DB,
    TABLE_SCHEMA AS SOURCE_SCHEMA,
    TABLE_NAME AS SOURCE_TABLE,
    STALE,
    CASE 
        WHEN STALE = 'false' THEN 'HEALTHY'
        ELSE 'STALE - NEEDS ATTENTION'
    END AS STATUS
FROM D_SILVER.INFORMATION_SCHEMA.STREAMS
WHERE TABLE_CATALOG = 'D_RAWCDC';

-- ============================================================
-- CHECK 4: View Dependency Validation
-- ============================================================

-- Ensure no views still reference D_BRONZE.SADB or D_BRONZE.AWS
SELECT 
    'ORPHANED_REFERENCE' AS CHECK_TYPE,
    TABLE_CATALOG AS DATABASE_NAME,
    TABLE_SCHEMA,
    TABLE_NAME AS VIEW_NAME,
    'References D_BRONZE.SADB or D_BRONZE.AWS - NEEDS UPDATE' AS ISSUE
FROM D_BRONZE.INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA NOT IN ('SADB', 'AWS', 'INFORMATION_SCHEMA')
    AND (
        VIEW_DEFINITION ILIKE '%D_BRONZE.SADB.%'
        OR VIEW_DEFINITION ILIKE '%D_BRONZE.AWS.%'
    )

UNION ALL

SELECT 
    'ORPHANED_REFERENCE' AS CHECK_TYPE,
    TABLE_CATALOG AS DATABASE_NAME,
    TABLE_SCHEMA,
    TABLE_NAME AS VIEW_NAME,
    'References D_BRONZE.SADB or D_BRONZE.AWS - NEEDS UPDATE' AS ISSUE
FROM D_SILVER.INFORMATION_SCHEMA.VIEWS
WHERE VIEW_DEFINITION ILIKE '%D_BRONZE.SADB.%'
   OR VIEW_DEFINITION ILIKE '%D_BRONZE.AWS.%'

UNION ALL

SELECT 
    'ORPHANED_REFERENCE' AS CHECK_TYPE,
    TABLE_CATALOG AS DATABASE_NAME,
    TABLE_SCHEMA,
    TABLE_NAME AS VIEW_NAME,
    'References D_BRONZE.SADB or D_BRONZE.AWS - NEEDS UPDATE' AS ISSUE
FROM D_GOLD.INFORMATION_SCHEMA.VIEWS
WHERE VIEW_DEFINITION ILIKE '%D_BRONZE.SADB.%'
   OR VIEW_DEFINITION ILIKE '%D_BRONZE.AWS.%';

-- Expected result: NO ROWS (all views should now reference D_RAWCDC)

-- ============================================================
-- CHECK 5: RBAC Validation
-- ============================================================

-- Verify key roles have expected access
SELECT 
    'RBAC_CHECK' AS CHECK_TYPE,
    PRIVILEGE,
    GRANTED_ON,
    NAME AS OBJECT_NAME,
    GRANTEE_NAME
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE NAME LIKE 'D_RAWCDC%'
    AND DELETED_ON IS NULL
ORDER BY GRANTEE_NAME, NAME;

-- ============================================================
-- CHECK 6: Task Health Check
-- ============================================================

SELECT 
    'TASK_HEALTH' AS CHECK_TYPE,
    DATABASE_NAME,
    SCHEMA_NAME,
    NAME AS TASK_NAME,
    STATE,
    CASE 
        WHEN DEFINITION ILIKE '%D_BRONZE.SADB.%' OR DEFINITION ILIKE '%D_BRONZE.AWS.%' 
        THEN 'NEEDS UPDATE - Still references D_BRONZE'
        ELSE 'OK'
    END AS STATUS
FROM SNOWFLAKE.ACCOUNT_USAGE.TASKS
WHERE DELETED IS NULL
    AND DATABASE_NAME IN ('D_BRONZE', 'D_SILVER', 'D_GOLD')
    AND (
        DEFINITION ILIKE '%SADB.%'
        OR DEFINITION ILIKE '%AWS.%'
    );
```

### Script 6.2: Test Data Flow

```sql
-- ============================================================================
-- SCRIPT 6.2: TEST DATA FLOW
-- Verify end-to-end data pipeline works with new structure
-- ============================================================================

USE ROLE SYSADMIN;

-- ============================================================
-- TEST 1: Verify D_RAWCDC tables are accessible
-- ============================================================

-- Test SELECT on key tables (customize with your actual table names)
SELECT COUNT(*) AS ROW_COUNT FROM D_RAWCDC.SADB.CUSTOMERS_BASE LIMIT 1;
SELECT COUNT(*) AS ROW_COUNT FROM D_RAWCDC.AWS.ORDERS_BASE LIMIT 1;

-- ============================================================
-- TEST 2: Verify streams can capture changes
-- ============================================================

-- Check stream has data (if any changes occurred)
-- Replace with your actual stream names
/*
SELECT COUNT(*) FROM D_BRONZE.PROCESSING.CUSTOMERS_CDC_STREAM;
*/

-- ============================================================
-- TEST 3: Verify views return data
-- ============================================================

-- Test downstream views (customize with your actual view names)
/*
SELECT COUNT(*) FROM D_SILVER.TRANSFORM.V_CUSTOMERS LIMIT 1;
SELECT COUNT(*) FROM D_GOLD.REPORTING.V_CUSTOMER_SUMMARY LIMIT 1;
*/

-- ============================================================
-- TEST 4: Log validation completion
-- ============================================================

INSERT INTO D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
    (MIGRATION_PHASE, STEP_NAME, STATUS)
VALUES 
    ('VALIDATION', 'Post-Migration Validation Complete', 'COMPLETED');
```

---

## Phase 7: Cleanup Original Schemas

### Script 7.1: Resume Operations Before Cleanup

```sql
-- ============================================================================
-- SCRIPT 7.1: RESUME OPERATIONS
-- Resume tasks and notify Informatica to resume CDC jobs
-- ============================================================================

USE ROLE SYSADMIN;

-- ============================================================
-- STEP 1: Resume tasks in dependency order (leaves first, then root)
-- ============================================================

-- Example pattern - customize based on your task DAGs:
/*
-- Resume D_GOLD tasks first (end of pipeline)
ALTER TASK D_GOLD.REPORTING.AGGREGATE_DAILY_SALES RESUME;

-- Resume D_SILVER tasks
ALTER TASK D_SILVER.TRANSFORM.PROCESS_ORDERS RESUME;
ALTER TASK D_SILVER.TRANSFORM.PROCESS_CUSTOMERS RESUME;

-- Resume D_BRONZE tasks last
ALTER TASK D_BRONZE.PROCESSING.LOAD_FROM_CDC RESUME;
*/

-- ============================================================
-- STEP 2: Verify task states
-- ============================================================

SELECT 
    DATABASE_NAME,
    SCHEMA_NAME,
    NAME AS TASK_NAME,
    STATE,
    LAST_COMMITTED_ON,
    LAST_SUSPENDED_ON
FROM SNOWFLAKE.ACCOUNT_USAGE.TASKS
WHERE DELETED IS NULL
    AND DATABASE_NAME IN ('D_BRONZE', 'D_SILVER', 'D_GOLD')
ORDER BY DATABASE_NAME, SCHEMA_NAME, NAME;

-- ============================================================
-- STEP 3: MANUAL STEP - Notify Informatica team to resume
-- ============================================================
/*
IMPORTANT: Contact your Informatica administrator to:
1. Update CDC job targets from D_BRONZE.SADB to D_RAWCDC.SADB
2. Update CDC job targets from D_BRONZE.AWS to D_RAWCDC.AWS
3. Resume all CDC jobs
4. Verify data is flowing to D_RAWCDC

Do NOT proceed to cleanup until Informatica confirms jobs are running 
successfully against D_RAWCDC.
*/
```

### Script 7.2: Cleanup Original Schemas (After Verification Period)

```sql
-- ============================================================================
-- SCRIPT 7.2: CLEANUP ORIGINAL SCHEMAS
-- IMPORTANT: Only run this AFTER successful verification period
-- Recommended: Wait 24-48 hours with data flowing to D_RAWCDC before cleanup
-- ============================================================================

USE ROLE SYSADMIN;

-- ============================================================
-- PRE-CLEANUP VERIFICATION
-- ============================================================

-- Verify no active queries against old schemas
SELECT 
    QUERY_ID,
    USER_NAME,
    QUERY_TEXT,
    START_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME > DATEADD('hour', -24, CURRENT_TIMESTAMP())
    AND (
        QUERY_TEXT ILIKE '%D_BRONZE.SADB.%'
        OR QUERY_TEXT ILIKE '%D_BRONZE.AWS.%'
    )
    AND QUERY_TEXT NOT ILIKE '%D_RAWCDC%'
ORDER BY START_TIME DESC;

-- Expected result: No recent queries (or only monitoring/validation queries)

-- ============================================================
-- STEP 1: Create backup before cleanup (optional but recommended)
-- ============================================================

-- Option A: Clone to backup database
CREATE DATABASE IF NOT EXISTS D_BRONZE_BACKUP_YYYYMMDD;
CREATE SCHEMA D_BRONZE_BACKUP_YYYYMMDD.SADB CLONE D_BRONZE.SADB;
CREATE SCHEMA D_BRONZE_BACKUP_YYYYMMDD.AWS CLONE D_BRONZE.AWS;

-- ============================================================
-- STEP 2: Drop original schemas
-- WARNING: This is destructive and cannot be undone after Time Travel expires
-- ============================================================

-- Log cleanup start
INSERT INTO D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
    (MIGRATION_PHASE, STEP_NAME, SOURCE_DATABASE, SOURCE_SCHEMA, STATUS)
VALUES 
    ('CLEANUP', 'Drop Original SADB Schema', 'D_BRONZE', 'SADB', 'STARTED');

-- Drop SADB schema
DROP SCHEMA D_BRONZE.SADB;

-- Log completion
UPDATE D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
SET STATUS = 'COMPLETED', COMPLETED_AT = CURRENT_TIMESTAMP()
WHERE MIGRATION_PHASE = 'CLEANUP' 
    AND STEP_NAME = 'Drop Original SADB Schema' 
    AND STATUS = 'STARTED';

-- Log cleanup start
INSERT INTO D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
    (MIGRATION_PHASE, STEP_NAME, SOURCE_DATABASE, SOURCE_SCHEMA, STATUS)
VALUES 
    ('CLEANUP', 'Drop Original AWS Schema', 'D_BRONZE', 'AWS', 'STARTED');

-- Drop AWS schema
DROP SCHEMA D_BRONZE.AWS;

-- Log completion
UPDATE D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
SET STATUS = 'COMPLETED', COMPLETED_AT = CURRENT_TIMESTAMP()
WHERE MIGRATION_PHASE = 'CLEANUP' 
    AND STEP_NAME = 'Drop Original AWS Schema' 
    AND STATUS = 'STARTED';

-- ============================================================
-- STEP 3: Verify cleanup
-- ============================================================

-- Confirm schemas no longer exist in D_BRONZE
SELECT SCHEMA_NAME 
FROM D_BRONZE.INFORMATION_SCHEMA.SCHEMATA 
WHERE SCHEMA_NAME IN ('SADB', 'AWS');

-- Expected result: NO ROWS

-- ============================================================
-- STEP 4: Log migration completion
-- ============================================================

INSERT INTO D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG 
    (MIGRATION_PHASE, STEP_NAME, STATUS)
VALUES 
    ('COMPLETE', 'Migration Finished Successfully', 'COMPLETED');

-- Final migration summary
SELECT 
    MIGRATION_PHASE,
    STEP_NAME,
    STATUS,
    STARTED_AT,
    COMPLETED_AT,
    DATEDIFF('minute', STARTED_AT, COALESCE(COMPLETED_AT, CURRENT_TIMESTAMP())) AS DURATION_MINUTES
FROM D_RAWCDC.MIGRATION_TRACKING.MIGRATION_LOG
ORDER BY STARTED_AT;
```

---

## Phase 8: Rollback Procedures

### Script 8.1: Rollback Before Cleanup

```sql
-- ============================================================================
-- SCRIPT 8.1: ROLLBACK PROCEDURE (Before Original Schemas Dropped)
-- Use this if issues are discovered BEFORE dropping D_BRONZE.SADB/AWS
-- ============================================================================

USE ROLE SYSADMIN;

-- ============================================================
-- STEP 1: Suspend all processing
-- ============================================================

-- Suspend tasks that were updated to reference D_RAWCDC
-- (List all tasks from Phase 4 that were modified)
-- ALTER TASK <task_name> SUSPEND;

-- ============================================================
-- STEP 2: Revert streams to original source
-- ============================================================

-- Recreate streams pointing back to D_BRONZE
-- Example:
/*
CREATE OR REPLACE STREAM D_BRONZE.PROCESSING.CUSTOMERS_CDC_STREAM
    ON TABLE D_BRONZE.SADB.CUSTOMERS_BASE
    COMMENT = 'Reverted to D_BRONZE during rollback';
*/

-- ============================================================
-- STEP 3: Revert views to original references
-- ============================================================

-- Use the ORIGINAL_DEFINITION from Script 4.2 to recreate views
-- Example:
/*
CREATE OR REPLACE VIEW D_SILVER.TRANSFORM.V_CUSTOMERS AS
SELECT * FROM D_BRONZE.SADB.CUSTOMERS_BASE;  -- Original reference
*/

-- ============================================================
-- STEP 4: Revert tasks to original references
-- ============================================================

-- Recreate tasks with original SQL
-- Example:
/*
CREATE OR REPLACE TASK D_BRONZE.PROCESSING.LOAD_CUSTOMERS
    WAREHOUSE = TRANSFORM_WH
    SCHEDULE = 'USING CRON 0 * * * * America/New_York'
AS
INSERT INTO D_BRONZE.PROCESSED.CUSTOMERS_HISTORY
SELECT * FROM D_BRONZE.SADB.CUSTOMERS_BASE;  -- Original reference
*/

-- ============================================================
-- STEP 5: Drop D_RAWCDC database
-- ============================================================

DROP DATABASE D_RAWCDC;

-- ============================================================
-- STEP 6: Resume original tasks
-- ============================================================

-- Resume all suspended tasks
-- ALTER TASK <task_name> RESUME;

-- ============================================================
-- STEP 7: Notify Informatica to continue with original targets
-- ============================================================
/*
Contact Informatica team:
- Keep CDC jobs pointing to D_BRONZE.SADB and D_BRONZE.AWS
- Resume normal operations
*/
```

### Script 8.2: Rollback After Cleanup (Time Travel)

```sql
-- ============================================================================
-- SCRIPT 8.2: ROLLBACK PROCEDURE (After Original Schemas Dropped)
-- Use Time Travel to recover dropped schemas (within retention period)
-- ============================================================================

USE ROLE SYSADMIN;

-- ============================================================
-- STEP 1: Check Time Travel availability
-- ============================================================

-- Check when schemas were dropped
SELECT 
    SCHEMA_NAME,
    DELETED
FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA
WHERE CATALOG_NAME = 'D_BRONZE'
    AND SCHEMA_NAME IN ('SADB', 'AWS')
    AND DELETED IS NOT NULL
ORDER BY DELETED DESC;

-- ============================================================
-- STEP 2: Undrop schemas (within retention period)
-- ============================================================

-- Undrop SADB
UNDROP SCHEMA D_BRONZE.SADB;

-- Undrop AWS
UNDROP SCHEMA D_BRONZE.AWS;

-- ============================================================
-- STEP 3: Verify recovery
-- ============================================================

SELECT SCHEMA_NAME, CREATED, LAST_ALTERED
FROM D_BRONZE.INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME IN ('SADB', 'AWS');

-- ============================================================
-- STEP 4: Follow Script 8.1 to revert dependencies
-- ============================================================

-- Continue with Script 8.1 steps 1-7 to complete rollback
```

---

## Appendix: Informatica IDMC Reconfiguration

### Informatica Target Configuration Changes

After migration, Informatica IDMC jobs must be updated to target D_RAWCDC instead of D_BRONZE.

**Before Migration:**
```
Target Database: D_BRONZE
Target Schema: SADB (or AWS)
```

**After Migration:**
```
Target Database: D_RAWCDC
Target Schema: SADB (or AWS)
```

### Verification Steps for Informatica Team

1. Update all CDC mappings to target D_RAWCDC
2. Test connectivity to D_RAWCDC database
3. Verify INFORMATICA_CDC_ROLE has ownership of D_RAWCDC.SADB and D_RAWCDC.AWS
4. Run a test CDC job and verify data lands in D_RAWCDC
5. Confirm _BASE, _LOG tables, streams, and views are created correctly

### Future Redeployment Behavior

With this architecture:
- When Informatica redeploys, objects in D_RAWCDC.SADB/AWS will be dropped and recreated
- FUTURE GRANTS ensure D_RAWCDC_READ_ROLE automatically gets SELECT on new tables
- Historical data in D_BRONZE is protected from Informatica operations
- Streams will become stale and need recreation (see Script 4.1)

---

## Migration Checklist

```
□ Phase 1: Pre-Migration Assessment
  □ 1.1 Inventory objects in SADB and AWS schemas
  □ 1.2 Identify cross-schema dependencies
  □ 1.3 Document current grants

□ Phase 2: Pre-Migration Preparation
  □ 2.1 Create D_RAWCDC database
  □ 2.2 Pause Informatica CDC jobs
  □ 2.2 Suspend dependent tasks
  □ 2.3 Create migration tracking table

□ Phase 3: Execute Migration
  □ 3.1 Clone SADB schema to D_RAWCDC
  □ 3.1 Clone AWS schema to D_RAWCDC
  □ 3.2 Verify data integrity

□ Phase 4: Update Dependencies
  □ 4.1 Recreate streams pointing to D_RAWCDC
  □ 4.2 Update views to reference D_RAWCDC
  □ 4.3 Update tasks to reference D_RAWCDC
  □ 4.4 Update stored procedures

□ Phase 5: RBAC Configuration
  □ 5.1 Create D_RAWCDC roles
  □ 5.2 Configure grants and future grants
  □ 5.2 Update existing role grants

□ Phase 6: Post-Migration Validation
  □ 6.1 Comprehensive validation checks
  □ 6.2 Test data flow end-to-end

□ Phase 7: Cleanup
  □ 7.1 Resume tasks
  □ 7.1 Coordinate Informatica reconfiguration
  □ 7.1 Verify data flowing to D_RAWCDC (24-48 hours)
  □ 7.2 Drop original schemas from D_BRONZE

□ Sign-off
  □ Data team sign-off
  □ Informatica team sign-off
  □ Business validation complete
```

---

## Document Information

| Item | Value |
|------|-------|
| Version | 1.0 |
| Created | 2025-02-20 |
| Author | Snowflake Solution Architect |
| Review Status | Ready for Review |

---

*End of Migration Guide*
