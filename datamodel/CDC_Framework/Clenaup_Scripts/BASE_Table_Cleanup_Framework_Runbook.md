# BASE Table Cleanup Framework
## Production Deployment Runbook

---

## Document Control

| Item | Value |
|------|-------|
| **Version** | 1.0 |
| **Last Updated** | 2026-02-16 |
| **Author** | Data Engineering Team |
| **Status** | Production Ready |
| **Tested On** | Snowflake Account tgb36949 |

---

## Table of Contents

1. [Pre-Deployment Checklist](#1-pre-deployment-checklist)
2. [Deployment Steps](#2-deployment-steps)
3. [Post-Deployment Validation](#3-post-deployment-validation)
4. [Configuration Guide](#4-configuration-guide)
5. [Operations Runbook](#5-operations-runbook)
6. [Troubleshooting Guide](#6-troubleshooting-guide)
7. [Rollback Procedures](#7-rollback-procedures)
8. [Maintenance Procedures](#8-maintenance-procedures)

---

## 1. Pre-Deployment Checklist

### 1.1 Prerequisites

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  PRE-DEPLOYMENT CHECKLIST                                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  [ ] 1. PERMISSIONS VERIFIED                                                     │
│      ─────────────────────                                                       │
│      • ACCOUNTADMIN or SYSADMIN role available                                  │
│      • CREATE SCHEMA privilege on CDC_PRESERVATION database                     │
│      • CREATE TASK privilege                                                     │
│      • DELETE privilege on target _BASE tables                                  │
│                                                                                  │
│  [ ] 2. TARGET SCHEMA ANALYSIS COMPLETED                                         │
│      ─────────────────────────────────                                           │
│      • List all _BASE tables identified                                          │
│      • Date column name confirmed for each schema                               │
│      • Current data volumes documented                                           │
│      • Retention requirements confirmed (default: 45 days)                      │
│                                                                                  │
│  [ ] 3. WAREHOUSE SIZING CONFIRMED                                               │
│      ────────────────────────────                                                │
│      • Warehouse exists and is accessible                                        │
│      • Size appropriate for data volume (X-Small for <1M rows/day)             │
│      • Auto-suspend configured                                                   │
│                                                                                  │
│  [ ] 4. MAINTENANCE WINDOW IDENTIFIED                                            │
│      ─────────────────────────────                                               │
│      • Low-activity period identified (default: 2 AM UTC)                       │
│      • No conflicting ETL jobs during window                                     │
│      • Stakeholders notified                                                     │
│                                                                                  │
│  [ ] 5. BACKUP STRATEGY CONFIRMED                                                │
│      ──────────────────────────                                                  │
│      • Time Travel enabled on target tables (default: 1 day minimum)            │
│      • Fail-safe period understood                                               │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Environment Variables

Document the following for your deployment:

| Parameter | DEV Value | PROD Value |
|-----------|-----------|------------|
| Database | `CDC_PRESERVATION` | `CDC_PRESERVATION` |
| Schema | `CLEANUP` | `CLEANUP` |
| Warehouse | `COMPUTE_WH` | `COMPUTE_WH` |
| Target Database | | |
| Target Schema | | |
| Date Column | | |
| Retention Days | | |
| Schedule (CRON) | `0 2 * * * UTC` | `0 2 * * * UTC` |

---

## 2. Deployment Steps

### 2.1 Step-by-Step Deployment

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  DEPLOYMENT WORKFLOW                                                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   STEP 1                    STEP 2                    STEP 3                    │
│   ┌──────────┐              ┌──────────┐              ┌──────────┐              │
│   │ Deploy   │              │ Add      │              │ Validate │              │
│   │ Schema & │────────────▶ │ Config   │────────────▶ │ Dry Run  │              │
│   │ Objects  │              │ Entries  │              │          │              │
│   └──────────┘              └──────────┘              └──────────┘              │
│                                                             │                   │
│                                                             ▼                   │
│   STEP 6                    STEP 5                    STEP 4                    │
│   ┌──────────┐              ┌──────────┐              ┌──────────┐              │
│   │ Enable   │              │ Create   │              │ Manual   │              │
│   │ Task     │◀──────────── │ Task     │◀──────────── │ Test     │              │
│   │          │              │          │              │ Execute  │              │
│   └──────────┘              └──────────┘              └──────────┘              │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Detailed Commands

#### STEP 1: Deploy Schema and Objects

```sql
-- ============================================================
-- STEP 1: DEPLOY FRAMEWORK
-- ============================================================
-- Estimated Time: 2 minutes
-- Prerequisites: ACCOUNTADMIN role

-- 1.1 Set context
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- 1.2 Create schema
CREATE SCHEMA IF NOT EXISTS CDC_PRESERVATION.CLEANUP;

-- 1.3 Deploy all objects
-- Run the entire BASE_Table_Cleanup_Framework.sql script
-- This creates:
--   • 3 tables (CLEANUP_CONFIG, CLEANUP_LOG, CLEANUP_EXCLUSIONS)
--   • 8 procedures
--   • 4 views

-- 1.4 Verify deployment
SHOW TABLES IN SCHEMA CDC_PRESERVATION.CLEANUP;
SHOW PROCEDURES IN SCHEMA CDC_PRESERVATION.CLEANUP;
SHOW VIEWS IN SCHEMA CDC_PRESERVATION.CLEANUP;
```

**Expected Output:**
- 3 tables created
- 8 procedures created
- 4 views created

#### STEP 2: Add Configuration Entries

```sql
-- ============================================================
-- STEP 2: ADD CONFIGURATION
-- ============================================================
-- Customize values for your environment

-- 2.1 Add configuration for each target schema
INSERT INTO CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG 
(DATABASE_NAME, SCHEMA_NAME, TABLE_PATTERN, DATE_COLUMN, RETENTION_DAYS, BATCH_SIZE, TASK_WAREHOUSE, NOTES)
VALUES 
-- Example: D_BRONZE.SALES
('D_BRONZE', 'SALES', '%_BASE', 'CREATED_DATE', 45, 100000, 'COMPUTE_WH', 'Production cleanup for SALES _BASE tables');

-- Add more schemas as needed:
-- ('D_BRONZE', 'ORDERS', '%_BASE', 'CREATED_DATE', 45, 100000, 'COMPUTE_WH', 'Production cleanup for ORDERS _BASE tables'),
-- ('D_BRONZE', 'INVENTORY', '%_BASE', 'LOAD_DATE', 30, 100000, 'COMPUTE_WH', '30-day retention for inventory');

-- 2.2 Verify configuration
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_CONFIG_STATUS;
```

#### STEP 3: Validate with Dry Run

```sql
-- ============================================================
-- STEP 3: DRY RUN VALIDATION
-- ============================================================
-- ⚠️ CRITICAL: Always run dry run before first execution

-- 3.1 Execute dry run for each configured schema
CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_DRY_RUN(
    'D_BRONZE',      -- Database
    'SALES',         -- Schema
    'CREATED_DATE',  -- Date column
    45,              -- Retention days
    '%_BASE'         -- Table pattern
);

-- 3.2 Review results carefully:
-- • Check table_count matches expected tables
-- • Verify rows_to_delete is reasonable
-- • Confirm delete_pct is acceptable (typically <50% on first run)
-- • Ensure no critical tables are listed
```

**Checkpoint:** ✅ Dry run shows expected tables and reasonable delete counts

#### STEP 4: Manual Test Execution

```sql
-- ============================================================
-- STEP 4: MANUAL TEST EXECUTION
-- ============================================================
-- Execute cleanup manually to verify behavior

-- 4.1 Execute cleanup (THIS WILL DELETE DATA)
CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_SCHEMA(
    'D_BRONZE',      -- Database
    'SALES',         -- Schema  
    'CREATED_DATE',  -- Date column
    45,              -- Retention days
    100000,          -- Batch size
    '%_BASE'         -- Table pattern
);

-- 4.2 Verify results
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_RECENT_CLEANUPS
WHERE DATABASE_NAME = 'D_BRONZE' AND SCHEMA_NAME = 'SALES'
ORDER BY CREATED_AT DESC;

-- 4.3 Check for any failures
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_FAILED_CLEANUPS
WHERE CREATED_AT > DATEADD('hour', -1, CURRENT_TIMESTAMP());
```

**Checkpoint:** ✅ Manual execution successful, no failures

#### STEP 5: Create Scheduled Task

```sql
-- ============================================================
-- STEP 5: CREATE SCHEDULED TASK
-- ============================================================

-- 5.1 Create the master cleanup task
CALL CDC_PRESERVATION.CLEANUP.SP_CREATE_MASTER_CLEANUP_TASK(
    'COMPUTE_WH',                    -- Warehouse
    'USING CRON 0 2 * * * UTC'       -- Schedule: 2 AM UTC daily
);

-- 5.2 Verify task created (will be SUSPENDED)
SHOW TASKS LIKE 'TASK_CLEANUP%' IN SCHEMA CDC_PRESERVATION.CLEANUP;
```

#### STEP 6: Enable Task

```sql
-- ============================================================
-- STEP 6: ENABLE TASK
-- ============================================================
-- ⚠️ Only enable after successful manual test

-- 6.1 Resume (enable) the task
ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS RESUME;

-- 6.2 Verify task is running
SHOW TASKS LIKE 'TASK_CLEANUP%' IN SCHEMA CDC_PRESERVATION.CLEANUP;
-- Confirm STATE = 'started'

-- 6.3 Check next scheduled run
SELECT 
    NAME,
    STATE,
    SCHEDULE,
    LAST_COMMITTED_ON
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'TASK_CLEANUP_ALL_SCHEMAS'
ORDER BY SCHEDULED_TIME DESC
LIMIT 1;
```

**Checkpoint:** ✅ Task is in 'started' state

---

## 3. Post-Deployment Validation

### 3.1 Validation Checklist

```sql
-- ============================================================
-- POST-DEPLOYMENT VALIDATION
-- ============================================================

-- 3.1.1 Verify all objects exist
SELECT 'TABLES' AS OBJECT_TYPE, COUNT(*) AS COUNT 
FROM CDC_PRESERVATION.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'CLEANUP'
UNION ALL
SELECT 'PROCEDURES', COUNT(*) 
FROM CDC_PRESERVATION.INFORMATION_SCHEMA.PROCEDURES 
WHERE PROCEDURE_SCHEMA = 'CLEANUP'
UNION ALL
SELECT 'VIEWS', COUNT(*) 
FROM CDC_PRESERVATION.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'CLEANUP';

-- Expected: TABLES=3, PROCEDURES=8, VIEWS=4

-- 3.1.2 Verify configuration is active
SELECT COUNT(*) AS ACTIVE_CONFIGS 
FROM CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG 
WHERE IS_ACTIVE = TRUE;

-- 3.1.3 Verify task is running
SELECT NAME, STATE 
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'TASK_CLEANUP_ALL_SCHEMAS'
LIMIT 1;

-- 3.1.4 Verify cleanup log has entries
SELECT COUNT(*) AS LOG_ENTRIES 
FROM CDC_PRESERVATION.CLEANUP.CLEANUP_LOG;
```

### 3.2 First Day Monitoring

After the first scheduled execution (next day after deployment):

```sql
-- Check task execution history
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    RETURN_VALUE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_CLEANUP_ALL_SCHEMAS',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;

-- Review cleanup summary
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_CLEANUP_SUMMARY
WHERE CLEANUP_DATE = CURRENT_DATE();

-- Check for failures
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_FAILED_CLEANUPS
WHERE CREATED_AT > DATEADD('day', -1, CURRENT_TIMESTAMP());
```

---

## 4. Configuration Guide

### 4.1 Adding New Schemas

```sql
-- ============================================================
-- ADD NEW SCHEMA TO CLEANUP
-- ============================================================

-- Step 1: Identify tables and date column
SELECT TABLE_NAME, COLUMN_NAME
FROM <DATABASE>.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '<SCHEMA>'
  AND TABLE_NAME LIKE '%_BASE'
  AND COLUMN_NAME IN ('CREATED_DATE', 'CREATED_AT', 'LOAD_DATE', 'MODIFIED_DATE')
ORDER BY TABLE_NAME, ORDINAL_POSITION;

-- Step 2: Run dry run to preview
CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_DRY_RUN(
    '<DATABASE>',
    '<SCHEMA>',
    '<DATE_COLUMN>',
    45,
    '%_BASE'
);

-- Step 3: Add configuration (after dry run approval)
INSERT INTO CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG 
(DATABASE_NAME, SCHEMA_NAME, TABLE_PATTERN, DATE_COLUMN, RETENTION_DAYS, NOTES)
VALUES ('<DATABASE>', '<SCHEMA>', '%_BASE', '<DATE_COLUMN>', 45, '<DESCRIPTION>');

-- Step 4: Verify
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_CONFIG_STATUS
WHERE DATABASE_NAME = '<DATABASE>' AND SCHEMA_NAME = '<SCHEMA>';
```

### 4.2 Excluding Tables

```sql
-- ============================================================
-- EXCLUDE SPECIFIC TABLES FROM CLEANUP
-- ============================================================

-- Add table to exclusion list
INSERT INTO CDC_PRESERVATION.CLEANUP.CLEANUP_EXCLUSIONS 
(DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, EXCLUSION_REASON)
VALUES 
('D_BRONZE', 'SALES', 'AUDIT_LOG_BASE', 'Compliance requirement - 7 year retention');

-- Verify exclusion
SELECT * FROM CDC_PRESERVATION.CLEANUP.CLEANUP_EXCLUSIONS;
```

### 4.3 Modifying Configuration

```sql
-- ============================================================
-- MODIFY EXISTING CONFIGURATION
-- ============================================================

-- Change retention period
UPDATE CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG
SET RETENTION_DAYS = 60, UPDATED_AT = CURRENT_TIMESTAMP()
WHERE DATABASE_NAME = 'D_BRONZE' AND SCHEMA_NAME = 'SALES';

-- Disable a configuration (temporary pause)
UPDATE CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG
SET IS_ACTIVE = FALSE, UPDATED_AT = CURRENT_TIMESTAMP()
WHERE CONFIG_ID = 1;

-- Re-enable a configuration
UPDATE CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG
SET IS_ACTIVE = TRUE, UPDATED_AT = CURRENT_TIMESTAMP()
WHERE CONFIG_ID = 1;
```

---

## 5. Operations Runbook

### 5.1 Daily Health Check

```sql
-- ============================================================
-- DAILY HEALTH CHECK (Run each morning)
-- ============================================================

-- 5.1.1 Check last night's cleanup results
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_CLEANUP_SUMMARY
WHERE CLEANUP_DATE >= CURRENT_DATE() - 1
ORDER BY CLEANUP_DATE DESC;

-- 5.1.2 Check for failures
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_FAILED_CLEANUPS
WHERE CREATED_AT > DATEADD('day', -1, CURRENT_TIMESTAMP());

-- 5.1.3 Verify task ran successfully
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_CLEANUP_ALL_SCHEMAS',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC
LIMIT 5;
```

### 5.2 Manual Cleanup Execution

```sql
-- ============================================================
-- MANUAL CLEANUP (When needed outside schedule)
-- ============================================================

-- Option 1: Run specific schema
CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_SCHEMA(
    'D_BRONZE', 'SALES', 'CREATED_DATE', 45, 100000, '%_BASE'
);

-- Option 2: Run using config ID
CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_BY_CONFIG(1);

-- Option 3: Run all active configs
CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_ALL_CONFIGS();
```

### 5.3 Task Management

```sql
-- ============================================================
-- TASK MANAGEMENT COMMANDS
-- ============================================================

-- Suspend task (stop scheduled execution)
ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS SUSPEND;
-- Or use procedure:
CALL CDC_PRESERVATION.CLEANUP.SP_SUSPEND_CLEANUP_TASK();

-- Resume task (restart scheduled execution)
ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS RESUME;
-- Or use procedure:
CALL CDC_PRESERVATION.CLEANUP.SP_RESUME_CLEANUP_TASK();

-- Check task status
SHOW TASKS LIKE 'TASK_CLEANUP%' IN SCHEMA CDC_PRESERVATION.CLEANUP;

-- View task schedule
DESCRIBE TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS;

-- Execute task immediately (for testing)
EXECUTE TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS;
```

---

## 6. Troubleshooting Guide

### 6.1 Common Issues and Solutions

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  TROUBLESHOOTING GUIDE                                                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ISSUE: Task not running                                                         │
│  ─────────────────────────                                                       │
│  Symptoms: No cleanup logs for expected date                                     │
│  Diagnosis:                                                                      │
│    SHOW TASKS LIKE 'TASK_CLEANUP%' IN SCHEMA CDC_PRESERVATION.CLEANUP;          │
│    -- Check STATE column                                                         │
│  Solutions:                                                                      │
│    • If STATE = 'suspended': ALTER TASK ... RESUME;                             │
│    • If STATE = 'started' but no logs: Check warehouse availability             │
│    • Check TASK_HISTORY for error messages                                       │
│                                                                                  │
│  ───────────────────────────────────────────────────────────────────────────    │
│                                                                                  │
│  ISSUE: "Date column not found" in logs                                          │
│  ──────────────────────────────────────                                          │
│  Symptoms: Tables show SKIPPED status                                            │
│  Diagnosis:                                                                      │
│    SELECT COLUMN_NAME FROM <DB>.INFORMATION_SCHEMA.COLUMNS                      │
│    WHERE TABLE_SCHEMA = '<SCHEMA>' AND TABLE_NAME = '<TABLE>';                  │
│  Solutions:                                                                      │
│    • Update config with correct DATE_COLUMN value                               │
│    • Ensure column name is exact match (case-sensitive)                         │
│                                                                                  │
│  ───────────────────────────────────────────────────────────────────────────    │
│                                                                                  │
│  ISSUE: No rows deleted (unexpected)                                             │
│  ───────────────────────────────────                                             │
│  Symptoms: rows_deleted = 0 when data exists                                     │
│  Diagnosis:                                                                      │
│    SELECT MIN(<DATE_COL>), MAX(<DATE_COL>) FROM <TABLE>;                        │
│    SELECT COUNT(*) FROM <TABLE> WHERE <DATE_COL> < DATEADD('day', -45, NOW()); │
│  Solutions:                                                                      │
│    • Verify retention days setting                                               │
│    • Check date column has correct data                                          │
│    • Ensure date column is TIMESTAMP or DATE type                               │
│                                                                                  │
│  ───────────────────────────────────────────────────────────────────────────    │
│                                                                                  │
│  ISSUE: Permission denied errors                                                 │
│  ────────────────────────────────                                                │
│  Symptoms: FAILED status with permission error                                   │
│  Diagnosis:                                                                      │
│    SELECT * FROM V_FAILED_CLEANUPS WHERE ERROR_MESSAGE LIKE '%permission%';     │
│  Solutions:                                                                      │
│    • Grant DELETE on target tables to task owner role                           │
│    • Grant USAGE on target database and schema                                  │
│    • Verify EXECUTE AS CALLER has appropriate permissions                       │
│                                                                                  │
│  ───────────────────────────────────────────────────────────────────────────    │
│                                                                                  │
│  ISSUE: Task timeout                                                             │
│  ─────────────────                                                               │
│  Symptoms: Task fails with timeout error                                         │
│  Diagnosis:                                                                      │
│    SELECT * FROM TASK_HISTORY WHERE ERROR_MESSAGE LIKE '%timeout%';             │
│  Solutions:                                                                      │
│    • Increase USER_TASK_TIMEOUT_MS (default: 4 hours)                          │
│    • Use larger warehouse                                                        │
│    • Reduce batch size to process fewer rows per run                            │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Diagnostic Queries

```sql
-- ============================================================
-- DIAGNOSTIC QUERIES
-- ============================================================

-- 6.2.1 View all failed cleanups with details
SELECT 
    BATCH_ID,
    DATABASE_NAME,
    SCHEMA_NAME,
    TABLE_NAME,
    ERROR_MESSAGE,
    CREATED_AT
FROM CDC_PRESERVATION.CLEANUP.CLEANUP_LOG
WHERE STATUS = 'FAILED'
ORDER BY CREATED_AT DESC
LIMIT 20;

-- 6.2.2 Check if date column exists in target table
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE
FROM <DATABASE>.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '<SCHEMA>'
  AND TABLE_NAME LIKE '%_BASE'
  AND COLUMN_NAME IN ('CREATED_DATE', 'CREATED_AT', 'LOAD_DATE')
ORDER BY TABLE_NAME;

-- 6.2.3 Check data age in target table
SELECT 
    MIN(<DATE_COLUMN>) AS OLDEST_RECORD,
    MAX(<DATE_COLUMN>) AS NEWEST_RECORD,
    COUNT(*) AS TOTAL_ROWS,
    SUM(CASE WHEN <DATE_COLUMN> < DATEADD('day', -45, CURRENT_DATE()) THEN 1 ELSE 0 END) AS ROWS_OVER_45_DAYS
FROM <DATABASE>.<SCHEMA>.<TABLE>;

-- 6.2.4 Task execution history
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    RETURN_VALUE,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_CLEANUP_ALL_SCHEMAS',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;
```

---

## 7. Rollback Procedures

### 7.1 Immediate Rollback (Stop Cleanup)

```sql
-- ============================================================
-- EMERGENCY STOP - Suspend Task Immediately
-- ============================================================

-- Stop the scheduled task
ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS SUSPEND;

-- Verify task is stopped
SHOW TASKS LIKE 'TASK_CLEANUP%' IN SCHEMA CDC_PRESERVATION.CLEANUP;
-- Confirm STATE = 'suspended'
```

### 7.2 Recover Deleted Data (Time Travel)

```sql
-- ============================================================
-- RECOVER DATA USING TIME TRAVEL
-- ============================================================

-- ⚠️ Time Travel must be enabled and within retention period

-- Step 1: Find the cleanup timestamp
SELECT 
    TABLE_NAME,
    EXECUTION_START,
    EXECUTION_END,
    ROWS_DELETED
FROM CDC_PRESERVATION.CLEANUP.CLEANUP_LOG
WHERE DATABASE_NAME = '<DATABASE>'
  AND SCHEMA_NAME = '<SCHEMA>'
  AND TABLE_NAME = '<TABLE>'
ORDER BY CREATED_AT DESC
LIMIT 1;

-- Step 2: Query data as of before deletion
SELECT COUNT(*) FROM <DATABASE>.<SCHEMA>.<TABLE>
AT(TIMESTAMP => '<EXECUTION_START_MINUS_1_MINUTE>'::TIMESTAMP);

-- Step 3: Recover deleted data (INSERT back)
INSERT INTO <DATABASE>.<SCHEMA>.<TABLE>
SELECT * FROM <DATABASE>.<SCHEMA>.<TABLE>
AT(TIMESTAMP => '<TIMESTAMP_BEFORE_DELETE>'::TIMESTAMP)
WHERE <DATE_COLUMN> < DATEADD('day', -45, CURRENT_DATE());

-- Alternative: Clone entire table to point in time
CREATE TABLE <DATABASE>.<SCHEMA>.<TABLE>_RECOVERED
CLONE <DATABASE>.<SCHEMA>.<TABLE>
AT(TIMESTAMP => '<TIMESTAMP_BEFORE_DELETE>'::TIMESTAMP);
```

### 7.3 Complete Framework Removal

```sql
-- ============================================================
-- COMPLETE FRAMEWORK REMOVAL (Use with caution)
-- ============================================================

-- Step 1: Suspend and drop task
ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS SUSPEND;
DROP TASK IF EXISTS CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS;

-- Step 2: Drop all objects in schema
DROP SCHEMA IF EXISTS CDC_PRESERVATION.CLEANUP CASCADE;

-- Step 3: Verify removal
SHOW SCHEMAS LIKE 'CLEANUP' IN DATABASE CDC_PRESERVATION;
-- Should return no rows
```

---

## 8. Maintenance Procedures

### 8.1 Log Table Maintenance

```sql
-- ============================================================
-- CLEANUP LOG TABLE MAINTENANCE
-- ============================================================
-- Run monthly to prevent log table from growing too large

-- Archive old log entries (older than 90 days)
CREATE TABLE IF NOT EXISTS CDC_PRESERVATION.CLEANUP.CLEANUP_LOG_ARCHIVE 
AS SELECT * FROM CDC_PRESERVATION.CLEANUP.CLEANUP_LOG WHERE 1=0;

INSERT INTO CDC_PRESERVATION.CLEANUP.CLEANUP_LOG_ARCHIVE
SELECT * FROM CDC_PRESERVATION.CLEANUP.CLEANUP_LOG
WHERE CREATED_AT < DATEADD('day', -90, CURRENT_TIMESTAMP());

-- Delete archived entries from main table
DELETE FROM CDC_PRESERVATION.CLEANUP.CLEANUP_LOG
WHERE CREATED_AT < DATEADD('day', -90, CURRENT_TIMESTAMP());

-- Verify counts
SELECT 
    'ACTIVE' AS TABLE_TYPE, COUNT(*) AS ROW_COUNT 
FROM CDC_PRESERVATION.CLEANUP.CLEANUP_LOG
UNION ALL
SELECT 'ARCHIVE', COUNT(*) 
FROM CDC_PRESERVATION.CLEANUP.CLEANUP_LOG_ARCHIVE;
```

### 8.2 Update Task Schedule

```sql
-- ============================================================
-- MODIFY TASK SCHEDULE
-- ============================================================

-- Step 1: Suspend task
ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS SUSPEND;

-- Step 2: Recreate with new schedule
CALL CDC_PRESERVATION.CLEANUP.SP_CREATE_MASTER_CLEANUP_TASK(
    'COMPUTE_WH',
    'USING CRON 0 3 * * * UTC'  -- Changed to 3 AM UTC
);

-- Step 3: Resume task
ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS RESUME;
```

### 8.3 Upgrade Framework

```sql
-- ============================================================
-- FRAMEWORK UPGRADE PROCEDURE
-- ============================================================

-- Step 1: Suspend task during upgrade
ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS SUSPEND;

-- Step 2: Backup configuration
CREATE TABLE CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG_BACKUP 
AS SELECT * FROM CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG;

CREATE TABLE CDC_PRESERVATION.CLEANUP.CLEANUP_EXCLUSIONS_BACKUP 
AS SELECT * FROM CDC_PRESERVATION.CLEANUP.CLEANUP_EXCLUSIONS;

-- Step 3: Run updated SQL script
-- (This will CREATE OR REPLACE procedures and views)

-- Step 4: Verify configuration preserved
SELECT COUNT(*) FROM CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG;

-- Step 5: Resume task
ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS RESUME;

-- Step 6: Cleanup backups after verification (optional)
DROP TABLE IF EXISTS CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG_BACKUP;
DROP TABLE IF EXISTS CDC_PRESERVATION.CLEANUP.CLEANUP_EXCLUSIONS_BACKUP;
```

---

## 9. Quick Reference Card

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           QUICK REFERENCE CARD                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  DAILY OPERATIONS                                                                │
│  ────────────────                                                                │
│  Health Check:     SELECT * FROM V_CLEANUP_SUMMARY WHERE CLEANUP_DATE = TODAY;  │
│  Check Failures:   SELECT * FROM V_FAILED_CLEANUPS;                             │
│  View Configs:     SELECT * FROM V_CONFIG_STATUS;                               │
│                                                                                  │
│  TASK MANAGEMENT                                                                 │
│  ───────────────                                                                 │
│  Stop Task:        ALTER TASK ...TASK_CLEANUP_ALL_SCHEMAS SUSPEND;              │
│  Start Task:       ALTER TASK ...TASK_CLEANUP_ALL_SCHEMAS RESUME;               │
│  Run Now:          EXECUTE TASK ...TASK_CLEANUP_ALL_SCHEMAS;                    │
│  Check Status:     SHOW TASKS LIKE 'TASK_CLEANUP%' IN SCHEMA ...CLEANUP;        │
│                                                                                  │
│  CLEANUP EXECUTION                                                               │
│  ─────────────────                                                               │
│  Preview:          CALL SP_CLEANUP_DRY_RUN('DB', 'SCHEMA', 'DATE_COL', 45);     │
│  Execute:          CALL SP_CLEANUP_SCHEMA('DB', 'SCHEMA', 'DATE_COL', 45);      │
│  By Config:        CALL SP_CLEANUP_BY_CONFIG(1);                                │
│  All Configs:      CALL SP_CLEANUP_ALL_CONFIGS();                               │
│                                                                                  │
│  CONFIGURATION                                                                   │
│  ─────────────                                                                   │
│  Add Schema:       INSERT INTO CLEANUP_CONFIG (...) VALUES (...);               │
│  Exclude Table:    INSERT INTO CLEANUP_EXCLUSIONS (...) VALUES (...);           │
│  Disable Config:   UPDATE CLEANUP_CONFIG SET IS_ACTIVE = FALSE WHERE ...;       │
│                                                                                  │
│  EMERGENCY                                                                       │
│  ─────────                                                                       │
│  Stop All:         ALTER TASK ...TASK_CLEANUP_ALL_SCHEMAS SUSPEND;              │
│  Recover Data:     Use Time Travel (see Section 7.2)                            │
│                                                                                  │
│  KEY OBJECTS                                                                     │
│  ───────────                                                                     │
│  Schema:           CDC_PRESERVATION.CLEANUP                                     │
│  Task:             TASK_CLEANUP_ALL_SCHEMAS                                     │
│  Config Table:     CLEANUP_CONFIG                                               │
│  Log Table:        CLEANUP_LOG                                                  │
│  Exclusion Table:  CLEANUP_EXCLUSIONS                                           │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 10. Support Contacts

| Role | Contact | Responsibility |
|------|---------|----------------|
| Framework Owner | Data Engineering Team | Framework updates, major issues |
| Operations | Platform Team | Daily monitoring, task management |
| On-Call | PagerDuty | Critical failures |

---

**Document Version History:**

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-02-16 | Data Engineering | Initial release |
