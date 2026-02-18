# TRKFC_TRSTN CDC Data Preservation - Deployment Runbook

**Document Version:** 1.0.0  
**Last Updated:** February 2026  
**Classification:** Production Deployment Guide  
**Estimated Total Duration:** 25-35 minutes

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Pre-Deployment Requirements](#2-pre-deployment-requirements)
3. [Deployment Timeline](#3-deployment-timeline)
4. [Step-by-Step Deployment](#4-step-by-step-deployment)
5. [Validation Checkpoints](#5-validation-checkpoints)
6. [Rollback Procedures](#6-rollback-procedures)
7. [Post-Deployment Verification](#7-post-deployment-verification)
8. [Troubleshooting Guide](#8-troubleshooting-guide)
9. [Appendix](#9-appendix)

---

## 1. Executive Summary

### 1.1 Purpose
This runbook provides step-by-step instructions for deploying the TRKFC_TRSTN CDC (Change Data Capture) data preservation solution in production environments.

### 1.2 Solution Overview

| Component | Description |
|-----------|-------------|
| **Source Table** | `TRKFC_TRSTN_BASE` (IDMC replicated) |
| **Target Table** | `TRKFC_TRSTN_V1` (Data preservation with soft deletes) |
| **Stream** | `TRKFC_TRSTN_BASE_HIST_STREAM` (CDC capture) |
| **Procedure** | `SP_PROCESS_TRKFC_TRSTN_CDC` (Processing logic) |
| **Task** | `TASK_PROCESS_TRKFC_TRSTN_CDC` (5-minute schedule) |

### 1.3 Key Benefits

- **Data Preservation**: Soft deletes retain historical records
- **IDMC Resilient**: Auto-recovery from source table redeployments
- **Audit Ready**: Full CDC tracking with timestamps and batch IDs
- **Zero Data Loss**: SHOW_INITIAL_ROWS captures existing data

### 1.4 Deployment Files

| File | Purpose |
|------|---------|
| `TRKFC_TRSTN_PRE_DEPLOYMENT_VALIDATION.sql` | Environment validation |
| `TRKFC_TRSTN_DEPLOY_PARAMETERIZED.sql` | Main deployment script |
| `TRKFC_TRSTN_ROLLBACK.sql` | Rollback/cleanup script |

---

## 2. Pre-Deployment Requirements

### 2.1 Access Requirements

| Requirement | Details | Verified |
|-------------|---------|:--------:|
| Snowflake Role | ACCOUNTADMIN or role with CREATE privileges | [ ] |
| Database Access | Read/Write to target database | [ ] |
| Warehouse Access | USAGE on deployment warehouse | [ ] |
| Task Execution | EXECUTE TASK privilege | [ ] |

### 2.2 Environment Prerequisites

| Prerequisite | Validation Query | Expected Result |
|--------------|------------------|-----------------|
| Source table exists | `SELECT COUNT(*) FROM DB.SCHEMA.TRKFC_TRSTN_BASE` | Row count > 0 |
| Database exists | `SHOW DATABASES LIKE 'DATABASE'` | 1 row returned |
| Schema exists | `SHOW SCHEMAS LIKE 'SCHEMA' IN DATABASE DB` | 1 row returned |
| Warehouse exists | `SHOW WAREHOUSES LIKE 'WAREHOUSE'` | 1 row returned |

### 2.3 Configuration Parameters

**CRITICAL: Set these values BEFORE deployment**

```sql
-- REQUIRED CONFIGURATION
SET V_DATABASE = 'YOUR_DATABASE';      -- e.g., 'D_BRONZE'
SET V_SCHEMA = 'YOUR_SCHEMA';          -- e.g., 'SADB'
SET V_WAREHOUSE = 'YOUR_WAREHOUSE';    -- e.g., 'INFA_INGEST_WH'
```

### 2.4 Pre-Deployment Checklist

| # | Item | Status |
|---|------|:------:|
| 1 | Configuration parameters documented | [ ] |
| 2 | Snowflake credentials available | [ ] |
| 3 | Deployment scripts downloaded/accessible | [ ] |
| 4 | Rollback scripts accessible | [ ] |
| 5 | Change ticket approved (if required) | [ ] |
| 6 | Stakeholders notified | [ ] |
| 7 | Deployment window confirmed | [ ] |
| 8 | Source table (TRKFC_TRSTN_BASE) verified | [ ] |

---

## 3. Deployment Timeline

### 3.1 Time Estimates

| Phase | Steps | Duration | Cumulative |
|-------|-------|:--------:|:----------:|
| **Pre-Validation** | 1-2 | 3-5 min | 5 min |
| **Deployment** | 3-7 | 10-15 min | 20 min |
| **Verification** | 8-10 | 5-10 min | 30 min |
| **Documentation** | 11 | 5 min | 35 min |

### 3.2 Deployment Window Recommendation

| Environment | Recommended Window | Reason |
|-------------|-------------------|--------|
| Development | Any time | Low risk |
| QA/Staging | Business hours | Support available |
| Production | Low-traffic period | Minimize impact |

### 3.3 Go/No-Go Decision Points

```
DEPLOYMENT DECISION TREE
========================

START
  |
  v
[Pre-Validation Pass?]--NO--> STOP: Fix validation failures
  |
  YES
  v
[V1 Table Created OK?]--NO--> ROLLBACK POINT 1
  |
  YES
  v
[Stream Created OK?]--NO--> ROLLBACK POINT 2
  |
  YES
  v
[Procedure Created?]--NO--> ROLLBACK POINT 3
  |
  YES
  v
[Initial Load OK?]--NO--> ROLLBACK POINT 4
  |
  YES
  v
[Task Started OK?]--NO--> ROLLBACK POINT 5
  |
  YES
  v
DEPLOYMENT COMPLETE
```

---

## 4. Step-by-Step Deployment

### Step 1: Connect to Snowflake
**Duration:** 1-2 minutes

```sql
-- Verify connection and context
SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE();
```

**Expected Result:** Your username, role (should be admin-level), and warehouse displayed.

**ROLLBACK TRIGGER:** Cannot connect - Contact Snowflake admin

---

### Step 2: Run Pre-Deployment Validation
**Duration:** 3-5 minutes

```sql
-- Open and execute: TRKFC_TRSTN_PRE_DEPLOYMENT_VALIDATION.sql
-- First, set your configuration:
SET V_DATABASE = 'YOUR_DATABASE';
SET V_SCHEMA = 'YOUR_SCHEMA';
SET V_WAREHOUSE = 'YOUR_WAREHOUSE';

-- Then run the entire validation script
```

**Expected Results:**

| Check | Expected Status |
|-------|-----------------|
| Database Exists | PASS |
| Schema Exists | PASS |
| Warehouse Exists | PASS |
| Source Table Exists | PASS |
| Source Has Data | PASS |
| PK Columns Exist | PASS |

**Decision Point:**
- All PASS or only WARN: **PROCEED to Step 3**
- Any FAIL: **STOP and resolve before continuing**

---

### Step 3: Configure Deployment Script
**Duration:** 2 minutes

Open `TRKFC_TRSTN_DEPLOY_PARAMETERIZED.sql` and update configuration:

```sql
-- =============================================================================
-- CONFIGURATION VARIABLES - MODIFY THESE FOR YOUR ENVIRONMENT
-- =============================================================================
SET V_DATABASE = 'D_BRONZE';           -- Update for your environment
SET V_SCHEMA = 'SADB';                 -- Update for your environment
SET V_WAREHOUSE = 'INFA_INGEST_WH';    -- Update for your environment
SET V_TASK_SCHEDULE = '5 MINUTE';      -- Default: 5 minutes
SET V_DATA_RETENTION_DAYS = 14;        -- Default: 14 days
```

---

### Step 4: Create Target Table (V1)
**Duration:** 1-2 minutes

Execute the CREATE TABLE statement from the deployment script (STEP 1).

**Verification:**
```sql
SHOW TABLES LIKE 'TRKFC_TRSTN_V1' IN SCHEMA DB.SCHEMA;
```

**Expected:** 1 row returned showing the new table.

**ROLLBACK POINT 1:** Table creation failed
- Action: Check error message, verify permissions
- Rollback: Not needed (nothing created yet)

---

### Step 5: Enable Change Tracking and Create Stream
**Duration:** 2-3 minutes

```sql
-- Execute STEP 2: Enable change tracking
ALTER TABLE DB.SCHEMA.TRKFC_TRSTN_BASE
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 14;

-- Execute STEP 3: Create stream
CREATE OR REPLACE STREAM DB.SCHEMA.TRKFC_TRSTN_BASE_HIST_STREAM
ON TABLE DB.SCHEMA.TRKFC_TRSTN_BASE
SHOW_INITIAL_ROWS = TRUE;
```

**Verification:**
```sql
SHOW STREAMS LIKE 'TRKFC_TRSTN_BASE_HIST_STREAM' IN SCHEMA DB.SCHEMA;
-- Verify: stale = false
```

**ROLLBACK POINT 2:** Stream creation failed
- Action: Check source table exists
- Rollback: `DROP TABLE IF EXISTS DB.SCHEMA.TRKFC_TRSTN_V1;`

---

### Step 6: Create Stored Procedure
**Duration:** 2-3 minutes

Execute the CREATE PROCEDURE statement from the deployment script (STEP 4).

**Verification:**
```sql
SHOW PROCEDURES LIKE 'SP_PROCESS_TRKFC_TRSTN_CDC' IN SCHEMA DB.SCHEMA;
```

**ROLLBACK POINT 3:** Procedure creation failed
- Rollback Commands:
```sql
DROP STREAM IF EXISTS DB.SCHEMA.TRKFC_TRSTN_BASE_HIST_STREAM;
DROP TABLE IF EXISTS DB.SCHEMA.TRKFC_TRSTN_V1;
```

---

### Step 7: Execute Initial Load
**Duration:** 2-5 minutes (depends on data volume)

```sql
-- Run the procedure manually for initial load
CALL DB.SCHEMA.SP_PROCESS_TRKFC_TRSTN_CDC();
```

**Expected Result:**
```
SUCCESS: Processed X CDC changes. Batch: BATCH_YYYYMMDD_HHMMSS
```

**Verification:**
```sql
SELECT 
    COUNT(*) AS TOTAL_ROWS,
    COUNT(CASE WHEN IS_DELETED = FALSE THEN 1 END) AS ACTIVE,
    MIN(CDC_TIMESTAMP) AS FIRST_LOAD
FROM DB.SCHEMA.TRKFC_TRSTN_V1;
```

**ROLLBACK POINT 4:** Initial load failed
- Run `TRKFC_TRSTN_ROLLBACK.sql`

---

### Step 8: Create and Start Task
**Duration:** 2 minutes

```sql
-- Execute STEP 5: Create task
CREATE OR REPLACE TASK DB.SCHEMA.TASK_PROCESS_TRKFC_TRSTN_CDC
    WAREHOUSE = WAREHOUSE_NAME
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
WHEN
    SYSTEM$STREAM_HAS_DATA('DB.SCHEMA.TRKFC_TRSTN_BASE_HIST_STREAM')
AS
    CALL DB.SCHEMA.SP_PROCESS_TRKFC_TRSTN_CDC();

-- Start the task
ALTER TASK DB.SCHEMA.TASK_PROCESS_TRKFC_TRSTN_CDC RESUME;
```

**Verification:**
```sql
SHOW TASKS LIKE 'TASK_PROCESS_TRKFC_TRSTN_CDC' IN SCHEMA DB.SCHEMA;
-- Verify: state = 'started'
```

**ROLLBACK POINT 5:** Task failed to start
- Run `TRKFC_TRSTN_ROLLBACK.sql`

---

### Step 9: Verify CDC Processing
**Duration:** 3-5 minutes

```sql
-- Optional: Test UPDATE (only if safe in your environment)
UPDATE DB.SCHEMA.TRKFC_TRSTN_BASE
SET VRSN_NBR = VRSN_NBR + 1
WHERE SCAC_CD = (SELECT MIN(SCAC_CD) FROM DB.SCHEMA.TRKFC_TRSTN_BASE);

-- Process change immediately
CALL DB.SCHEMA.SP_PROCESS_TRKFC_TRSTN_CDC();

-- Verify update captured
SELECT SCAC_CD, VRSN_NBR, CDC_OPERATION, CDC_TIMESTAMP
FROM DB.SCHEMA.TRKFC_TRSTN_V1
WHERE CDC_OPERATION = 'UPDATE'
ORDER BY CDC_TIMESTAMP DESC LIMIT 5;
```

---

### Step 10: Document Deployment
**Duration:** 5 minutes

| Item | Value |
|------|-------|
| Deployment Date | _________________ |
| Deployed By | _________________ |
| Environment | _________________ |
| Database.Schema | _________________ |
| Source Row Count | _________________ |
| V1 Row Count | _________________ |
| Task Status | [ ] Started |
| Change Ticket # | _________________ |

---

## 5. Validation Checkpoints

### 5.1 Critical Validation Queries

```sql
-- 1. Verify all objects exist
SHOW TABLES LIKE 'TRKFC_TRSTN_V1' IN SCHEMA DB.SCHEMA;
SHOW STREAMS LIKE 'TRKFC_TRSTN_BASE_HIST_STREAM' IN SCHEMA DB.SCHEMA;
SHOW PROCEDURES LIKE 'SP_PROCESS_TRKFC_TRSTN_CDC' IN SCHEMA DB.SCHEMA;
SHOW TASKS LIKE 'TASK_PROCESS_TRKFC_TRSTN_CDC' IN SCHEMA DB.SCHEMA;

-- 2. Verify data consistency
SELECT 'Source' AS TBL, COUNT(*) AS CNT FROM DB.SCHEMA.TRKFC_TRSTN_BASE
UNION ALL
SELECT 'V1 Active', COUNT(*) FROM DB.SCHEMA.TRKFC_TRSTN_V1 WHERE IS_DELETED = FALSE;
```

### 5.2 Success Criteria

| Criteria | Check | Expected |
|----------|-------|----------|
| V1 table exists | SHOW TABLES | 1 row |
| V1 has data | SELECT COUNT(*) | > 0 |
| Stream not stale | SHOW STREAMS | stale = false |
| Task running | SHOW TASKS | state = started |

---

## 6. Rollback Procedures

### 6.1 Rollback Decision Matrix

| Scenario | Impact | Action | Time |
|----------|--------|--------|:----:|
| Pre-validation fails | None | Fix and retry | 10 min |
| V1 creation fails | Low | Retry | 5 min |
| Stream creation fails | Low | Check source | 5 min |
| Procedure fails | Medium | Review syntax | 10 min |
| Initial load fails | Medium | Check columns | 15 min |
| Task won't start | Medium | Check privileges | 10 min |

### 6.2 Quick Rollback Commands

```sql
-- EMERGENCY ROLLBACK (preserves V1 data)
ALTER TASK DB.SCHEMA.TASK_PROCESS_TRKFC_TRSTN_CDC SUSPEND;
DROP TASK IF EXISTS DB.SCHEMA.TASK_PROCESS_TRKFC_TRSTN_CDC;
DROP PROCEDURE IF EXISTS DB.SCHEMA.SP_PROCESS_TRKFC_TRSTN_CDC();
DROP STREAM IF EXISTS DB.SCHEMA.TRKFC_TRSTN_BASE_HIST_STREAM;
-- V1 table preserved for data recovery
```

### 6.3 Full Rollback

1. Open `TRKFC_TRSTN_ROLLBACK.sql`
2. Set configuration variables
3. Set `V_PRESERVE_V1_DATA = TRUE` (or FALSE for complete removal)
4. Execute entire script
5. Verify rollback completed

---

## 7. Post-Deployment Verification

### 7.1 Immediate Verification (Within 15 minutes)

```sql
-- Object check
SELECT 
    (SELECT COUNT(*) FROM DB.INFORMATION_SCHEMA.TABLES 
     WHERE TABLE_NAME = 'TRKFC_TRSTN_V1') AS V1_EXISTS,
    (SELECT COUNT(*) FROM DB.INFORMATION_SCHEMA.PROCEDURES 
     WHERE PROCEDURE_NAME = 'SP_PROCESS_TRKFC_TRSTN_CDC') AS PROC_EXISTS;

-- Data check
SELECT COUNT(*) AS V1_ROWS FROM DB.SCHEMA.TRKFC_TRSTN_V1;

-- Task status
SHOW TASKS LIKE 'TASK_PROCESS_TRKFC_TRSTN_CDC' IN SCHEMA DB.SCHEMA;
```

### 7.2 24-Hour Verification

- Check task execution history
- Verify no failures
- Compare source vs V1 row counts

### 7.3 Weekly Verification

```sql
SELECT 
    DATE_TRUNC('day', CDC_TIMESTAMP) AS LOAD_DATE,
    CDC_OPERATION,
    COUNT(*) AS RECORDS
FROM DB.SCHEMA.TRKFC_TRSTN_V1
WHERE CDC_TIMESTAMP >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC;
```

---

## 8. Troubleshooting Guide

### 8.1 Common Issues

| Issue | Symptom | Solution |
|-------|---------|----------|
| Task not running | state = suspended | `ALTER TASK ... RESUME;` |
| Stream stale | stale = true | Procedure auto-recovers |
| Warehouse suspended | Task fails | Resume warehouse |
| Permission denied | SQL error | Grant privileges |
| Data mismatch | V1 < Source | Run procedure manually |

### 8.2 Error Messages

| Error | Cause | Fix |
|-------|-------|-----|
| Stream is stale | Source recreated | Procedure handles automatically |
| Warehouse does not exist | Wrong name | Update V_WAREHOUSE |
| Insufficient privileges | Missing grants | Grant CREATE/EXECUTE |

### 8.3 Escalation

| Level | Condition | Contact |
|-------|-----------|---------|
| L1 | Configuration issues | DBA Team |
| L2 | Data inconsistencies | Data Engineering |
| L3 | Procedure logic | Development Team |

---

## 9. Appendix

### 9.1 Object Reference

| Object | Full Name |
|--------|-----------|
| Source Table | DB.SCHEMA.TRKFC_TRSTN_BASE |
| Target Table | DB.SCHEMA.TRKFC_TRSTN_V1 |
| Stream | DB.SCHEMA.TRKFC_TRSTN_BASE_HIST_STREAM |
| Procedure | DB.SCHEMA.SP_PROCESS_TRKFC_TRSTN_CDC |
| Task | DB.SCHEMA.TASK_PROCESS_TRKFC_TRSTN_CDC |

### 9.2 CDC Metadata Columns (V1 Table)

| Column | Type | Description |
|--------|------|-------------|
| CDC_OPERATION | VARCHAR(10) | INSERT/UPDATE/DELETE |
| CDC_TIMESTAMP | TIMESTAMP_NTZ | When change was processed |
| IS_DELETED | BOOLEAN | TRUE = soft deleted |
| RECORD_CREATED_AT | TIMESTAMP_NTZ | First load timestamp |
| RECORD_UPDATED_AT | TIMESTAMP_NTZ | Last update timestamp |
| SOURCE_LOAD_BATCH_ID | VARCHAR(100) | Processing batch ID |

### 9.3 Deployment Summary Checklist

```
PRE-DEPLOYMENT
[ ] Access verified
[ ] Configuration set
[ ] Validation passed

DEPLOYMENT
[ ] Step 1: Connected
[ ] Step 2: Validated
[ ] Step 3: Configured
[ ] Step 4: V1 created
[ ] Step 5: Stream created
[ ] Step 6: Procedure created
[ ] Step 7: Initial load success
[ ] Step 8: Task started
[ ] Step 9: CDC verified
[ ] Step 10: Documented

POST-DEPLOYMENT
[ ] Verification passed
[ ] Monitoring configured
[ ] Handoff complete
```

---

## Sign-Off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Deployer | | | |
| Reviewer | | | |
| Approver | | | |

---

**END OF RUNBOOK**
