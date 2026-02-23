# CDC Data Preservation - Prerequisites Checklist

**Document Version:** 1.0  
**Date:** February 23, 2026  
**Project:** Snowflake CDC Data Preservation Strategy

---

## Executive Summary

This checklist must be completed **BEFORE** executing any rollback or maintenance operations on the CDC Data Preservation infrastructure.

| Category | Checks | Critical |
|----------|--------|----------|
| Access & Permissions | 5 | Yes |
| Object Existence | 5 | Yes |
| Object State | 4 | Yes |
| Data Validation | 3 | Yes |
| Environment | 4 | Yes |
| **TOTAL** | **21** | - |

---

## 📋 PREREQUISITES CHECKLIST

### 1. ACCESS & PERMISSIONS

| # | Requirement | How to Verify | Status |
|---|-------------|---------------|--------|
| 1.1 | User has role **D-SNW-DEVBI1-ETL** or higher | `SELECT CURRENT_ROLE();` | ☐ |
| 1.2 | Access to **D_RAW** database | `USE DATABASE D_RAW;` | ☐ |
| 1.3 | Access to **D_BRONZE** database | `USE DATABASE D_BRONZE;` | ☐ |
| 1.4 | Permission to ALTER/DROP TASK | Test with `SHOW TASKS IN SCHEMA D_RAW.SADB;` | ☐ |
| 1.5 | Permission to DROP PROCEDURE | Test with `SHOW PROCEDURES IN SCHEMA D_RAW.SADB;` | ☐ |

```sql
-- Verification Script
SELECT CURRENT_USER() AS USER_NAME, 
       CURRENT_ROLE() AS CURRENT_ROLE,
       CURRENT_DATABASE() AS DATABASE,
       CURRENT_SCHEMA() AS SCHEMA;
```

---

### 2. OBJECT EXISTENCE (21 Tables × 5 Objects = 105 Total)

| # | Object Type | Expected Count | How to Verify | Status |
|---|-------------|----------------|---------------|--------|
| 2.1 | Tasks | 21 | `SHOW TASKS LIKE 'TASK_PROCESS_%' IN SCHEMA D_RAW.SADB;` | ☐ |
| 2.2 | Stored Procedures | 21 | `SHOW PROCEDURES LIKE 'SP_PROCESS_%' IN SCHEMA D_RAW.SADB;` | ☐ |
| 2.3 | Streams | 21 | `SHOW STREAMS LIKE '%_HIST_STREAM' IN SCHEMA D_RAW.SADB;` | ☐ |
| 2.4 | Target Tables | 21 | `SHOW TABLES IN SCHEMA D_BRONZE.SADB;` | ☐ |
| 2.5 | Source Tables (with CT) | 21 | Check change_tracking on source tables | ☐ |

```sql
-- Quick Count Verification
SELECT 'TASKS' AS OBJECT_TYPE, COUNT(*) AS COUNT 
FROM D_RAW.INFORMATION_SCHEMA.TASKS 
WHERE TASK_SCHEMA = 'SADB' AND TASK_NAME LIKE 'TASK_PROCESS_%'
UNION ALL
SELECT 'PROCEDURES', COUNT(*) 
FROM D_RAW.INFORMATION_SCHEMA.PROCEDURES 
WHERE PROCEDURE_SCHEMA = 'SADB' AND PROCEDURE_NAME LIKE 'SP_PROCESS_%'
UNION ALL
SELECT 'STREAMS', COUNT(*) 
FROM D_RAW.INFORMATION_SCHEMA.STREAMS 
WHERE STREAM_SCHEMA = 'SADB' AND STREAM_NAME LIKE '%_HIST_STREAM';
```

---

### 3. OBJECT STATE VALIDATION

| # | Check | Expected State | How to Verify | Status |
|---|-------|----------------|---------------|--------|
| 3.1 | Tasks are RUNNING | started | Check task state | ☐ |
| 3.2 | Streams are NOT STALE | STALE = false | Check stream stale flag | ☐ |
| 3.3 | No active task executions | No running | Check TASK_HISTORY | ☐ |
| 3.4 | Warehouse available | INFA_INGEST_WH | `SHOW WAREHOUSES LIKE 'INFA_INGEST_WH';` | ☐ |

```sql
-- Check Task States
SELECT TASK_NAME, STATE, SCHEDULE
FROM D_RAW.INFORMATION_SCHEMA.TASKS
WHERE TASK_SCHEMA = 'SADB' AND TASK_NAME LIKE 'TASK_PROCESS_%'
ORDER BY TASK_NAME;

-- Check Stream States
SELECT STREAM_NAME, STALE, TABLE_NAME
FROM D_RAW.INFORMATION_SCHEMA.STREAMS
WHERE STREAM_SCHEMA = 'SADB' AND STREAM_NAME LIKE '%_HIST_STREAM'
ORDER BY STREAM_NAME;

-- Check for Running Task Executions
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100
))
WHERE NAME LIKE 'TASK_PROCESS_%'
AND STATE = 'EXECUTING'
ORDER BY SCHEDULED_TIME DESC;
```

---

### 4. DATA VALIDATION

| # | Check | Description | How to Verify | Status |
|---|-------|-------------|---------------|--------|
| 4.1 | Target tables have data | Row count > 0 | Query each table | ☐ |
| 4.2 | No active DML on targets | No locks | Check QUERY_HISTORY | ☐ |
| 4.3 | Backup confirmed (if FULL) | Backup exists | Verify backup location | ☐ |

```sql
-- Check Target Table Row Counts
SELECT TABLE_NAME, ROW_COUNT
FROM D_BRONZE.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SADB'
AND TABLE_NAME IN (
    'OPTRN', 'OPTRN_LEG', 'OPTRN_EVENT', 'TRAIN_PLAN', 'TRAIN_PLAN_LEG',
    'TRAIN_PLAN_EVENT', 'LCMTV_MVMNT_EVENT', 'EQPMV_RFEQP_MVMNT_EVENT',
    'EQPMV_EQPMT_EVENT_TYPE', 'TRAIN_CNST_SMRY', 'TRAIN_CNST_DTL_RAIL_EQPT',
    'TRKFC_TRSTN', 'EQPMNT_AAR_BASE', 'STNWYB_MSG_DN', 'LCMTV_EMIS',
    'TRKFCG_FIXED_PLANT_ASSET', 'TRKFCG_FXPLA_TRACK_LCTN_DN',
    'TRKFCG_TRACK_SGMNT_DN', 'TRKFCG_SBDVSN', 'TRKFCG_SRVC_AREA',
    'CTNAPP_CTNG_LINE_DN'
)
ORDER BY TABLE_NAME;
```

---

### 5. ENVIRONMENT CHECKS

| # | Check | Description | How to Verify | Status |
|---|-------|-------------|---------------|--------|
| 5.1 | Correct account | YYB42718 | `SELECT CURRENT_ACCOUNT();` | ☐ |
| 5.2 | Correct environment | DEV/UAT/PROD | Verify environment | ☐ |
| 5.3 | Change window approved | Maintenance window | Check schedule | ☐ |
| 5.4 | Stakeholders notified | Communication sent | Confirm notification | ☐ |

```sql
-- Environment Verification
SELECT 
    CURRENT_ACCOUNT() AS ACCOUNT,
    CURRENT_USER() AS USER_NAME,
    CURRENT_ROLE() AS ROLE,
    CURRENT_DATABASE() AS DATABASE,
    CURRENT_WAREHOUSE() AS WAREHOUSE,
    CURRENT_TIMESTAMP() AS TIMESTAMP;
```

---

## 🔍 AUTOMATED VALIDATION

### Run Pre-Deployment Validation Script

```sql
-- STEP 1: Execute full validation
CALL D_RAW.SADB.SP_CDC_PREDEPLOYMENT_VALIDATION('ALL', 'ALL');

-- STEP 2: Review output
-- Expected: "status": "PASS" or "PASS_WITH_WARNINGS"
-- Expected: "ready_for_rollback": true
```

### Expected Output Structure

```json
{
  "status": "PASS",
  "ready_for_rollback": true,
  "summary": {
    "total_checks": 105,
    "passed": 105,
    "failed": 0,
    "warnings": 0,
    "pass_rate": 100.0,
    "tasks": { "found": 21, "running": 21, "suspended": 0, "missing": 0 },
    "procedures": { "found": 21, "missing": 0 },
    "streams": { "found": 21, "stale": 0, "missing": 0 },
    "target_tables": { "found": 21, "missing": 0, "total_rows": XXXXX },
    "source_change_tracking": { "enabled": 21, "disabled": 0 }
  },
  "recommendation": "All checks passed. Safe to proceed with rollback."
}
```

---

## ✅ PRE-ROLLBACK APPROVAL

### Checklist Summary

| Section | Checks | Passed | Status |
|---------|--------|--------|--------|
| Access & Permissions | 5 | ☐/5 | ☐ |
| Object Existence | 5 | ☐/5 | ☐ |
| Object State | 4 | ☐/4 | ☐ |
| Data Validation | 3 | ☐/3 | ☐ |
| Environment | 4 | ☐/4 | ☐ |
| **TOTAL** | **21** | ☐/21 | ☐ |

### Approval Sign-Off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| DBA | _________________ | _________ | _________ |
| ETL Lead | _________________ | _________ | _________ |
| Manager | _________________ | _________ | _________ |

---

## ⚠️ STOP CONDITIONS

**DO NOT PROCEED with rollback if ANY of the following:**

1. ❌ Automated validation returns `"status": "FAIL"`
2. ❌ Any task is currently EXECUTING
3. ❌ Active DML operations on target tables
4. ❌ Missing objects detected
5. ❌ No backup confirmed (for FULL rollback)
6. ❌ Outside approved maintenance window
7. ❌ Required approvals not obtained

---

## 📞 ESCALATION CONTACTS

| Role | Contact | Phone |
|------|---------|-------|
| DBA On-Call | _________________ | _________ |
| ETL Support | _________________ | _________ |
| Manager | _________________ | _________ |

---

## 🔄 ROLLBACK EXECUTION SEQUENCE

After ALL prerequisites are met:

```sql
-- 1. Run pre-validation
CALL D_RAW.SADB.SP_CDC_PREDEPLOYMENT_VALIDATION('ALL', 'ALL');

-- 2. If PASS, execute dry-run first
CALL D_RAW.SADB.SP_CDC_ROLLBACK('SOFT', TRUE, 'ALL');

-- 3. Review dry-run output

-- 4. Execute actual rollback
CALL D_RAW.SADB.SP_CDC_ROLLBACK('SOFT', FALSE, 'ALL');

-- 5. Verify rollback completed
SHOW TASKS LIKE 'TASK_PROCESS_%' IN SCHEMA D_RAW.SADB;
```

---

*Document Version: 1.0*  
*Last Updated: February 23, 2026*
