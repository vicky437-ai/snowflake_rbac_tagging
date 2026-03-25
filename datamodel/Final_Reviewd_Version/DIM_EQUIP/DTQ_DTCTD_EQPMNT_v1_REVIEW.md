# Code Review: DTQ_DTCTD_EQPMNT CDC Data Preservation Script (V2.1)
**Review Date:** March 24, 2026  
**Reviewer:** Cortex Code  
**Script:** cpkc/DTQ_DTCTD_EQPMNT_v1.sql  

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE` (46 source + 3 SNW metadata = 49 columns) |
| Target Table | `D_BRONZE.EHMS.DTQ_DTCTD_EQPMNT` (49 source + 6 CDC metadata = 55 columns) |
| Stream | `D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE_HIST_STREAM` |
| Procedure | `D_RAW.EHMS.SP_PROCESS_DTQ_DTCTD_EQPMNT()` |
| Task | `D_RAW.EHMS.TASK_SP_PROCESS_DTQ_DTCTD_EQPMNT` (5 min) |
| Primary Key | `DTCTD_EQPMNT_ID` (single) |
| Filter | NONE (no purge filter required for EHMS schema) |
| Execution Log | `D_BRONZE.MONITORING.CDC_EXECUTION_LOG` |

---

## 2. Script Steps Summary

| Step | Description | Lines |
|------|-------------|-------|
| 1 | Create target bronze table with 55 columns + inline COMMENT | 28-94 |
| 2 | Enable change tracking on source (45-day retention + 15-day extension) | 99-102 |
| 3 | Create stream with `SHOW_INITIAL_ROWS = TRUE` | 107-110 |
| 4 | Create stored procedure for CDC processing | 115-640 |
| 5 | Create scheduled task (5 min, no overlap) + RESUME | 645-653 |

---

## 3. Key Features (v2.1 Enhancements)

| # | Feature | Status | Impact |
|---|---------|--------|--------|
| 1 | Enhanced exception handling (STATEMENT_ERROR + WHEN OTHER) | ✅ | Specific SQL error classification |
| 2 | Stale stream detection via `SELECT COUNT(*) WHERE 1=0` | ✅ | Safe staleness probe without consuming stream |
| 3 | Stale stream auto-recovery (recreate + full reload) | ✅ | Self-healing pipeline |
| 4 | Base table validation before stream recreation | ✅ | Prevents orphaned streams |
| 5 | Execution logging to `CDC_EXECUTION_LOG` | ✅ | Audit trail & monitoring |
| 6 | Monitoring table auto-creation (`CREATE TABLE IF NOT EXISTS`) | ✅ | Preventive — no deployment dependency |
| 7 | Staging table pattern (single stream read) | ✅ | Best practice — avoids double-consume |
| 8 | Duplicate INSERT resolution (MATCHED + INSERT + IS_UPDATE=FALSE) | ✅ | Handles out-of-order CDC events |
| 9 | `ADDITIONAL_METRICS` VARIANT column for extensible metrics | ✅ | Future-proof monitoring |

---

## 4. Exception Handling Architecture

### 4.1 Stream Staleness Detection

| Check | Method | Error Codes Checked |
|-------|--------|---------------------|
| Stream health | `SELECT COUNT(*) WHERE 1=0` | ILIKE `%invalid%`, `%stale%`, codes 2000/2003/2043 |
| Non-staleness errors | Re-raised via `RAISE` | All other STATEMENT_ERROR codes |

### 4.2 Exception Handlers

| Handler | Scope | Action |
|---------|-------|--------|
| Stream staleness (inner BEGIN) | Stream health check only | Set flag, continue to recovery |
| Base table validation (inner BEGIN) | Recovery path | Log RECOVERY_FAILED, return error |
| STATEMENT_ERROR (global) | Entire procedure | Cleanup temp table → Log SQL_ERROR → Return |
| WHEN OTHER (global) | Entire procedure | Cleanup temp table → Log UNKNOWN_ERROR → Return |
| Nested WHEN OTHER in loggers | Insert to monitoring | Silent catch — ensures error message still returned |

### 4.3 Best Practice Compliance

| Best Practice | Implemented? | Details |
|---------------|--------------|---------|
| Log errors to persistent table | ✅ | `CDC_EXECUTION_LOG` captures all statuses |
| Include SQLCODE, SQLSTATE, SQLERRM | ✅ | All three captured in error messages |
| Track error context | ✅ | Error message includes captured code + state |
| Cleanup resources in handlers | ✅ | `DROP TABLE IF EXISTS` in both global handlers |
| Safe logging (won't mask real error) | ✅ | Nested exception handler around log INSERT |
| Re-raise critical errors (fail task) | ❌ | Returns error string; task sees success |

---

## 5. MERGE Branch Validation

### 5.1 Recovery MERGE (Stale Stream Path)

| Branch | Condition | Columns Affected | Status |
|--------|-----------|-----------------|--------|
| UPDATE (existing rows) | MATCHED | 46 source + 3 SNW + 5 CDC | ✅ PASS |
| INSERT (new rows) | NOT MATCHED | All 55 columns | ✅ PASS |

### 5.2 Main MERGE (Normal CDC Path)

| Branch | Condition | Operation | Columns Affected | Status |
|--------|-----------|-----------|-----------------|--------|
| Standard UPDATE | MATCHED + INSERT + IS_UPDATE=TRUE | UPDATE | 46 source + 3 SNW + 5 CDC | ✅ PASS |
| Soft DELETE | MATCHED + DELETE + IS_UPDATE=FALSE | UPDATE (soft delete) | 5 CDC only | ✅ PASS |
| Duplicate INSERT resolution | MATCHED + INSERT + IS_UPDATE=FALSE | UPDATE | 46 source + 3 SNW + 5 CDC (CDC_OPERATION='DUPLICATE_RESOLVED') | ✅ PASS |
| New INSERT | NOT MATCHED + INSERT | INSERT | All 55 columns | ✅ PASS |

---

## 6. Column Mapping Verification

| Category | Count | Status |
|----------|-------|--------|
| Source business columns | 46/46 | ✅ 100% |
| SNW metadata columns (OPERATION_TYPE, LAST_REPLICATED, OPERATION_OWNER) | 3/3 | ✅ 100% |
| CDC metadata columns (CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID) | 6/6 | ✅ 100% |
| Total target columns | 55 | ✅ Complete |

---

## 7. CDC Metadata Columns

| Column | Type | Set On INSERT | Set On UPDATE | Set On DELETE | Set On RECOVERY |
|--------|------|---------------|---------------|---------------|-----------------|
| CDC_OPERATION | VARCHAR(10) | 'INSERT' | 'UPDATE' | 'DELETE' | 'RELOADED' |
| CDC_TIMESTAMP | TIMESTAMP_NTZ | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() |
| IS_DELETED | BOOLEAN | FALSE | FALSE | TRUE | FALSE |
| RECORD_CREATED_AT | TIMESTAMP_NTZ | CURRENT_TIMESTAMP() | — (unchanged) | — (unchanged) | — (unchanged) |
| RECORD_UPDATED_AT | TIMESTAMP_NTZ | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() |
| SOURCE_LOAD_BATCH_ID | VARCHAR(100) | Batch ID | Batch ID | Batch ID | Batch ID |

---

## 8. Monitoring & Logging

### 8.1 CDC_EXECUTION_LOG Schema

| Column | Type | Purpose |
|--------|------|---------|
| TABLE_NAME | VARCHAR(100) | Always 'DTQ_DTCTD_EQPMNT' |
| BATCH_ID | VARCHAR(100) | `BATCH_YYYYMMDD_HH24MISS` format |
| EXECUTION_STATUS | VARCHAR(50) | SUCCESS, NO_DATA, RECOVERY, SQL_ERROR, UNKNOWN_ERROR, RECOVERY_FAILED |
| START_TIME | TIMESTAMP_NTZ | Procedure start |
| END_TIME | TIMESTAMP_NTZ | Procedure end |
| ROWS_PROCESSED | NUMBER | Total MERGE row count |
| ROWS_INSERTED | NUMBER | CDC INSERT count |
| ROWS_UPDATED | NUMBER | CDC UPDATE count |
| ROWS_DELETED | NUMBER | CDC DELETE count |
| ERROR_MESSAGE | VARCHAR(5000) | Error details if failed |
| CREATED_AT | TIMESTAMP_NTZ | Log timestamp |
| ADDITIONAL_METRICS | VARIANT | `{"duplicate_resolved_count": N}` |

### 8.2 Execution Status Values

| Status | Meaning | Logged By |
|--------|---------|-----------|
| SUCCESS | Normal CDC merge completed | Main path |
| NO_DATA | Stream had no changes | Empty stream check |
| RECOVERY | Stream was stale, recreated and reloaded | Recovery path |
| RECOVERY_FAILED | Base table inaccessible during recovery | Recovery validation |
| SQL_ERROR | SQL statement failed | STATEMENT_ERROR handler |
| UNKNOWN_ERROR | Unexpected error | WHEN OTHER handler |

---

## 9. Task Configuration

| Setting | Value | Notes |
|---------|-------|-------|
| Warehouse | `INFA_INGEST_WH` | Shared ingestion warehouse |
| Schedule | 5 MINUTE | Matches V2 pattern |
| ALLOW_OVERLAPPING_EXECUTION | FALSE | Prevents concurrent runs |
| WHEN clause | None | Runs every interval regardless |
| Auto-resume | Yes (`ALTER TASK ... RESUME`) | Immediately active |

---

## 10. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 100% coverage across all MERGE branches |
| MERGE Logic | 10 | 10 | 4 branches + recovery MERGE, all correct |
| Filter Coverage | 10 | 10 | N/A — no purge filter needed for EHMS |
| Object Naming | 10 | 10 | Consistent `TASK_SP_PROCESS_` pattern, prefix cleanup done |
| Error Handling | 8.5 | 10 | Good staleness detection + dual global handlers; lacks RAISE to fail task |
| Logging/Audit | 9 | 10 | Centralized CDC_EXECUTION_LOG with VARIANT metrics; no separate error log |
| Transaction Control | 6 | 10 | No explicit BEGIN/COMMIT/ROLLBACK around MERGE |
| Data Validation | 5 | 10 | Duplicate detection tracked but no NULL PK validation |
| Code Standards | 10 | 10 | Clean comments, consistent formatting, inline table COMMENT |
| Production Readiness | 9 | 10 | Self-healing, monitoring, auto-create log table |
| **TOTAL** | **87.5** | **100** | |

---

## 11. Issues & Recommendations

### 11.1 Issues Found

| # | Severity | Issue | Location |
|---|----------|-------|----------|
| 1 | Medium | No `RAISE` after error logging — task always reports success | Lines 613, 638 |
| 2 | Medium | No explicit transaction control (`BEGIN TRANSACTION`/`COMMIT`/`ROLLBACK`) around MERGE | Lines 395-567 |
| 3 | Low | No NULL primary key validation on staging data | Post-staging, pre-MERGE |
| 4 | Low | `BATCH_ID` uses `YYYYMMDD_HH24MISS` — may collide if two manual calls within same second | Line 139 |
| 5 | Info | Recovery MERGE CDC_OPERATION is 'RELOADED'; Main duplicate is 'DUPLICATE_RESOLVED' — good distinction | — |

### 11.2 Recommendations for V3

| # | Enhancement | Priority | Effort |
|---|-------------|----------|--------|
| 1 | Add `RAISE` at end of exception handlers to fail the task on error | High | Low |
| 2 | Wrap MERGE operations in explicit `BEGIN TRANSACTION` / `COMMIT` with `ROLLBACK` in handlers | High | Low |
| 3 | Add NULL PK validation (`WHERE DTCTD_EQPMNT_ID IS NULL`) on staging table | Medium | Low |
| 4 | Add separate `SP_ERROR_LOG` table for detailed error tracking (match V2 TRAIN_OPTRN_EVENT pattern) | Medium | Low |
| 5 | Use `UUID_STRING()` or millisecond-precision for batch ID uniqueness | Low | Low |
| 6 | Add `WHEN SYSTEM$STREAM_HAS_DATA(...)` clause to task to skip empty runs | Low | Low |
| 7 | Add email/Slack alerting via external function on error | Low | Medium |

---

## 12. Deployment Checklist

- [ ] Ensure `D_BRONZE.EHMS` schema exists
- [ ] Ensure `D_BRONZE.MONITORING` schema exists
- [ ] Run STEP 1: Create target table
- [ ] Run STEP 2: Enable change tracking on source
- [ ] Run STEP 3: Create stream
- [ ] Run STEP 4: Deploy stored procedure
- [ ] Verify procedure compiles: `CALL D_RAW.EHMS.SP_PROCESS_DTQ_DTCTD_EQPMNT()`
- [ ] Verify execution logged to `D_BRONZE.MONITORING.CDC_EXECUTION_LOG`
- [ ] Run STEP 5: Create and resume task
- [ ] Monitor first few automated executions
- [ ] Verify row counts in `D_BRONZE.EHMS.DTQ_DTCTD_EQPMNT`

---

## 13. Verification Queries

```sql
-- Check target table
SHOW TABLES LIKE 'DTQ_DTCTD_EQPMNT%' IN SCHEMA D_BRONZE.EHMS;

-- Check stream
SHOW STREAMS LIKE 'DTQ_DTCTD_EQPMNT%' IN SCHEMA D_RAW.EHMS;

-- Check task status
SHOW TASKS LIKE 'TASK_SP_PROCESS_DTQ_DTCTD_EQPMNT%' IN SCHEMA D_RAW.EHMS;

-- Manual execution test
CALL D_RAW.EHMS.SP_PROCESS_DTQ_DTCTD_EQPMNT();

-- Review execution log
SELECT * FROM D_BRONZE.MONITORING.CDC_EXECUTION_LOG 
WHERE TABLE_NAME = 'DTQ_DTCTD_EQPMNT' 
ORDER BY CREATED_AT DESC LIMIT 20;

-- Check for errors in last 24 hours
SELECT * FROM D_BRONZE.MONITORING.CDC_EXECUTION_LOG 
WHERE TABLE_NAME = 'DTQ_DTCTD_EQPMNT' 
  AND EXECUTION_STATUS NOT IN ('SUCCESS', 'NO_DATA')
  AND CREATED_AT > DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY CREATED_AT DESC;

-- Row count and CDC operation breakdown
SELECT CDC_OPERATION, IS_DELETED, COUNT(*) AS CNT
FROM D_BRONZE.EHMS.DTQ_DTCTD_EQPMNT
GROUP BY CDC_OPERATION, IS_DELETED
ORDER BY CNT DESC;
```

---

## 14. Execution Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│              SP_PROCESS_DTQ_DTCTD_EQPMNT()                      │
│              Batch ID: BATCH_YYYYMMDD_HH24MISS                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ ENSURE: CDC_EXECUTION_LOG exists (CREATE IF NOT EXISTS)         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ CHECK 1: Stream Staleness Detection                             │
│ Method: SELECT COUNT(*) WHERE 1=0                               │
│ Exception: STATEMENT_ERROR → check for stale/invalid keywords   │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌─────────────────────────┐     ┌─────────────────────────────────┐
│ STREAM STALE = TRUE     │     │ STREAM STALE = FALSE            │
│                         │     │                                 │
│ 1. Validate base table  │     │ 1. Stage stream → temp table    │
│ 2. Recreate stream      │     │ 2. Check staging count          │
│ 3. Stage into temp      │     │    → 0 rows: Log NO_DATA, exit │
│ 4. Recovery MERGE       │     │ 3. Pre-merge metrics            │
│ 5. Log RECOVERY         │     │ 4. Duplicate detection count    │
│ 6. Return               │     │ 5. Main MERGE (4 branches)     │
└─────────────────────────┘     │ 6. Log SUCCESS                 │
                                │ 7. Return                       │
                                └─────────────────────────────────┘
                                              │
              ┌───────────────────────────────┘
              ▼
┌─────────────────────────────────────────────────────────────────┐
│ GLOBAL EXCEPTION HANDLERS                                       │
│ STATEMENT_ERROR → Cleanup → Log SQL_ERROR → Return error        │
│ WHEN OTHER      → Cleanup → Log UNKNOWN_ERROR → Return error    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 15. Verdict

**✅ APPROVED FOR PRODUCTION** — Script implements a solid CDC data preservation pipeline with:

- ✅ Complete 55-column mapping across all MERGE branches
- ✅ Self-healing stale stream recovery with base table validation
- ✅ Duplicate INSERT resolution for out-of-order CDC events
- ✅ Centralized execution logging with extensible VARIANT metrics
- ✅ Dual-level exception handling (STATEMENT_ERROR + WHEN OTHER)
- ✅ Auto-creation of monitoring table (zero deployment dependency)
- ⚠️ Consider adding explicit transactions and RAISE for V3

**Production Readiness Score: 8.75/10**
