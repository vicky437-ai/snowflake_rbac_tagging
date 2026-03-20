# Consolidated Code Review: AWS CDC Data Preservation Scripts (v2.1)
**Review Date:** March 20, 2026  
**Reviewer:** Cortex Code  
**Folder:** Scripts/AWS/  
**Total Scripts:** 2 SQL files  
**Reference Pattern:** Scripts/v2_production_ready/TRAIN_OPTRN_EVENT_v2.sql  
**Version:** v2.1 — Inline COMMENT + TASK_SP_PROCESS_ naming + Execution logging  
**Source System:** AWS PostgreSQL (not Oracle SADB)  
**Verdict:** **BOTH SCRIPTS APPROVED FOR PRODUCTION**

---

## 1. Inventory

| # | Script | Target Table | PK | Src Cols | Tgt Cols | Task (v2.1) |
|---|--------|-------------|-----|----------|----------|-------------|
| 1 | EVENTS_LOCOMOTIVE_MVMNT_EVENT.sql | `EVENTS_LOCOMOTIVE_MVMNT_EVENT` | Composite (2) | 25 | 31 | `TASK_SP_PROCESS_EVENTS_LOCOMOTIVE_MVMNT_EVENT` |
| 2 | EVENTS_TRAIN_MVMNT_EVENT.sql | `EVENTS_TRAIN_MVMNT_EVENT` | Composite (2) | 27 | 33 | `TASK_SP_PROCESS_EVENTS_TRAIN_MVMNT_EVENT` |

**Total: 52 source columns mapped. 2/2 scripts at v2.1.**

---

## 2. v2.1 Changes Validation

| # | Change | LOCOMOTIVE | TRAIN | Both PASS? |
|---|--------|:-:|:-:|:-:|
| 1 | Inline COMMENT (AWS PostgreSQL source) | PASS | PASS | PASS |
| 2 | TASK_SP_PROCESS_ naming | PASS | PASS | PASS |
| 3 | Execution logging — RECOVERY path | PASS | PASS | PASS |
| 4 | Execution logging — NO_DATA path | PASS | PASS | PASS |
| 5 | Execution logging — SUCCESS path | PASS | PASS | PASS |
| 6 | Execution logging — ERROR path | PASS | PASS | PASS |
| 7 | SP variables (12/12) | PASS | PASS | PASS |
| 8 | Pre-merge I/U/D metrics | PASS | PASS | PASS |
| 9 | VERSION header v2.1 | PASS | PASS | PASS |

**Result: 18/18 checks PASSED.**

---

## 3. Column Mapping — 52/52 = 100%

| # | Script | Src | CREATE TABLE | Staging | Recovery MERGE | Main MERGE | Result |
|---|--------|-----|-------------|---------|---------------|------------|--------|
| 1 | LOCOMOTIVE | 25 | 31 | 25 | 23 UPD + 31 INS | 23 UPD + 31 INS | **100%** |
| 2 | TRAIN | 27 | 33 | 27 | 25 UPD + 33 INS | 25 UPD + 33 INS | **100%** |

### Shared Columns (23 common)

Both scripts share 23 identical business columns. TRAIN has 2 additional: `"train_id"` (NUMBER(38,0)) and `"train_symbol"` (VARCHAR(80)).

### Composite Primary Key (identical in both)

`"event_id"` (NUMBER(38,0)) + `"event_timestamp_utc"` (TIMESTAMP_NTZ(6)) — verified in Recovery and Main MERGE ON clauses for both scripts.

---

## 4. MERGE Branch Validation (6 × 2 = 12 total)

| Branch | LOCOMOTIVE | TRAIN | Both? |
|--------|:-:|:-:|:-:|
| Recovery UPDATE (RELOADED) | PASS | PASS | PASS |
| Recovery INSERT | PASS | PASS | PASS |
| Main UPDATE | PASS | PASS | PASS |
| Main DELETE (soft-delete) | PASS | PASS | PASS |
| Main RE-INSERT | PASS | PASS | PASS |
| Main NEW INSERT | PASS | PASS | PASS |

**12/12 MERGE branches validated.**

---

## 5. AWS-Specific Pattern Differences

| Aspect | SADB Pattern | AWS Pattern |
|--------|-------------|-------------|
| Schema | D_RAW.SADB / D_BRONZE.SADB | D_RAW.AWS / D_BRONZE.AWS |
| Source | Oracle SADB via IDMC | AWS PostgreSQL via CDC |
| Column Casing | UPPERCASE, no quotes | **lowercase, double-quoted** |
| SNW Columns | 3 (TYPE, OWNER, REPLICATED) | **2** (TYPE, REPLICATED) |
| Purge Filter | `NVL(SNW_OPERATION_OWNER,'') NOT IN (...)` | **NONE** |
| Execution Logging (v2.1) | Same | Same |
| Task Naming (v2.1) | TASK_SP_PROCESS_ | TASK_SP_PROCESS_ |
| Inline COMMENT (v2.1) | Oracle SADB source | **AWS PostgreSQL source** |

---

## 6. Execution Logging Paths (4 × 2 = 8 total)

| Path | Status | LOCOMOTIVE | TRAIN | Both? |
|------|--------|:-:|:-:|:-:|
| Recovery | `RECOVERY` | PASS | PASS | PASS |
| No data | `NO_DATA` | PASS | PASS | PASS |
| Success | `SUCCESS` | PASS | PASS | PASS |
| Error | `ERROR` | PASS | PASS | PASS |

**8/8 logging paths validated.**

---

## 7. Coding Standards

| Standard | Both? |
|----------|:-:|
| 5-Step structure | PASS |
| Fully qualified names (D_RAW.AWS / D_BRONZE.AWS) | PASS |
| `CREATE OR ALTER TABLE` (idempotent) | PASS |
| Inline COMMENT (v2.1) | PASS |
| `TASK_SP_PROCESS_` naming (v2.1) | PASS |
| Execution logging in 4 paths (v2.1) | PASS |
| Pre-merge I/U/D metrics (v2.1) | PASS |
| 12 SP variables (v2.1) | PASS |
| `INFA_INGEST_WH` / `5 MINUTE` / no overlap | PASS |
| Retention 45 days / extension 15 days | PASS |
| Case-sensitive quoting (all lowercase cols) | PASS |
| Verification queries at bottom | PASS |

---

## 8. Scoring

| Category | Max | LOCOMOTIVE | TRAIN |
|----------|:---:|:-:|:-:|
| Column Mapping | 10 | 10 | 10 |
| MERGE Logic | 10 | 10 | 10 |
| No Filter (AWS) | 10 | 10 | 10 |
| Schema (AWS) | 10 | 10 | 10 |
| Task Naming (v2.1) | 10 | 10 | 10 |
| Table COMMENT (v2.1) | 10 | 10 | 10 |
| Error Handling (v2.1) | 10 | 10 | 10 |
| Execution Logging (v2.1) | 10 | 10 | 10 |
| Pre-Merge Metrics (v2.1) | 10 | 10 | 10 |
| Data Type Accuracy | 10 | 10 | 10 |
| Case-Sensitive Handling | 10 | 10 | 10 |
| **TOTAL** | **110** | **110** | **110** |

**Aggregate: 220/220 = 100%**

---

## 9. Post-Deployment Validation Query

```sql
WITH counts AS (
    SELECT 'EVENTS_LOCOMOTIVE_MVMNT_EVENT' AS TBL,
           (SELECT COUNT(*) FROM D_RAW.AWS.EVENTS_LOCOMOTIVE_MVMNT_EVENT_BASE) AS SOURCE,
           (SELECT COUNT(*) FROM D_RAW.AWS.EVENTS_LOCOMOTIVE_MVMNT_EVENT_BASE_HIST_STREAM) AS STREAM,
           (SELECT COUNT(*) FROM D_BRONZE.AWS.EVENTS_LOCOMOTIVE_MVMNT_EVENT) AS TARGET
    UNION ALL SELECT 'EVENTS_TRAIN_MVMNT_EVENT',
           (SELECT COUNT(*) FROM D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE),
           (SELECT COUNT(*) FROM D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE_HIST_STREAM),
           (SELECT COUNT(*) FROM D_BRONZE.AWS.EVENTS_TRAIN_MVMNT_EVENT)
)
SELECT TBL, SOURCE, STREAM, TARGET,
       (STREAM + TARGET) AS STREAM_PLUS_TARGET,
       CASE WHEN SOURCE = (STREAM + TARGET) THEN 'PASS' ELSE 'FAIL' END AS VALIDATION
FROM counts
ORDER BY TBL;
```

---

## 10. Monitoring Query

```sql
SELECT TABLE_NAME, EXECUTION_STATUS, COUNT(*) AS EXECUTIONS,
       SUM(ROWS_PROCESSED) AS TOTAL_ROWS,
       MIN(START_TIME) AS FIRST_RUN, MAX(END_TIME) AS LAST_RUN
FROM D_BRONZE.MONITORING.CDC_EXECUTION_LOG
WHERE TABLE_NAME LIKE 'EVENTS_%_MVMNT_EVENT'
  AND CREATED_AT >= DATEADD('DAY', -1, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1, 2;
```

---

## 11. Final Verdict

### **BOTH AWS SCRIPTS APPROVED FOR PRODUCTION (v2.1)**

| Metric | Value |
|--------|-------|
| Scripts | 2 |
| Source columns mapped | 52/52 (100%) |
| MERGE branches validated | 12/12 (100%) |
| Execution logging paths | 8/8 (100%) |
| v2.1 changes applied | 18/18 (100%) |
| Aggregate score | 220/220 (100%) |
| Source system | AWS PostgreSQL |
| Schema | D_RAW.AWS → D_BRONZE.AWS |
| Purge filter | NONE (AWS schema) |
| Column casing | lowercase, double-quoted |
