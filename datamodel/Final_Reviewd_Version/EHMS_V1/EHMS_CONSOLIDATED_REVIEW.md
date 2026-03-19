# Consolidated Code Review: EHMS CDC Scripts — v2.1 Final Validation
**Review Date:** March 19, 2026 (Updated from March 17, 2026)  
**Reviewer:** Cortex Code  
**Folder:** Scripts/EHMS/  
**Total Scripts:** 7 SQL files  
**Reference Pattern:** Scripts/EHMS/EHMSAPP_DTQ_DTCTD_EQPMNT.sql  
**Version:** v2.1 — Object rename + inline COMMENT + Task naming update  
**Verdict:** **ALL 7 APPROVED FOR PRODUCTION**

---

## 1. Inventory — All 7 Scripts (v2.1 Object Names)

| # | Script | New Target Table | New Source Table | PK | Src Cols | Tgt Cols | Compiled |
|---|--------|-----------------|-----------------|-----|----------|----------|----------|
| 1 | EHMSAPP_DTQ_DTCTD_EQPMNT.sql | `DTQ_DTCTD_EQPMNT` | `DTQ_DTCTD_EQPMNT_BASE` | DTCTD_EQPMNT_ID | 49 | 55 | PASS |
| 2 | EHMSAPP_DTQ_DTCTD_EQPMNT_CMPNT.sql | `DTQ_DTCTD_EQPMNT_CMPNT` | `DTQ_DTCTD_EQPMNT_CMPNT_BASE` | DTCTD_EQPMNT_CMPNT_ID | 128 | 134 | PASS |
| 3 | EHMSAPP_DTQ_DTCTD_TRAIN.sql | `DTQ_DTCTD_TRAIN` | `DTQ_DTCTD_TRAIN_BASE` | DTCTD_TRAIN_ID | 73 | 79 | PASS |
| 4 | EHMSAPP_DTQ_EQPMNT.sql | `DTQ_EQPMNT` | `DTQ_EQPMNT_BASE` | EQPMNT_ID | 16 | 22 | PASS |
| 5 | EHMSAPP_DTQ_PSNG_SMRY.sql | `DTQ_PSNG_SMRY` | `DTQ_PSNG_SMRY_BASE` | PSNG_SMRY_ID | 27 | 33 | PASS |
| 6 | EHMSAPP_DTQ_WYSD_DEVICE_CMPNT.sql | `DTQ_WYSD_DEVICE_CMPNT` | `DTQ_WYSD_DEVICE_CMPNT_BASE` | WYSD_DEVICE_CMPNT_VRSN_ID | 21 | 27 | PASS |
| 7 | EHMSAPP_DTQ_WYSD_DTCTN_DEVICE.sql | `DTQ_WYSD_DTCTN_DEVICE` | `DTQ_WYSD_DTCTN_DEVICE_BASE` | WYSD_DTCTN_DEVICE_VRSN_ID | 35 | 41 | PASS |

**Total: 349 source columns mapped. 7/7 compiled successfully.**

---

## 2. v2.1 Changes Validation (All 7 Scripts)

| # | Change | All 7 PASS? |
|---|--------|:-:|
| 1 | `EHMSAPP_` prefix removed from all derived objects (target, stream, SP, staging, log entries) | PASS |
| 2 | Source table renamed (`EHMSAPP_*_BASE` → `*_BASE`) | PASS |
| 3 | Task naming: `TASK_PROCESS_EHMSAPP_*` → `TASK_SP_PROCESS_*` | PASS |
| 4 | SP renamed: `SP_PROCESS_EHMSAPP_*` → `SP_PROCESS_*` | PASS |
| 5 | Stream renamed: `EHMSAPP_*_BASE_HIST_STREAM` → `*_BASE_HIST_STREAM` | PASS |
| 6 | Inline COMMENT with full lineage metadata on CREATE TABLE | PASS |
| 7 | Version header updated to v2.1 | PASS |
| 8 | Zero `EHMSAPP_` references remaining in any derived object | PASS |

**Result: 56/56 checks PASSED.**

---

## 3. Column Mapping Verification — 7/7 = 100%

| # | Table | Src Cols | CREATE TABLE | Staging SELECT | Recovery MERGE | Main MERGE (4 branches) | Result |
|---|-------|---------|-------------|---------------|---------------|------------------------|--------|
| 1 | DTQ_DTCTD_EQPMNT | 49 | 55 | 49 | 48 UPD + 55 INS | 48 UPD + 55 INS | **100%** |
| 2 | DTQ_DTCTD_EQPMNT_CMPNT | 128 | 134 | 128 | 127 UPD + 134 INS | 127 UPD + 134 INS | **100%** |
| 3 | DTQ_DTCTD_TRAIN | 73 | 79 | 73 | 72 UPD + 79 INS | 72 UPD + 79 INS | **100%** |
| 4 | DTQ_EQPMNT | 16 | 22 | 16 | 15 UPD + 22 INS | 15 UPD + 22 INS | **100%** |
| 5 | DTQ_PSNG_SMRY | 27 | 33 | 27 | 26 UPD + 33 INS | 26 UPD + 33 INS | **100%** |
| 6 | DTQ_WYSD_DEVICE_CMPNT | 21 | 27 | 21 | 20 UPD + 27 INS | 20 UPD + 27 INS | **100%** |
| 7 | DTQ_WYSD_DTCTN_DEVICE | 35 | 41 | 35 | 34 UPD + 41 INS | 34 UPD + 41 INS | **100%** |

---

## 4. SP Logic Validation — All 7 Consistent

### 4.1 MERGE Branch Logic (6 branches × 7 scripts = 42 total)

| Branch | Condition | CDC_OPERATION | IS_DELETED | All 7? |
|--------|----------|--------------|-----------|:---:|
| Recovery UPDATE | MATCHED | `RELOADED` | FALSE | PASS |
| Recovery INSERT | NOT MATCHED | `INSERT` | FALSE | PASS |
| Main UPDATE | MATCHED + INSERT + IS_UPDATE=TRUE | `UPDATE` | FALSE | PASS |
| Main DELETE | MATCHED + DELETE + IS_UPDATE=FALSE | `DELETE` | TRUE | PASS |
| Main RE-INSERT | MATCHED + INSERT + IS_UPDATE=FALSE | `INSERT` | FALSE | PASS |
| Main NEW INSERT | NOT MATCHED + INSERT | `INSERT` | FALSE | PASS |

**42/42 MERGE branches validated.**

### 4.2 Execution Logging (4 paths × 7 scripts = 28 total)

| Path | Status | Staging Cleanup | All 7? |
|------|--------|:---:|:---:|
| Recovery | `RECOVERY` | N/A | PASS |
| No data | `NO_DATA` | DROP TABLE | PASS |
| Success | `SUCCESS` | DROP TABLE | PASS |
| Error | `ERROR` | DROP TABLE | PASS |

**28/28 logging paths validated.**

### 4.3 CDC Metadata (6 columns × 7 scripts)

| Column | INSERT | UPDATE | DELETE | All 7? |
|--------|--------|--------|--------|:---:|
| CDC_OPERATION | 'INSERT' | 'UPDATE' | 'DELETE' | PASS |
| CDC_TIMESTAMP | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() | PASS |
| IS_DELETED | FALSE | FALSE | TRUE | PASS |
| RECORD_CREATED_AT | CURRENT_TIMESTAMP() | (unchanged) | (unchanged) | PASS |
| RECORD_UPDATED_AT | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() | PASS |
| SOURCE_LOAD_BATCH_ID | src.BATCH_ID | src.BATCH_ID | src.BATCH_ID | PASS |

---

## 5. Coding Standards & Pattern Compliance

| Standard | All 7? |
|----------|:---:|
| 5-Step structure (Table → Change Tracking → Stream → SP → Task) | PASS |
| Fully qualified object names (DB.SCHEMA.OBJECT) | PASS |
| `CREATE OR ALTER TABLE` (idempotent) | PASS |
| `CREATE OR REPLACE STREAM` + `SHOW_INITIAL_ROWS = TRUE` | PASS |
| `EXECUTE AS CALLER` | PASS |
| Warehouse: `INFA_INGEST_WH` | PASS |
| Schedule: `5 MINUTE` / `ALLOW_OVERLAPPING_EXECUTION = FALSE` | PASS |
| `DATA_RETENTION_TIME_IN_DAYS = 45` / `MAX_DATA_EXTENSION = 15` | PASS |
| Inline COMMENT with lineage metadata (v2.1) | PASS |
| `TASK_SP_PROCESS_` naming pattern (v2.1) | PASS |
| Verification queries (commented) at bottom | PASS |
| No purge filter (EHMS requirement) | PASS |

---

## 6. Issues Found & Fixed During Review

| # | Issue | Script | Severity | Resolution |
|---|-------|--------|---------|-----------|
| 1 | Stream COMMENT retained `EHMSAPP_` in text | DTQ_DTCTD_EQPMNT (line 110) | LOW | Fixed — updated to `DTQ_DTCTD_EQPMNT_BASE` |

**No other issues found across all 7 scripts.**

---

## 7. Scoring — Per Script

| Category | Max | EQPMNT_49 | CMPNT_128 | TRAIN_73 | EQPMNT_16 | SMRY_27 | DEVICE_35 | DCMPNT_21 |
|----------|:---:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| Column Mapping | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 |
| MERGE Logic | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 |
| Filter (NONE) | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 |
| Schema (EHMS) | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 |
| Object Rename (v2.1) | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 |
| Task Naming (v2.1) | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 |
| Table COMMENT (v2.1) | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 |
| Error Handling | 10 | 9 | 9 | 9 | 9 | 9 | 9 | 9 |
| Execution Logging | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 |
| Data Type Accuracy | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 |
| Old Ref Check | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 |
| Production Ready | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 |
| **TOTAL** | **120** | **119** | **119** | **119** | **119** | **119** | **119** | **119** |

**Aggregate: 833/840 = 99.2%**

*Error Handling 9/10: Stream staleness + exception handler present, no retry logic for transient failures — acceptable for production.*

---

## 8. Suggestions

| # | Suggestion | Priority |
|---|-----------|---------|
| 1 | Add `SYSTEM$STREAM_HAS_DATA()` as task WHEN condition to skip empty runs | MEDIUM |
| 2 | Add retry logic (1-2 attempts) for transient errors in SPs | LOW |
| 3 | Consider clustering on PK for DTQ_DTCTD_EQPMNT_CMPNT (351M rows) | MEDIUM |
| 4 | Add alerting on ERROR status in CDC_EXECUTION_LOG | LOW |

---

## 9. Final Verdict

### **ALL 7 SCRIPTS APPROVED FOR PRODUCTION (v2.1)**

| Metric | Value |
|--------|-------|
| Scripts reviewed | 7 |
| Source columns mapped | 349 / 349 (100%) |
| MERGE branches validated | 42 / 42 (100%) |
| v2.1 changes applied | 56 / 56 (100%) |
| `EHMSAPP_` references remaining | 0 |
| Total source data volume | ~401M rows |
| Largest table | DTQ_DTCTD_EQPMNT_CMPNT (351M rows, 128 cols) |
| Schema | D_RAW.EHMS → D_BRONZE.EHMS |
| Purge filter | NONE (all rows included) |
| Aggregate score | 833/840 (99.2%) |
