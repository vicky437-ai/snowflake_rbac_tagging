# Code Review: TRAIN_CNST_DTL_RAIL_EQPT CDC Data Preservation Script
**Review Date:** March 11, 2026 (Rev 2 - Bug Fix updates)  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/TRAIN_CNST_DTL_RAIL_EQPT.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.TRAIN_CNST_DTL_RAIL_EQPT_BASE` (78 columns) |
| Target Table | `D_BRONZE.SADB.TRAIN_CNST_DTL_RAIL_EQPT` (78 source + 6 CDC = 84 columns) |
| Stream | `D_RAW.SADB.TRAIN_CNST_DTL_RAIL_EQPT_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_TRAIN_CNST_DTL_RAIL_EQPT()` |
| Task | `D_RAW.SADB.TASK_PROCESS_TRAIN_CNST_DTL_RAIL_EQPT` (5 min, no WHEN clause) |
| Primary Key | **3-COLUMN COMPOSITE** (TRAIN_CNST_SMRY_ID, TRAIN_CNST_SMRY_VRSN_NBR, SQNC_NBR) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Previous Version (Rev 1 dated 2026-02-24)

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 2 | New column | N/A (77 source) | `SNW_OPERATION_OWNER VARCHAR(256)` added (78 source) | DONE |
| 3 | Filter | None | `NOT IN ('TSDPRG', 'EMEPRG')` at all entry points | DONE |
| 4 | Recovery MERGE | Base table LEFT JOIN | Stream read with `SHOW_INITIAL_ROWS` | DONE |
| 5 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional for stale recovery) | DONE |
| 6 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |
| 7 | Column count | 77 source + 6 CDC = 83 | 78 source + 6 CDC = 84 | DONE |

---

## 3. Column Mapping (Source -> Target) -- 78/78 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | TRAIN_CNST_SMRY_ID | NUMBER(18,0) NOT NULL | YES (PK1) |
| 2 | TRAIN_CNST_SMRY_VRSN_NBR | NUMBER(4,0) NOT NULL | YES (PK2) |
| 3 | SQNC_NBR | NUMBER(4,0) NOT NULL | YES (PK3) |
| 4-75 | RECORD_CREATE_TMS through CNST_LCM_OPRSTS_ID | Various | YES (all 72) |
| 76 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 77 | SNW_OPERATION_OWNER | VARCHAR(256) | YES (NEW) |
| 78 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. 3-Column Composite PK Validation

| Check | Value | Result |
|-------|-------|--------|
| PK columns | TRAIN_CNST_SMRY_ID, TRAIN_CNST_SMRY_VRSN_NBR, SQNC_NBR | PASS |
| DDL PRIMARY KEY | All 3 columns | PASS |
| Recovery MERGE ON | All 3 PK columns | PASS |
| Main MERGE ON | All 3 PK columns | PASS |

---

## 5. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 74/74 | YES | 5/5 | PASS |
| Recovery INSERT | 84 cols = 84 vals | YES | 6/6 | PASS |
| Main UPDATE | 74/74 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 74/74 | YES | 5/5 | PASS |
| Main NEW INSERT | 84 cols = 84 vals | YES | 6/6 | PASS |

---

## 6. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact:** 15,865,279 NULL rows included, 2,552,752 TSDMGR rows included (not filtered), **3,610,524 TSDPRG rows excluded (16.4%)**. 0 EMEPRG (preventive).

---

## 7. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_TRAIN_CNST_DTL_RAIL_EQPT()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 8. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 78/78 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| 3-Column Composite PK | 10 | 10 | All 3 PK columns in all MERGE ON clauses |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **69** | **70** | **99%** |

---

## 9. Suggestions

1. **Info:** 3,610,524 TSDPRG rows (16.4%) will be actively filtered. 2,552,752 TSDMGR rows correctly NOT filtered.

---

## 10. Verdict

**APPROVED FOR PRODUCTION** -- 100% column mapping (78/78 source + 6/6 CDC = 84 total), 3-column composite PK correctly implemented in all MERGE ON clauses, all branches validated, dual-value filter correctly applied (3.6M TSDPRG rows excluded), stream-based recovery implemented, SP compiled successfully.
