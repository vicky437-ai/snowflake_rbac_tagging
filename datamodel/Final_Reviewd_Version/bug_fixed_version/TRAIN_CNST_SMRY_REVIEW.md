# Code Review: TRAIN_CNST_SMRY CDC Data Preservation Script
**Review Date:** March 11, 2026 (Rev 2 - Bug Fix updates)  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/TRAIN_CNST_SMRY.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.TRAIN_CNST_SMRY_BASE` (88 columns) |
| Target Table | `D_BRONZE.SADB.TRAIN_CNST_SMRY` (88 source + 6 CDC = 94 columns) |
| Stream | `D_RAW.SADB.TRAIN_CNST_SMRY_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_TRAIN_CNST_SMRY()` |
| Task | `D_RAW.SADB.TASK_PROCESS_TRAIN_CNST_SMRY` (5 min, no WHEN clause) |
| Primary Key | **COMPOSITE** (TRAIN_CNST_SMRY_ID, TRAIN_CNST_SMRY_VRSN_NBR) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Previous Version (Rev 1 dated 2026-02-24)

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 2 | New column | N/A (87 source) | `SNW_OPERATION_OWNER VARCHAR(256)` added (88 source) | DONE |
| 3 | Filter | None | `NOT IN ('TSDPRG', 'EMEPRG')` at all entry points | DONE |
| 4 | Recovery MERGE | Base table LEFT JOIN | Stream read with `SHOW_INITIAL_ROWS` | DONE |
| 5 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional for stale recovery) | DONE |
| 6 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |
| 7 | Column count | 87 source + 6 CDC = 93 | 88 source + 6 CDC = 94 | DONE |

---

## 3. Column Mapping (Source -> Target) -- 88/88 = 100%

88 source columns all mapped. Key columns:

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | TRAIN_CNST_SMRY_ID | NUMBER(18,0) NOT NULL | YES (PK1) |
| 2 | TRAIN_CNST_SMRY_VRSN_NBR | NUMBER(4,0) NOT NULL | YES (PK2) |
| 3-85 | RECORD_CREATE_TMS through CNST_RAIL_EQPMNT_QTY | Various | YES (all 83) |
| 86 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 87 | SNW_OPERATION_OWNER | VARCHAR(256) | YES (NEW) |
| 88 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. Composite PK Validation

| Check | Value | Result |
|-------|-------|--------|
| PK columns | TRAIN_CNST_SMRY_ID, TRAIN_CNST_SMRY_VRSN_NBR | PASS |
| DDL PRIMARY KEY | Both columns | PASS |
| Recovery MERGE ON | Both PK columns | PASS |
| Main MERGE ON | Both PK columns | PASS |

---

## 5. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 85/85 | YES | 5/5 | PASS |
| Recovery INSERT | 94 cols = 94 vals | YES | 6/6 | PASS |
| Main UPDATE | 85/85 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 85/85 | YES | 5/5 | PASS |
| Main NEW INSERT | 94 cols = 94 vals | YES | 6/6 | PASS |

---

## 6. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact:** 350,720 NULL rows included, 59 TSDMGR rows included (not filtered), **64,884 TSDPRG rows excluded (15.6%)**. 0 EMEPRG (preventive).

---

## 7. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_TRAIN_CNST_SMRY()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 8. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 88/88 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Composite PK | 10 | 10 | Both MERGE ON clauses use both PK columns |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **69** | **70** | **99%** |

---

## 9. Suggestions

1. **Info:** 64,884 TSDPRG rows (15.6% of data) will be actively filtered. 59 TSDMGR rows correctly NOT filtered.

---

## 10. Verdict

**APPROVED FOR PRODUCTION** -- 100% column mapping (88/88 source + 6/6 CDC = 94 total), composite PK correctly implemented in all MERGE ON clauses, all branches validated, dual-value filter correctly applied (64,884 TSDPRG rows excluded), stream-based recovery implemented, SP compiled successfully.
