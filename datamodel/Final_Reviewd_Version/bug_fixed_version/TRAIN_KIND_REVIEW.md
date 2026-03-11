# Code Review: TRAIN_KIND CDC Data Preservation Script
**Review Date:** March 11, 2026 (Rev 2 - Bug Fix updates)  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/TRAIN_KIND.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.TRAIN_KIND_BASE` (11 columns) |
| Target Table | `D_BRONZE.SADB.TRAIN_KIND` (11 source + 6 CDC = 17 columns) |
| Stream | `D_RAW.SADB.TRAIN_KIND_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_TRAIN_KIND()` |
| Task | `D_RAW.SADB.TASK_PROCESS_TRAIN_KIND` (5 min, no WHEN clause) |
| Primary Key | `TRAIN_KIND_CD` (single, VARCHAR) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Previous Version (Rev 1 dated 2026-03-06)

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 2 | DDL column order | `SNW_OPERATION_OWNER` between SNW_OPERATION_TYPE and SNW_LAST_REPLICATED | Moved to end (after CDC cols) for CREATE OR ALTER compatibility | DONE |
| 3 | Filter | None | `NOT IN ('TSDPRG', 'EMEPRG')` at all entry points | DONE |
| 4 | Recovery MERGE | Base table LEFT JOIN | Stream read with `SHOW_INITIAL_ROWS` | DONE |
| 5 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional for stale recovery) | DONE |
| 6 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |

**Note:** SNW_OPERATION_OWNER was already mapped in all SP MERGE branches from Rev 1.

---

## 3. Column Mapping (Source -> Target) -- 11/11 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | TRAIN_KIND_CD | VARCHAR(16) NOT NULL | YES (PK) |
| 2 | DSCRPT_TXT | VARCHAR(400) | YES |
| 3 | EXPIRY_DT | TIMESTAMP_NTZ(0) | YES |
| 4 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 5 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 6 | CREATE_USER_ID | VARCHAR(32) | YES |
| 7 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 8 | TRAIN_KIND_ROLLUP_ID | NUMBER(18,0) | YES |
| 9 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 10 | SNW_OPERATION_OWNER | VARCHAR(256) | YES |
| 11 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 9/9 | YES | 5/5 | PASS |
| Recovery INSERT | 17 cols = 17 vals | YES | 6/6 | PASS |
| Main UPDATE | 9/9 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 9/9 | YES | 5/5 | PASS |
| Main NEW INSERT | 17 cols = 17 vals | YES | 6/6 | PASS |

---

## 5. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact:** 72 NULL rows included. 0 TSDPRG/EMEPRG -- filter is preventive.

---

## 6. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_TRAIN_KIND()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 7. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 11/11 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **59** | **60** | **98%** |

---

## 8. Suggestions

1. **Info:** No TSDPRG/EMEPRG rows in source (72 rows, all NULL owner) -- filter is preventive.

---

## 9. Verdict

**APPROVED FOR PRODUCTION** -- 100% column mapping (11/11 source + 6/6 CDC = 17 total), all MERGE branches validated, dual-value filter correctly applied, stream-based recovery implemented, SP compiled successfully.
