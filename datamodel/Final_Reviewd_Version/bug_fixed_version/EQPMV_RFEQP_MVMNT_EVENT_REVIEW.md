# Code Review: EQPMV_RFEQP_MVMNT_EVENT CDC Data Preservation Script
**Review Date:** March 11, 2026 (Rev 2 - Bug Fix updates)  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/EQPMV_RFEQP_MVMNT_EVENT.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.EQPMV_RFEQP_MVMNT_EVENT_BASE` (91 columns) |
| Target Table | `D_BRONZE.SADB.EQPMV_RFEQP_MVMNT_EVENT` (91 source + 6 CDC = 97 columns) |
| Stream | `D_RAW.SADB.EQPMV_RFEQP_MVMNT_EVENT_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_EQPMV_RFEQP_MVMNT_EVENT()` |
| Task | `D_RAW.SADB.TASK_PROCESS_EQPMV_RFEQP_MVMNT_EVENT` (5 min, no WHEN clause) |
| Primary Key | `EVENT_ID` (single) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Previous Version (Rev 1 dated 2026-02-24)

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 2 | New column | N/A (90 source) | `SNW_OPERATION_OWNER VARCHAR(256)` added (91 source) | DONE |
| 3 | Filter | None | `NOT IN ('TSDPRG', 'EMEPRG')` at all entry points | DONE |
| 4 | Recovery MERGE | Base table LEFT JOIN | Stream read with `SHOW_INITIAL_ROWS` | DONE |
| 5 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional for stale recovery) | DONE |
| 6 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |
| 7 | Column count | 90 source + 6 CDC = 96 | 91 source + 6 CDC = 97 | DONE |

---

## 3. Column Mapping (Source -> Target) -- 91/91 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | EVENT_ID | NUMBER(18,0) NOT NULL | YES (PK) |
| 2-88 | EQPMT_EVENT_TYPE_ID through EXCTN_RPRTD_BLOCK_NM | Various | YES (all 87) |
| 89 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 90 | SNW_OPERATION_OWNER | VARCHAR(256) | YES (NEW) |
| 91 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 89/89 | YES | 5/5 | PASS |
| Recovery INSERT | 97 cols = 97 vals | YES | 6/6 | PASS |
| Main UPDATE | 89/89 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 89/89 | YES | 5/5 | PASS |
| Main NEW INSERT | 97 cols = 97 vals | YES | 6/6 | PASS |

---

## 5. Filter Validation -- HIGH IMPACT

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

### Data Impact (SIGNIFICANT)

| Owner | Row Count | % of Total | Action |
|-------|-----------|------------|--------|
| NULL | 96,143,366 | 89.86% | INCLUDED |
| **EMEPRG** | **10,738,364** | **10.03%** | **EXCLUDED** |
| EMEMGR | 110,460 | 0.10% | INCLUDED |
| CSDMGR | 35 | 0.00% | INCLUDED |
| TSDPRG | 0 | 0.00% | (would be excluded) |
| **Total excluded** | **10,738,364** | **10.03%** | |

**This is the first table where EMEPRG filter has significant real impact -- 10.7M rows (10%) will be excluded.**

---

## 6. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_EQPMV_RFEQP_MVMNT_EVENT()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 7. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 91/91 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **59** | **60** | **98%** |

---

## 8. Suggestions

1. **IMPORTANT:** 10,738,364 EMEPRG rows (10.03% of data) will be actively excluded. Verify this is the intended behavior before deploying.
2. **Info:** 110,460 EMEMGR + 35 CSDMGR rows exist but are correctly NOT in the filter list.

---

## 9. Verdict

**APPROVED FOR PRODUCTION** -- 100% column mapping (91/91 source + 6/6 CDC = 97 total), all MERGE branches validated, dual-value filter correctly applied (10.7M EMEPRG rows excluded), stream-based recovery implemented, SP compiled successfully.
