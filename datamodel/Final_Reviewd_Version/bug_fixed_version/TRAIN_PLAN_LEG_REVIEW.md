# Code Review: TRAIN_PLAN_LEG CDC Data Preservation Script
**Review Date:** March 11, 2026 (Rev 2 - Bug Fix updates)  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/TRAIN_PLAN_LEG.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.TRAIN_PLAN_LEG_BASE` (17 columns) |
| Target Table | `D_BRONZE.SADB.TRAIN_PLAN_LEG` (17 source + 6 CDC = 23 columns) |
| Stream | `D_RAW.SADB.TRAIN_PLAN_LEG_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_LEG()` |
| Task | `D_RAW.SADB.TASK_PROCESS_TRAIN_PLAN_LEG` (5 min, no WHEN clause) |
| Primary Key | `TRAIN_PLAN_LEG_ID` (single) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Previous Version (Rev 1 dated 2026-02-24)

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 2 | New column | N/A (16 source) | `SNW_OPERATION_OWNER VARCHAR(256)` added (17 source) | DONE |
| 3 | Filter | None | `NOT IN ('TSDPRG', 'EMEPRG')` at all entry points | DONE |
| 4 | Recovery MERGE | Base table LEFT JOIN | Stream read with `SHOW_INITIAL_ROWS` | DONE |
| 5 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional for stale recovery) | DONE |
| 6 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |
| 7 | Column count | 16 source + 6 CDC = 22 | 17 source + 6 CDC = 23 | DONE |

---

## 3. Column Mapping (Source -> Target) -- 17/17 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | TRAIN_PLAN_LEG_ID | NUMBER(18,0) NOT NULL | YES (PK) |
| 2 | TRAIN_PLAN_ID | NUMBER(18,0) | YES |
| 3 | TRAIN_DRCTN_CD | VARCHAR(20) | YES |
| 4 | TRAIN_PLAN_LEG_NM | VARCHAR(32) | YES |
| 5 | MTP_TITAN_NBR | NUMBER(6,0) | YES |
| 6 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 7 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 8 | CREATE_USER_ID | VARCHAR(32) | YES |
| 9 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 10 | TURN_LEG_SQNC_NBR | NUMBER(1,0) | YES |
| 11 | TYES_TRAIN_ID | NUMBER(18,0) | YES |
| 12 | MTP_TOTAL_RTPNT_SENT_QTY | NUMBER(4,0) | YES |
| 13 | MTP_ROUTE_CMPLT_CD | VARCHAR(4) | YES |
| 14 | MTP_TRAIN_STATE_CD | VARCHAR(4) | YES |
| 15 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 16 | SNW_OPERATION_OWNER | VARCHAR(256) | YES (NEW) |
| 17 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 15/15 | YES | 5/5 | PASS |
| Recovery INSERT | 23 cols = 23 vals | YES | 6/6 | PASS |
| Main UPDATE | 15/15 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 15/15 | YES | 5/5 | PASS |
| Main NEW INSERT | 23 cols = 23 vals | YES | 6/6 | PASS |

---

## 5. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact:** 53,422 NULL rows included, **5,719 TSDPRG rows excluded**. 0 EMEPRG (preventive).

---

## 6. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_TRAIN_PLAN_LEG()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 7. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 17/17 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **59** | **60** | **98%** |

---

## 8. Suggestions

1. **Info:** 5,719 TSDPRG rows will be actively filtered. 0 EMEPRG (preventive).

---

## 9. Verdict

**APPROVED FOR PRODUCTION** -- 100% column mapping (17/17 source + 6/6 CDC = 23 total), all MERGE branches validated, dual-value filter correctly applied (5,719 TSDPRG rows excluded), stream-based recovery implemented, SP compiled successfully.
