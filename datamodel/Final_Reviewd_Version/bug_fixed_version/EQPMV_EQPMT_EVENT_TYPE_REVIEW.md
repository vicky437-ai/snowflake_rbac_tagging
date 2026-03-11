# Code Review: EQPMV_EQPMT_EVENT_TYPE CDC Data Preservation Script
**Review Date:** March 11, 2026 (Rev 2 - Bug Fix updates)  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/EQPMV_EQPMT_EVENT_TYPE.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE` (25 columns) |
| Target Table | `D_BRONZE.SADB.EQPMV_EQPMT_EVENT_TYPE` (25 source + 6 CDC = 31 columns) |
| Stream | `D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_EQPMV_EQPMT_EVENT_TYPE()` |
| Task | `D_RAW.SADB.TASK_PROCESS_EQPMV_EQPMT_EVENT_TYPE` (5 min, no WHEN clause) |
| Primary Key | `EQPMT_EVENT_TYPE_ID` (single) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Previous Version (Rev 1 dated 2026-02-24)

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 2 | New column | N/A (24 source cols) | `SNW_OPERATION_OWNER VARCHAR(256)` added (25 source cols) | DONE |
| 3 | Filter | None | `NOT IN ('TSDPRG', 'EMEPRG')` at all entry points | DONE |
| 4 | Recovery MERGE | Base table LEFT JOIN | Stream read with `SHOW_INITIAL_ROWS` | DONE |
| 5 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional for stale recovery) | DONE |
| 6 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |
| 7 | Column count | 24 source + 6 CDC = 30 | 25 source + 6 CDC = 31 | DONE |

---

## 3. Column Mapping (Source -> Target) -- 25/25 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | EQPMT_EVENT_TYPE_ID | NUMBER(18,0) NOT NULL | YES (PK) |
| 2 | TRNII_EVENT_CD | VARCHAR(16) | YES |
| 3 | YARDS_EVENT_CD | VARCHAR(12) | YES |
| 4 | IMDL_EVENT_CD | VARCHAR(12) | YES |
| 5 | TRAIN_EVENT_CD | VARCHAR(16) | YES |
| 6 | MTP_CAR_EVENT_CD | VARCHAR(4) | YES |
| 7 | FSTWY_EVENT_CD | VARCHAR(12) | YES |
| 8 | BAD_ORDER_EVENT_CD | VARCHAR(12) | YES |
| 9 | SMS_EVENT_CD | VARCHAR(12) | YES |
| 10 | AEI_EVENT_CD | VARCHAR(32) | YES |
| 11 | DSCRPT_TEXT | VARCHAR(320) | YES |
| 12 | CREATE_USER_ID | VARCHAR(32) | YES |
| 13 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 14 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 15 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 16 | LMS_EVENT_CD | VARCHAR(16) | YES |
| 17 | EVENT_ACTIVE_IND | VARCHAR(4) | YES |
| 18 | EVENT_TYPE_CD | VARCHAR(40) | YES |
| 19 | AAR_STNDRD_EVENT_CD | VARCHAR(16) | YES |
| 20 | EDI_EVENT_CD | VARCHAR(8) | YES |
| 21 | EDI_EVENT_CD_QLFR | VARCHAR(48) | YES |
| 22 | YARD_TRACK_MVMNT_EVENT_CD | VARCHAR(32) | YES |
| 23 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 24 | SNW_OPERATION_OWNER | VARCHAR(256) | YES (NEW) |
| 25 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 23/23 | YES | 5/5 | PASS |
| Recovery INSERT | 31 cols = 31 vals | YES | 6/6 | PASS |
| Main UPDATE | 23/23 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 23/23 | YES | 5/5 | PASS |
| Main NEW INSERT | 31 cols = 31 vals | YES | 6/6 | PASS |

---

## 5. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact:** 2,077 NULL rows included. 0 TSDPRG/EMEPRG currently -- filter is proactive/preventive.

---

## 6. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_EQPMV_EQPMT_EVENT_TYPE()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 7. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 25/25 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **59** | **60** | **98%** |

---

## 8. Suggestions

1. **Info:** No TSDPRG/EMEPRG rows currently in source (2,077 rows all NULL owner) -- filter is preventive, good practice.

---

## 9. Verdict

**APPROVED FOR PRODUCTION** -- 100% column mapping (25/25 source + 6/6 CDC = 31 total), all MERGE branches validated, dual-value filter correctly applied with NULL-safe NVL, stream-based recovery implemented, SP compiled successfully.
