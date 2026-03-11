# Code Review: STNWYB_MSG_DN CDC Data Preservation Script
**Review Date:** March 11, 2026 (Rev 2 - Bug Fix updates)  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/STNWYB_MSG_DN.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.STNWYB_MSG_DN_BASE` (131 columns) |
| Target Table | `D_BRONZE.SADB.STNWYB_MSG_DN` (131 source + 6 CDC = 137 columns) |
| Stream | `D_RAW.SADB.STNWYB_MSG_DN_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_STNWYB_MSG_DN()` |
| Task | `D_RAW.SADB.TASK_PROCESS_STNWYB_MSG_DN` (5 min, no WHEN clause) |
| Primary Key | `STNWYB_MSG_VRSN_ID` (single) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Previous Version (Rev 1 dated 2026-02-24)

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 2 | New column | N/A (130 source) | `SNW_OPERATION_OWNER VARCHAR(256)` added (131 source) | DONE |
| 3 | Filter | None | `NOT IN ('TSDPRG', 'EMEPRG')` at all entry points | DONE |
| 4 | Recovery MERGE | Base table LEFT JOIN | Stream read with `SHOW_INITIAL_ROWS` | DONE |
| 5 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional for stale recovery) | DONE |
| 6 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |
| 7 | Column count | 130 source + 6 CDC = 136 | 131 source + 6 CDC = 137 | DONE |

---

## 3. Column Mapping (Source -> Target) -- 131/131 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | STNWYB_MSG_VRSN_ID | NUMBER(18,0) NOT NULL | YES (PK) |
| 2 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 3 | CREATE_USER_ID | VARCHAR(32) | YES |
| 4 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 5 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 6 | CRNT_VRSN_CD | VARCHAR(4) | YES |
| 7 | SOURCE_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 8 | SRVASG_EQPMT_ASGNMN_ID | NUMBER(18,0) | YES |
| 9 | SRVASG_SHPSRV_ITEM_SHPMT_ID | NUMBER(18,0) | YES |
| 10 | SRVASG_SHPSRV_ITEM_SQNC_NBR | NUMBER(6,0) | YES |
| 11-126 | SRVASG_SQNC_NBR through CBP_HOLD_IND | Various | YES (all 116) |
| 127 | PBLSHD_WYBL_VRSN_NBR | NUMBER(4,0) | YES |
| 128 | FRA_APRVL_AGRMNT_NBR_RFRNC_ID | NUMBER(18,0) | YES |
| 129 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 130 | SNW_OPERATION_OWNER | VARCHAR(256) | YES (NEW) |
| 131 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 129/129 | YES | 5/5 | PASS |
| Recovery INSERT | 137 cols = 137 vals | YES | 6/6 | PASS |
| Main UPDATE | 129/129 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 129/129 | YES | 5/5 | PASS |
| Main NEW INSERT | 137 cols = 137 vals | YES | 6/6 | PASS |

---

## 5. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact:** 2,218,566 NULL rows included, 36,301 CSDMGR rows included (not filtered), 476 EMEMGR rows included (not filtered). 0 TSDPRG/EMEPRG -- filter is preventive.

---

## 6. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_STNWYB_MSG_DN()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 7. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 131/131 source + 6/6 CDC = 100% (largest table: 137 total cols) |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **59** | **60** | **98%** |

---

## 8. Suggestions

1. **Info:** No TSDPRG/EMEPRG rows in source. 36,301 CSDMGR + 476 EMEMGR rows exist but are correctly NOT in the filter list.
2. **Info:** This is the **largest table** in the Bug Fix batch with 131 source columns (137 total).

---

## 9. Verdict

**APPROVED FOR PRODUCTION** -- 100% column mapping (131/131 source + 6/6 CDC = 137 total), all MERGE branches validated, dual-value filter correctly applied, stream-based recovery implemented, SP compiled successfully.
