# Code Review: LCMTV_MVMNT_EVENT CDC Data Preservation Script
**Review Date:** March 11, 2026 (Rev 2 - Bug Fix updates)  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/LCMTV_MVMNT_EVENT.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE` (44 columns) |
| Target Table | `D_BRONZE.SADB.LCMTV_MVMNT_EVENT` (44 source + 6 CDC = 50 columns) |
| Stream | `D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_LCMTV_MVMNT_EVENT()` |
| Task | `D_RAW.SADB.TASK_PROCESS_LCMTV_MVMNT_EVENT` (5 min, no WHEN clause) |
| Primary Key | `EVENT_ID` (single) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Previous Version (Rev 1 dated 2026-02-24)

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 2 | New column | N/A (43 source) | `SNW_OPERATION_OWNER VARCHAR(256)` added (44 source) | DONE |
| 3 | Filter | None | `NOT IN ('TSDPRG', 'EMEPRG')` at all entry points | DONE |
| 4 | Recovery MERGE | Base table LEFT JOIN | Stream read with `SHOW_INITIAL_ROWS` | DONE |
| 5 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional for stale recovery) | DONE |
| 6 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |
| 7 | Column count | 43 source + 6 CDC = 49 | 44 source + 6 CDC = 50 | DONE |

---

## 3. Column Mapping (Source -> Target) -- 44/44 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | EVENT_ID | NUMBER(18,0) NOT NULL | YES (PK) |
| 2 | EQPMT_EVENT_TYPE_ID | NUMBER(18,0) | YES |
| 3 | SCAC_CD | VARCHAR(16) | YES |
| 4 | FSAC_CD | VARCHAR(20) | YES |
| 5 | TRSTN_VRSN_NBR | NUMBER(5,0) | YES |
| 6 | REPORT_TMS | TIMESTAMP_NTZ(0) | YES |
| 7 | SQNC_NBR | NUMBER(5,0) | YES |
| 8 | DRCTN_CD | VARCHAR(4) | YES |
| 9 | SOURCE_SYSTEM_NM | VARCHAR(40) | YES |
| 10 | MARK_CD | VARCHAR(16) | YES |
| 11 | EQPUN_NBR | VARCHAR(40) | YES |
| 12 | OPTRN_EVENT_ID | NUMBER(18,0) | YES |
| 13 | ORNTTN_CD | VARCHAR(4) | YES |
| 14 | DEAD_HEAD_IND | VARCHAR(4) | YES |
| 15 | RGN_NM_TRK_NBR | NUMBER(18,0) | YES |
| 16 | LMS_PRFL_SQNC_NBR | NUMBER(3,0) | YES |
| 17 | MILE_NBR | NUMBER(8,3) | YES |
| 18 | PLAN_EVENT_ID | NUMBER(18,0) | YES |
| 19 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 20 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 21 | CREATE_USER_ID | VARCHAR(32) | YES |
| 22 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 23 | AEIRD_NBR | VARCHAR(16) | YES |
| 24 | CNST_NBR | VARCHAR(16) | YES |
| 25 | CNST_ORIGIN_SCAC_CD | VARCHAR(16) | YES |
| 26 | CNST_ORIGIN_FSAC_CD | VARCHAR(20) | YES |
| 27 | COMMON_YARDS_SITE_CD | VARCHAR(4) | YES |
| 28 | COMMON_YARDS_TRACK_NM | VARCHAR(20) | YES |
| 29 | DSTNC_RUN_MILES_QTY | NUMBER(6,1) | YES |
| 30 | INTRCH_SCAC_CD | VARCHAR(16) | YES |
| 31 | MTP_ROUTE_POINT_SQNC_NBR | NUMBER(3,0) | YES |
| 32 | REPORT_TIME_ZONE_CD | VARCHAR(8) | YES |
| 33 | RUN_NBR_CD | VARCHAR(12) | YES |
| 34 | SHORT_DSTRCT_NM | VARCHAR(40) | YES |
| 35 | SPLC_CD | VARCHAR(24) | YES |
| 36 | TITAN_NBR | NUMBER(6,0) | YES |
| 37 | CNST_DSTNTN_SCAC_CD | VARCHAR(16) | YES |
| 38 | CNST_DSTNTN_FSAC_CD | VARCHAR(20) | YES |
| 39 | TRAVEL_DRCTN_CD | VARCHAR(20) | YES |
| 40 | SWITCH_LIST_NBR | NUMBER(5,0) | YES |
| 41 | TYES_TRAIN_ID | NUMBER(18,0) | YES |
| 42 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 43 | SNW_OPERATION_OWNER | VARCHAR(256) | YES (NEW) |
| 44 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 42/42 | YES | 5/5 | PASS |
| Recovery INSERT | 50 cols = 50 vals | YES | 6/6 | PASS |
| Main UPDATE | 42/42 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 42/42 | YES | 5/5 | PASS |
| Main NEW INSERT | 50 cols = 50 vals | YES | 6/6 | PASS |

---

## 5. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact:** 1,467,205 NULL rows included, 265,610 LMSMGR rows included (not filtered), 69 LMSAPIMGR rows included (not filtered). 0 TSDPRG/EMEPRG -- filter is preventive.

---

## 6. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_LCMTV_MVMNT_EVENT()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 7. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 44/44 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **59** | **60** | **98%** |

---

## 8. Suggestions

1. **Info:** No TSDPRG/EMEPRG rows in source. 265,610 LMSMGR + 69 LMSAPIMGR rows exist but are correctly NOT in the filter list.

---

## 9. Verdict

**APPROVED FOR PRODUCTION** -- 100% column mapping (44/44 source + 6/6 CDC = 50 total), all MERGE branches validated, dual-value filter correctly applied, stream-based recovery implemented, SP compiled successfully.
