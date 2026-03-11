# Code Review: TRAIN_PLAN_EVENT CDC Data Preservation Script
**Review Date:** March 11, 2026 (Rev 2 - Bug Fix updates)  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/TRAIN_PLAN_EVENT.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.TRAIN_PLAN_EVENT_BASE` (37 columns) |
| Target Table | `D_BRONZE.SADB.TRAIN_PLAN_EVENT` (37 source + 6 CDC = 43 columns) |
| Stream | `D_RAW.SADB.TRAIN_PLAN_EVENT_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_EVENT()` |
| Task | `D_RAW.SADB.TASK_PROCESS_TRAIN_PLAN_EVENT` (5 min, no WHEN clause) |
| Primary Key | `TRAIN_PLAN_EVENT_ID` (single) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Previous Version (Rev 1 dated 2026-02-24)

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 2 | New column | N/A (36 source) | `SNW_OPERATION_OWNER VARCHAR(256)` added (37 source) | DONE |
| 3 | Filter | None | `NOT IN ('TSDPRG', 'EMEPRG')` at all entry points | DONE |
| 4 | Recovery MERGE | Base table LEFT JOIN | Stream read with `SHOW_INITIAL_ROWS` | DONE |
| 5 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional for stale recovery) | DONE |
| 6 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |
| 7 | Column count | 36 source + 6 CDC = 42 | 37 source + 6 CDC = 43 | DONE |

---

## 3. Column Mapping (Source -> Target) -- 37/37 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | TRAIN_PLAN_EVENT_ID | NUMBER(18,0) NOT NULL | YES (PK) |
| 2 | TRAIN_PLAN_LEG_ID | NUMBER(18,0) | YES |
| 3 | TRAIN_EVENT_TYPE_CD | VARCHAR(16) | YES |
| 4 | EVENT_TMS | TIMESTAMP_NTZ(0) | YES |
| 5 | MTP_ROUTE_POINT_SQNC_NBR | NUMBER(3,0) | YES |
| 6 | TRAVEL_DRCTN_CD | VARCHAR(20) | YES |
| 7 | EVENT_CRTNTY_CD | VARCHAR(24) | YES |
| 8 | EVENT_STATUS_CD | VARCHAR(24) | YES |
| 9 | ANCHOR_TMS | TIMESTAMP_NTZ(0) | YES |
| 10 | SCAC_CD | VARCHAR(16) | YES |
| 11 | FSAC_CD | VARCHAR(20) | YES |
| 12 | TRSTN_VRSN_NBR | NUMBER(5,0) | YES |
| 13 | RGN_NM_TRK_NBR | NUMBER(18,0) | YES |
| 14 | REGION_NBR | NUMBER(18,0) | YES |
| 15 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 16 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 17 | CREATE_USER_ID | VARCHAR(32) | YES |
| 18 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 19 | TIME_ZONE_CD | VARCHAR(8) | YES |
| 20 | TIME_ZONE_YEAR_NBR | NUMBER(4,0) | YES |
| 21 | EVENT_SOURCE_CD | VARCHAR(32) | YES |
| 22 | MILE_NBR | NUMBER(8,3) | YES |
| 23 | SCHDLD_EVENT_TMS | TIMESTAMP_NTZ(0) | YES |
| 24 | THRTCL_EVENT_TMS | TIMESTAMP_NTZ(0) | YES |
| 25 | RQRD_OMTS_RPTNG_POINT_IND | VARCHAR(4) | YES |
| 26 | YARD_RPRTNG_IND | VARCHAR(4) | YES |
| 27 | CNST_CHNG_POINT_IND | VARCHAR(4) | YES |
| 28 | LCMTV_CHNG_IND | VARCHAR(4) | YES |
| 29 | TRAIN_LINE_UP_RPRTNG_IND | VARCHAR(4) | YES |
| 30 | CREW_CHANGE_CD | VARCHAR(4) | YES |
| 31 | ROUTE_POINT_ACTVTY_IND | VARCHAR(4) | YES |
| 32 | PRFL_YARD_REPORT_CD | VARCHAR(4) | YES |
| 33 | STN_CNTXT_CD | VARCHAR(4) | YES |
| 34 | SBDVSN_CNTXT_CD | VARCHAR(4) | YES |
| 35 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 36 | SNW_OPERATION_OWNER | VARCHAR(256) | YES (NEW) |
| 37 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 35/35 | YES | 5/5 | PASS |
| Recovery INSERT | 43 cols = 43 vals | YES | 6/6 | PASS |
| Main UPDATE | 35/35 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 35/35 | YES | 5/5 | PASS |
| Main NEW INSERT | 43 cols = 43 vals | YES | 6/6 | PASS |

---

## 5. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact:** 950,939 NULL rows included, 334 TSDAPIMGR rows included (not filtered), **95,767 TSDPRG rows excluded**. 0 EMEPRG (preventive).

---

## 6. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_TRAIN_PLAN_EVENT()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 7. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 37/37 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **59** | **60** | **98%** |

---

## 8. Suggestions

1. **Info:** 95,767 TSDPRG rows will be actively filtered (9.15% of source data). 334 TSDAPIMGR rows exist but are correctly NOT filtered. 0 EMEPRG (preventive).

---

## 9. Verdict

**APPROVED FOR PRODUCTION** -- 100% column mapping (37/37 source + 6/6 CDC = 43 total), all MERGE branches validated, dual-value filter correctly applied (95,767 TSDPRG rows excluded), stream-based recovery implemented, SP compiled successfully.
