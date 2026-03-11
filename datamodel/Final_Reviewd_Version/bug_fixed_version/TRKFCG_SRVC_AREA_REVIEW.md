# Code Review: TRKFCG_SRVC_AREA CDC Data Preservation Script
**Review Date:** March 11, 2026 (Rev 2 - Bug Fix updates)  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/TRKFCG_SRVC_AREA.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.TRKFCG_SRVC_AREA_BASE` (26 columns) |
| Target Table | `D_BRONZE.SADB.TRKFCG_SRVC_AREA` (26 source + 6 CDC = 32 columns) |
| Stream | `D_RAW.SADB.TRKFCG_SRVC_AREA_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_TRKFCG_SRVC_AREA()` |
| Task | `D_RAW.SADB.TASK_PROCESS_TRKFCG_SRVC_AREA` (5 min, no WHEN clause) |
| Primary Key | `GRPHC_OBJECT_VRSN_ID` (single) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Previous Version (Rev 1 dated 2026-02-24)

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 2 | New column | N/A (25 source) | `SNW_OPERATION_OWNER VARCHAR(256)` added (26 source) | DONE |
| 3 | Filter | None | `NOT IN ('TSDPRG', 'EMEPRG')` at all entry points | DONE |
| 4 | Recovery MERGE | Base table LEFT JOIN | Stream read with `SHOW_INITIAL_ROWS` | DONE |
| 5 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional for stale recovery) | DONE |
| 6 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |
| 7 | Column count | 25 source + 6 CDC = 31 | 26 source + 6 CDC = 32 | DONE |

---

## 3. Column Mapping (Source -> Target) -- 26/26 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | GRPHC_OBJECT_VRSN_ID | NUMBER(18,0) NOT NULL | YES (PK) |
| 2 | VRSN_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 3 | VRSN_USER_ID | VARCHAR(32) | YES |
| 4 | FIRST_GRPHC_OBJECT_VRSN_ID | NUMBER(18,0) | YES |
| 5 | PRVS_GRPHC_OBJECT_VRSN_ID | NUMBER(18,0) | YES |
| 6 | GRPHC_OBJECT_MDFCTN_CD | VARCHAR(36) | YES |
| 7 | GRPHC_OBJECT_STATUS_CD | VARCHAR(32) | YES |
| 8 | GRPHC_TRNSCT_ID | NUMBER(18,0) | YES |
| 9 | SRVC_AREA_ID | NUMBER(18,0) | YES |
| 10 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 11 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 12 | CREATE_USER_ID | VARCHAR(32) | YES |
| 13 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 14 | SRVC_AREA_CD | VARCHAR(8) | YES |
| 15 | EFCTV_TMS | TIMESTAMP_NTZ(0) | YES |
| 16 | LONG_ENGLSH_NM | VARCHAR(320) | YES |
| 17 | SHORT_ENGLSH_NM | VARCHAR(40) | YES |
| 18 | EXPIRY_TMS | TIMESTAMP_NTZ(0) | YES |
| 19 | GRPHC_BSNS_AREA_NM | VARCHAR(32) | YES |
| 20 | LONG_FRENCH_NM | VARCHAR(320) | YES |
| 21 | SHORT_FRENCH_NM | VARCHAR(40) | YES |
| 22 | SRVC_AREA_NOTE_TXT | VARCHAR(320) | YES |
| 23 | SRVC_AREA_ST | VARCHAR(160) | YES |
| 24 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 25 | SNW_OPERATION_OWNER | VARCHAR(256) | YES (NEW) |
| 26 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 24/24 | YES | 5/5 | PASS |
| Recovery INSERT | 32 cols = 32 vals | YES | 6/6 | PASS |
| Main UPDATE | 24/24 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 24/24 | YES | 5/5 | PASS |
| Main NEW INSERT | 32 cols = 32 vals | YES | 6/6 | PASS |

---

## 5. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact:** 50 NULL rows included. 0 TSDPRG/EMEPRG -- filter is preventive.

---

## 6. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_TRKFCG_SRVC_AREA()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 7. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 26/26 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **59** | **60** | **98%** |

---

## 8. Suggestions

1. **Info:** No TSDPRG/EMEPRG rows in source (50 rows all NULL owner) -- filter is preventive, good practice.

---

## 9. Verdict

**APPROVED FOR PRODUCTION** -- 100% column mapping (26/26 source + 6/6 CDC = 32 total), all MERGE branches validated, dual-value filter correctly applied, stream-based recovery implemented, SP compiled successfully.
