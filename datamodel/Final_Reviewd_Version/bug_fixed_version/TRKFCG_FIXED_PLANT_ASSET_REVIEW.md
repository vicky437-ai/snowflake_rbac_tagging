# Code Review: TRKFCG_FIXED_PLANT_ASSET CDC Data Preservation Script
**Review Date:** March 11, 2026 (Rev 2 - Bug Fix updates)  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/TRKFCG_FIXED_PLANT_ASSET.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.TRKFCG_FIXED_PLANT_ASSET_BASE` (53 columns) |
| Target Table | `D_BRONZE.SADB.TRKFCG_FIXED_PLANT_ASSET` (53 source + 6 CDC = 59 columns) |
| Stream | `D_RAW.SADB.TRKFCG_FIXED_PLANT_ASSET_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_TRKFCG_FIXED_PLANT_ASSET()` |
| Task | `D_RAW.SADB.TASK_PROCESS_TRKFCG_FIXED_PLANT_ASSET` (5 min, no WHEN clause) |
| Primary Key | `GRPHC_OBJECT_VRSN_ID` (single) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Previous Version (Rev 1 dated 2026-02-24)

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 2 | New column | N/A (52 source) | `SNW_OPERATION_OWNER VARCHAR(256)` added (53 source) | DONE |
| 3 | Filter | None | `NOT IN ('TSDPRG', 'EMEPRG')` at all entry points | DONE |
| 4 | Recovery MERGE | Base table LEFT JOIN | Stream read with `SHOW_INITIAL_ROWS` | DONE |
| 5 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional for stale recovery) | DONE |
| 6 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |
| 7 | Column count | 52 source + 6 CDC = 58 | 53 source + 6 CDC = 59 | DONE |

---

## 3. Column Mapping (Source -> Target) -- 53/53 = 100%

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
| 9 | FIXED_PLANT_ASSET_ID | NUMBER(18,0) | YES |
| 10-48 | RECORD_CREATE_TMS through OCS_LONG_FRENCH_NM | Various | YES (all 39) |
| 49 | PRMRY_REGION_ID | NUMBER(18,0) | YES |
| 50 | ALTRNT_LONG_ENGLSH_NM | VARCHAR(320) | YES |
| 51 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 52 | SNW_OPERATION_OWNER | VARCHAR(256) | YES (NEW) |
| 53 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 51/51 | YES | 5/5 | PASS |
| Recovery INSERT | 59 cols = 59 vals | YES | 6/6 | PASS |
| Main UPDATE | 51/51 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 51/51 | YES | 5/5 | PASS |
| Main NEW INSERT | 59 cols = 59 vals | YES | 6/6 | PASS |

---

## 5. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact:** 172,838 NULL rows included. 0 TSDPRG/EMEPRG -- filter is preventive.

---

## 6. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_TRKFCG_FIXED_PLANT_ASSET()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 7. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 53/53 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **59** | **60** | **98%** |

---

## 8. Suggestions

1. **Info:** No TSDPRG/EMEPRG rows in source (172,838 rows, all NULL owner) -- filter is preventive.

---

## 9. Verdict

**APPROVED FOR PRODUCTION** -- 100% column mapping (53/53 source + 6/6 CDC = 59 total), all MERGE branches validated, dual-value filter correctly applied, stream-based recovery implemented, SP compiled successfully.
