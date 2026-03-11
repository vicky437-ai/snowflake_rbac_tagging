# Code Review: EQPMNT_AAR_BASE CDC Data Preservation Script
**Review Date:** March 11, 2026 (Rev 2 - Bug Fix updates)  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/EQPMNT_AAR_BASE.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.EQPMNT_AAR_BASE_BASE` (82 columns) |
| Target Table | `D_BRONZE.SADB.EQPMNT_AAR_BASE` (82 source + 6 CDC = 88 columns) |
| Stream | `D_RAW.SADB.EQPMNT_AAR_BASE_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_EQPMNT_AAR_BASE()` |
| Task | `D_RAW.SADB.TASK_PROCESS_EQPMNT_AAR_BASE` (5 min, no WHEN clause) |
| Primary Key | `EQPMNT_ID` (single) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Previous Version (Rev 1 dated 2026-02-24)

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 2 | New column | N/A (81 source) | `SNW_OPERATION_OWNER VARCHAR(256)` added (82 source) | DONE |
| 3 | Filter | None | `NOT IN ('TSDPRG', 'EMEPRG')` at all entry points | DONE |
| 4 | Recovery MERGE | Base table LEFT JOIN | Stream read with `SHOW_INITIAL_ROWS` | DONE |
| 5 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional for stale recovery) | DONE |
| 6 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |
| 7 | Column count | 81 source + 6 CDC = 87 | 82 source + 6 CDC = 88 | DONE |

---

## 3. Column Mapping (Source -> Target) -- 82/82 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | EQPMNT_ID | NUMBER(18,0) NOT NULL | YES (PK) |
| 2 | MASTER_EQPMNT_ID | NUMBER(18,0) | YES |
| 3 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 4 | CREATE_USER_ID | VARCHAR(32) | YES |
| 5 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 6 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 7 | CREATE_DATA_SOURCE_CD | VARCHAR(40) | YES |
| 8 | SOURCE_CREATE_USER_ID | VARCHAR(32) | YES |
| 9 | SOURCE_UPDATE_USER_ID | VARCHAR(32) | YES |
| 10 | UPDATE_DATA_SOURCE_CD | VARCHAR(40) | YES |
| 11 | AAR_ADD_TMS | TIMESTAMP_NTZ(0) | YES |
| 12 | AAR_CAR_CD | VARCHAR(16) | YES |
| 13 | AAR_CAR_CODE_ID | NUMBER(18,0) | YES |
| 14 | AAR_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 15 | EQPMNT_GROUP_CD | VARCHAR(16) | YES |
| 16 | EQPUN_NBR | VARCHAR(40) | YES |
| 17 | MARK_CD | VARCHAR(16) | YES |
| 18 | MCHNCL_DSGNTN_CD | VARCHAR(16) | YES |
| 19 | ALTRNT_CAR_CD_ID | NUMBER(18,0) | YES |
| 20 | ALTRNT_CAR_CD | VARCHAR(16) | YES |
| 21 | AXLE_QTY | NUMBER(4,0) | YES |
| 22 | BLDR_CD | VARCHAR(16) | YES |
| 23 | DLTD_TMS | TIMESTAMP_NTZ(0) | YES |
| 24 | BUILT_DT | TIMESTAMP_NTZ(0) | YES |
| 25 | DMTR_UOM_BASIS_CD | VARCHAR(8) | YES |
| 26 | EFCTV_TMS | TIMESTAMP_NTZ(0) | YES |
| 27 | EIN_NBR | VARCHAR(40) | YES |
| 28 | EQPMNT_MNGMNT_CODE_ASCTN_ID | NUMBER(18,0) | YES |
| 29 | EQPMNT_POOL_ID | NUMBER(18,0) | YES |
| 30 | EXPIRY_TMS | TIMESTAMP_NTZ(0) | YES |
| 31 | GROSS_WEIGHT_QTY | NUMBER(7,0) | YES |
| 32 | GROSS_WEIGHT_UOM_BASIS_CD | VARCHAR(8) | YES |
| 33 | LINEAL_FRCTNL_UOM_BASIS_CD | VARCHAR(8) | YES |
| 34 | LINEAL_UOM_BASIS_CD | VARCHAR(8) | YES |
| 35 | LIQUID_UOM_BASIS_CD | VARCHAR(8) | YES |
| 36 | OPRTNG_BRAKE_QTY | NUMBER(2,0) | YES |
| 37 | OTSD_EXTRM_HEIGHT_QTY | NUMBER(4,0) | YES |
| 38 | OTSD_EXTRM_WIDTH_QTY | NUMBER(4,0) | YES |
| 39 | OTSD_HEIGHT_EXTRM_WIDTH_QTY | NUMBER(4,0) | YES |
| 40 | OTSD_LENGTH_QTY | NUMBER(5,0) | YES |
| 41 | OWNER_MARK_CD | VARCHAR(16) | YES |
| 42 | PLATE_CLRNC_CD | VARCHAR(8) | YES |
| 43 | PRIOR_EQPUN_NBR | VARCHAR(40) | YES |
| 44 | PRIOR_MARK_CD | VARCHAR(16) | YES |
| 45 | RBLT_CD | VARCHAR(4) | YES |
| 46 | RBLT_DT | TIMESTAMP_NTZ(0) | YES |
| 47 | SOURCE_CREATE_PARTY_RLTNSH_ID | NUMBER(18,0) | YES |
| 48 | SOURCE_UPDATE_PARTY_RLTNSH_ID | NUMBER(18,0) | YES |
| 49 | SPEED_UOM_BASIS_CD | VARCHAR(8) | YES |
| 50 | SRVC_LIFE_CD | VARCHAR(8) | YES |
| 51 | STATUS_CD | VARCHAR(8) | YES |
| 52 | STNCL_MARK_OWNER_CLASS_CD | VARCHAR(8) | YES |
| 53 | TARE_WEIGHT_QTY | NUMBER(7,0) | YES |
| 54 | TRUCK_CENTER_LENGTH_QTY | NUMBER(4,0) | YES |
| 55 | VOLUME_UOM_BASIS_CD | VARCHAR(8) | YES |
| 56 | WEIGHT_UOM_BASIS_CD | VARCHAR(8) | YES |
| 57 | EQPMNT_DSGNTN_CD | VARCHAR(20) | YES |
| 58 | WHEEL_BRNG_CD | VARCHAR(12) | YES |
| 59 | FIRST_MVMNT_TMS | TIMESTAMP_NTZ(0) | YES |
| 60 | STATUS_CHANGE_REASON_CD | VARCHAR(8) | YES |
| 61 | STATUS_CHANGE_TMS | TIMESTAMP_NTZ(0) | YES |
| 62 | DELETE_REASON_CD | VARCHAR(8) | YES |
| 63 | BLDR_LOT_TXT | VARCHAR(100) | YES |
| 64 | CMRCL_LESSEE_CIF_TXT | VARCHAR(60) | YES |
| 65 | CMRCL_OWNER_CIF_TXT | VARCHAR(60) | YES |
| 66 | CNFLCT_STATUS_CD | VARCHAR(8) | YES |
| 67 | CRNT_CNFLCT_STATUS_DT | TIMESTAMP_NTZ(0) | YES |
| 68 | ECP_BRAKE_BLDR_CD | VARCHAR(20) | YES |
| 69 | ECP_BRAKE_TYPE_CD | VARCHAR(8) | YES |
| 70 | EIN_DPLCTN_CD | VARCHAR(4) | YES |
| 71 | EQPMNT_ADD_RPRTR_CMPNY_MARK_CD | VARCHAR(20) | YES |
| 72 | MNFCTR_CNTRY_CD | VARCHAR(8) | YES |
| 73 | NEXT_CNFLCT_STATUS_CD | VARCHAR(8) | YES |
| 74 | NEXT_CNFLCT_STATUS_DT | TIMESTAMP_NTZ(0) | YES |
| 75 | NOTICE_MGMNT_INDCTR_CD | VARCHAR(4) | YES |
| 76 | RGSTRN_REASON_CD | VARCHAR(8) | YES |
| 77 | RSTNCL_PRGRM_CD | VARCHAR(4) | YES |
| 78 | TRUCK_QTY | NUMBER(3,0) | YES |
| 79 | RATE_CD | VARCHAR(8) | YES |
| 80 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 81 | SNW_OPERATION_OWNER | VARCHAR(256) | YES (NEW) |
| 82 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 80/80 | YES | 5/5 | PASS |
| Recovery INSERT | 88 cols = 88 vals | YES | 6/6 | PASS |
| Main UPDATE | 80/80 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 80/80 | YES | 5/5 | PASS |
| Main NEW INSERT | 88 cols = 88 vals | YES | 6/6 | PASS |

---

## 5. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact:** 2,295,407 NULL rows included, 1,429 EDMGR rows included (not filtered). 0 TSDPRG/EMEPRG -- filter is preventive.

---

## 6. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_EQPMNT_AAR_BASE()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 7. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 82/82 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **59** | **60** | **98%** |

---

## 8. Suggestions

1. **Info:** No TSDPRG/EMEPRG rows in source. 1,429 EDMGR rows exist but are NOT in the filter list (correctly included).

---

## 9. Verdict

**APPROVED FOR PRODUCTION** -- 100% column mapping (82/82 source + 6/6 CDC = 88 total), all MERGE branches validated, dual-value filter correctly applied, stream-based recovery implemented, SP compiled successfully.
