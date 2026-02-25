# TRAIN_CNST_SMRY Script Review Report

**Review Date:** 2026-02-24  
**Script:** Scripts/Final/TRAIN_CNST_SMRY.sql  
**Source Table:** D_RAW.SADB.TRAIN_CNST_SMRY_BASE  
**Target Table:** D_BRONZE.SADB.TRAIN_CNST_SMRY

---

## Executive Summary

| Category | Score | Status |
|----------|-------|--------|
| **Column Mapping** | 100% | ✅ PERFECT |
| **Data Type Mapping** | 100% | ✅ PERFECT |
| **Primary Key (COMPOSITE)** | 100% | ✅ CORRECT |
| **SP Logic** | 100% | ✅ COMPLETE |
| **Syntax** | 100% | ✅ VALID |
| **Coding Standards** | 100% | ✅ EXCELLENT |
| **Overall Score** | **100/100** | ✅ **PRODUCTION READY** |

---

## 1. Column Mapping Validation (100% ✅)

### Source Table Columns (87 columns):
| # | Source Column | Source Type | Script Line | Match |
|---|---------------|-------------|-------------|-------|
| 1 | TRAIN_CNST_SMRY_ID | NUMBER(18,0) NOT NULL | 19 | ✅ |
| 2 | TRAIN_CNST_SMRY_VRSN_NBR | NUMBER(4,0) NOT NULL | 20 | ✅ |
| 3 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | 21 | ✅ |
| 4 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | 22 | ✅ |
| 5 | SOURCE_SYSTEM_UPDATE_TMS | TIMESTAMP_NTZ(0) | 23 | ✅ |
| 6 | CREATE_USER_ID | VARCHAR(32) | 24 | ✅ |
| 7 | UPDATE_USER_ID | VARCHAR(32) | 25 | ✅ |
| 8 | SOURCE_SYSTEM_UPDATE_USER_ID | VARCHAR(32) | 26 | ✅ |
| 9 | TRAIN_NM | VARCHAR(32) | 27 | ✅ |
| 10 | CNST_PRTY_NBR | NUMBER(1,0) | 28 | ✅ |
| 11 | LOADED_CARS_QTY | NUMBER(3,0) | 29 | ✅ |
| 12 | EMPTY_CARS_QTY | NUMBER(3,0) | 30 | ✅ |
| 13 | TOTAL_LENGTH_FEET_QTY | NUMBER(10,0) | 31 | ✅ |
| 14 | ESTMTD_GROSS_TONS_QTY | NUMBER(10,0) | 32 | ✅ |
| 15 | RQRD_LCMTV_QTY | NUMBER(2,0) | 33 | ✅ |
| 16 | RQRD_TOTAL_HRSPWR_QTY | NUMBER(5,0) | 34 | ✅ |
| 17 | RQRD_HRSPWR_PER_TON_QTY | NUMBER(3,2) | 35 | ✅ |
| 18 | ACTUAL_LCMTV_QTY | NUMBER(2,0) | 36 | ✅ |
| 19 | ACTUAL_TOTAL_HRSPWR_QTY | NUMBER(5,0) | 37 | ✅ |
| 20 | ACTUAL_HRSPWR_PER_TON_QTY | NUMBER(3,2) | 38 | ✅ |
| 21 | CNTNT_WEIGHT_TONS_QTY | NUMBER(10,0) | 39 | ✅ |
| 22 | TRAIN_SPEED_RSTRCN_MPH_QTY | NUMBER(3,0) | 40 | ✅ |
| 23 | TARE_WEIGHT_TONS_QTY | NUMBER(10,0) | 41 | ✅ |
| 24 | SOURCE_SYSTEM_TRAIN_CNST_NBR | VARCHAR(16) | 42 | ✅ |
| 25 | TITAN_NBR | NUMBER(6,0) | 43 | ✅ |
| 26 | DATA_SOURCE_CD | VARCHAR(40) | 44 | ✅ |
| 27 | TRAIN_DMNSNL_LOAD_RSTRCN_CD | VARCHAR(12) | 45 | ✅ |
| 28 | DNGRS_CARS_STATUS_CD | VARCHAR(68) | 46 | ✅ |
| 29 | CHNG_CNST_SMRY_STATUS_CD | VARCHAR(60) | 47 | ✅ |
| 30 | LAST_RPRTD_CNST_SMRY_STATUS_CD | VARCHAR(60) | 48 | ✅ |
| 31 | CNST_ORIGIN_TRN_PLN_EVENT_ID | NUMBER(18,0) | 49 | ✅ |
| 32 | CNST_ORIGIN_SCAC_CD | VARCHAR(16) | 50 | ✅ |
| 33 | CNST_ORIGIN_FSAC_CD | VARCHAR(20) | 51 | ✅ |
| 34 | CNST_ORIGIN_TRSTN_VRSN_NBR | NUMBER(5,0) | 52 | ✅ |
| 35 | CNST_ORIGIN_RTPT_SQNC_NBR | NUMBER(3,0) | 53 | ✅ |
| 36 | CNST_DSTNTN_TRN_PLN_EVENT_ID | NUMBER(18,0) | 54 | ✅ |
| 37 | CNST_DSTNTN_SCAC_CD | VARCHAR(16) | 55 | ✅ |
| 38 | CNST_DSTNTN_FSAC_CD | VARCHAR(20) | 56 | ✅ |
| 39 | CNST_DSTNTN_TRSTN_VRSN_NBR | NUMBER(5,0) | 57 | ✅ |
| 40 | CNST_DSTNTN_RTPT_SQNC_NBR | NUMBER(3,0) | 58 | ✅ |
| 41 | CNST_CHNG_PT_TRN_PLN_EVENT_ID | NUMBER(18,0) | 59 | ✅ |
| 42 | CNST_CHNG_PT_OPTRN_EVENT_ID | NUMBER(18,0) | 60 | ✅ |
| 43 | CNST_CHNG_PT_MVMNT_EVENT_ID | NUMBER(18,0) | 61 | ✅ |
| 44 | CNST_CHNG_PT_SCAC_CD | VARCHAR(16) | 62 | ✅ |
| 45 | CNST_CHNG_PT_FSAC_CD | VARCHAR(20) | 63 | ✅ |
| 46 | CNST_CHNG_PT_TRSTN_VRSN_NBR | NUMBER(5,0) | 64 | ✅ |
| 47 | CNST_CHNG_PT_RTPT_SQNC_NBR | NUMBER(3,0) | 65 | ✅ |
| 48 | CNST_CHNG_PT_EVENT_TMS | TIMESTAMP_NTZ(0) | 66 | ✅ |
| 49 | CNST_CHNG_RPRTNG_TM_ZN_CD | VARCHAR(8) | 67 | ✅ |
| 50 | CNST_CHNG_RPRTNG_TM_ZN_YR_NBR | NUMBER(4,0) | 68 | ✅ |
| 51 | LAST_RPRTD_OPTRN_EVENT_ID | NUMBER(18,0) | 69 | ✅ |
| 52 | LAST_RPRTD_MVMNT_EVENT_ID | NUMBER(18,0) | 70 | ✅ |
| 53 | LAST_RPRTD_SCAC_CD | VARCHAR(16) | 71 | ✅ |
| 54 | LAST_RPRTD_FSAC_CD | VARCHAR(20) | 72 | ✅ |
| 55 | LAST_RPRTD_TRSTN_VRSN_NBR | NUMBER(5,0) | 73 | ✅ |
| 56 | LAST_RPRTD_RTPT_SQNC_NBR | NUMBER(3,0) | 74 | ✅ |
| 57 | LAST_RPRTD_EVENT_TMS | TIMESTAMP_NTZ(0) | 75 | ✅ |
| 58 | LAST_RPRTD_RPRTNG_TM_ZN_CD | VARCHAR(8) | 76 | ✅ |
| 59 | LAST_RPRTD_RPRTNG_TM_ZN_YR_NBR | NUMBER(4,0) | 77 | ✅ |
| 60 | LEAD_LCMTV_MARK_CD | VARCHAR(16) | 78 | ✅ |
| 61 | LEAD_LCMTV_EQPUN_NBR | VARCHAR(40) | 79 | ✅ |
| 62 | SBU_MARK_CD | VARCHAR(16) | 80 | ✅ |
| 63 | SBU_EQPUN_NBR | VARCHAR(40) | 81 | ✅ |
| 64 | TYES_TRAIN_ID | NUMBER(18,0) | 82 | ✅ |
| 65 | TRMNTD_TRAIN_IND | VARCHAR(4) | 83 | ✅ |
| 66 | CNST_CHNG_DYLGHT_SVNGS_IND | VARCHAR(4) | 84 | ✅ |
| 67 | CNST_CHNG_LOCAL_TMS | TIMESTAMP_NTZ(0) | 85 | ✅ |
| 68 | CNST_CHNG_INTRMD_RTPNT_NBR | NUMBER(5,0) | 86 | ✅ |
| 69 | TYES_FUNCTION_CD | VARCHAR(32) | 87 | ✅ |
| 70 | EGT_WHLG_FACTOR_QTY | NUMBER(2,0) | 88 | ✅ |
| 71 | CDN_MRSHL_VLTN_CD | VARCHAR(4) | 89 | ✅ |
| 72 | USA_MRSHL_VLTN_CD | VARCHAR(4) | 90 | ✅ |
| 73 | CLV_REJECT_IND | VARCHAR(4) | 91 | ✅ |
| 74 | RCVRD_TRAIN_IND | VARCHAR(4) | 92 | ✅ |
| 75 | SENT_TO_CLV_TMS | TIMESTAMP_NTZ(0) | 93 | ✅ |
| 76 | MTCHD_DPRTR_ARVL_VRSN_NBR | NUMBER(4,0) | 94 | ✅ |
| 77 | LAST_RPRTD_DYLGHT_SVNGS_IND | VARCHAR(4) | 95 | ✅ |
| 78 | LAST_RPRTD_LOCAL_TMS | TIMESTAMP_NTZ(0) | 96 | ✅ |
| 79 | LAST_RPRTD_INTRMD_RTPNT_NBR | NUMBER(5,0) | 97 | ✅ |
| 80 | RUN_NBR | VARCHAR(12) | 98 | ✅ |
| 81 | TRNSFR_RUN_NUMBER_TXT | VARCHAR(16) | 99 | ✅ |
| 82 | TRAIN_CNST_ID | NUMBER(18,0) | 100 | ✅ |
| 83 | AEIRD_NBR | VARCHAR(16) | 101 | ✅ |
| 84 | OCS_UPDATE_CD | VARCHAR(4) | 102 | ✅ |
| 85 | CNST_RAIL_EQPMNT_QTY | NUMBER(3,0) | 103 | ✅ |
| 86 | SNW_OPERATION_TYPE | VARCHAR(1) | 104 | ✅ |
| 87 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | 105 | ✅ |

**Result: 87/87 columns mapped correctly (100%)**

### CDC Metadata Columns (6 additional):
| Column | Line | Purpose | Status |
|--------|------|---------|--------|
| CDC_OPERATION | 108 | Track INSERT/UPDATE/DELETE | ✅ |
| CDC_TIMESTAMP | 109 | When CDC captured | ✅ |
| IS_DELETED | 110 | Soft delete flag | ✅ |
| RECORD_CREATED_AT | 111 | Row creation time | ✅ |
| RECORD_UPDATED_AT | 112 | Row update time | ✅ |
| SOURCE_LOAD_BATCH_ID | 113 | Batch tracking | ✅ |

**Total: 93 columns (87 source + 6 CDC) ✅**

---

## 2. Primary Key Validation (100% ✅) - COMPOSITE KEY

| Attribute | Source | Script | Match |
|-----------|--------|--------|-------|
| PK Column 1 | TRAIN_CNST_SMRY_ID | TRAIN_CNST_SMRY_ID | ✅ |
| PK Column 2 | TRAIN_CNST_SMRY_VRSN_NBR | TRAIN_CNST_SMRY_VRSN_NBR | ✅ |
| PK Type | COMPOSITE | COMPOSITE | ✅ |
| Target PK | Line 115 | `PRIMARY KEY (TRAIN_CNST_SMRY_ID, TRAIN_CNST_SMRY_VRSN_NBR)` | ✅ |

### MERGE ON Clause Verification (CRITICAL for Composite PK):
| Location | Lines | Composite Key Handling | Status |
|----------|-------|----------------------|--------|
| Recovery MERGE | 193-194 | `ON tgt.TRAIN_CNST_SMRY_ID = src.TRAIN_CNST_SMRY_ID AND tgt.TRAIN_CNST_SMRY_VRSN_NBR = src.TRAIN_CNST_SMRY_VRSN_NBR` | ✅ |
| Main MERGE | 445-446 | `ON tgt.TRAIN_CNST_SMRY_ID = src.TRAIN_CNST_SMRY_ID AND tgt.TRAIN_CNST_SMRY_VRSN_NBR = src.TRAIN_CNST_SMRY_VRSN_NBR` | ✅ |
| Recovery Join | 188-189 | Both PK columns used in LEFT JOIN | ✅ |

---

## 3. Stored Procedure Logic Validation (100% ✅)

### Pattern Compliance:
| Pattern | Required | Line(s) | Status |
|---------|----------|---------|--------|
| EXECUTE AS CALLER | ✅ | 140 | ✅ |
| Stream staleness detection | ✅ | 156-167 | ✅ |
| Stream recovery (SHOW_INITIAL_ROWS=TRUE) | ✅ | 175-178 | ✅ |
| Staging table pattern | ✅ | 359-394 | ✅ |
| 4 MERGE scenarios | ✅ | 448-711 | ✅ |
| All columns in UPDATE | ✅ | 450-540, 553-643 | ✅ |
| Error handling | ✅ | 718-721 | ✅ |
| Temp table cleanup | ✅ | 714, 720 | ✅ |

### MERGE Scenario Coverage:
| Scenario | Condition | Lines | Columns Updated | Status |
|----------|-----------|-------|-----------------|--------|
| UPDATE | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=TRUE | 449-540 | All 85 non-PK source + 5 CDC | ✅ |
| DELETE | CDC_ACTION='DELETE' AND CDC_IS_UPDATE=FALSE | 543-549 | 4 CDC (soft delete) | ✅ |
| RE-INSERT | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=FALSE (MATCHED) | 552-643 | All 85 non-PK source + 5 CDC | ✅ |
| INSERT | CDC_ACTION='INSERT' (NOT MATCHED) | 646-711 | All 93 columns | ✅ |

### Staging Table Column Verification:
| Location | Lines | All 87 Source Columns | Status |
|----------|-------|----------------------|--------|
| Staging SELECT | 360-394 | ✅ Complete | ✅ |
| MERGE Source SELECT | 408-444 | ✅ Complete | ✅ |
| Recovery MERGE INSERT | 286-350 | ✅ Complete | ✅ |
| Main MERGE INSERT | 647-711 | ✅ Complete | ✅ |

---

## 4. Syntax Validation (100% ✅)

| Check | Line(s) | Status |
|-------|---------|--------|
| CREATE TABLE IF NOT EXISTS | 18-116 | ✅ Valid |
| ALTER TABLE SET CHANGE_TRACKING | 121-124 | ✅ Valid |
| CREATE OR REPLACE STREAM | 129-132 | ✅ Valid |
| CREATE OR REPLACE PROCEDURE | 137-723 | ✅ Valid |
| MERGE statement (Recovery) | 180-350 | ✅ Valid |
| MERGE statement (Main) | 406-711 | ✅ Valid |
| CREATE OR REPLACE TASK | 728-736 | ✅ Valid |
| Variable declarations | 143-149 | ✅ Valid |
| Exception handling | 718-721 | ✅ Valid |
| ALTER TASK RESUME | 738 | ✅ Valid |

---

## 5. Coding Standards (100% ✅)

| Standard | Score | Evidence |
|----------|-------|----------|
| Consistent naming | 100% | All objects follow `TRAIN_CNST_SMRY*` pattern |
| Fully qualified names | 100% | `D_RAW.SADB.*`, `D_BRONZE.SADB.*` throughout |
| Clear section headers | 100% | STEP 1-5 clearly labeled |
| Meaningful comments | 100% | Each section has descriptive comments |
| Error messages | 100% | Informative RETURN values with timestamps |
| Batch ID format | 100% | `BATCH_YYYYMMDD_HH24MISS` |
| Indentation | 100% | Consistent formatting |
| Column alignment | 100% | Columns aligned in INSERT/UPDATE statements |
| Header documentation | 100% | Complete header with PK info (Line 10) |

---

## 6. Task Configuration (100% ✅)

| Setting | Value | Best Practice | Status |
|---------|-------|---------------|--------|
| WAREHOUSE | INFA_INGEST_WH | ✅ Dedicated WH | ✅ |
| SCHEDULE | '5 MINUTE' | ✅ Reasonable CDC interval | ✅ |
| ALLOW_OVERLAPPING_EXECUTION | FALSE | ✅ Prevents conflicts | ✅ |
| WHEN condition | SYSTEM$STREAM_HAS_DATA() | ✅ Efficient | ✅ |
| ALTER TASK RESUME | Included (Line 738) | ✅ Task activated | ✅ |

---

## 7. Change Tracking Configuration (100% ✅)

| Setting | Value | Line | Status |
|---------|-------|------|--------|
| CHANGE_TRACKING | TRUE | 122 | ✅ |
| DATA_RETENTION_TIME_IN_DAYS | 45 | 123 | ✅ |
| MAX_DATA_EXTENSION_TIME_IN_DAYS | 15 | 124 | ✅ |

---

## 8. Special Considerations - Composite Primary Key ✅

This table uses a **COMPOSITE PRIMARY KEY** consisting of two columns:
- `TRAIN_CNST_SMRY_ID`
- `TRAIN_CNST_SMRY_VRSN_NBR`

### Verification Points:
| Check | Location | Status |
|-------|----------|--------|
| Both PK columns NOT NULL in target | Lines 19-20 | ✅ |
| Composite PK defined in target | Line 115 | ✅ |
| Recovery MERGE uses both columns | Lines 188-189, 193-194 | ✅ |
| Main MERGE uses both columns | Lines 445-446 | ✅ |
| Both columns in staging table | Lines 361 | ✅ |
| Both columns in INSERT statements | Lines 287, 319, 648, 680 | ✅ |

---

## Final Verification Checklist

| Requirement | Status |
|-------------|--------|
| All 87 source columns present in target table | ✅ |
| All 87 source columns in staging table | ✅ |
| All 87 source columns in UPDATE scenarios | ✅ |
| All 93 columns in INSERT scenarios | ✅ |
| Composite PK (TRAIN_CNST_SMRY_ID, TRAIN_CNST_SMRY_VRSN_NBR) defined | ✅ |
| MERGE ON clause uses BOTH PK columns | ✅ |
| Data types match source exactly | ✅ |
| Stream staleness handled | ✅ |
| Error handling with cleanup | ✅ |
| Task properly configured and resumed | ✅ |

---

## Verdict

### ✅ **APPROVED FOR PRODUCTION DEPLOYMENT**

**Score: 100/100**

| Category | Points |
|----------|--------|
| Column Mapping | 30/30 |
| Data Types | 15/15 |
| Primary Key (Composite) | 10/10 |
| SP Logic | 20/20 |
| Syntax | 10/10 |
| Coding Standards | 10/10 |
| Task Configuration | 5/5 |
| **Total** | **100/100** |

---

## Summary

The `TRAIN_CNST_SMRY.sql` script is **100% aligned** with the source table definition:

- ✅ All 87 source columns correctly mapped with exact data types
- ✅ 6 CDC metadata columns properly added (total 93)
- ✅ **COMPOSITE Primary Key** (TRAIN_CNST_SMRY_ID + TRAIN_CNST_SMRY_VRSN_NBR) correctly implemented
- ✅ All MERGE ON clauses use BOTH primary key columns
- ✅ All 4 MERGE scenarios handle full column updates
- ✅ Stream staleness detection and recovery mechanism
- ✅ Proper error handling with temp table cleanup
- ✅ Task properly configured with SYSTEM$STREAM_HAS_DATA()
- ✅ Change tracking with 45-day retention configured

**No modifications required. Ready for production implementation.**

---

**Reviewed By:** Snowflake CDC Expert  
**Date:** 2026-02-24  
**Status:** ✅ PRODUCTION READY
