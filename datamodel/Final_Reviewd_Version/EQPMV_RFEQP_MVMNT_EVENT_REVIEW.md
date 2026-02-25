# EQPMV_RFEQP_MVMNT_EVENT Script Review Report

**Review Date:** 2026-02-24  
**Script:** Scripts/Final/EQPMV_RFEQP_MVMNT_EVENT.sql  
**Source Table:** D_RAW.SADB.EQPMV_RFEQP_MVMNT_EVENT_BASE  
**Target Table:** D_BRONZE.SADB.EQPMV_RFEQP_MVMNT_EVENT

---

## Executive Summary

| Category | Score | Status |
|----------|-------|--------|
| **Column Mapping** | 100% | ✅ PERFECT |
| **Data Type Mapping** | 100% | ✅ PERFECT |
| **Primary Key** | 100% | ✅ CORRECT |
| **SP Logic** | 100% | ✅ COMPLETE |
| **Syntax** | 100% | ✅ VALID |
| **Coding Standards** | 100% | ✅ EXCELLENT |
| **Overall Score** | **100/100** | ✅ **PRODUCTION READY** |

---

## 1. Column Mapping Validation (100% ✅)

### Source Table Columns (90 columns):
| # | Source Column | Source Type | Script Line | Match |
|---|---------------|-------------|-------------|-------|
| 1 | EVENT_ID | NUMBER(18,0) NOT NULL | 19 | ✅ |
| 2 | EQPMT_EVENT_TYPE_ID | NUMBER(18,0) | 20 | ✅ |
| 3 | SCAC_CD | VARCHAR(16) | 21 | ✅ |
| 4 | FSAC_CD | VARCHAR(20) | 22 | ✅ |
| 5 | TRSTN_VRSN_NBR | NUMBER(5,0) | 23 | ✅ |
| 6 | RPT_SCAC_CD | VARCHAR(16) | 24 | ✅ |
| 7 | RPT_FSAC_CD | VARCHAR(20) | 25 | ✅ |
| 8 | RPT_TRSTN_VRSN_NBR | NUMBER(5,0) | 26 | ✅ |
| 9 | AAR_CAR_TYPE_CD | VARCHAR(16) | 27 | ✅ |
| 10 | AAR_CAR_TYPE_VRSN_NBR | NUMBER(5,0) | 28 | ✅ |
| 11 | LGLNT_SLCTN_ID | NUMBER(5,0) | 29 | ✅ |
| 12 | SLCTN_VRSN_NBR | NUMBER(5,0) | 30 | ✅ |
| 13 | SLCTN_DATA_BASE_CN | VARCHAR(8) | 31 | ✅ |
| 14 | LCTN_DATA_BASE_CN | VARCHAR(8) | 32 | ✅ |
| 15 | LGLNT_LCTN_ID | NUMBER(10,0) | 33 | ✅ |
| 16 | SPLC_CD | VARCHAR(24) | 34 | ✅ |
| 17 | NTWRK_NODE_ID | NUMBER(18,0) | 35 | ✅ |
| 18 | REPORT_TMS | TIMESTAMP_NTZ(0) | 36 | ✅ |
| 19 | SQNC_NBR | NUMBER(5,0) | 37 | ✅ |
| 20 | LOAD_EMPTY_IND | VARCHAR(4) | 38 | ✅ |
| 21 | DRCTN_CD | VARCHAR(4) | 39 | ✅ |
| 22 | SOURCE_SYSTEM_NM | VARCHAR(40) | 40 | ✅ |
| 23 | LSOP_PRFL_NBR | VARCHAR(32) | 41 | ✅ |
| 24 | TITAN_NBR | VARCHAR(24) | 42 | ✅ |
| 25 | MARK_CD | VARCHAR(16) | 43 | ✅ |
| 26 | EQPUN_NBR | VARCHAR(40) | 44 | ✅ |
| 27 | OPTRN_LEG_ID | NUMBER(18,0) | 45 | ✅ |
| 28 | OPTRN_TMS | TIMESTAMP_NTZ(0) | 46 | ✅ |
| 29 | TRAIN_PLAN_LEG_ID | NUMBER(18,0) | 47 | ✅ |
| 30 | TRAIN_PLAN_EVENT_ID | NUMBER(18,0) | 48 | ✅ |
| 31 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | 49 | ✅ |
| 32 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | 50 | ✅ |
| 33 | CREATE_USER_ID | VARCHAR(32) | 51 | ✅ |
| 34 | UPDATE_USER_ID | VARCHAR(32) | 52 | ✅ |
| 35 | REPORT_TIME_ZONE | VARCHAR(8) | 53 | ✅ |
| 36 | ESTRN_STND_REPORT_TMS | TIMESTAMP_NTZ(0) | 54 | ✅ |
| 37 | CONSIST_ORIGIN_NBR | NUMBER(6,0) | 55 | ✅ |
| 38 | CONSIST_NBR | VARCHAR(16) | 56 | ✅ |
| 39 | COMMON_YARDS_SITE_CD | VARCHAR(4) | 57 | ✅ |
| 40 | COMMON_YARDS_TRACK_NM | VARCHAR(20) | 58 | ✅ |
| 41 | MILE_NBR | NUMBER(8,3) | 59 | ✅ |
| 42 | SHRT_DSTR_NM | VARCHAR(40) | 60 | ✅ |
| 43 | RUN_NBR_CD | VARCHAR(12) | 61 | ✅ |
| 44 | CYCLE_SERIAL_NBR | NUMBER(10,0) | 62 | ✅ |
| 45 | EQPUN_ORNTN_CD | VARCHAR(4) | 63 | ✅ |
| 46 | INTRCH_MOVE_ATHRTY_CD | VARCHAR(8) | 64 | ✅ |
| 47 | COMMON_YARDS_TAG_ID | VARCHAR(24) | 65 | ✅ |
| 48 | LGLNT_ID | NUMBER(10,0) | 66 | ✅ |
| 49 | LGLNT_DATA_BASE_CN | VARCHAR(8) | 67 | ✅ |
| 50 | LGLNT_VRSN_NBR | NUMBER(5,0) | 68 | ✅ |
| 51 | CSTMR_ENTITY_IDNTFR_CD | VARCHAR(12) | 69 | ✅ |
| 52 | DMRG_TYPE_CD | VARCHAR(4) | 70 | ✅ |
| 53 | EVENT_STATUS_CD | VARCHAR(4) | 71 | ✅ |
| 54 | DMRG_RPRTNG_EXCPTN_CD | VARCHAR(4) | 72 | ✅ |
| 55 | TRAIN_TYPE_CD | VARCHAR(16) | 73 | ✅ |
| 56 | AAR_LBLTY_CNTNTY_MSG_CD | VARCHAR(4) | 74 | ✅ |
| 57 | TRNSFR_OF_LBLTY_CD | VARCHAR(8) | 75 | ✅ |
| 58 | CNST_DSTNTN_FSAC_CD | VARCHAR(20) | 76 | ✅ |
| 59 | MTP_ROUTE_POINT_SQNC_NBR | NUMBER(3,0) | 77 | ✅ |
| 60 | EQPMT_EVENT_REASON_CD | VARCHAR(32) | 78 | ✅ |
| 61 | AEIRD_NBR | VARCHAR(16) | 79 | ✅ |
| 62 | OPTRN_EVENT_ID | NUMBER(18,0) | 80 | ✅ |
| 63 | TRAVEL_DRCTN_CD | VARCHAR(20) | 81 | ✅ |
| 64 | SWITCH_LIST_NBR | NUMBER(5,0) | 82 | ✅ |
| 65 | TYES_TRAIN_ID | NUMBER(18,0) | 83 | ✅ |
| 66 | LGLNT_LCTN_STN_RFRNC_ID | NUMBER(18,0) | 84 | ✅ |
| 67 | INDSTR_RLTV_SQNC_NBR | NUMBER(2,0) | 85 | ✅ |
| 68 | TRACK_FSAC_CD | VARCHAR(20) | 86 | ✅ |
| 69 | TRACK_RFRNC_LCTN_ID | NUMBER(18,0) | 87 | ✅ |
| 70 | TRACK_SHORT_ENGL_NM | VARCHAR(40) | 88 | ✅ |
| 71 | LGLNT_VRSN_ID | NUMBER(18,0) | 89 | ✅ |
| 72 | AAR_SHPR_RJCTN_CD | VARCHAR(8) | 90 | ✅ |
| 73 | TRIP_END_IND | VARCHAR(4) | 91 | ✅ |
| 74 | EQPMT_ID | NUMBER(18,0) | 92 | ✅ |
| 75 | BLOCK_NM | VARCHAR(140) | 93 | ✅ |
| 76 | CLASS_CD | VARCHAR(28) | 94 | ✅ |
| 77 | CLASS_CODE_CTGRY_CD | VARCHAR(12) | 95 | ✅ |
| 78 | INTRNL_OPRTNL_HNDLNG_CD | VARCHAR(16) | 96 | ✅ |
| 79 | OPRTNL_HNDLNG_CD | VARCHAR(16) | 97 | ✅ |
| 80 | INDSTR_NM | VARCHAR(12) | 98 | ✅ |
| 81 | EXCTN_BLOCK_ID | NUMBER(18,0) | 99 | ✅ |
| 82 | EXCTN_BLOCK_CTGRY_CD | VARCHAR(40) | 100 | ✅ |
| 83 | EXCTN_BLOCK_ORIGIN_SCAC_CD | VARCHAR(16) | 101 | ✅ |
| 84 | EXCTN_BLOCK_ORIGIN_FSAC_CD | VARCHAR(20) | 102 | ✅ |
| 85 | EXCTN_BLOCK_DSTNTN_SCAC_CD | VARCHAR(16) | 103 | ✅ |
| 86 | EXCTN_BLOCK_DSTNTN_FSAC_CD | VARCHAR(20) | 104 | ✅ |
| 87 | EXCTN_PLND_BLOCK_NM | VARCHAR(140) | 105 | ✅ |
| 88 | EXCTN_RPRTD_BLOCK_NM | VARCHAR(140) | 106 | ✅ |
| 89 | SNW_OPERATION_TYPE | VARCHAR(1) | 107 | ✅ |
| 90 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | 108 | ✅ |

**Result: 90/90 columns mapped correctly (100%)**

### CDC Metadata Columns (6 additional):
| Column | Line | Purpose | Status |
|--------|------|---------|--------|
| CDC_OPERATION | 111 | Track INSERT/UPDATE/DELETE | ✅ |
| CDC_TIMESTAMP | 112 | When CDC captured | ✅ |
| IS_DELETED | 113 | Soft delete flag | ✅ |
| RECORD_CREATED_AT | 114 | Row creation time | ✅ |
| RECORD_UPDATED_AT | 115 | Row update time | ✅ |
| SOURCE_LOAD_BATCH_ID | 116 | Batch tracking | ✅ |

**Total: 96 columns (90 source + 6 CDC) ✅**

---

## 2. Primary Key Validation (100% ✅)

| Attribute | Source | Script | Match |
|-----------|--------|--------|-------|
| PK Column | EVENT_ID | EVENT_ID | ✅ |
| PK Type | Single | Single | ✅ |
| Target PK | Line 118 | `PRIMARY KEY (EVENT_ID)` | ✅ |
| MERGE ON | Lines 195, 402 | `ON tgt.EVENT_ID = src.EVENT_ID` | ✅ |

---

## 3. Stored Procedure Logic Validation (100% ✅)

### Pattern Compliance:
| Pattern | Required | Line(s) | Status |
|---------|----------|---------|--------|
| EXECUTE AS CALLER | ✅ | 143 | ✅ |
| Stream staleness detection | ✅ | 159-170 | ✅ |
| Stream recovery (SHOW_INITIAL_ROWS=TRUE) | ✅ | 178-181 | ✅ |
| Staging table pattern | ✅ | 340-363 | ✅ |
| 4 MERGE scenarios | ✅ | 405-651 | ✅ |
| All columns in UPDATE | ✅ | 407-500, 514-607 | ✅ |
| Error handling | ✅ | 658-661 | ✅ |
| Temp table cleanup | ✅ | 654, 660 | ✅ |

### MERGE Scenario Coverage:
| Scenario | Condition | Lines | Columns Updated | Status |
|----------|-----------|-------|-----------------|--------|
| UPDATE | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=TRUE | 405-500 | All 90 source + 5 CDC | ✅ |
| DELETE | CDC_ACTION='DELETE' AND CDC_IS_UPDATE=FALSE | 503-509 | 4 CDC (soft delete) | ✅ |
| RE-INSERT | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=FALSE (MATCHED) | 512-607 | All 90 source + 5 CDC | ✅ |
| INSERT | CDC_ACTION='INSERT' (NOT MATCHED) | 610-651 | All 96 columns | ✅ |

---

## 4. Syntax Validation (100% ✅)

| Check | Line(s) | Status |
|-------|---------|--------|
| CREATE TABLE IF NOT EXISTS | 18-119 | ✅ Valid |
| ALTER TABLE SET CHANGE_TRACKING | 124-127 | ✅ Valid |
| CREATE OR REPLACE STREAM | 132-135 | ✅ Valid |
| CREATE OR REPLACE PROCEDURE | 140-663 | ✅ Valid |
| MERGE statement (Recovery) | 183-331 | ✅ Valid |
| MERGE statement (Main) | 375-651 | ✅ Valid |
| CREATE OR REPLACE TASK | 668-676 | ✅ Valid |
| Variable declarations | 147-152 | ✅ Valid |
| Exception handling | 658-661 | ✅ Valid |
| ALTER TASK RESUME | 678 | ✅ Valid |

---

## 5. Coding Standards (100% ✅)

| Standard | Score | Evidence |
|----------|-------|----------|
| Consistent naming | 100% | All objects follow `EQPMV_RFEQP_MVMNT_EVENT*` pattern |
| Fully qualified names | 100% | `D_RAW.SADB.*`, `D_BRONZE.SADB.*` throughout |
| Clear section headers | 100% | STEP 1-5 clearly labeled |
| Meaningful comments | 100% | Each section has descriptive comments |
| Error messages | 100% | Informative RETURN values with timestamps |
| Batch ID format | 100% | `BATCH_YYYYMMDD_HH24MISS` |
| Indentation | 100% | Consistent formatting |
| Column alignment | 100% | Columns aligned in INSERT/UPDATE statements |

---

## 6. Task Configuration (100% ✅)

| Setting | Value | Best Practice | Status |
|---------|-------|---------------|--------|
| WAREHOUSE | INFA_INGEST_WH | ✅ Dedicated WH | ✅ |
| SCHEDULE | '5 MINUTE' | ✅ Reasonable CDC interval | ✅ |
| ALLOW_OVERLAPPING_EXECUTION | FALSE | ✅ Prevents conflicts | ✅ |
| WHEN condition | SYSTEM$STREAM_HAS_DATA() | ✅ Efficient | ✅ |
| ALTER TASK RESUME | Included (Line 678) | ✅ Task activated | ✅ |

---

## Final Verification Checklist

| Requirement | Status |
|-------------|--------|
| All 90 source columns present in target table | ✅ |
| All 90 source columns in staging table | ✅ |
| All 90 source columns in UPDATE scenarios | ✅ |
| All 96 columns in INSERT scenarios | ✅ |
| Primary key (EVENT_ID) correctly defined | ✅ |
| MERGE ON clause uses PK | ✅ |
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
| Primary Key | 10/10 |
| SP Logic | 20/20 |
| Syntax | 10/10 |
| Coding Standards | 10/10 |
| Task Configuration | 5/5 |
| **Total** | **100/100** |

---

## Summary

The `EQPMV_RFEQP_MVMNT_EVENT.sql` script is **100% aligned** with the source table definition:

- ✅ All 90 source columns correctly mapped with exact data types
- ✅ Primary key (EVENT_ID) correctly implemented
- ✅ All SP logic patterns implemented correctly
- ✅ All 4 MERGE scenarios handle full column updates
- ✅ Stream staleness detection and recovery
- ✅ Proper error handling with temp table cleanup
- ✅ Task properly configured with SYSTEM$STREAM_HAS_DATA()

**No modifications required. Ready for production implementation.**

---

**Reviewed By:** Snowflake CDC Expert  
**Date:** 2026-02-24  
**Status:** ✅ PRODUCTION READY
