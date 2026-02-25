# TRAIN_CNST_DTL_RAIL_EQPT Script Review Report

**Review Date:** 2026-02-24  
**Script:** Scripts/Final/TRAIN_CNST_DTL_RAIL_EQPT.sql  
**Source Table:** D_RAW.SADB.TRAIN_CNST_DTL_RAIL_EQPT_BASE  
**Target Table:** D_BRONZE.SADB.TRAIN_CNST_DTL_RAIL_EQPT

---

## Executive Summary

| Category | Score | Status |
|----------|-------|--------|
| **Column Mapping** | 100% | ✅ PERFECT |
| **Data Type Mapping** | 100% | ✅ PERFECT |
| **Primary Key (3-COL COMPOSITE)** | 100% | ✅ CORRECT |
| **SP Logic** | 100% | ✅ COMPLETE |
| **Syntax** | 100% | ✅ VALID |
| **Coding Standards** | 100% | ✅ EXCELLENT |
| **Overall Score** | **100/100** | ✅ **PRODUCTION READY** |

---

## 1. Column Mapping Validation (100% ✅)

### Source Table Columns (77 columns):
| # | Source Column | Source Type | Script Line | Match |
|---|---------------|-------------|-------------|-------|
| 1 | TRAIN_CNST_SMRY_ID | NUMBER(18,0) NOT NULL | 19 | ✅ |
| 2 | TRAIN_CNST_SMRY_VRSN_NBR | NUMBER(4,0) NOT NULL | 20 | ✅ |
| 3 | SQNC_NBR | NUMBER(4,0) NOT NULL | 21 | ✅ |
| 4 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | 22 | ✅ |
| 5 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | 23 | ✅ |
| 6 | CREATE_USER_ID | VARCHAR(32) | 24 | ✅ |
| 7 | UPDATE_USER_ID | VARCHAR(32) | 25 | ✅ |
| 8 | MARK_CD | VARCHAR(16) | 26 | ✅ |
| 9 | EQPUN_NBR | VARCHAR(40) | 27 | ✅ |
| 10 | AAR_CAR_TYPE_CD | VARCHAR(16) | 28 | ✅ |
| 11 | OTSD_LENGTH_FEET_QTY | NUMBER(10,0) | 29 | ✅ |
| 12 | OTSD_LENGTH_INCHES_QTY | NUMBER(10,0) | 30 | ✅ |
| 13 | TARE_WEIGHT_TONS_QTY | NUMBER(10,2) | 31 | ✅ |
| 14 | ORIGIN_SCAC_CD | VARCHAR(16) | 32 | ✅ |
| 15 | ORIGIN_FSAC_CD | VARCHAR(20) | 33 | ✅ |
| 16 | ORIGIN_TRSTN_VRSN_NBR | NUMBER(5,0) | 34 | ✅ |
| 17 | DSTNTN_SCAC_CD | VARCHAR(16) | 35 | ✅ |
| 18 | DSTNTN_FSAC_CD | VARCHAR(20) | 36 | ✅ |
| 19 | DSTNTN_TRSTN_VRSN_NBR | NUMBER(5,0) | 37 | ✅ |
| 20 | CYCLE_SERIAL_NBR | NUMBER(10,0) | 38 | ✅ |
| 21 | LOAD_EMPTY_IND | VARCHAR(4) | 39 | ✅ |
| 22 | NET_WEIGHT_TONS_QTY | NUMBER(10,0) | 40 | ✅ |
| 23 | CLRNC_PLATE_CD | VARCHAR(4) | 41 | ✅ |
| 24 | BAD_ORDER_CAR_GRADE_CD | VARCHAR(4) | 42 | ✅ |
| 25 | BAD_ORDER_REASON_CD | VARCHAR(16) | 43 | ✅ |
| 26 | SHPPR_CPRS_CSTMR_ID | VARCHAR(32) | 44 | ✅ |
| 27 | CNSGN_CPRS_CSTMR_ID | VARCHAR(32) | 45 | ✅ |
| 28 | FSTWY_WYBL_NBR | NUMBER(6,0) | 46 | ✅ |
| 29 | WYBL_TMS | TIMESTAMP_NTZ(0) | 47 | ✅ |
| 30 | STCC_CD | VARCHAR(28) | 48 | ✅ |
| 31 | HZCMD_MTRL_RSPNS_CD | NUMBER(7,0) | 49 | ✅ |
| 32 | ON_JNCTN_SCAC_CD | VARCHAR(16) | 50 | ✅ |
| 33 | ON_JNCTN_FSAC_CD | VARCHAR(20) | 51 | ✅ |
| 34 | ON_JNCTN_TRSTN_VRSN_NBR | NUMBER(5,0) | 52 | ✅ |
| 35 | RCVD_FROM_SCAC_CD | VARCHAR(16) | 53 | ✅ |
| 36 | OFF_JNCTN_SCAC_CD | VARCHAR(16) | 54 | ✅ |
| 37 | OFF_JNCTN_FSAC_CD | VARCHAR(20) | 55 | ✅ |
| 38 | OFF_JNCTN_TRSTN_VRSN_NBR | NUMBER(5,0) | 56 | ✅ |
| 39 | DLVR_TO_SCAC_CD | VARCHAR(16) | 57 | ✅ |
| 40 | RFEQP_ASGN_NBR | NUMBER(18,0) | 58 | ✅ |
| 41 | RFEQP_DATA_BASE_CN | VARCHAR(8) | 59 | ✅ |
| 42 | SHPMT_NBR | NUMBER(18,0) | 60 | ✅ |
| 43 | SHPMT_DATA_BASE_CN | VARCHAR(8) | 61 | ✅ |
| 44 | ORNTN_CD | VARCHAR(4) | 62 | ✅ |
| 45 | LEAD_LCMTV_IND | VARCHAR(4) | 63 | ✅ |
| 46 | LCMTV_CNST_STATUS_CD | VARCHAR(40) | 64 | ✅ |
| 47 | LCMTV_MCHNCL_STATUS_CD | VARCHAR(40) | 65 | ✅ |
| 48 | BRNG_TYPE_CD | VARCHAR(4) | 66 | ✅ |
| 49 | LIFT_MVMNT_EVENT_ID | NUMBER(18,0) | 67 | ✅ |
| 50 | LIFT_EVENT_TMS | TIMESTAMP_NTZ(0) | 68 | ✅ |
| 51 | LIFT_SCAC_CD | VARCHAR(16) | 69 | ✅ |
| 52 | LIFT_FSAC_CD | VARCHAR(20) | 70 | ✅ |
| 53 | LIFT_TRSTN_VRSN_NBR | NUMBER(5,0) | 71 | ✅ |
| 54 | SET_MVMNT_EVENT_ID | NUMBER(18,0) | 72 | ✅ |
| 55 | SET_EVENT_TMS | TIMESTAMP_NTZ(0) | 73 | ✅ |
| 56 | SET_SCAC_CD | VARCHAR(16) | 74 | ✅ |
| 57 | SET_FSAC_CD | VARCHAR(20) | 75 | ✅ |
| 58 | SET_TRSTN_VRSN_NBR | NUMBER(5,0) | 76 | ✅ |
| 59 | CNST_CHNG_EVENT_TMS | TIMESTAMP_NTZ(0) | 77 | ✅ |
| 60 | CNST_CHNG_MVMNT_EVENT_ID | NUMBER(18,0) | 78 | ✅ |
| 61 | CNST_CHNG_SCAC_CD | VARCHAR(16) | 79 | ✅ |
| 62 | CNST_CHNG_FSAC_CD | VARCHAR(20) | 80 | ✅ |
| 63 | CNST_CHNG_TRSTN_VRSN_NBR | NUMBER(5,0) | 81 | ✅ |
| 64 | CARE_OF_PARTY_CPRS_CSTMR_ID | VARCHAR(32) | 82 | ✅ |
| 65 | LIFT_RPRTNG_TM_ZN_CD | VARCHAR(8) | 83 | ✅ |
| 66 | LIFT_RPRTNG_TM_ZN_YR_NBR | NUMBER(4,0) | 84 | ✅ |
| 67 | SET_RPRTNG_TM_ZN_CD | VARCHAR(8) | 85 | ✅ |
| 68 | SET_RPRTNG_TM_ZN_YR_NBR | NUMBER(4,0) | 86 | ✅ |
| 69 | CNST_CHNG_RPRTNG_TM_ZN_CD | VARCHAR(8) | 87 | ✅ |
| 70 | CNST_CHNG_RPRTNG_TM_ZN_YR_NBR | NUMBER(4,0) | 88 | ✅ |
| 71 | INTRCH_MVMNT_ATHRTY_CD | VARCHAR(8) | 89 | ✅ |
| 72 | OCS_UPDATE_CD | VARCHAR(4) | 90 | ✅ |
| 73 | EQPMT_ASGNMN_ID | NUMBER(18,0) | 91 | ✅ |
| 74 | SHPMT_ID | NUMBER(18,0) | 92 | ✅ |
| 75 | CNST_LCM_OPRSTS_ID | NUMBER(18,0) | 93 | ✅ |
| 76 | SNW_OPERATION_TYPE | VARCHAR(1) | 94 | ✅ |
| 77 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | 95 | ✅ |

**Result: 77/77 columns mapped correctly (100%)**

### CDC Metadata Columns (6 additional):
| Column | Line | Purpose | Status |
|--------|------|---------|--------|
| CDC_OPERATION | 98 | Track INSERT/UPDATE/DELETE | ✅ |
| CDC_TIMESTAMP | 99 | When CDC captured | ✅ |
| IS_DELETED | 100 | Soft delete flag | ✅ |
| RECORD_CREATED_AT | 101 | Row creation time | ✅ |
| RECORD_UPDATED_AT | 102 | Row update time | ✅ |
| SOURCE_LOAD_BATCH_ID | 103 | Batch tracking | ✅ |

**Total: 83 columns (77 source + 6 CDC) ✅**

---

## 2. Primary Key Validation (100% ✅) - 3-COLUMN COMPOSITE KEY

| Attribute | Source | Script | Match |
|-----------|--------|--------|-------|
| PK Column 1 | TRAIN_CNST_SMRY_ID | TRAIN_CNST_SMRY_ID | ✅ |
| PK Column 2 | TRAIN_CNST_SMRY_VRSN_NBR | TRAIN_CNST_SMRY_VRSN_NBR | ✅ |
| PK Column 3 | SQNC_NBR | SQNC_NBR | ✅ |
| PK Type | 3-COLUMN COMPOSITE | 3-COLUMN COMPOSITE | ✅ |
| Target PK | Line 105 | `PRIMARY KEY (TRAIN_CNST_SMRY_ID, TRAIN_CNST_SMRY_VRSN_NBR, SQNC_NBR)` | ✅ |

### MERGE ON Clause Verification (CRITICAL for 3-Column Composite PK):
| Location | Lines | All 3 PK Columns Used | Status |
|----------|-------|----------------------|--------|
| Recovery LEFT JOIN | 178-180 | ✅ `src.TRAIN_CNST_SMRY_ID = tgt.TRAIN_CNST_SMRY_ID AND src.TRAIN_CNST_SMRY_VRSN_NBR = tgt.TRAIN_CNST_SMRY_VRSN_NBR AND src.SQNC_NBR = tgt.SQNC_NBR` | ✅ |
| Recovery MERGE ON | 184-186 | ✅ All 3 columns | ✅ |
| Main MERGE ON | 382-384 | ✅ All 3 columns | ✅ |

---

## 3. Stored Procedure Logic Validation (100% ✅)

### Pattern Compliance:
| Pattern | Required | Line(s) | Status |
|---------|----------|---------|--------|
| EXECUTE AS CALLER | ✅ | 130 | ✅ |
| Stream staleness detection | ✅ | 146-157 | ✅ |
| Stream recovery (SHOW_INITIAL_ROWS=TRUE) | ✅ | 165-168 | ✅ |
| Staging table pattern | ✅ | 318-342 | ✅ |
| 4 MERGE scenarios | ✅ | 387-605 | ✅ |
| All columns in UPDATE | ✅ | 388-467, 480-559 | ✅ |
| Error handling | ✅ | 612-615 | ✅ |
| Temp table cleanup | ✅ | 608, 614 | ✅ |

### MERGE Scenario Coverage:
| Scenario | Condition | Lines | Columns Updated | Status |
|----------|-----------|-------|-----------------|--------|
| UPDATE | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=TRUE | 387-467 | All 74 non-PK source + 5 CDC | ✅ |
| DELETE | CDC_ACTION='DELETE' AND CDC_IS_UPDATE=FALSE | 470-476 | 4 CDC (soft delete) | ✅ |
| RE-INSERT | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=FALSE (MATCHED) | 479-559 | All 74 non-PK source + 5 CDC | ✅ |
| INSERT | CDC_ACTION='INSERT' (NOT MATCHED) | 562-605 | All 83 columns | ✅ |

### Staging Table Column Verification:
| Location | Lines | All 77 Source Columns | Status |
|----------|-------|----------------------|--------|
| Staging SELECT | 319-342 | ✅ Complete | ✅ |
| MERGE Source SELECT | 356-381 | ✅ Complete | ✅ |
| Recovery MERGE INSERT | 267-309 | ✅ Complete | ✅ |
| Main MERGE INSERT | 563-605 | ✅ Complete | ✅ |

---

## 4. Syntax Validation (100% ✅)

| Check | Line(s) | Status |
|-------|---------|--------|
| CREATE TABLE IF NOT EXISTS | 18-106 | ✅ Valid |
| ALTER TABLE SET CHANGE_TRACKING | 111-114 | ✅ Valid |
| CREATE OR REPLACE STREAM | 119-122 | ✅ Valid |
| CREATE OR REPLACE PROCEDURE | 127-617 | ✅ Valid |
| MERGE statement (Recovery) | 170-309 | ✅ Valid |
| MERGE statement (Main) | 354-605 | ✅ Valid |
| CREATE OR REPLACE TASK | 622-630 | ✅ Valid |
| Variable declarations | 133-139 | ✅ Valid |
| Exception handling | 612-615 | ✅ Valid |
| ALTER TASK RESUME | 632 | ✅ Valid |

---

## 5. Coding Standards (100% ✅)

| Standard | Score | Evidence |
|----------|-------|----------|
| Consistent naming | 100% | All objects follow `TRAIN_CNST_DTL_RAIL_EQPT*` pattern |
| Fully qualified names | 100% | `D_RAW.SADB.*`, `D_BRONZE.SADB.*` throughout |
| Clear section headers | 100% | STEP 1-5 clearly labeled |
| Meaningful comments | 100% | Each section has descriptive comments |
| Error messages | 100% | Informative RETURN values with timestamps |
| Batch ID format | 100% | `BATCH_YYYYMMDD_HH24MISS` |
| Indentation | 100% | Consistent formatting |
| Column alignment | 100% | Columns aligned in INSERT/UPDATE statements |
| Header documentation | 100% | Complete header with 3-col PK info (Line 10) |

---

## 6. Task Configuration (100% ✅)

| Setting | Value | Best Practice | Status |
|---------|-------|---------------|--------|
| WAREHOUSE | INFA_INGEST_WH | ✅ Dedicated WH | ✅ |
| SCHEDULE | '5 MINUTE' | ✅ Reasonable CDC interval | ✅ |
| ALLOW_OVERLAPPING_EXECUTION | FALSE | ✅ Prevents conflicts | ✅ |
| WHEN condition | SYSTEM$STREAM_HAS_DATA() | ✅ Efficient | ✅ |
| ALTER TASK RESUME | Included (Line 632) | ✅ Task activated | ✅ |

---

## 7. Change Tracking Configuration (100% ✅)

| Setting | Value | Line | Status |
|---------|-------|------|--------|
| CHANGE_TRACKING | TRUE | 112 | ✅ |
| DATA_RETENTION_TIME_IN_DAYS | 45 | 113 | ✅ |
| MAX_DATA_EXTENSION_TIME_IN_DAYS | 15 | 114 | ✅ |

---

## 8. Special Considerations - 3-Column Composite Primary Key ✅

This table uses a **3-COLUMN COMPOSITE PRIMARY KEY** consisting of:
- `TRAIN_CNST_SMRY_ID`
- `TRAIN_CNST_SMRY_VRSN_NBR`
- `SQNC_NBR`

### Verification Points:
| Check | Location | Status |
|-------|----------|--------|
| All 3 PK columns NOT NULL in target | Lines 19-21 | ✅ |
| 3-Column Composite PK defined in target | Line 105 | ✅ |
| Recovery LEFT JOIN uses all 3 columns | Lines 178-180 | ✅ |
| Recovery MERGE ON uses all 3 columns | Lines 184-186 | ✅ |
| Main MERGE ON uses all 3 columns | Lines 382-384 | ✅ |
| All 3 columns in staging table | Lines 320 | ✅ |
| All 3 columns in INSERT statements | Lines 268, 289, 564, 585 | ✅ |

---

## Final Verification Checklist

| Requirement | Status |
|-------------|--------|
| All 77 source columns present in target table | ✅ |
| All 77 source columns in staging table | ✅ |
| All 77 source columns in UPDATE scenarios | ✅ |
| All 83 columns in INSERT scenarios | ✅ |
| 3-Column Composite PK defined correctly | ✅ |
| MERGE ON clause uses ALL 3 PK columns | ✅ |
| Recovery JOIN uses ALL 3 PK columns | ✅ |
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
| Primary Key (3-Col Composite) | 10/10 |
| SP Logic | 20/20 |
| Syntax | 10/10 |
| Coding Standards | 10/10 |
| Task Configuration | 5/5 |
| **Total** | **100/100** |

---

## Summary

The `TRAIN_CNST_DTL_RAIL_EQPT.sql` script is **100% aligned** with the source table definition:

- ✅ All 77 source columns correctly mapped with exact data types
- ✅ 6 CDC metadata columns properly added (total 83)
- ✅ **3-Column Composite Primary Key** (TRAIN_CNST_SMRY_ID + TRAIN_CNST_SMRY_VRSN_NBR + SQNC_NBR) correctly implemented
- ✅ All MERGE ON clauses use ALL THREE primary key columns
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
