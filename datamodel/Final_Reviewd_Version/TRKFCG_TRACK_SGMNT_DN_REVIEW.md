# TRKFCG_TRACK_SGMNT_DN Script Review Report

**Review Date:** 2026-02-24  
**Script:** Scripts/Final/TRKFCG_TRACK_SGMNT_DN.sql  
**Source Table:** D_RAW.SADB.TRKFCG_TRACK_SGMNT_DN_BASE  
**Target Table:** D_BRONZE.SADB.TRKFCG_TRACK_SGMNT_DN

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

### Source Table Columns (58 columns):
| # | Source Column | Source Type | Script Line | Match |
|---|---------------|-------------|-------------|-------|
| 1 | GRPHC_OBJECT_VRSN_ID | NUMBER(18,0) NOT NULL | 19 | ✅ |
| 2 | VRSN_CREATE_TMS | TIMESTAMP_NTZ(0) | 20 | ✅ |
| 3 | VRSN_USER_ID | VARCHAR(32) | 21 | ✅ |
| 4 | FIRST_GRPHC_OBJECT_VRSN_ID | NUMBER(18,0) | 22 | ✅ |
| 5 | PRVS_GRPHC_OBJECT_VRSN_ID | NUMBER(18,0) | 23 | ✅ |
| 6 | GRPHC_OBJECT_MDFCTN_CD | VARCHAR(36) | 24 | ✅ |
| 7 | GRPHC_OBJECT_STATUS_CD | VARCHAR(32) | 25 | ✅ |
| 8 | GRPHC_TRNSCT_ID | NUMBER(18,0) | 26 | ✅ |
| 9 | TRACK_SGMNT_ID | NUMBER(18,0) | 27 | ✅ |
| 10 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | 28 | ✅ |
| 11 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | 29 | ✅ |
| 12 | CREATE_USER_ID | VARCHAR(32) | 30 | ✅ |
| 13 | UPDATE_USER_ID | VARCHAR(32) | 31 | ✅ |
| 14 | TRACK_AREA_BNDRY_ST | VARCHAR(160) | 32 | ✅ |
| 15 | EFCTV_TMS | TIMESTAMP_NTZ(0) | 33 | ✅ |
| 16 | ENGLSH_BASE_NM | VARCHAR(320) | 34 | ✅ |
| 17 | LONG_ENGLSH_NM | VARCHAR(320) | 35 | ✅ |
| 18 | PRMRY_RFRNC_IND | VARCHAR(4) | 36 | ✅ |
| 19 | SHORT_ENGLSH_NM | VARCHAR(40) | 37 | ✅ |
| 20 | DATA_SOURCE_CD | VARCHAR(40) | 38 | ✅ |
| 21 | NAME_GNRTN_RULE_ID | NUMBER(18,0) | 39 | ✅ |
| 22 | ORGNL_DATA_SOURCE_CD | VARCHAR(40) | 40 | ✅ |
| 23 | PHYSCL_TRACK_ID | NUMBER(18,0) | 41 | ✅ |
| 24 | TRACK_CD | VARCHAR(40) | 42 | ✅ |
| 25 | AMAP_TRACK_RFRNC_NM | VARCHAR(64) | 43 | ✅ |
| 26 | AMAP_MAIN_TRACK_RFRNC_NM | VARCHAR(64) | 44 | ✅ |
| 27 | AMAP_TRACK_SIDE_CD | VARCHAR(20) | 45 | ✅ |
| 28 | AMAP_RFRNC_HIGH_MILES_QTY | NUMBER(8,3) | 46 | ✅ |
| 29 | AMAP_TRACK_SQNC_NBR | NUMBER(3,1) | 47 | ✅ |
| 30 | EXPIRY_TMS | TIMESTAMP_NTZ(0) | 48 | ✅ |
| 31 | FRENCH_BASE_NM | VARCHAR(320) | 49 | ✅ |
| 32 | LONG_FRENCH_NM | VARCHAR(320) | 50 | ✅ |
| 33 | ORGNL_SHORT_ENGLSH_NM | VARCHAR(40) | 51 | ✅ |
| 34 | RFRNC_HIGH_MILES_QTY | NUMBER(8,3) | 52 | ✅ |
| 35 | RFRNC_LOW_MILES_QTY | NUMBER(8,3) | 53 | ✅ |
| 36 | SBDVSN_RFRNC_LOW_MILES_QTY | NUMBER(8,3) | 54 | ✅ |
| 37 | SHORT_FRENCH_NM | VARCHAR(40) | 55 | ✅ |
| 38 | STRG_FEET_QTY | NUMBER(5,0) | 56 | ✅ |
| 39 | YARD_TRACK_NM | VARCHAR(24) | 57 | ✅ |
| 40 | YARD_SUB_TRACK_NM | VARCHAR(8) | 58 | ✅ |
| 41 | REGION_ID | NUMBER(18,0) | 59 | ✅ |
| 42 | TRACK_MLG_CD | VARCHAR(4) | 60 | ✅ |
| 43 | TRACK_USAGE_CD | VARCHAR(40) | 61 | ✅ |
| 44 | UMBRL_STN_ID | NUMBER(18,0) | 62 | ✅ |
| 45 | OCS_DSPTCH_CD | VARCHAR(4) | 63 | ✅ |
| 46 | TRFC_DRCTN_CD | VARCHAR(20) | 64 | ✅ |
| 47 | SUB_YARD_NM | VARCHAR(8) | 65 | ✅ |
| 48 | INSPCT_FRQNCY_CD | VARCHAR(40) | 66 | ✅ |
| 49 | POST_START_NM | VARCHAR(64) | 67 | ✅ |
| 50 | POST_START_DSTNC_MILES_QTY | NUMBER(8,3) | 68 | ✅ |
| 51 | POST_END_NM | VARCHAR(64) | 69 | ✅ |
| 52 | POST_END_DSTNC_MILES_QTY | NUMBER(8,3) | 70 | ✅ |
| 53 | NTWRK_SNAP_CD | NUMBER(1,0) | 71 | ✅ |
| 54 | PSTN_MODE_CD | NUMBER(1,0) | 72 | ✅ |
| 55 | OPRTNG_CPCTY_FEET_QTY | VARCHAR(255) | 73 | ✅ |
| 56 | OPRTNG_CPCTY_CARS_QTY | VARCHAR(255) | 74 | ✅ |
| 57 | SNW_OPERATION_TYPE | VARCHAR(1) | 75 | ✅ |
| 58 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | 76 | ✅ |

**Result: 58/58 columns mapped correctly (100%)**

### CDC Metadata Columns (6 additional):
| Column | Line | Purpose | Status |
|--------|------|---------|--------|
| CDC_OPERATION | 78 | Track INSERT/UPDATE/DELETE | ✅ |
| CDC_TIMESTAMP | 79 | When CDC captured | ✅ |
| IS_DELETED | 80 | Soft delete flag | ✅ |
| RECORD_CREATED_AT | 81 | Row creation time | ✅ |
| RECORD_UPDATED_AT | 82 | Row update time | ✅ |
| SOURCE_LOAD_BATCH_ID | 83 | Batch tracking | ✅ |

**Total: 64 columns (58 source + 6 CDC) ✅**

---

## 2. Primary Key Validation (100% ✅)

| Attribute | Source | Script | Match |
|-----------|--------|--------|-------|
| PK Column | GRPHC_OBJECT_VRSN_ID | GRPHC_OBJECT_VRSN_ID | ✅ |
| PK Type | Single | Single | ✅ |
| Target PK | Line 85 | `PRIMARY KEY (GRPHC_OBJECT_VRSN_ID)` | ✅ |
| MERGE ON | Lines 162, 313 | `ON tgt.GRPHC_OBJECT_VRSN_ID = src.GRPHC_OBJECT_VRSN_ID` | ✅ |

---

## 3. Stored Procedure Logic Validation (100% ✅)

### Pattern Compliance:
| Pattern | Required | Line(s) | Status |
|---------|----------|---------|--------|
| EXECUTE AS CALLER | ✅ | 110 | ✅ |
| Stream staleness detection | ✅ | 126-137 | ✅ |
| Stream recovery (SHOW_INITIAL_ROWS=TRUE) | ✅ | 145-148 | ✅ |
| Staging table pattern | ✅ | 263-280 | ✅ |
| 4 MERGE scenarios | ✅ | 316-486 | ✅ |
| All columns in UPDATE | ✅ | 318-379, 393-454 | ✅ |
| Error handling | ✅ | 493-496 | ✅ |
| Temp table cleanup | ✅ | 489, 495 | ✅ |

### MERGE Scenario Coverage:
| Scenario | Condition | Lines | Columns Updated | Status |
|----------|-----------|-------|-----------------|--------|
| UPDATE | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=TRUE | 316-379 | All 57 non-PK source + 5 CDC | ✅ |
| DELETE | CDC_ACTION='DELETE' AND CDC_IS_UPDATE=FALSE | 382-388 | 4 CDC (soft delete) | ✅ |
| RE-INSERT | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=FALSE (MATCHED) | 391-454 | All 57 non-PK source + 5 CDC | ✅ |
| INSERT | CDC_ACTION='INSERT' (NOT MATCHED) | 457-486 | All 64 columns | ✅ |

### Staging Table Column Verification:
| Location | Lines | All 58 Source Columns | Status |
|----------|-------|----------------------|--------|
| Staging SELECT | 265-276 | ✅ Complete | ✅ |
| MERGE Source SELECT | 295-306 | ✅ Complete | ✅ |
| Recovery MERGE INSERT | 227-253 | ✅ Complete | ✅ |
| Main MERGE INSERT | 459-485 | ✅ Complete | ✅ |

---

## 4. Syntax Validation (100% ✅)

| Check | Line(s) | Status |
|-------|---------|--------|
| CREATE TABLE IF NOT EXISTS | 18-86 | ✅ Valid |
| ALTER TABLE SET CHANGE_TRACKING | 91-94 | ✅ Valid |
| CREATE OR REPLACE STREAM | 99-102 | ✅ Valid |
| CREATE OR REPLACE PROCEDURE | 107-498 | ✅ Valid |
| MERGE statement (Recovery) | 150-254 | ✅ Valid |
| MERGE statement (Main) | 292-486 | ✅ Valid |
| CREATE OR REPLACE TASK | 503-511 | ✅ Valid |
| Variable declarations | 113-119 | ✅ Valid |
| Exception handling | 493-496 | ✅ Valid |
| ALTER TASK RESUME | 513 | ✅ Valid |

---

## 5. Coding Standards (100% ✅)

| Standard | Score | Evidence |
|----------|-------|----------|
| Consistent naming | 100% | All objects follow `TRKFCG_TRACK_SGMNT_DN*` pattern |
| Fully qualified names | 100% | `D_RAW.SADB.*`, `D_BRONZE.SADB.*` throughout |
| Clear section headers | 100% | STEP 1-5 clearly labeled |
| Meaningful comments | 100% | Each section has descriptive comments |
| Error messages | 100% | Informative RETURN values with timestamps |
| Batch ID format | 100% | `BATCH_YYYYMMDD_HH24MISS` |
| Indentation | 100% | Consistent formatting |
| Column alignment | 100% | Columns aligned in INSERT/UPDATE statements |
| Header documentation | 100% | Complete header with column count (Line 11) |

---

## 6. Task Configuration (100% ✅)

| Setting | Value | Best Practice | Status |
|---------|-------|---------------|--------|
| WAREHOUSE | INFA_INGEST_WH | ✅ Dedicated WH | ✅ |
| SCHEDULE | '5 MINUTE' | ✅ Reasonable CDC interval | ✅ |
| ALLOW_OVERLAPPING_EXECUTION | FALSE | ✅ Prevents conflicts | ✅ |
| WHEN condition | SYSTEM$STREAM_HAS_DATA() | ✅ Efficient | ✅ |
| ALTER TASK RESUME | Included (Line 513) | ✅ Task activated | ✅ |

---

## 7. Change Tracking Configuration (100% ✅)

| Setting | Value | Line | Status |
|---------|-------|------|--------|
| CHANGE_TRACKING | TRUE | 92 | ✅ |
| DATA_RETENTION_TIME_IN_DAYS | 45 | 93 | ✅ |
| MAX_DATA_EXTENSION_TIME_IN_DAYS | 15 | 94 | ✅ |

---

## 8. Header Documentation Accuracy ✅

| Item | Header Value | Actual | Status |
|------|--------------|--------|--------|
| Total Columns | "58 source + 6 CDC = 64" | 58 + 6 = 64 | ✅ |
| Primary Key | "GRPHC_OBJECT_VRSN_ID (Single)" | Single PK | ✅ |

---

## Final Verification Checklist

| Requirement | Status |
|-------------|--------|
| All 58 source columns present in target table | ✅ |
| All 58 source columns in staging table | ✅ |
| All 58 source columns in UPDATE scenarios | ✅ |
| All 64 columns in INSERT scenarios | ✅ |
| Primary key (GRPHC_OBJECT_VRSN_ID) correctly defined | ✅ |
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

The `TRKFCG_TRACK_SGMNT_DN.sql` script is **100% aligned** with the source table definition:

- ✅ All 58 source columns correctly mapped with exact data types
- ✅ 6 CDC metadata columns properly added (total 64)
- ✅ Primary key (GRPHC_OBJECT_VRSN_ID) correctly implemented
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
