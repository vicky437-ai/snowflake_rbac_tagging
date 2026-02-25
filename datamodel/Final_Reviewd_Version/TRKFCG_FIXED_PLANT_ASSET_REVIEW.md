# TRKFCG_FIXED_PLANT_ASSET Script Review Report

**Review Date:** 2026-02-24  
**Script:** Scripts/Final/TRKFCG_FIXED_PLANT_ASSET.sql  
**Source Table:** D_RAW.SADB.TRKFCG_FIXED_PLANT_ASSET_BASE  
**Target Table:** D_BRONZE.SADB.TRKFCG_FIXED_PLANT_ASSET

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

### Source Table Columns (52 columns):
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
| 9 | FIXED_PLANT_ASSET_ID | NUMBER(18,0) | 27 | ✅ |
| 10 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | 28 | ✅ |
| 11 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | 29 | ✅ |
| 12 | CREATE_USER_ID | VARCHAR(32) | 30 | ✅ |
| 13 | UPDATE_USER_ID | VARCHAR(32) | 31 | ✅ |
| 14 | CRNT_DATA_SOURCE_CD | VARCHAR(40) | 32 | ✅ |
| 15 | ORGNL_DATA_SOURCE_CD | VARCHAR(40) | 33 | ✅ |
| 16 | ASSET_CD | VARCHAR(16) | 34 | ✅ |
| 17 | NAME_GNRTN_RULE_ID | NUMBER(18,0) | 35 | ✅ |
| 18 | ASSET_STATUS_CD | VARCHAR(40) | 36 | ✅ |
| 19 | ENGLSH_BASE_NM | VARCHAR(320) | 37 | ✅ |
| 20 | FRENCH_BASE_NM | VARCHAR(320) | 38 | ✅ |
| 21 | LONG_ENGLSH_NM | VARCHAR(320) | 39 | ✅ |
| 22 | SHORT_ENGLSH_NM | VARCHAR(40) | 40 | ✅ |
| 23 | LONG_FRENCH_NM | VARCHAR(320) | 41 | ✅ |
| 24 | SHORT_FRENCH_NM | VARCHAR(40) | 42 | ✅ |
| 25 | SAP_USER_ID | VARCHAR(32) | 43 | ✅ |
| 26 | SCAC_CD | VARCHAR(16) | 44 | ✅ |
| 27 | FSAC_CD | VARCHAR(20) | 45 | ✅ |
| 28 | NODE_5_SPELL_NM | VARCHAR(20) | 46 | ✅ |
| 29 | NODE_8_SPELL_NM | VARCHAR(40) | 47 | ✅ |
| 30 | ALK_IMPRT_NBR | NUMBER(5,0) | 48 | ✅ |
| 31 | NODE_IMPRT_NBR | NUMBER(5,0) | 49 | ✅ |
| 32 | NODE_NBR | NUMBER(6,0) | 50 | ✅ |
| 33 | CRSNG_ID | VARCHAR(40) | 51 | ✅ |
| 34 | CRSNG_CD | VARCHAR(40) | 52 | ✅ |
| 35 | CRSNG_PRTCTN_CD | VARCHAR(52) | 53 | ✅ |
| 36 | SIGN_ID | NUMBER(4,0) | 54 | ✅ |
| 37 | SIGN_CD | VARCHAR(12) | 55 | ✅ |
| 38 | CTC_SIGNAL_ID | VARCHAR(24) | 56 | ✅ |
| 39 | CTC_AEI_READER_IND | VARCHAR(4) | 57 | ✅ |
| 40 | CTC_OS_IND | VARCHAR(4) | 58 | ✅ |
| 41 | SWITCH_CD | VARCHAR(40) | 59 | ✅ |
| 42 | TRNT_NUMBER_CD | VARCHAR(8) | 60 | ✅ |
| 43 | TRNT_SPEED_MPH_QTY | NUMBER(2,0) | 61 | ✅ |
| 44 | RVRSBL_IND | VARCHAR(4) | 62 | ✅ |
| 45 | NODE_DATA_SOURCE_CD | VARCHAR(40) | 63 | ✅ |
| 46 | PRMRY_REGION_ID | NUMBER(18,0) | 64 | ✅ |
| 47 | ALTRNT_LONG_ENGLSH_NM | VARCHAR(320) | 65 | ✅ |
| 48 | ALTRNT_LONG_FRENCH_NM | VARCHAR(320) | 66 | ✅ |
| 49 | OCS_LONG_ENGLSH_NM | VARCHAR(320) | 67 | ✅ |
| 50 | OCS_LONG_FRENCH_NM | VARCHAR(320) | 68 | ✅ |
| 51 | SNW_OPERATION_TYPE | VARCHAR(1) | 69 | ✅ |
| 52 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | 70 | ✅ |

**Result: 52/52 columns mapped correctly (100%)**

### CDC Metadata Columns (6 additional):
| Column | Line | Purpose | Status |
|--------|------|---------|--------|
| CDC_OPERATION | 72 | Track INSERT/UPDATE/DELETE | ✅ |
| CDC_TIMESTAMP | 73 | When CDC captured | ✅ |
| IS_DELETED | 74 | Soft delete flag | ✅ |
| RECORD_CREATED_AT | 75 | Row creation time | ✅ |
| RECORD_UPDATED_AT | 76 | Row update time | ✅ |
| SOURCE_LOAD_BATCH_ID | 77 | Batch tracking | ✅ |

**Total: 58 columns (52 source + 6 CDC) ✅**

---

## 2. Primary Key Validation (100% ✅)

| Attribute | Source | Script | Match |
|-----------|--------|--------|-------|
| PK Column | GRPHC_OBJECT_VRSN_ID | GRPHC_OBJECT_VRSN_ID | ✅ |
| PK Type | Single | Single | ✅ |
| Target PK | Line 79 | `PRIMARY KEY (GRPHC_OBJECT_VRSN_ID)` | ✅ |
| MERGE ON | Lines 156, 297 | `ON tgt.GRPHC_OBJECT_VRSN_ID = src.GRPHC_OBJECT_VRSN_ID` | ✅ |

---

## 3. Stored Procedure Logic Validation (100% ✅)

### Pattern Compliance:
| Pattern | Required | Line(s) | Status |
|---------|----------|---------|--------|
| EXECUTE AS CALLER | ✅ | 104 | ✅ |
| Stream staleness detection | ✅ | 120-131 | ✅ |
| Stream recovery (SHOW_INITIAL_ROWS=TRUE) | ✅ | 139-142 | ✅ |
| Staging table pattern | ✅ | 249-265 | ✅ |
| 4 MERGE scenarios | ✅ | 299-456 | ✅ |
| All columns in UPDATE | ✅ | 301-357, 370-426 | ✅ |
| Error handling | ✅ | 463-466 | ✅ |
| Temp table cleanup | ✅ | 459, 465 | ✅ |

### MERGE Scenario Coverage:
| Scenario | Condition | Lines | Columns Updated | Status |
|----------|-----------|-------|-----------------|--------|
| UPDATE | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=TRUE | 300-357 | All 51 source + 5 CDC | ✅ |
| DELETE | CDC_ACTION='DELETE' AND CDC_IS_UPDATE=FALSE | 360-366 | 4 CDC (soft delete) | ✅ |
| RE-INSERT | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=FALSE (MATCHED) | 369-426 | All 51 source + 5 CDC | ✅ |
| INSERT | CDC_ACTION='INSERT' (NOT MATCHED) | 429-456 | All 58 columns | ✅ |

### Staging Table Column Verification:
| Location | Lines | All 52 Source Columns | Status |
|----------|-------|----------------------|--------|
| Staging SELECT | 250-265 | ✅ Complete | ✅ |
| MERGE Source SELECT | 279-296 | ✅ Complete | ✅ |
| Recovery MERGE INSERT | 214-240 | ✅ Complete | ✅ |
| Main MERGE INSERT | 430-456 | ✅ Complete | ✅ |

---

## 4. Syntax Validation (100% ✅)

| Check | Line(s) | Status |
|-------|---------|--------|
| CREATE TABLE IF NOT EXISTS | 18-80 | ✅ Valid |
| ALTER TABLE SET CHANGE_TRACKING | 85-88 | ✅ Valid |
| CREATE OR REPLACE STREAM | 93-96 | ✅ Valid |
| CREATE OR REPLACE PROCEDURE | 101-468 | ✅ Valid |
| MERGE statement (Recovery) | 144-240 | ✅ Valid |
| MERGE statement (Main) | 277-456 | ✅ Valid |
| CREATE OR REPLACE TASK | 473-481 | ✅ Valid |
| Variable declarations | 107-113 | ✅ Valid |
| Exception handling | 463-466 | ✅ Valid |
| ALTER TASK RESUME | 483 | ✅ Valid |

---

## 5. Coding Standards (100% ✅)

| Standard | Score | Evidence |
|----------|-------|----------|
| Consistent naming | 100% | All objects follow `TRKFCG_FIXED_PLANT_ASSET*` pattern |
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
| ALTER TASK RESUME | Included (Line 483) | ✅ Task activated | ✅ |

---

## 7. Change Tracking Configuration (100% ✅)

| Setting | Value | Line | Status |
|---------|-------|------|--------|
| CHANGE_TRACKING | TRUE | 86 | ✅ |
| DATA_RETENTION_TIME_IN_DAYS | 45 | 87 | ✅ |
| MAX_DATA_EXTENSION_TIME_IN_DAYS | 15 | 88 | ✅ |

---

## Final Verification Checklist

| Requirement | Status |
|-------------|--------|
| All 52 source columns present in target table | ✅ |
| All 52 source columns in staging table | ✅ |
| All 52 source columns in UPDATE scenarios | ✅ |
| All 58 columns in INSERT scenarios | ✅ |
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

The `TRKFCG_FIXED_PLANT_ASSET.sql` script is **100% aligned** with the source table definition:

- ✅ All 52 source columns correctly mapped with exact data types
- ✅ 6 CDC metadata columns properly added (total 58)
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
