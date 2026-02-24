# TRKFCG_SBDVSN Script Review Report

**Review Date:** 2026-02-24  
**Script:** Scripts/Final/TRKFCG_SBDVSN.sql  
**Source Table:** D_RAW.SADB.TRKFCG_SBDVSN_BASE  
**Target Table:** D_BRONZE.SADB.TRKFCG_SBDVSN

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

### Source Table Columns (49 columns):
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
| 9 | REGION_ID | NUMBER(18,0) | 27 | ✅ |
| 10 | SBDVSN_ID | NUMBER(4,0) | 28 | ✅ |
| 11 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | 29 | ✅ |
| 12 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | 30 | ✅ |
| 13 | CREATE_USER_ID | VARCHAR(32) | 31 | ✅ |
| 14 | UPDATE_USER_ID | VARCHAR(32) | 32 | ✅ |
| 15 | ATCS_IND | VARCHAR(4) | 33 | ✅ |
| 16 | DSPLY_SCHMTC_RVRS_IND | VARCHAR(4) | 34 | ✅ |
| 17 | DYLGHT_SVNGS_IND | VARCHAR(4) | 35 | ✅ |
| 18 | FRMN_ATHRTY_CD | VARCHAR(4) | 36 | ✅ |
| 19 | LNG_NM | VARCHAR(28) | 37 | ✅ |
| 20 | MTP_ONLY_IND | VARCHAR(4) | 38 | ✅ |
| 21 | RGLR_TRAINS_IND | VARCHAR(4) | 39 | ✅ |
| 22 | RLWY_RULE_SET_CD | VARCHAR(20) | 40 | ✅ |
| 23 | RVRSD_SWITCH_IND | VARCHAR(4) | 41 | ✅ |
| 24 | SCAC_CD | VARCHAR(16) | 42 | ✅ |
| 25 | LOW_MILE_ORNTN_CD | VARCHAR(20) | 43 | ✅ |
| 26 | HIGH_MILE_ORNTN_CD | VARCHAR(20) | 44 | ✅ |
| 27 | LOW_WHLG_FACTOR_NBR | NUMBER(3,0) | 45 | ✅ |
| 28 | HIGH_WHLG_FACTOR_NBR | NUMBER(3,0) | 46 | ✅ |
| 29 | RADIO_CHNL_CD | VARCHAR(8) | 47 | ✅ |
| 30 | RTC_TRTRY_ID | NUMBER(18,0) | 48 | ✅ |
| 31 | SRVC_AREA_ID | NUMBER(18,0) | 49 | ✅ |
| 32 | TIME_ZONE_CD | VARCHAR(8) | 50 | ✅ |
| 33 | DATA_SOURCE_CD | VARCHAR(40) | 51 | ✅ |
| 34 | EFCTV_TMS | TIMESTAMP_NTZ(0) | 52 | ✅ |
| 35 | LONG_ENGLSH_NM | VARCHAR(320) | 53 | ✅ |
| 36 | REGION_CD | VARCHAR(16) | 54 | ✅ |
| 37 | EXPIRY_TMS | TIMESTAMP_NTZ(0) | 55 | ✅ |
| 38 | LONG_FRENCH_NM | VARCHAR(320) | 56 | ✅ |
| 39 | SHORT_ENGLSH_NM | VARCHAR(40) | 57 | ✅ |
| 40 | SHORT_FRENCH_NM | VARCHAR(40) | 58 | ✅ |
| 41 | CNTRY_CD | VARCHAR(8) | 59 | ✅ |
| 42 | ENGINE_TRAIN_SUPPLY_CD | VARCHAR(4) | 60 | ✅ |
| 43 | OCS_DSPTCH_CD | VARCHAR(4) | 61 | ✅ |
| 44 | OCS_SBDVSN_SQNC_NBR | NUMBER(3,0) | 62 | ✅ |
| 45 | TGBO_DSPTCH_IND | VARCHAR(4) | 63 | ✅ |
| 46 | ALTRNT_LONG_ENGLSH_NM | VARCHAR(320) | 64 | ✅ |
| 47 | ALTRNT_LONG_FRENCH_NM | VARCHAR(320) | 65 | ✅ |
| 48 | SNW_OPERATION_TYPE | VARCHAR(1) | 66 | ✅ |
| 49 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | 67 | ✅ |

**Result: 49/49 columns mapped correctly (100%)**

### CDC Metadata Columns (6 additional):
| Column | Line | Purpose | Status |
|--------|------|---------|--------|
| CDC_OPERATION | 69 | Track INSERT/UPDATE/DELETE | ✅ |
| CDC_TIMESTAMP | 70 | When CDC captured | ✅ |
| IS_DELETED | 71 | Soft delete flag | ✅ |
| RECORD_CREATED_AT | 72 | Row creation time | ✅ |
| RECORD_UPDATED_AT | 73 | Row update time | ✅ |
| SOURCE_LOAD_BATCH_ID | 74 | Batch tracking | ✅ |

**Total: 55 columns (49 source + 6 CDC) ✅**

---

## 2. Primary Key Validation (100% ✅)

| Attribute | Source | Script | Match |
|-----------|--------|--------|-------|
| PK Column | GRPHC_OBJECT_VRSN_ID | GRPHC_OBJECT_VRSN_ID | ✅ |
| PK Type | Single | Single | ✅ |
| Target PK | Line 76 | `PRIMARY KEY (GRPHC_OBJECT_VRSN_ID)` | ✅ |
| MERGE ON | Lines 153, 287 | `ON tgt.GRPHC_OBJECT_VRSN_ID = src.GRPHC_OBJECT_VRSN_ID` | ✅ |

---

## 3. Stored Procedure Logic Validation (100% ✅)

### Pattern Compliance:
| Pattern | Required | Line(s) | Status |
|---------|----------|---------|--------|
| EXECUTE AS CALLER | ✅ | 101 | ✅ |
| Stream staleness detection | ✅ | 117-128 | ✅ |
| Stream recovery (SHOW_INITIAL_ROWS=TRUE) | ✅ | 136-139 | ✅ |
| Staging table pattern | ✅ | 241-256 | ✅ |
| 4 MERGE scenarios | ✅ | 289-438 | ✅ |
| All columns in UPDATE | ✅ | 291-344, 357-410 | ✅ |
| Error handling | ✅ | 445-448 | ✅ |
| Temp table cleanup | ✅ | 441, 447 | ✅ |

### MERGE Scenario Coverage:
| Scenario | Condition | Lines | Columns Updated | Status |
|----------|-----------|-------|-----------------|--------|
| UPDATE | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=TRUE | 290-344 | All 49 source + 5 CDC | ✅ |
| DELETE | CDC_ACTION='DELETE' AND CDC_IS_UPDATE=FALSE | 347-353 | 4 CDC (soft delete) | ✅ |
| RE-INSERT | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=FALSE (MATCHED) | 356-410 | All 49 source + 5 CDC | ✅ |
| INSERT | CDC_ACTION='INSERT' (NOT MATCHED) | 413-438 | All 55 columns | ✅ |

### Column Mapping Verification (All Locations):

| Location | Description | Lines | Columns | Status |
|----------|-------------|-------|---------|--------|
| 1 | Target table DDL | 18-77 | 55 | ✅ |
| 2 | Staging table CTAS | 241-256 | 49+3 CDC | ✅ |
| 3 | Recovery MERGE UPDATE | 155-207 | 49 | ✅ |
| 4 | Recovery MERGE INSERT | 208-232 | 55 | ✅ |
| 5 | Main MERGE UPDATE | 291-344 | 49 | ✅ |
| 6 | Main MERGE DELETE | 348-353 | 4 CDC | ✅ |
| 7 | Main MERGE RE-INSERT | 357-410 | 49 | ✅ |
| 8 | Main MERGE INSERT | 414-438 | 55 | ✅ |

---

## 4. Syntax Validation (100% ✅)

| Check | Line(s) | Status |
|-------|---------|--------|
| CREATE TABLE IF NOT EXISTS | 18-77 | ✅ Valid |
| ALTER TABLE SET CHANGE_TRACKING | 82-85 | ✅ Valid |
| CREATE OR REPLACE STREAM | 90-93 | ✅ Valid |
| CREATE OR REPLACE PROCEDURE | 98-450 | ✅ Valid |
| MERGE statement | 268-438 | ✅ Valid |
| CREATE OR REPLACE TASK | 455-463 | ✅ Valid |
| Variable declarations | 104-110 | ✅ Valid |
| Exception handling | 445-449 | ✅ Valid |
| ALTER TASK RESUME | 465 | ✅ Valid |

---

## 5. Coding Standards (100% ✅)

| Standard | Score | Evidence |
|----------|-------|----------|
| Consistent naming | 100% | All objects follow `TRKFCG_SBDVSN*` pattern |
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
| ALTER TASK RESUME | Included (Line 465) | ✅ Task activated | ✅ |

---

## 7. Data Preservation Features (100% ✅)

| Feature | Implementation | Status |
|---------|----------------|--------|
| Soft deletes | IS_DELETED = TRUE on DELETE | ✅ |
| No data loss | All historical data preserved | ✅ |
| Audit trail | CDC_OPERATION tracks all changes | ✅ |
| Timestamp tracking | CDC_TIMESTAMP, RECORD_UPDATED_AT | ✅ |
| Batch tracking | SOURCE_LOAD_BATCH_ID | ✅ |
| Recovery capability | Stream staleness detection + recovery | ✅ |

---

## 8. Edge Case Handling (100% ✅)

| Edge Case | Handling | Lines | Status |
|-----------|----------|-------|--------|
| Stale stream | Detect and recreate | 117-128, 133-236 | ✅ |
| Empty stream | Early return with message | 260-263 | ✅ |
| Re-insert after delete | Separate MERGE scenario | 356-410 | ✅ |
| SQL errors | Exception handler with cleanup | 445-449 | ✅ |
| IDMC truncate/reload | Stream recovery with full sync | 133-236 | ✅ |

---

## Final Verification Checklist

| Requirement | Status |
|-------------|--------|
| All 49 source columns present in target table | ✅ |
| All 49 source columns in staging table | ✅ |
| All 49 source columns in UPDATE scenarios | ✅ |
| All 55 columns in INSERT scenarios | ✅ |
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

The `TRKFCG_SBDVSN.sql` script is **100% aligned** with the source table definition:

- ✅ All 49 source columns correctly mapped with exact data types
- ✅ Primary key (GRPHC_OBJECT_VRSN_ID) correctly implemented
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
