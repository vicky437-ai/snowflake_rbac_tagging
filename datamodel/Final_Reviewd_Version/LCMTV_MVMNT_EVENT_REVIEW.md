# LCMTV_MVMNT_EVENT Script Review Report

**Review Date:** 2026-02-24  
**Script:** Scripts/Final/LCMTV_MVMNT_EVENT.sql  
**Source Table:** D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE  
**Target Table:** D_BRONZE.SADB.LCMTV_MVMNT_EVENT

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

### Source Table Columns (43 columns):
| # | Source Column | Source Type | Script Line | Match |
|---|---------------|-------------|-------------|-------|
| 1 | EVENT_ID | NUMBER(18,0) NOT NULL | 19 | ✅ |
| 2 | EQPMT_EVENT_TYPE_ID | NUMBER(18,0) | 20 | ✅ |
| 3 | SCAC_CD | VARCHAR(16) | 21 | ✅ |
| 4 | FSAC_CD | VARCHAR(20) | 22 | ✅ |
| 5 | TRSTN_VRSN_NBR | NUMBER(5,0) | 23 | ✅ |
| 6 | REPORT_TMS | TIMESTAMP_NTZ(0) | 24 | ✅ |
| 7 | SQNC_NBR | NUMBER(5,0) | 25 | ✅ |
| 8 | DRCTN_CD | VARCHAR(4) | 26 | ✅ |
| 9 | SOURCE_SYSTEM_NM | VARCHAR(40) | 27 | ✅ |
| 10 | MARK_CD | VARCHAR(16) | 28 | ✅ |
| 11 | EQPUN_NBR | VARCHAR(40) | 29 | ✅ |
| 12 | OPTRN_EVENT_ID | NUMBER(18,0) | 30 | ✅ |
| 13 | ORNTTN_CD | VARCHAR(4) | 31 | ✅ |
| 14 | DEAD_HEAD_IND | VARCHAR(4) | 32 | ✅ |
| 15 | RGN_NM_TRK_NBR | NUMBER(18,0) | 33 | ✅ |
| 16 | LMS_PRFL_SQNC_NBR | NUMBER(3,0) | 34 | ✅ |
| 17 | MILE_NBR | NUMBER(8,3) | 35 | ✅ |
| 18 | PLAN_EVENT_ID | NUMBER(18,0) | 36 | ✅ |
| 19 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | 37 | ✅ |
| 20 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | 38 | ✅ |
| 21 | CREATE_USER_ID | VARCHAR(32) | 39 | ✅ |
| 22 | UPDATE_USER_ID | VARCHAR(32) | 40 | ✅ |
| 23 | AEIRD_NBR | VARCHAR(16) | 41 | ✅ |
| 24 | CNST_NBR | VARCHAR(16) | 42 | ✅ |
| 25 | CNST_ORIGIN_SCAC_CD | VARCHAR(16) | 43 | ✅ |
| 26 | CNST_ORIGIN_FSAC_CD | VARCHAR(20) | 44 | ✅ |
| 27 | COMMON_YARDS_SITE_CD | VARCHAR(4) | 45 | ✅ |
| 28 | COMMON_YARDS_TRACK_NM | VARCHAR(20) | 46 | ✅ |
| 29 | DSTNC_RUN_MILES_QTY | NUMBER(6,1) | 47 | ✅ |
| 30 | INTRCH_SCAC_CD | VARCHAR(16) | 48 | ✅ |
| 31 | MTP_ROUTE_POINT_SQNC_NBR | NUMBER(3,0) | 49 | ✅ |
| 32 | REPORT_TIME_ZONE_CD | VARCHAR(8) | 50 | ✅ |
| 33 | RUN_NBR_CD | VARCHAR(12) | 51 | ✅ |
| 34 | SHORT_DSTRCT_NM | VARCHAR(40) | 52 | ✅ |
| 35 | SPLC_CD | VARCHAR(24) | 53 | ✅ |
| 36 | TITAN_NBR | NUMBER(6,0) | 54 | ✅ |
| 37 | CNST_DSTNTN_SCAC_CD | VARCHAR(16) | 55 | ✅ |
| 38 | CNST_DSTNTN_FSAC_CD | VARCHAR(20) | 56 | ✅ |
| 39 | TRAVEL_DRCTN_CD | VARCHAR(20) | 57 | ✅ |
| 40 | SWITCH_LIST_NBR | NUMBER(5,0) | 58 | ✅ |
| 41 | TYES_TRAIN_ID | NUMBER(18,0) | 59 | ✅ |
| 42 | SNW_OPERATION_TYPE | VARCHAR(1) | 60 | ✅ |
| 43 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | 61 | ✅ |

**Result: 43/43 columns mapped correctly (100%)**

### CDC Metadata Columns (6 additional):
| Column | Line | Purpose | Status |
|--------|------|---------|--------|
| CDC_OPERATION | 64 | Track INSERT/UPDATE/DELETE | ✅ |
| CDC_TIMESTAMP | 65 | When CDC captured | ✅ |
| IS_DELETED | 66 | Soft delete flag | ✅ |
| RECORD_CREATED_AT | 67 | Row creation time | ✅ |
| RECORD_UPDATED_AT | 68 | Row update time | ✅ |
| SOURCE_LOAD_BATCH_ID | 69 | Batch tracking | ✅ |

**Total: 49 columns (43 source + 6 CDC) ✅**

---

## 2. Primary Key Validation (100% ✅)

| Attribute | Source | Script | Match |
|-----------|--------|--------|-------|
| PK Column | EVENT_ID | EVENT_ID | ✅ |
| PK Type | Single | Single | ✅ |
| Target PK | Line 71 | `PRIMARY KEY (EVENT_ID)` | ✅ |
| MERGE ON | Lines 148, 274 | `ON tgt.EVENT_ID = src.EVENT_ID` | ✅ |

---

## 3. Stored Procedure Logic Validation (100% ✅)

### Pattern Compliance:
| Pattern | Required | Line(s) | Status |
|---------|----------|---------|--------|
| EXECUTE AS CALLER | ✅ | 96 | ✅ |
| Stream staleness detection | ✅ | 112-123 | ✅ |
| Stream recovery (SHOW_INITIAL_ROWS=TRUE) | ✅ | 131-134 | ✅ |
| Staging table pattern | ✅ | 230-244 | ✅ |
| 4 MERGE scenarios | ✅ | 276-413 | ✅ |
| All columns in UPDATE | ✅ | 278-325, 338-385 | ✅ |
| Error handling | ✅ | 420-423 | ✅ |
| Temp table cleanup | ✅ | 416, 422 | ✅ |

### MERGE Scenario Coverage:
| Scenario | Condition | Lines | Columns Updated | Status |
|----------|-----------|-------|-----------------|--------|
| UPDATE | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=TRUE | 277-325 | All 43 source + 5 CDC | ✅ |
| DELETE | CDC_ACTION='DELETE' AND CDC_IS_UPDATE=FALSE | 328-334 | 4 CDC (soft delete) | ✅ |
| RE-INSERT | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=FALSE (MATCHED) | 337-385 | All 43 source + 5 CDC | ✅ |
| INSERT | CDC_ACTION='INSERT' (NOT MATCHED) | 388-413 | All 49 columns | ✅ |

---

## 4. Syntax Validation (100% ✅)

| Check | Line(s) | Status |
|-------|---------|--------|
| CREATE TABLE IF NOT EXISTS | 18-72 | ✅ Valid |
| ALTER TABLE SET CHANGE_TRACKING | 77-80 | ✅ Valid |
| CREATE OR REPLACE STREAM | 85-88 | ✅ Valid |
| CREATE OR REPLACE PROCEDURE | 93-425 | ✅ Valid |
| MERGE statement | 256-413 | ✅ Valid |
| CREATE OR REPLACE TASK | 430-438 | ✅ Valid |
| Variable declarations | 99-105 | ✅ Valid |
| Exception handling | 420-424 | ✅ Valid |
| ALTER TASK RESUME | 440 | ✅ Valid |

---

## 5. Coding Standards (100% ✅)

| Standard | Score | Evidence |
|----------|-------|----------|
| Consistent naming | 100% | All objects follow `LCMTV_MVMNT_EVENT*` pattern |
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
| ALTER TASK RESUME | Included (Line 440) | ✅ Task activated | ✅ |

---

## Final Verification Checklist

| Requirement | Status |
|-------------|--------|
| All 43 source columns present in target table | ✅ |
| All 43 source columns in staging table | ✅ |
| All 43 source columns in UPDATE scenarios | ✅ |
| All 49 columns in INSERT scenarios | ✅ |
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

The `LCMTV_MVMNT_EVENT.sql` script is **100% aligned** with the source table definition:

- ✅ All 43 source columns correctly mapped with exact data types
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
