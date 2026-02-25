# LCMTV_EMIS Script Review Report

**Review Date:** 2026-02-24  
**Script:** Scripts/Final/LCMTV_EMIS.sql  
**Source Table:** D_RAW.SADB.LCMTV_EMIS_BASE  
**Target Table:** D_BRONZE.SADB.LCMTV_EMIS

---

## Executive Summary

| Category | Score | Status |
|----------|-------|--------|
| **Column Mapping** | 100% | ✅ PERFECT |
| **Data Type Mapping** | 100% | ✅ PERFECT |
| **Primary Key (Composite)** | 100% | ✅ CORRECT |
| **SP Logic** | 100% | ✅ COMPLETE |
| **Syntax** | 100% | ✅ VALID |
| **Coding Standards** | 100% | ✅ EXCELLENT |
| **Overall Score** | **100/100** | ✅ **PRODUCTION READY** |

---

## 1. Column Mapping Validation (100% ✅)

### Source Table Columns (84 columns):
| # | Source Column | Source Type | Script Line | Match |
|---|---------------|-------------|-------------|-------|
| 1 | MARK_CD | VARCHAR(16) NOT NULL | 19 | ✅ |
| 2 | EQPUN_NBR | VARCHAR(40) NOT NULL | 20 | ✅ |
| 3 | AIR_BRAKE_HOOKP_CD | VARCHAR(4) | 21 | ✅ |
| 4 | AIR_BRAKE_MDL_NBR | VARCHAR(20) | 22 | ✅ |
| 5 | AIR_CNDTN_IND | VARCHAR(4) | 23 | ✅ |
| 6 | ALGN_CNTRL_CPLR_CD | VARCHAR(4) | 24 | ✅ |
| 7 | ATCS_CD | VARCHAR(12) | 25 | ✅ |
| 8 | AXLE_CT | NUMBER(5,0) | 26 | ✅ |
| 9 | BRNG_TYPE_CD | VARCHAR(4) | 27 | ✅ |
| 10 | CAB_HEATER_CD | VARCHAR(4) | 28 | ✅ |
| 11 | CAB_SIGNAL_CD | VARCHAR(4) | 29 | ✅ |
| 12 | CLRNC_PLATE_CD | VARCHAR(4) | 30 | ✅ |
| 13 | CPLR_A_END_TYPE_CD | VARCHAR(32) | 31 | ✅ |
| 14 | CPLR_B_END_TYPE_CD | VARCHAR(32) | 32 | ✅ |
| 15 | CSTMS_CD | VARCHAR(4) | 33 | ✅ |
| 16 | DYN_BRK_INTLK_IND | VARCHAR(4) | 34 | ✅ |
| 17 | DYN_BRK_MAX_EF_QTY | NUMBER(5,0) | 35 | ✅ |
| 18 | DYN_BRK_TYPE_CD | VARCHAR(4) | 36 | ✅ |
| 19 | FUEL_CPCTY_QTY | NUMBER(5,0) | 37 | ✅ |
| 20 | FUEL_PRHTR_IND | VARCHAR(4) | 38 | ✅ |
| 21 | FUEL_SHTF_CD | VARCHAR(4) | 39 | ✅ |
| 22 | FUEL_SVR_MNFCTR_CD | VARCHAR(4) | 40 | ✅ |
| 23 | FULL_WEIGHT_QTY | NUMBER(10,0) | 41 | ✅ |
| 24 | GEAR_AXL_TEETH_QTY | NUMBER(5,0) | 42 | ✅ |
| 25 | GRND_RELAY_RST_CD | VARCHAR(4) | 43 | ✅ |
| 26 | HOOD_CNFGRT_CD | VARCHAR(4) | 44 | ✅ |
| 27 | HRSPWR_QTY | NUMBER(5,0) | 45 | ✅ |
| 28 | IND_PRSR_SWTCH_IND | VARCHAR(4) | 46 | ✅ |
| 29 | INTRNT_SRVC_CD | VARCHAR(4) | 47 | ✅ |
| 30 | JMPR_CBL_CNCTN_CD | VARCHAR(4) | 48 | ✅ |
| 31 | LOW_IDLE_IND | VARCHAR(4) | 49 | ✅ |
| 32 | MNM_CNTNS_SPD_QTY | NUMBER(5,0) | 50 | ✅ |
| 33 | MNM_CRV_50_FT_QTY | NUMBER(5,0) | 51 | ✅ |
| 34 | MNM_CRV_MLTPL_QTY | NUMBER(5,0) | 52 | ✅ |
| 35 | MNM_CRV_SNGL_QTY | NUMBER(5,0) | 53 | ✅ |
| 36 | MODEL_NBR | VARCHAR(32) | 54 | ✅ |
| 37 | MXM_SPEED_QTY | NUMBER(5,0) | 55 | ✅ |
| 38 | OPRT_BRAKE_CT | NUMBER(5,0) | 56 | ✅ |
| 39 | PNLTY_AIR_BRAKE_CD | VARCHAR(4) | 57 | ✅ |
| 40 | PNM_CTRL_DELAY_CD | VARCHAR(8) | 58 | ✅ |
| 41 | PNM_DYNMC_DLY_CD | VARCHAR(8) | 59 | ✅ |
| 42 | PNM_PNLTY_DLY_CD | VARCHAR(8) | 60 | ✅ |
| 43 | PNM_UNCTRL_DLY_CD | VARCHAR(8) | 61 | ✅ |
| 44 | PNN_GEAR_TEETH_QTY | NUMBER(5,0) | 62 | ✅ |
| 45 | RADIO_MNFCTR_CD | VARCHAR(4) | 63 | ✅ |
| 46 | RADIO_MODEL_NBR | VARCHAR(40) | 64 | ✅ |
| 47 | SAFETY_CNTRL_CD | VARCHAR(4) | 65 | ✅ |
| 48 | SAND_CPCTY_QTY | NUMBER(5,0) | 66 | ✅ |
| 49 | SNW_PLW_HGHT_A_CD | VARCHAR(4) | 67 | ✅ |
| 50 | SNW_PLW_HGHT_B_CD | VARCHAR(4) | 68 | ✅ |
| 51 | SPARK_ARSTR_CD | VARCHAR(4) | 69 | ✅ |
| 52 | SPEED_TAPE_CNTL_CD | VARCHAR(4) | 70 | ✅ |
| 53 | STNG_CPCTY_CD | VARCHAR(4) | 71 | ✅ |
| 54 | STRTR_TYPE_CD | VARCHAR(4) | 72 | ✅ |
| 55 | TOILET_TYPE_CD | VARCHAR(4) | 73 | ✅ |
| 56 | TRCTN_MTR_CUT_IND | VARCHAR(4) | 74 | ✅ |
| 57 | TRCTN_MTR_TYPE_CD | VARCHAR(8) | 75 | ✅ |
| 58 | TRUCK_CNTR_LGT_QTY | NUMBER(5,0) | 76 | ✅ |
| 59 | WATER_COOLER_CD | VARCHAR(4) | 77 | ✅ |
| 60 | WATER_DRAIN_IND | VARCHAR(4) | 78 | ✅ |
| 61 | WHEEL_SIZE_QTY | NUMBER(5,0) | 79 | ✅ |
| 62 | RECORD_CREATE_DT | TIMESTAMP_NTZ(0) | 80 | ✅ |
| 63 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | 81 | ✅ |
| 64 | RECORD_UPDATE_TMS_NBR | NUMBER(6,0) | 82 | ✅ |
| 65 | SPEED_TAPE_CD | VARCHAR(4) | 83 | ✅ |
| 66 | TRUCK_MNFCTR_CD | VARCHAR(4) | 84 | ✅ |
| 67 | ENGNR_SEAT_TYPE_CD | VARCHAR(4) | 85 | ✅ |
| 68 | BLDR_CD | VARCHAR(4) | 86 | ✅ |
| 69 | END_TRAIN_INFO_SYSTEM_CD | VARCHAR(4) | 87 | ✅ |
| 70 | ETIS_MNTNG_TYPE_CD | VARCHAR(4) | 88 | ✅ |
| 71 | CLU_INITIAL_CD | VARCHAR(16) | 89 | ✅ |
| 72 | CLU_SERIAL_NBR | NUMBER(6,0) | 90 | ✅ |
| 73 | IDU_INITIAL_SERIAL_CD | VARCHAR(16) | 91 | ✅ |
| 74 | IDU_SERIAL_NBR | NUMBER(6,0) | 92 | ✅ |
| 75 | LCMTV_TRUCK_TYPE_CD | VARCHAR(8) | 93 | ✅ |
| 76 | POWER_AXLE_QTY | NUMBER(5,0) | 94 | ✅ |
| 77 | LCMTV_FRA_INSPECT_DT | TIMESTAMP_NTZ(0) | 95 | ✅ |
| 78 | LCMTV_CTC_INSPECT_DT | TIMESTAMP_NTZ(0) | 96 | ✅ |
| 79 | CREATE_USER_ID | VARCHAR(32) | 97 | ✅ |
| 80 | UPDATE_USER_ID | VARCHAR(32) | 98 | ✅ |
| 81 | CODED_CAB_SIGNAL_CD | VARCHAR(4) | 99 | ✅ |
| 82 | LCMTV_STRTR_TYPE_CD | VARCHAR(4) | 100 | ✅ |
| 83 | SNW_OPERATION_TYPE | VARCHAR(1) | 101 | ✅ |
| 84 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | 102 | ✅ |

**Result: 84/84 columns mapped correctly (100%)**

### CDC Metadata Columns (6 additional):
| Column | Line | Purpose | Status |
|--------|------|---------|--------|
| CDC_OPERATION | 104 | Track INSERT/UPDATE/DELETE | ✅ |
| CDC_TIMESTAMP | 105 | When CDC captured | ✅ |
| IS_DELETED | 106 | Soft delete flag | ✅ |
| RECORD_CREATED_AT | 107 | Row creation time | ✅ |
| RECORD_UPDATED_AT | 108 | Row update time | ✅ |
| SOURCE_LOAD_BATCH_ID | 109 | Batch tracking | ✅ |

**Total: 90 columns (84 source + 6 CDC) ✅**

---

## 2. Composite Primary Key Validation (100% ✅)

| Attribute | Source | Script | Match |
|-----------|--------|--------|-------|
| PK Column 1 | MARK_CD | MARK_CD | ✅ |
| PK Column 2 | EQPUN_NBR | EQPUN_NBR | ✅ |
| PK Type | Composite (2) | Composite (2) | ✅ |
| Target PK | Line 111 | `PRIMARY KEY (MARK_CD, EQPUN_NBR)` | ✅ |
| MERGE ON | Lines 189-190, 386-387 | Both PK columns used | ✅ |

```sql
-- Composite PK correctly defined:
PRIMARY KEY (MARK_CD, EQPUN_NBR)  -- Line 111

-- MERGE ON correctly uses composite key:
ON tgt.MARK_CD = src.MARK_CD
   AND tgt.EQPUN_NBR = src.EQPUN_NBR  -- Lines 386-387
```

---

## 3. Stored Procedure Logic Validation (100% ✅)

### Pattern Compliance:
| Pattern | Required | Line(s) | Status |
|---------|----------|---------|--------|
| EXECUTE AS CALLER | ✅ | 136 | ✅ |
| Stream staleness detection | ✅ | 152-163 | ✅ |
| Stream recovery (SHOW_INITIAL_ROWS=TRUE) | ✅ | 171-174 | ✅ |
| Staging table pattern | ✅ | 326-348 | ✅ |
| 4 MERGE scenarios | ✅ | 389-620 | ✅ |
| All columns in UPDATE | ✅ | 391-478, 491-578 | ✅ |
| Error handling | ✅ | 627-630 | ✅ |
| Temp table cleanup | ✅ | 623, 629 | ✅ |

### MERGE Scenario Coverage:
| Scenario | Condition | Lines | Columns Updated | Status |
|----------|-----------|-------|-----------------|--------|
| UPDATE | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=TRUE | 390-478 | All 84 source + 5 CDC | ✅ |
| DELETE | CDC_ACTION='DELETE' AND CDC_IS_UPDATE=FALSE | 481-487 | 4 CDC (soft delete) | ✅ |
| RE-INSERT | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=FALSE (MATCHED) | 490-578 | All 84 source + 5 CDC | ✅ |
| INSERT | CDC_ACTION='INSERT' (NOT MATCHED) | 581-620 | All 90 columns | ✅ |

---

## 4. Syntax Validation (100% ✅)

| Check | Line(s) | Status |
|-------|---------|--------|
| CREATE TABLE IF NOT EXISTS | 18-112 | ✅ Valid |
| ALTER TABLE SET CHANGE_TRACKING | 117-120 | ✅ Valid |
| CREATE OR REPLACE STREAM | 125-128 | ✅ Valid |
| CREATE OR REPLACE PROCEDURE | 133-632 | ✅ Valid |
| MERGE statement | 360-620 | ✅ Valid |
| CREATE OR REPLACE TASK | 637-645 | ✅ Valid |
| Variable declarations | 139-145 | ✅ Valid |
| Exception handling | 627-631 | ✅ Valid |
| ALTER TASK RESUME | 647 | ✅ Valid |

---

## 5. Coding Standards (100% ✅)

| Standard | Score | Evidence |
|----------|-------|----------|
| Consistent naming | 100% | All objects follow `LCMTV_EMIS*` pattern |
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
| ALTER TASK RESUME | Included (Line 647) | ✅ Task activated | ✅ |

---

## Final Verification Checklist

| Requirement | Status |
|-------------|--------|
| All 84 source columns present in target table | ✅ |
| All 84 source columns in staging table | ✅ |
| All 84 source columns in UPDATE scenarios | ✅ |
| All 90 columns in INSERT scenarios | ✅ |
| Composite PK (MARK_CD, EQPUN_NBR) correctly defined | ✅ |
| MERGE ON clause uses both PK columns | ✅ |
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

The `LCMTV_EMIS.sql` script is **100% aligned** with the source table definition:

- ✅ All 84 source columns correctly mapped with exact data types
- ✅ Composite primary key (MARK_CD, EQPUN_NBR) correctly implemented
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
