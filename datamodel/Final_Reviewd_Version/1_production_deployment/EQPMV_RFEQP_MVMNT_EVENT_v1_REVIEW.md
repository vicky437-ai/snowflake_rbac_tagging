# CDC Script Review: EQPMV_RFEQP_MVMNT_EVENT

## Document Information
| Field | Value |
|-------|-------|
| Script Name | EQPMV_RFEQP_MVMNT_EVENT_v1.sql |
| Version | v1.0 |
| Review Date | 2026-03-13 |
| Reviewer | CDC Enhancement Team |

## Overview
This document provides a comprehensive review of the enhanced CDC script for the EQPMV_RFEQP_MVMNT_EVENT table. The v1 enhancements add proactive staleness detection and execution logging capabilities.

## Table Information
| Attribute | Value |
|-----------|-------|
| Source Table | D_RAW.SADB.EQPMV_RFEQP_MVMNT_EVENT_BASE |
| Target Table | D_BRONZE.SADB.EQPMV_RFEQP_MVMNT_EVENT |
| Stream Name | D_RAW.SADB.EQPMV_RFEQP_MVMNT_EVENT_BASE_HIST_STREAM |
| Primary Key | EVENT_ID |
| Total Columns | 97 (91 source + 6 CDC metadata) |

## v1 Enhancements

### 1. Proactive Staleness Detection
- **Function Used**: `SYSTEM$STREAM_GET_STALE_AFTER()`
- **Purpose**: Detect stream staleness BEFORE it becomes stale
- **Behavior**: 
  - Retrieves the stale_after timestamp for the stream
  - If timestamp is in the past, stream is already stale
  - Logs failure and exits early to prevent data loss
- **Benefits**:
  - Early warning system for stream health
  - Prevents silent data loss from stale streams
  - Enables proactive monitoring and alerting

### 2. Execution Logging
- **Log Table**: D_BRONZE.MONITORING.CDC_EXECUTION_LOG
- **Metrics Captured**:
  - BATCH_ID (unique identifier per execution)
  - TABLE_NAME
  - EXECUTION_STATUS (SUCCESS/FAILED)
  - ERROR_MESSAGE (if applicable)
  - ROWS_INSERTED
  - ROWS_UPDATED
  - ROWS_DELETED
  - EXECUTION_START_TIME
  - EXECUTION_END_TIME

### 3. Comprehensive Error Handling
- Try-catch blocks for staleness detection
- Exception handling in main procedure
- Detailed error messages logged for troubleshooting

## Script Sections

| Section | Description |
|---------|-------------|
| 1 | Drop existing objects (clean slate deployment) |
| 2 | Create target table with all 97 columns |
| 3 | Create staging table (mirrors target structure) |
| 4 | Create stream with SHOW_INITIAL_ROWS = TRUE |
| 5 | Create CDC stored procedure with v1 enhancements |
| 6 | Create scheduled task (5-minute interval) |
| 7 | Task enablement (commented for safety) |
| 8 | Stream recovery procedure |

## Column Inventory

### Source Columns (91)
EVENT_ID, EQPMT_EVENT_TYPE_ID, SCAC_CD, FSAC_CD, TRSTN_VRSN_NBR, RPT_SCAC_CD, RPT_FSAC_CD, RPT_TRSTN_VRSN_NBR, AAR_CAR_TYPE_CD, AAR_CAR_TYPE_VRSN_NBR, LGLNT_SLCTN_ID, SLCTN_VRSN_NBR, SLCTN_DATA_BASE_CN, LCTN_DATA_BASE_CN, LGLNT_LCTN_ID, SPLC_CD, NTWRK_NODE_ID, REPORT_TMS, SQNC_NBR, LOAD_EMPTY_IND, DRCTN_CD, SOURCE_SYSTEM_NM, LSOP_PRFL_NBR, TITAN_NBR, MARK_CD, EQPUN_NBR, OPTRN_LEG_ID, OPTRN_TMS, TRAIN_PLAN_LEG_ID, TRAIN_PLAN_EVENT_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS, CREATE_USER_ID, UPDATE_USER_ID, REPORT_TIME_ZONE, ESTRN_STND_REPORT_TMS, CONSIST_ORIGIN_NBR, CONSIST_NBR, COMMON_YARDS_SITE_CD, COMMON_YARDS_TRACK_NM, MILE_NBR, SHRT_DSTR_NM, RUN_NBR_CD, CYCLE_SERIAL_NBR, EQPUN_ORNTN_CD, INTRCH_MOVE_ATHRTY_CD, COMMON_YARDS_TAG_ID, LGLNT_ID, LGLNT_DATA_BASE_CN, LGLNT_VRSN_NBR, CSTMR_ENTITY_IDNTFR_CD, DMRG_TYPE_CD, EVENT_STATUS_CD, DMRG_RPRTNG_EXCPTN_CD, TRAIN_TYPE_CD, AAR_LBLTY_CNTNTY_MSG_CD, TRNSFR_OF_LBLTY_CD, CNST_DSTNTN_FSAC_CD, MTP_ROUTE_POINT_SQNC_NBR, EQPMT_EVENT_REASON_CD, AEIRD_NBR, OPTRN_EVENT_ID, TRAVEL_DRCTN_CD, SWITCH_LIST_NBR, TYES_TRAIN_ID, LGLNT_LCTN_STN_RFRNC_ID, INDSTR_RLTV_SQNC_NBR, TRACK_FSAC_CD, TRACK_RFRNC_LCTN_ID, TRACK_SHORT_ENGL_NM, LGLNT_VRSN_ID, AAR_SHPR_RJCTN_CD, TRIP_END_IND, EQPMT_ID, BLOCK_NM, CLASS_CD, CLASS_CODE_CTGRY_CD, INTRNL_OPRTNL_HNDLNG_CD, OPRTNL_HNDLNG_CD, INDSTR_NM, EXCTN_BLOCK_ID, EXCTN_BLOCK_CTGRY_CD, EXCTN_BLOCK_ORIGIN_SCAC_CD, EXCTN_BLOCK_ORIGIN_FSAC_CD, EXCTN_BLOCK_DSTNTN_SCAC_CD, EXCTN_BLOCK_DSTNTN_FSAC_CD, EXCTN_PLND_BLOCK_NM, EXCTN_RPRTD_BLOCK_NM, SNW_OPERATION_TYPE, SNW_LAST_REPLICATED

### CDC Metadata Columns (6)
| Column | Type | Purpose |
|--------|------|---------|
| CDC_OPERATION | VARCHAR(10) | INSERT/UPDATE/DELETE |
| CDC_TIMESTAMP | TIMESTAMP_NTZ | When change was captured |
| IS_DELETED | BOOLEAN | Soft delete flag |
| RECORD_CREATED_AT | TIMESTAMP_NTZ | Target row creation time |
| RECORD_UPDATED_AT | TIMESTAMP_NTZ | Target row last update time |
| SOURCE_LOAD_BATCH_ID | VARCHAR(100) | Batch identifier |

### Additional Column
| Column | Type | Purpose |
|--------|------|---------|
| SNW_OPERATION_OWNER | VARCHAR | Snowflake replication owner |

## Data Filtering
- **Excluded Records**: SNW_OPERATION_OWNER IN ('TSDPRG', 'EMEPRG')
- **Purpose**: Filter out purged records from CDC processing

## Operational Details

### Task Configuration
| Setting | Value |
|---------|-------|
| Warehouse | INFA_INGEST_WH |
| Schedule | Every 5 minutes |
| Allow Overlapping | FALSE |

### Stream Configuration
| Setting | Value |
|---------|-------|
| Type | Standard (not append-only) |
| Show Initial Rows | TRUE |

### Data Retention Considerations
- Source table retention: 45 days standard + 15 days extended
- Stream must be consumed before data_retention_time_in_days expires
- Proactive staleness detection helps prevent data loss

## Recovery Procedures

### Stream Recovery
If stream becomes stale:
1. Call `D_BRONZE.SADB.SP_RECOVER_STREAM_EQPMV_RFEQP_MVMNT_EVENT()`
2. Verify stream recreation was successful
3. Manually resume task after verification

### Manual Execution
```sql
CALL D_BRONZE.SADB.SP_CDC_EQPMV_RFEQP_MVMNT_EVENT();
```

## Monitoring Queries

### Check Recent Executions
```sql
SELECT *
FROM D_BRONZE.MONITORING.CDC_EXECUTION_LOG
WHERE TABLE_NAME = 'EQPMV_RFEQP_MVMNT_EVENT'
ORDER BY EXECUTION_START_TIME DESC
LIMIT 10;
```

### Check Stream Health
```sql
SELECT SYSTEM$STREAM_GET_STALE_AFTER('D_RAW.SADB.EQPMV_RFEQP_MVMNT_EVENT_BASE_HIST_STREAM');
```

### Check Task Status
```sql
SHOW TASKS LIKE 'TSK_CDC_EQPMV_RFEQP_MVMNT_EVENT' IN SCHEMA D_BRONZE.SADB;
```

## Approval

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Developer | | | |
| Reviewer | | | |
| Approver | | | |

## Change History

| Version | Date | Author | Description |
|---------|------|--------|-------------|
| v1.0 | 2026-03-13 | CDC Enhancement Team | Initial v1 with proactive staleness detection and execution logging |
