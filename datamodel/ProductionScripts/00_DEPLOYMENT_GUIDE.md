# CDC Data Preservation - Production Deployment Guide

## Overview
This folder contains **production-ready, parameterized** CDC data preservation scripts for migrating data from D_RAW to D_BRONZE layer with full change tracking and soft delete support.

---

## Pre-Deployment Checklist

- [ ] Verify source database (D_RAW equivalent) exists with all _BASE tables
- [ ] Verify target database (D_BRONZE equivalent) exists or will be created
- [ ] Confirm warehouse exists and has appropriate size (recommend SMALL or MEDIUM)
- [ ] Verify executing role has required privileges
- [ ] Review data retention requirements (currently set to 45+15=60 days)
- [ ] Confirm task schedule frequency (default: 5 MINUTE)

---

## Deployment Order

**IMPORTANT: Execute scripts in this exact order**

```
1. 01_SET_PARAMETERS.sql          -- Set environment variables
2. 02_CDC_INFRASTRUCTURE.sql      -- Create audit table & monitoring views
3. 03_CREATE_ALL_TARGET_TABLES.sql -- Create all D_BRONZE tables
4. 04_ENABLE_CHANGE_TRACKING.sql  -- Enable change tracking on source tables
5. 05_CREATE_ALL_STREAMS.sql      -- Create CDC streams
6. 06_CREATE_ALL_PROCEDURES.sql   -- Create stored procedures
7. 07_CREATE_ALL_TASKS.sql        -- Create scheduled tasks
8. 08_RESUME_ALL_TASKS.sql        -- Activate all tasks
```

---

## Configuration Parameters

Edit `01_SET_PARAMETERS.sql` with your environment values:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SOURCE_DATABASE` | D_RAW | Source database name |
| `SOURCE_SCHEMA` | SADB | Source schema name |
| `TARGET_DATABASE` | D_BRONZE | Target database name |
| `TARGET_SCHEMA` | SADB | Target schema name |
| `CDC_WAREHOUSE` | INFA_INGEST_WH | Warehouse for CDC tasks |
| `TASK_SCHEDULE` | 5 MINUTE | Task execution frequency |
| `DATA_RETENTION_DAYS` | 45 | Time Travel retention |
| `MAX_EXTENSION_DAYS` | 15 | Additional retention extension |

---

## Tables Included (21 Total)

| # | Table Name | Columns | Primary Key |
|---|------------|---------|-------------|
| 1 | TRAIN_PLAN | 17 | TRAIN_PLAN_ID |
| 2 | OPTRN | 17 | OPTRN_ID |
| 3 | OPTRN_LEG | 13 | OPTRN_LEG_ID |
| 4 | OPTRN_EVENT | 28 | OPTRN_EVENT_ID |
| 5 | TRKFC_TRSTN | 40 | SCAC_CD, FSAC_CD |
| 6 | TRAIN_CNST_SMRY | 87 | CNST_NBR, SCAC_CD, TRAIN_CNST_SMRY_VRSN_NBR |
| 7 | STNWYB_MSG_DN | 130 | STNWYB_MSG_ID |
| 8 | EQPMNT_AAR_BASE | 80 | AAR_CAR_TYPE_CD |
| 9 | EQPMV_RFEQP_MVMNT_EVENT | 90 | EVENT_ID |
| 10 | CTNAPP_CTNG_LINE_DN | 65 | CTNG_LINE_ID |
| 11 | EQPMV_EQPMT_EVENT_TYPE | 24 | EQPMT_EVENT_TYPE_ID |
| 12 | TRAIN_PLAN_EVENT | 36 | TRAIN_PLAN_EVENT_ID |
| 13 | LCMTV_MVMNT_EVENT | 43 | EVENT_ID |
| 14 | LCMTV_EMIS | 84 | MARK_CD, EQPUN_NBR |
| 15 | TRAIN_PLAN_LEG | 16 | TRAIN_PLAN_LEG_ID |
| 16 | TRKFCG_SBDVSN | 49 | GRPHC_OBJECT_VRSN_ID |
| 17 | TRKFCG_FIXED_PLANT_ASSET | 52 | GRPHC_OBJECT_VRSN_ID |
| 18 | TRKFCG_FXPLA_TRACK_LCTN_DN | 56 | GRPHC_OBJECT_VRSN_ID |
| 19 | TRAIN_CNST_DTL_RAIL_EQPT | 77 | TRAIN_CNST_SMRY_ID, TRAIN_CNST_SMRY_VRSN_NBR, SQNC_NBR |
| 20 | TRKFCG_TRACK_SGMNT_DN | 58 | GRPHC_OBJECT_VRSN_ID |
| 21 | TRKFCG_SRVC_AREA | 25 | GRPHC_OBJECT_VRSN_ID |

---

## Security & Privileges Required

### Executing Role Requirements:
```sql
-- On SOURCE database/schema
GRANT USAGE ON DATABASE <SOURCE_DATABASE> TO ROLE <DEPLOYER_ROLE>;
GRANT USAGE ON SCHEMA <SOURCE_DATABASE>.<SOURCE_SCHEMA> TO ROLE <DEPLOYER_ROLE>;
GRANT SELECT ON ALL TABLES IN SCHEMA <SOURCE_DATABASE>.<SOURCE_SCHEMA> TO ROLE <DEPLOYER_ROLE>;
GRANT CREATE STREAM ON SCHEMA <SOURCE_DATABASE>.<SOURCE_SCHEMA> TO ROLE <DEPLOYER_ROLE>;

-- On TARGET database/schema
GRANT USAGE ON DATABASE <TARGET_DATABASE> TO ROLE <DEPLOYER_ROLE>;
GRANT USAGE ON SCHEMA <TARGET_DATABASE>.<TARGET_SCHEMA> TO ROLE <DEPLOYER_ROLE>;
GRANT CREATE TABLE ON SCHEMA <TARGET_DATABASE>.<TARGET_SCHEMA> TO ROLE <DEPLOYER_ROLE>;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA <TARGET_DATABASE>.<TARGET_SCHEMA> TO ROLE <DEPLOYER_ROLE>;

-- Warehouse
GRANT USAGE ON WAREHOUSE <CDC_WAREHOUSE> TO ROLE <DEPLOYER_ROLE>;

-- Task privileges
GRANT EXECUTE TASK ON ACCOUNT TO ROLE <DEPLOYER_ROLE>;
```

---

## Post-Deployment Verification

```sql
-- 1. Verify all tasks are running
SHOW TASKS LIKE 'TASK_PROCESS_%' IN SCHEMA <SOURCE_DATABASE>.<SOURCE_SCHEMA>;

-- 2. Check task execution history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME LIKE 'TASK_PROCESS_%'
ORDER BY SCHEDULED_TIME DESC
LIMIT 50;

-- 3. Check audit log for processing stats
SELECT * FROM <TARGET_DATABASE>.<TARGET_SCHEMA>.CDC_PROCESSING_LOG
ORDER BY PROCESSING_START DESC
LIMIT 100;

-- 4. Verify row counts
SELECT 
    'TRAIN_PLAN' AS TABLE_NAME,
    (SELECT COUNT(*) FROM <SOURCE_DATABASE>.<SOURCE_SCHEMA>.TRAIN_PLAN_BASE) AS SOURCE_COUNT,
    (SELECT COUNT(*) FROM <TARGET_DATABASE>.<TARGET_SCHEMA>.TRAIN_PLAN) AS TARGET_COUNT,
    (SELECT COUNT(*) FROM <TARGET_DATABASE>.<TARGET_SCHEMA>.TRAIN_PLAN WHERE IS_DELETED = FALSE) AS ACTIVE_COUNT;
-- Repeat for all tables...
```

---

## Monitoring & Alerting

### Recommended Alerts:
1. **Task Failure Alert**: Trigger when task state = 'FAILED'
2. **Stream Staleness Alert**: Trigger when stream becomes stale
3. **Processing Lag Alert**: Trigger when processing takes > 10 minutes
4. **Zero Rows Alert**: Trigger when multiple consecutive runs process 0 rows

### Monitoring Query:
```sql
-- Daily processing summary
SELECT 
    DATE(PROCESSING_START) AS PROCESSING_DATE,
    TABLE_NAME,
    COUNT(*) AS RUNS,
    SUM(ROWS_PROCESSED) AS TOTAL_ROWS,
    AVG(DATEDIFF('second', PROCESSING_START, PROCESSING_END)) AS AVG_DURATION_SECS,
    COUNT(CASE WHEN STATUS = 'ERROR' THEN 1 END) AS ERROR_COUNT
FROM <TARGET_DATABASE>.<TARGET_SCHEMA>.CDC_PROCESSING_LOG
WHERE PROCESSING_START >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

---

## Rollback Procedure

If issues occur, execute in this order:
```sql
-- 1. Suspend all tasks
ALTER TASK <SOURCE_DATABASE>.<SOURCE_SCHEMA>.TASK_PROCESS_<TABLE> SUSPEND;
-- Repeat for all 21 tasks

-- 2. Drop tasks (optional)
DROP TASK IF EXISTS <SOURCE_DATABASE>.<SOURCE_SCHEMA>.TASK_PROCESS_<TABLE>;

-- 3. Drop procedures (optional)
DROP PROCEDURE IF EXISTS <SOURCE_DATABASE>.<SOURCE_SCHEMA>.SP_PROCESS_<TABLE>();

-- 4. Drop streams (optional)
DROP STREAM IF EXISTS <SOURCE_DATABASE>.<SOURCE_SCHEMA>.<TABLE>_BASE_HIST_STREAM;

-- 5. Target tables can be kept or dropped based on requirement
```

---

## Support & Troubleshooting

### Common Issues:

1. **Stream Stale Error**
   - The procedure auto-recovers by recreating the stream
   - Check CDC_PROCESSING_LOG for STREAM_STALE_DETECTED status

2. **Task Not Running**
   - Verify task is resumed: `ALTER TASK ... RESUME;`
   - Check warehouse is available
   - Verify role has EXECUTE TASK privilege

3. **Privilege Errors**
   - Verify EXECUTE AS OWNER is set (not CALLER)
   - Check procedure owner has required privileges

4. **Performance Issues**
   - Consider increasing warehouse size
   - Review task schedule frequency
   - Check for large initial loads

---

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-02-23 | Snowflake Consultant | Initial production release |

---

## Contact

For issues or questions, contact your Snowflake implementation team.
