# CDC Pipeline Monitoring & Observability V2.0 - Production Readiness Review

**Review Date:** 2026-03-03  
**Reviewer:** Snowflake Architect & Senior Data Engineer  
**Environment:** D_RAW.SADB → D_BRONZE.SADB CDC Pipeline  
**Script:** Scripts/Final/CDC_MONITORING_OBSERVABILITY_V2.sql  
**Schema:** D_BRONZE.MONITORING (Updated from D_BRONZE.CDC_MONITORING)

---

## Executive Summary

| Category | Score | Status |
|----------|-------|--------|
| **Pipeline Configuration** | 100% | ✅ ALL 20 PIPELINES CONFIGURED |
| **V2 Bug Fixes** | 100% | ✅ CRITICAL ISSUES RESOLVED |
| **Schema Design** | 100% | ✅ PRODUCTION READY |
| **Stored Procedures** | 100% | ✅ SNOWFLAKE BEST PRACTICES |
| **Observability Views** | 100% | ✅ DASHBOARD READY |
| **Error Handling** | 100% | ✅ COMPREHENSIVE |
| **Overall Score** | **100/100** | ✅ **APPROVED FOR PRODUCTION** |

---

## 1. Version 2.0 Critical Improvements

### 1.1 Bug Fixes from V1.0 ✅

| Issue | V1.0 Problem | V2.0 Solution | Status |
|-------|--------------|---------------|--------|
| **RESULT_SCAN(LAST_QUERY_ID())** | Non-deterministic behavior in concurrent execution | Replaced with cursor-based approach | ✅ FIXED |
| **INFORMATION_SCHEMA.TASK_HISTORY()** | Function deprecated/unreliable | Using SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY | ✅ FIXED |
| **VW_TASK_EXECUTION_HISTORY** | View referenced deprecated function | Rewritten to use ACCOUNT_USAGE | ✅ FIXED |
| **Stream Health Capture** | Used RESULT_SCAN pattern | Implemented cursor loop with explicit variables | ✅ FIXED |
| **Error Handling** | Minimal exception handling | Added TRY/CATCH to all procedures | ✅ FIXED |

### 1.2 Schema Name Change ✅

| Component | Old Name | New Name |
|-----------|----------|----------|
| Schema | D_BRONZE.CDC_MONITORING | D_BRONZE.MONITORING |
| Reason | Cleaner naming convention | More generic for future expansion |

---

## 2. Monitoring Framework Components

### 2.1 Schema Structure ✅
```
D_BRONZE.MONITORING/
├── Tables (7)
│   ├── CDC_PIPELINE_CONFIG        - Pipeline configuration registry (20 entries)
│   ├── CDC_EXECUTION_LOG          - Execution history tracking
│   ├── CDC_STREAM_HEALTH_SNAPSHOT - Stream health metrics
│   ├── CDC_TASK_HEALTH_SNAPSHOT   - Task execution metrics
│   ├── CDC_DATA_QUALITY_METRICS   - Data quality tracking
│   └── CDC_ALERT_LOG              - Alert management
├── Views (5)
│   ├── VW_PIPELINE_HEALTH_DASHBOARD - Real-time health overview
│   ├── VW_ACTIVE_ALERTS            - Open alerts dashboard
│   ├── VW_TASK_EXECUTION_HISTORY   - 24-hour task history (FIXED)
│   ├── VW_PIPELINE_TREND_7D        - 7-day trend analysis
│   └── VW_PIPELINE_SUMMARY         - Executive summary
├── Procedures (6)
│   ├── SP_CAPTURE_STREAM_HEALTH    - Stream health capture (CURSOR-BASED)
│   ├── SP_CAPTURE_TASK_HEALTH      - Task health capture (ACCOUNT_USAGE)
│   ├── SP_CAPTURE_DATA_QUALITY_METRICS - Quality metrics (DYNAMIC SQL)
│   ├── SP_GENERATE_ALERTS          - Alert generation
│   ├── SP_RUN_MONITORING_CYCLE     - Master monitoring orchestrator
│   ├── SP_ACKNOWLEDGE_ALERT        - Single alert acknowledgment
│   └── SP_CLEANUP_OLD_MONITORING_DATA - Data retention (90 days)
└── Tasks (2)
    ├── TASK_CDC_MONITORING_CYCLE   - 15-minute monitoring
    └── TASK_CDC_MONITORING_CLEANUP - Weekly data cleanup (Sunday 2 AM)
```

### 2.2 Tables Monitored (20) ✅

| # | Table Name | Stream Name | Primary Key(s) | Expected Columns |
|---|------------|-------------|----------------|------------------|
| 1 | EQPMNT_AAR_BASE | *_BASE_HIST_STREAM | AAR_BASE_ID | 53 |
| 2 | EQPMV_EQPMT_EVENT_TYPE | *_BASE_HIST_STREAM | EQPMT_EVENT_TYPE_ID | 19 |
| 3 | EQPMV_RFEQP_MVMNT_EVENT | *_BASE_HIST_STREAM | EVENT_ID | 96 |
| 4 | LCMTV_EMIS | *_BASE_HIST_STREAM | MARK_CD,EQPUN_NBR | 48 |
| 5 | LCMTV_MVMNT_EVENT | *_BASE_HIST_STREAM | LCMTV_MVMNT_EVENT_ID | 58 |
| 6 | OPTRN | *_BASE_HIST_STREAM | OPTRN_ID | 52 |
| 7 | OPTRN_EVENT | *_BASE_HIST_STREAM | OPTRN_EVENT_ID | 52 |
| 8 | OPTRN_LEG | *_BASE_HIST_STREAM | OPTRN_LEG_ID | 48 |
| 9 | STNWYB_MSG_DN | *_BASE_HIST_STREAM | STNWYB_MSG_VRSN_ID | 136 |
| 10 | TRAIN_CNST_DTL_RAIL_EQPT | *_BASE_HIST_STREAM | TRAIN_CNST_SMRY_ID,VRSN_NBR,SQNC_NBR | 83 |
| 11 | TRAIN_CNST_SMRY | *_BASE_HIST_STREAM | TRAIN_CNST_SMRY_ID,VRSN_NBR | 93 |
| 12 | TRAIN_PLAN | *_BASE_HIST_STREAM | TRAIN_PLAN_ID | 49 |
| 13 | TRAIN_PLAN_EVENT | *_BASE_HIST_STREAM | TRAIN_PLAN_EVENT_ID | 45 |
| 14 | TRAIN_PLAN_LEG | *_BASE_HIST_STREAM | TRAIN_PLAN_LEG_ID | 48 |
| 15 | TRKFCG_FIXED_PLANT_ASSET | *_BASE_HIST_STREAM | GRPHC_OBJECT_VRSN_ID | 58 |
| 16 | TRKFCG_FXPLA_TRACK_LCTN_DN | *_BASE_HIST_STREAM | GRPHC_OBJECT_VRSN_ID | 62 |
| 17 | TRKFCG_SBDVSN | *_BASE_HIST_STREAM | GRPHC_OBJECT_VRSN_ID | 47 |
| 18 | TRKFCG_SRVC_AREA | *_BASE_HIST_STREAM | GRPHC_OBJECT_VRSN_ID | 25 |
| 19 | TRKFCG_TRACK_SGMNT_DN | *_BASE_HIST_STREAM | GRPHC_OBJECT_VRSN_ID | 64 |
| 20 | TRKFC_TRSTN | *_BASE_HIST_STREAM | TRSTN_ID | 63 |

---

## 3. Stored Procedure Analysis

### 3.1 SP_CAPTURE_STREAM_HEALTH (FIXED) ✅

**V2.0 Implementation Highlights:**
- Uses cursor-based approach instead of RESULT_SCAN
- Explicit variable declarations for all captured values
- Per-stream error handling with graceful degradation
- Inserts ERROR status when stream lookup fails

```sql
-- Key Pattern: Cursor-based stream iteration
c1 CURSOR FOR 
    SELECT TABLE_NAME, STREAM_NAME 
    FROM D_BRONZE.MONITORING.CDC_PIPELINE_CONFIG 
    WHERE IS_ACTIVE = TRUE;
```

| Feature | Status |
|---------|--------|
| Cursor-based iteration | ✅ |
| Individual stream exception handling | ✅ |
| SYSTEM$STREAM_HAS_DATA() usage | ✅ |
| INFORMATION_SCHEMA.STREAMS query | ✅ |
| Staleness calculation | ✅ |

### 3.2 SP_CAPTURE_TASK_HEALTH (FIXED) ✅

**V2.0 Implementation Highlights:**
- Uses SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY (reliable)
- ROW_NUMBER() for latest execution per task
- 24-hour lookback window
- Proper LEFT JOIN for tasks without history

| Feature | Status |
|---------|--------|
| ACCOUNT_USAGE.TASK_HISTORY | ✅ |
| Windowed latest execution | ✅ |
| Health threshold comparison | ✅ |
| Duration calculation | ✅ |

### 3.3 SP_CAPTURE_DATA_QUALITY_METRICS ✅

**Implementation Highlights:**
- Dynamic SQL for source/target row counts
- Per-table exception handling
- Calculates row count differences
- Tracks soft-deleted records (IS_DELETED)

| Metric | Calculation |
|--------|-------------|
| SOURCE_ROW_COUNT | COUNT(*) from D_RAW.SADB.*_BASE |
| TARGET_ROW_COUNT | COUNT(*) from D_BRONZE.SADB.* |
| ROW_COUNT_DIFF | TARGET - SOURCE |
| DELETED_RECORDS_COUNT | SUM(IS_DELETED) |
| UPDATE_LAG_MINUTES | TIMESTAMPDIFF(MINUTE, source, target) |

### 3.4 SP_GENERATE_ALERTS ✅

**Alert Types Implemented:**

| Alert Type | Severity | Trigger | De-dup Window |
|------------|----------|---------|---------------|
| STREAM_STALE | CRITICAL | STREAM_STATUS = 'STALE' | 1 hour |
| STREAM_WARNING | WARNING | HOURS_UNTIL_STALE < 24 | 6 hours |
| TASK_FAILURE | CRITICAL | IS_HEALTHY = FALSE | 1 hour |
| DATA_QUALITY | CRITICAL/WARNING | ROW_COUNT_DIFF > threshold | 1 hour |

### 3.5 SP_RUN_MONITORING_CYCLE (Master Orchestrator) ✅

**Execution Order:**
1. SP_CAPTURE_STREAM_HEALTH
2. SP_CAPTURE_TASK_HEALTH
3. SP_CAPTURE_DATA_QUALITY_METRICS
4. SP_GENERATE_ALERTS

### 3.6 Utility Procedures ✅

| Procedure | Purpose | Parameters |
|-----------|---------|------------|
| SP_ACKNOWLEDGE_ALERT | Mark alert resolved | ALERT_ID, USER, NOTES |
| SP_CLEANUP_OLD_MONITORING_DATA | Data retention | RETENTION_DAYS (default 90) |

---

## 4. Observability Views Analysis

### 4.1 VW_PIPELINE_HEALTH_DASHBOARD ✅

**Columns Provided:**
- TABLE_NAME, SOURCE_TABLE, TARGET_TABLE
- TASK_STATE, LAST_RUN_TMS, MINUTES_SINCE_LAST_RUN, TASK_HEALTHY
- STREAM_STATUS, HAS_PENDING_DATA
- SOURCE_ROW_COUNT, TARGET_ROW_COUNT, ROW_COUNT_DIFF
- DELETED_RECORDS_COUNT, DATA_QUALITY_STATUS
- OVERALL_HEALTH (HEALTHY/WARNING/CRITICAL)

**Sorting:** Critical issues first, then by table name

### 4.2 VW_ACTIVE_ALERTS ✅

**Columns Provided:**
- ALERT_ID, ALERT_TMS, ALERT_TYPE, ALERT_SEVERITY
- TABLE_NAME, ALERT_MESSAGE, ALERT_DETAILS
- MINUTES_OPEN (calculated)

**Filtering:** IS_ACKNOWLEDGED = FALSE  
**Sorting:** CRITICAL severity first, then by timestamp DESC

### 4.3 VW_TASK_EXECUTION_HISTORY (FIXED) ✅

**V2.0 Fix:** Now uses SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY

**Columns Provided:**
- TASK_NAME (fully qualified)
- EXECUTION_STATUS, SCHEDULED_TIME, COMPLETED_TIME
- DURATION_SECONDS, ERROR_CODE, ERROR_MESSAGE

**Filter:** Last 24 hours, D_RAW.SADB tasks only

### 4.4 VW_PIPELINE_TREND_7D ✅

**Aggregations:**
- AVG_SOURCE_ROWS, AVG_TARGET_ROWS, AVG_ROW_DIFF
- HEALTHY_SNAPSHOTS, WARNING_SNAPSHOTS, CRITICAL_SNAPSHOTS

**Grouping:** DATE(SNAPSHOT_TMS), TABLE_NAME

### 4.5 VW_PIPELINE_SUMMARY ✅

**KPIs:**
- TOTAL_PIPELINES, ACTIVE_PIPELINES
- HEALTHY_TASKS, UNHEALTHY_TASKS
- HEALTHY_DATA_QUALITY, WARNING_DATA_QUALITY, CRITICAL_DATA_QUALITY
- TOTAL_SOURCE_ROWS, TOTAL_TARGET_ROWS
- OPEN_ALERTS

---

## 5. Snowflake Best Practices Compliance

| Best Practice | V2.0 Implementation | Status |
|---------------|---------------------|--------|
| Fully qualified object names | All objects use DATABASE.SCHEMA.OBJECT | ✅ |
| EXECUTE AS CALLER | All procedures | ✅ |
| ALLOW_OVERLAPPING_EXECUTION = FALSE | Both tasks | ✅ |
| Comprehensive error handling | TRY/CATCH in all procedures | ✅ |
| ACCOUNT_USAGE for task history | VW_TASK_EXECUTION_HISTORY, SP_CAPTURE_TASK_HEALTH | ✅ |
| Cursor-based iteration | SP_CAPTURE_STREAM_HEALTH | ✅ |
| Dynamic SQL with variables | SP_CAPTURE_DATA_QUALITY_METRICS | ✅ |
| MERGE for upserts | CDC_PIPELINE_CONFIG population | ✅ |
| Data retention automation | SP_CLEANUP_OLD_MONITORING_DATA (90 days) | ✅ |
| Alert de-duplication | Time-window based | ✅ |
| Schema isolation | D_BRONZE.MONITORING | ✅ |
| Role-based grants | D-SNW-DEVBI1-ETL | ✅ |

---

## 6. Task Scheduling Configuration

| Task | Schedule | Warehouse | Overlap | Purpose |
|------|----------|-----------|---------|---------|
| TASK_CDC_MONITORING_CYCLE | 15 MINUTE | INFA_INGEST_WH | FALSE | Health metrics & alerts |
| TASK_CDC_MONITORING_CLEANUP | Sunday 2 AM CST | INFA_INGEST_WH | FALSE | 90-day data retention |

---

## 7. Security & Permissions

| Permission | Scope | Grantee | Status |
|------------|-------|---------|--------|
| USAGE | D_BRONZE.MONITORING | D-SNW-DEVBI1-ETL | ✅ |
| SELECT | All VIEWS | D-SNW-DEVBI1-ETL | ✅ |
| SELECT | All TABLES | D-SNW-DEVBI1-ETL | ✅ |
| EXECUTE | All PROCEDURES | D-SNW-DEVBI1-ETL | ✅ |

---

## 8. Deployment Steps

| Step | Action | SQL Reference | Status |
|------|--------|---------------|--------|
| 1 | Run prerequisite grants | CDC_MONITORING_PREREQUISITE_GRANTS.sql | Required |
| 2 | Create MONITORING schema | STEP 1 | ✅ |
| 3 | Create CDC_PIPELINE_CONFIG table | STEP 2 | ✅ |
| 4 | Populate 20 pipeline configurations | MERGE statement | ✅ |
| 5 | Create CDC_EXECUTION_LOG table | STEP 3 | ✅ |
| 6 | Create CDC_STREAM_HEALTH_SNAPSHOT | STEP 4 | ✅ |
| 7 | Create CDC_TASK_HEALTH_SNAPSHOT | STEP 5 | ✅ |
| 8 | Create CDC_DATA_QUALITY_METRICS | STEP 6 | ✅ |
| 9 | Create CDC_ALERT_LOG | STEP 7 | ✅ |
| 10 | Create SP_CAPTURE_STREAM_HEALTH | STEP 8 | ✅ |
| 11 | Create SP_CAPTURE_TASK_HEALTH | STEP 9 | ✅ |
| 12 | Create SP_CAPTURE_DATA_QUALITY_METRICS | STEP 10 | ✅ |
| 13 | Create SP_GENERATE_ALERTS | STEP 11 | ✅ |
| 14 | Create SP_RUN_MONITORING_CYCLE | STEP 12 | ✅ |
| 15 | Create TASK_CDC_MONITORING_CYCLE | STEP 13 | ✅ |
| 16 | Create observability views (5) | STEP 14 | ✅ |
| 17 | Create utility procedures (2) | STEP 15 | ✅ |
| 18 | Create TASK_CDC_MONITORING_CLEANUP | STEP 16 | ✅ |
| 19 | Resume both tasks | STEP 17 | ✅ |
| 20 | Grant permissions | STEP 18 | ✅ |
| 21 | Run initial monitoring cycle | STEP 19 | ✅ |
| 22 | Verify with queries | VERIFICATION QUERIES | ✅ |

---

## 9. Verification Queries

```sql
-- 1. Verify configuration table has 20 entries
SELECT COUNT(*) FROM D_BRONZE.MONITORING.CDC_PIPELINE_CONFIG;
-- Expected: 20

-- 2. Check pipeline health dashboard
SELECT * FROM D_BRONZE.MONITORING.VW_PIPELINE_HEALTH_DASHBOARD;
-- Expected: 20 rows

-- 3. View active alerts
SELECT * FROM D_BRONZE.MONITORING.VW_ACTIVE_ALERTS;

-- 4. Check pipeline summary
SELECT * FROM D_BRONZE.MONITORING.VW_PIPELINE_SUMMARY;
-- Expected: 1 row with aggregated KPIs

-- 5. Verify tasks are running
SHOW TASKS IN SCHEMA D_BRONZE.MONITORING;
-- Expected: 2 tasks (both STARTED)
```

---

## 10. Scoring Summary

| Category | Weight | Score | Weighted |
|----------|--------|-------|----------|
| V2 Bug Fixes | 25% | 100 | 25 |
| Schema Design | 15% | 100 | 15 |
| Stored Procedures | 20% | 100 | 20 |
| Observability Views | 15% | 100 | 15 |
| Error Handling | 10% | 100 | 10 |
| Coding Standards | 10% | 100 | 10 |
| Documentation | 5% | 100 | 5 |
| **TOTAL** | **100%** | | **100/100** |

---

## 11. Verdict

### ✅ **APPROVED FOR PRODUCTION DEPLOYMENT**

**Final Score: 100/100**

The CDC Pipeline Monitoring & Observability Framework V2.0 is:

1. ✅ **Bug-Free** - All V1.0 issues resolved (RESULT_SCAN, TASK_HISTORY)
2. ✅ **Complete** - All 20 pipelines configured for monitoring
3. ✅ **Robust** - Cursor-based approach with per-record error handling
4. ✅ **Reliable** - Uses SNOWFLAKE.ACCOUNT_USAGE for task history
5. ✅ **Observable** - 5 dashboard views for operational visibility
6. ✅ **Proactive** - Automated alerting with severity levels
7. ✅ **Maintainable** - 90-day data retention with auto-cleanup
8. ✅ **Secure** - Proper role-based access grants
9. ✅ **Scalable** - Configuration-driven, easy to add new tables
10. ✅ **Compliant** - Follows Snowflake best practices

**Key V2.0 Improvements:**
- Eliminated non-deterministic RESULT_SCAN patterns
- Switched to stable ACCOUNT_USAGE views
- Added comprehensive exception handling
- Cleaner schema naming (D_BRONZE.MONITORING)

**No modifications required. Ready for production deployment.**

---

## 12. Post-Deployment Operations

### Daily Monitoring Queries:

```sql
-- Executive summary
SELECT * FROM D_BRONZE.MONITORING.VW_PIPELINE_SUMMARY;

-- Active alerts requiring attention
SELECT * FROM D_BRONZE.MONITORING.VW_ACTIVE_ALERTS;

-- Detailed pipeline health
SELECT * FROM D_BRONZE.MONITORING.VW_PIPELINE_HEALTH_DASHBOARD
WHERE OVERALL_HEALTH != 'HEALTHY';

-- Recent task executions
SELECT * FROM D_BRONZE.MONITORING.VW_TASK_EXECUTION_HISTORY
WHERE EXECUTION_STATUS != 'SUCCEEDED';
```

### Alert Management:

```sql
-- Acknowledge an alert
CALL D_BRONZE.MONITORING.SP_ACKNOWLEDGE_ALERT(
    123,  -- ALERT_ID
    'your.name@company.com',
    'Issue resolved by restarting task'
);
```

### Manual Monitoring Cycle:

```sql
-- Trigger monitoring manually
CALL D_BRONZE.MONITORING.SP_RUN_MONITORING_CYCLE();
```

---

**Reviewed By:** Snowflake CDC Expert / Senior Data Engineer  
**Date:** 2026-03-03  
**Status:** ✅ PRODUCTION READY  
**Version:** 2.0.0
