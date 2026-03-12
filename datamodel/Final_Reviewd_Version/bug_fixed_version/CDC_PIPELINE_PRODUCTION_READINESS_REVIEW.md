# CPKC Rail 360 CDC Pipeline - Production Readiness Review

**Review Date:** March 12, 2026  
**Pipeline Version:** 3.0.0 (Bug Fixed)  
**Reviewer:** Cortex Code Automated Review  
**Environment:** D_RAW.SADB → D_BRONZE.SADB

---

## DELIVERABLE 1: Consolidated Review Document

### Executive Summary

All 22 tables follow a consistent, well-designed CDC pattern using Snowflake best practices. The implementation demonstrates mature engineering with proper stream staleness handling, temporary table staging, and comprehensive CDC operation coverage.

---

### Per-Table Review Scores

| # | Table Name | Score | Critical | Warning | Info |
|---|------------|-------|----------|---------|------|
| 1 | EQPMNT_AAR_BASE | 92/100 | 0 | 1 | 2 |
| 2 | EQPMV_EQPMT_EVENT_TYPE | 92/100 | 0 | 1 | 2 |
| 3 | EQPMV_RFEQP_MVMNT_EVENT | 92/100 | 0 | 1 | 2 |
| 4 | LCMTV_EMIS | 94/100 | 0 | 1 | 1 |
| 5 | LCMTV_MVMNT_EVENT | 92/100 | 0 | 1 | 2 |
| 6 | STNWYB_MSG_DN | 92/100 | 0 | 1 | 2 |
| 7 | TRAIN_CNST_DTL_RAIL_EQPT | 94/100 | 0 | 1 | 1 |
| 8 | TRAIN_CNST_SMRY | 94/100 | 0 | 1 | 1 |
| 9 | TRAIN_OPTRN | 92/100 | 0 | 1 | 2 |
| 10 | TRAIN_OPTRN_EVENT | 92/100 | 0 | 1 | 2 |
| 11 | TRAIN_OPTRN_LEG | 92/100 | 0 | 1 | 2 |
| 12 | TRAIN_PLAN | 92/100 | 0 | 1 | 2 |
| 13 | TRAIN_PLAN_EVENT | 92/100 | 0 | 1 | 2 |
| 14 | TRAIN_PLAN_LEG | 92/100 | 0 | 1 | 2 |
| 15 | TRAIN_TYPE | 92/100 | 0 | 1 | 2 |
| 16 | TRAIN_KIND | 92/100 | 0 | 1 | 2 |
| 17 | TRKFCG_FIXED_PLANT_ASSET | 92/100 | 0 | 1 | 2 |
| 18 | TRKFCG_FXPLA_TRACK_LCTN_DN | 92/100 | 0 | 1 | 2 |
| 19 | TRKFCG_SBDVSN | 92/100 | 0 | 1 | 2 |
| 20 | TRKFCG_SRVC_AREA | 92/100 | 0 | 1 | 2 |
| 21 | TRKFCG_TRACK_SGMNT_DN | 92/100 | 0 | 1 | 2 |
| 22 | TRKFC_TRSTN | 94/100 | 0 | 1 | 1 |

**Average Score: 92.4/100**

---

### Common Strengths Across All Tables

| Category | Implementation | Rating |
|----------|----------------|--------|
| **Stream Configuration** | SHOW_INITIAL_ROWS=TRUE, proper CHANGE_TRACKING | Excellent |
| **Data Retention** | 45 days + 15 days extension | Excellent |
| **Staleness Detection** | Exception-based detection with auto-recovery | Excellent |
| **Staging Pattern** | Temporary table for single stream read | Best Practice |
| **MERGE Logic** | 4 WHEN clauses (UPDATE/DELETE/RE-INSERT/INSERT) | Excellent |
| **CDC Metadata** | All 6 columns properly maintained | Excellent |
| **Filter Logic** | SNW_OPERATION_OWNER exclusion consistent | Excellent |
| **Error Handling** | TRY-CATCH with temp table cleanup | Good |
| **Task Configuration** | ALLOW_OVERLAPPING_EXECUTION=FALSE | Excellent |
| **Batch Tracking** | Unique BATCH_ID per execution | Excellent |

---

### Common Issues Found (All Tables)

#### Warning Level Issues

**W1: Missing WHEN NOT MATCHED for DELETE events**
- **Severity**: Warning
- **Location**: All MERGE statements
- **Issue**: DELETE events arriving for non-existent records in target are silently ignored
- **Impact**: Low - edge case, but could indicate data drift
- **Recommendation**: Add logging for unmatched DELETE events:
```sql
WHEN NOT MATCHED AND src.CDC_ACTION = 'DELETE' THEN
    INSERT (...) VALUES (..., 'ORPHAN_DELETE', ...)
```

#### Info Level Issues

**I1: No explicit row count logging to monitoring tables**
- **Severity**: Info
- **Location**: All stored procedures
- **Issue**: SQLROWCOUNT captured but only returned in string format
- **Recommendation**: Consider INSERT into CDC_EXECUTION_LOG table within procedure

**I2: Timestamp precision inconsistency**
- **Severity**: Info
- **Location**: Target table DDLs
- **Issue**: Source SNW_LAST_REPLICATED is TIMESTAMP_NTZ(9), CDC_TIMESTAMP is TIMESTAMP_NTZ (default 9)
- **Recommendation**: Standardize all timestamp precisions explicitly

---

### Table-Specific Notes

**Composite Primary Key Tables** (Higher scores for proper implementation):
- `LCMTV_EMIS`: PK = (MARK_CD, EQPUN_NBR) - Correctly implemented
- `TRAIN_CNST_DTL_RAIL_EQPT`: PK = (TRAIN_CNST_SMRY_ID, TRAIN_CNST_SMRY_VRSN_NBR, SQNC_NBR) - Correctly implemented
- `TRAIN_CNST_SMRY`: PK = (TRAIN_CNST_SMRY_ID, TRAIN_CNST_SMRY_VRSN_NBR) - Correctly implemented
- `TRKFC_TRSTN`: PK = (SCAC_CD, FSAC_CD) - Correctly implemented

---

### Individual Table Details

#### Equipment Domain (EQPMNT, EQPMV)

| Table | Columns | Primary Key | Notes |
|-------|---------|-------------|-------|
| EQPMNT_AAR_BASE | 88 | EQPMNT_ID | Standard implementation |
| EQPMV_EQPMT_EVENT_TYPE | 31 | EQPMT_EVENT_TYPE_ID | Standard implementation |
| EQPMV_RFEQP_MVMNT_EVENT | 97 | EVENT_ID | Largest column count in equipment domain |

#### Locomotive Domain (LCMTV)

| Table | Columns | Primary Key | Notes |
|-------|---------|-------------|-------|
| LCMTV_EMIS | 91 | MARK_CD, EQPUN_NBR | Composite PK properly handled |
| LCMTV_MVMNT_EVENT | 50 | EVENT_ID | Standard implementation |

#### Message Domain (STNWYB)

| Table | Columns | Primary Key | Notes |
|-------|---------|-------------|-------|
| STNWYB_MSG_DN | 137 | STNWYB_MSG_VRSN_ID | Highest column count in pipeline |

#### Train Domain (TRAIN)

| Table | Columns | Primary Key | Notes |
|-------|---------|-------------|-------|
| TRAIN_CNST_DTL_RAIL_EQPT | 84 | 3-column composite | Most complex PK |
| TRAIN_CNST_SMRY | 94 | 2-column composite | High column count |
| TRAIN_OPTRN | 24 | OPTRN_ID | Standard implementation |
| TRAIN_OPTRN_EVENT | 35 | OPTRN_EVENT_ID | Standard implementation |
| TRAIN_OPTRN_LEG | 20 | OPTRN_LEG_ID | Standard implementation |
| TRAIN_PLAN | 24 | TRAIN_PLAN_ID | Standard implementation |
| TRAIN_PLAN_EVENT | 43 | TRAIN_PLAN_EVENT_ID | Standard implementation |
| TRAIN_PLAN_LEG | 23 | TRAIN_PLAN_LEG_ID | Standard implementation |
| TRAIN_TYPE | 15 | TRAIN_TYPE_CD | Reference table, VARCHAR PK |
| TRAIN_KIND | 17 | TRAIN_KIND_CD | Reference table, VARCHAR PK |

#### Track Configuration Domain (TRKFCG, TRKFC)

| Table | Columns | Primary Key | Notes |
|-------|---------|-------------|-------|
| TRKFCG_FIXED_PLANT_ASSET | 59 | GRPHC_OBJECT_VRSN_ID | Standard implementation |
| TRKFCG_FXPLA_TRACK_LCTN_DN | 63 | GRPHC_OBJECT_VRSN_ID | Standard implementation |
| TRKFCG_SBDVSN | 56 | GRPHC_OBJECT_VRSN_ID | Standard implementation |
| TRKFCG_SRVC_AREA | 32 | GRPHC_OBJECT_VRSN_ID | Standard implementation |
| TRKFCG_TRACK_SGMNT_DN | 65 | GRPHC_OBJECT_VRSN_ID | Standard implementation |
| TRKFC_TRSTN | 47 | SCAC_CD, FSAC_CD | Composite PK properly handled |

---

## DELIVERABLE 2: Monitoring & Observability Review

### Monitoring Framework Score: **88/100**

### Implemented Components

| Component | Status | Quality |
|-----------|--------|---------|
| CDC_PIPELINE_CONFIG | Implemented | Excellent |
| CDC_EXECUTION_LOG | Implemented | Good |
| CDC_STREAM_HEALTH_SNAPSHOT | Implemented | Good |
| CDC_TASK_HEALTH_SNAPSHOT | Implemented | Excellent |
| CDC_DATA_QUALITY_METRICS | Implemented | Excellent |
| CDC_ALERT_LOG | Implemented | Good |
| Monitoring Task (15 min) | Implemented | Good |
| Dashboard Views | Implemented | Excellent |

### Strengths

1. **Comprehensive configuration table** with all 22 tables properly registered
2. **Filter-aware data quality checks** accounting for TSDPRG/EMEPRG exclusions
3. **ACCOUNT_USAGE integration** for task history monitoring
4. **Alert deduplication** prevents alert flooding (1-hour window)
5. **Health threshold configuration** (30 min default per table)
6. **Retention policy** (90 days on monitoring schema)

### Gaps Identified

| Gap | Severity | Description | Recommendation |
|-----|----------|-------------|----------------|
| **G1** | Warning | Stream staleness detection incomplete in SP_CAPTURE_STREAM_HEALTH | Use SYSTEM$STREAM_HAS_DATA() AND check "stale" column from SHOW STREAMS |
| **G2** | Warning | No alerting integration (email/Slack/PagerDuty) | Implement NOTIFICATION_INTEGRATION with alert procedures |
| **G3** | Info | Monitoring runs every 15 min vs CDC every 5 min | Consider 5-min monitoring alignment for faster detection |
| **G4** | Info | No historical trend analysis views | Add views for week-over-week processing trends |
| **G5** | Info | Missing column count validation | Add source vs target column count verification |

### Prerequisite Grants Review

**Score: 95/100** - Comprehensive and well-structured

| Section | Status | Notes |
|---------|--------|-------|
| Database grants | Complete | D_RAW, D_BRONZE |
| Schema grants | Complete | Including CREATE SCHEMA |
| Table grants | Complete | All 22 source tables |
| Stream grants | Complete | Including FUTURE streams |
| Task grants | Complete | OPERATE, MONITOR, EXECUTE TASK |
| Procedure grants | Complete | Including FUTURE procedures |
| Warehouse grants | Complete | INFA_INGEST_WH |
| ACCOUNT_USAGE | Complete | IMPORTED PRIVILEGES |

---

## DELIVERABLE 3: Overall Production Readiness Summary

### Overall Score: **91/100** - PRODUCTION READY

---

### Top 5 Critical Findings

| # | Finding | Impact | Affected Tables |
|---|---------|--------|-----------------|
| 1 | **Stream staleness detection relies on exception handling** | Medium - May miss some staleness scenarios | All 22 |
| 2 | **No external alerting configured** | Medium - Ops team won't receive real-time notifications | Monitoring |
| 3 | **Orphan DELETE events silently dropped** | Low - Could mask data integrity issues | All 22 |
| 4 | **No execution metrics written to log within procedures** | Low - Requires external monitoring query | All 22 |
| 5 | **5-min task vs 15-min monitoring mismatch** | Low - Delayed issue detection | Monitoring |

---

### Top 5 Recommendations

| # | Recommendation | Priority | Effort |
|---|----------------|----------|--------|
| 1 | **Add SYSTEM$STREAM_GET_STALE_AFTER() check** before stream read for proactive staleness detection | High | Low |
| 2 | **Implement NOTIFICATION_INTEGRATION** with email/Slack for CRITICAL alerts | High | Medium |
| 3 | **Add execution logging within procedures** - INSERT into CDC_EXECUTION_LOG with row counts | Medium | Low |
| 4 | **Align monitoring frequency** to 5 minutes to match CDC task schedule | Medium | Low |
| 5 | **Add orphan DELETE tracking** - Log DELETE events for non-existent records | Low | Low |

---

### Go/No-Go Recommendation

## **GO** - Ready for Production Deployment

### Justification

| Criteria | Assessment | Status |
|----------|------------|--------|
| **CDC Logic Correctness** | All INSERT/UPDATE/DELETE/RE-INSERT scenarios properly handled | PASS |
| **Idempotency** | MERGE with proper WHEN clauses ensures idempotent processing | PASS |
| **Stream Resilience** | Auto-recovery on staleness with SHOW_INITIAL_ROWS=TRUE | PASS |
| **Data Retention** | 45+15 days exceeds typical requirements | PASS |
| **Concurrency Control** | ALLOW_OVERLAPPING_EXECUTION=FALSE prevents race conditions | PASS |
| **Error Handling** | Exception handling with temp table cleanup | PASS |
| **Monitoring Coverage** | Comprehensive framework with alerting foundation | PASS |
| **Security/Grants** | Proper RBAC configuration documented | PASS |
| **Naming Conventions** | Consistent across all objects | PASS |
| **Documentation** | Excellent header comments in all files | PASS |

### Pre-Production Checklist

- [ ] Run MONITORING_OBSERVABILITY_PREREQUISITE_GRANTS.sql
- [ ] Deploy all 22 table scripts in sequence
- [ ] Deploy MONITORING_OBSERVABILITY.sql
- [ ] Resume all 22 CDC tasks
- [ ] Resume monitoring task
- [ ] Validate initial data load completes
- [ ] Configure external alerting (recommended)

---

## Appendix A: CDC Pattern Reference

### Stream Metadata Interpretation

| METADATA$ACTION | METADATA$ISUPDATE | Meaning | Target Operation |
|-----------------|-------------------|---------|------------------|
| INSERT | FALSE | New row inserted | INSERT new record |
| INSERT | TRUE | Row updated | UPDATE existing record |
| DELETE | FALSE | Row deleted | Soft DELETE (IS_DELETED=TRUE) |
| INSERT | FALSE | Row re-inserted | UPDATE with IS_DELETED=FALSE |

### Standard CDC Metadata Columns

| Column | Type | Purpose |
|--------|------|---------|
| CDC_OPERATION | VARCHAR(10) | INSERT/UPDATE/DELETE/RELOADED |
| CDC_TIMESTAMP | TIMESTAMP_NTZ | When change was processed |
| IS_DELETED | BOOLEAN | Soft delete flag |
| RECORD_CREATED_AT | TIMESTAMP_NTZ | First insertion timestamp |
| RECORD_UPDATED_AT | TIMESTAMP_NTZ | Last modification timestamp |
| SOURCE_LOAD_BATCH_ID | VARCHAR(100) | Batch tracking identifier |

---

## Appendix B: Deployment Order

Execute scripts in this order for clean deployment:

```
1.  MONITORING_OBSERVABILITY_PREREQUISITE_GRANTS.sql
2.  EQPMNT_AAR_BASE.sql
3.  EQPMV_EQPMT_EVENT_TYPE.sql
4.  EQPMV_RFEQP_MVMNT_EVENT.sql
5.  LCMTV_EMIS.sql
6.  LCMTV_MVMNT_EVENT.sql
7.  STNWYB_MSG_DN.sql
8.  TRAIN_CNST_DTL_RAIL_EQPT.sql
9.  TRAIN_CNST_SMRY.sql
10. TRAIN_OPTRN.sql
11. TRAIN_OPTRN_EVENT.sql
12. TRAIN_OPTRN_LEG.sql
13. TRAIN_PLAN.sql
14. TRAIN_PLAN_EVENT.sql
15. TRAIN_PLAN_LEG.sql
16. TRAIN_TYPE.sql
17. TRAIN_KIND.sql
18. TRKFCG_FIXED_PLANT_ASSET.sql
19. TRKFCG_FXPLA_TRACK_LCTN_DN.sql
20. TRKFCG_SBDVSN.sql
21. TRKFCG_SRVC_AREA.sql
22. TRKFCG_TRACK_SGMNT_DN.sql
23. TRKFC_TRSTN.sql
24. MONITORING_OBSERVABILITY.sql
```

---

## Appendix C: Configuration Summary

| Parameter | Value | Notes |
|-----------|-------|-------|
| Source Schema | D_RAW.SADB | IDMC replication target |
| Target Schema | D_BRONZE.SADB | Data preservation layer |
| Warehouse | INFA_INGEST_WH | Shared ingestion warehouse |
| Task Schedule | 5 MINUTE | All 22 CDC tasks |
| Monitoring Schedule | 15 MINUTE | Health check cycle |
| Data Retention | 45 days | Source tables |
| Max Extension | 15 days | Additional retention buffer |
| Stream Type | Standard | With SHOW_INITIAL_ROWS=TRUE |
| Procedure Mode | EXECUTE AS CALLER | Uses caller's context |
| Filter Values | TSDPRG, EMEPRG | Excluded operation owners |

---

*End of Production Readiness Review*
