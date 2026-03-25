# CDC Monitoring & Observability Framework — Final Production Review

**Version:** v4.1 (Final) | **Review Date:** March 25, 2026 | **Score: 8.8 / 10**
**Validation:** Clean-room deployment — DROP → CREATE → DEPLOY → EXECUTE → VALIDATE (all passed)
**Script:** `MONITORING_OBSERVABILITY.sql` | **Grants:** `MONITORING_OBSERVABILITY_PREREQUISITE_GRANTS.sql`
**Account:** qsb28595 | **Role:** ACCOUNTADMIN | **Warehouse:** NIFI_WH

---

## 1. VERDICT: APPROVED FOR PRODUCTION

| Dimension | Score | Status |
|:---|:---:|:---:|
| Architecture & Design | 9.0 | PASS |
| Stream Health SP | 8.5 | PASS |
| Task Health SP | 8.5 | PASS |
| Alerting & Deduplication | 8.5 | PASS |
| Observability Views (4 active) | 8.0 | PASS |
| Security & RBAC | 9.0 | PASS |
| Operations & Cleanup | 8.5 | PASS |
| **OVERALL** | **8.8** | **APPROVED** |

> **DQ SP excluded** from v4.1 deployment per decision on 2026-03-25. Code preserved in script for future phase.

### Score History

| Version | Score | Status | Key Change |
|:---|:---:|:---:|:---|
| v4.0 | 6.8 | CONDITIONAL | 2 P0 bugs (INFORMATION_SCHEMA.STREAMS, GRANT ALL) |
| v4.1 Rev 2 | 8.8 | APPROVED | Both P0 bugs fixed |
| v4.1 Rev 3 | 8.8 | APPROVED | EXECUTE IMMEDIATE INTO runtime bug found & fixed |
| **v4.1 Final** | **8.8** | **APPROVED** | Clean-room validated; DQ excluded; multi-schema stream fix |

---

## 2. Architecture Flow

```
┌─────────────────────────────────────────────────┐
│     TASK_CDC_MONITORING_CYCLE (15 min)           │
│     Warehouse: INFA_INGEST_WH                    │
└───────────────────┬─────────────────────────────┘
                    ▼
        ┌───────────────────────┐
        │ SP_RUN_MONITORING     │
        │       _CYCLE          │
        └───┬───────┬───────┬──┘
            │       │       │
   ┌────────▼──┐ ┌──▼────┐ ┌▼──────────────┐
   │ STREAM    │ │ TASK  │ │ ALERT         │
   │ HEALTH    │ │ HEALTH│ │ GENERATION    │
   │           │ │       │ │ (3 types)     │
   │ SHOW      │ │ INFO  │ │ • STALE       │
   │ STREAMS   │ │ SCHEMA│ │ • TASK_FAIL   │
   │ SADB+EHMS │ │ TASK  │ │ • STALENESS   │
   │ +RESULT   │ │ HIST  │ │   WARNING     │
   │  _SCAN    │ │ ()    │ │ +1hr dedup    │
   └─────┬─────┘ └──┬────┘ └──────┬────────┘
         ▼          ▼             ▼
   ┌──────────┐ ┌────────┐ ┌──────────┐
   │ STREAM   │ │ TASK   │ │ ALERT    │
   │ HEALTH   │ │ HEALTH │ │ LOG      │
   │ SNAPSHOT │ │ SNAP   │ │          │
   └────┬─────┘ └───┬────┘ └────┬─────┘
        └────────────┼───────────┘
                     ▼
   ┌──────────────────────────────────────┐
   │         4 ACTIVE VIEWS               │
   │  • VW_PIPELINE_HEALTH_DASHBOARD     │
   │  • VW_PIPELINE_SUMMARY             │
   │  • VW_ACTIVE_ALERTS                │
   │  • VW_TASK_EXECUTION_HISTORY       │
   └──────────────────────────────────────┘

   ┌─────────────────┐  ┌─────────────────┐
   │ CLEANUP TASK     │  │ CONFIG TABLE    │
   │ SUN 2AM | 90d   │  │ 22 pipelines   │
   └─────────────────┘  └─────────────────┘

   ╔═══════════════════════════════════════╗
   ║  EXCLUDED (Future Phase):             ║
   ║  • SP_CAPTURE_DATA_QUALITY_METRICS    ║
   ║  • CDC_DATA_QUALITY_METRICS table     ║
   ║  • VW_PIPELINE_TREND_7D              ║
   ║  • VW_FILTER_IMPACT_ANALYSIS         ║
   ║  • DATA_QUALITY alert type           ║
   ╚═══════════════════════════════════════╝
```

### Alert Flow

```
  Stream/Task Snapshot Data
         │
    ┌────▼────┐
    │ STALE?  │──YES──▶ STREAM_STALE (CRITICAL)
    └────┬────┘
         │NO
    ┌────▼────┐
    │ <12hrs? │──YES──▶ STREAM_STALENESS_WARNING (WARNING)
    └────┬────┘
         │NO
    ┌────▼────────┐
    │ TASK FAILED? │──YES──▶ TASK_FAILURE (CRITICAL)
    └─────────────┘
         │
    ┌────▼──────────────┐
    │ DEDUP: same alert │
    │ type+table within │──EXISTS──▶ SKIP (no duplicate)
    │ 1hr (4hr stale)   │
    └───────────────────┘
         │
    ┌────▼──────────────┐
    │ SP_ACKNOWLEDGE     │
    │ _ALERT(id, user,  │
    │  notes)           │
    └───────────────────┘
```

---

## 3. Active Object Inventory (17 objects)

### Tables (5)

| Table | Purpose |
|:---|:---|
| `CDC_PIPELINE_CONFIG` | 22 pipeline definitions — drives all SPs |
| `CDC_STREAM_HEALTH_SNAPSHOT` | Timestamped stream health (IS_STALE, STALE_AFTER, HAS_PENDING_DATA) |
| `CDC_TASK_HEALTH_SNAPSHOT` | Timestamped task health (STATE, LAST_RUN, DURATION, IS_HEALTHY) |
| `CDC_ALERT_LOG` | Alerts + acknowledgement workflow (user, timestamp, notes) |
| `CDC_EXECUTION_LOG` | Fed by 22 CDC pipeline SPs (BATCH_ID, row counts) |

### Stored Procedures (6)

| Procedure | Purpose |
|:---|:---|
| `SP_CAPTURE_STREAM_HEALTH` | SHOW STREAMS + RESULT_SCAN for SADB and EHMS. Stale guard before STREAM_HAS_DATA. |
| `SP_CAPTURE_TASK_HEALTH` | INFORMATION_SCHEMA.TASK_HISTORY() — real-time, 0 latency |
| `SP_GENERATE_ALERTS` | 3 alert types (STREAM_STALE, TASK_FAILURE, STALENESS_WARNING) + dedup |
| `SP_RUN_MONITORING_CYCLE` | Master orchestrator — calls Stream → Task → Alerts sequentially |
| `SP_ACKNOWLEDGE_ALERT` | Mark alert resolved with user + timestamp + resolution notes |
| `SP_CLEANUP_MONITORING_DATA` | 90-day retention, weekly. Only deletes acknowledged alerts. |

### Views (4)

| View | Columns | Purpose |
|:---|:---:|:---|
| `VW_PIPELINE_HEALTH_DASHBOARD` | 12 | Primary ops view: 1 row per pipeline with OVERALL_HEALTH |
| `VW_PIPELINE_SUMMARY` | 6 | Executive KPIs: pipelines, healthy/unhealthy tasks, stale streams, open alerts |
| `VW_ACTIVE_ALERTS` | 7 | Unacknowledged alerts sorted by severity (CRITICAL first) |
| `VW_TASK_EXECUTION_HISTORY` | 8 | Last 24h task runs from INFORMATION_SCHEMA (real-time) |

### Tasks (2)

| Task | Schedule | Purpose |
|:---|:---|:---|
| `TASK_CDC_MONITORING_CYCLE` | Every 15 min | CALL SP_RUN_MONITORING_CYCLE() |
| `TASK_CDC_MONITORING_CLEANUP` | CRON SUN 2AM CST | CALL SP_CLEANUP_MONITORING_DATA(90) |

---

## 4. Bugs Found & Resolved (5 total)

| # | Bug | Severity | Version | Fix Applied |
|:--|:---|:---:|:---:|:---|
| 1 | `INFORMATION_SCHEMA.STREAMS` does not exist in Snowflake | **P0** | v4.0 | Rewritten with `SHOW STREAMS IN SCHEMA` + `TABLE(RESULT_SCAN(:v_query_id))` |
| 2 | `GRANT ALL ON SCHEMA` overly permissive | **P0** | v4.0 | Replaced with explicit USAGE, SELECT, INSERT, UPDATE, DELETE per object type |
| 3 | `EXECUTE IMMEDIATE '...' INTO v_var` not supported in SQL Scripting | **P0** | v4.1 | Replaced with `RESULTSET` + `CURSOR` + `FETCH` pattern |
| 4 | Stream Health SP only queried D_RAW.SADB, missed D_RAW.EHMS | **P1** | v4.1 | Now runs SHOW STREAMS for BOTH schemas, routes via config's STREAM_NAME |
| 5 | `SYSTEM$STREAM_HAS_DATA()` on stale stream throws error | **P0** | v4.0 | Added stale guard: checks `"stale"` column before calling the function |

---

## 5. DQ Exclusion (Future Phase)

The following components are **commented out** in the script — code preserved for future enablement:

| Component | Status |
|:---|:---|
| `CDC_DATA_QUALITY_METRICS` table DDL | Commented out |
| `SP_CAPTURE_DATA_QUALITY_METRICS` | Commented out (bug was fixed before commenting) |
| `VW_PIPELINE_TREND_7D` | Commented out (depends on DQ metrics) |
| `VW_FILTER_IMPACT_ANALYSIS` | Commented out (depends on DQ metrics) |
| DATA_QUALITY alert (#3) in SP_GENERATE_ALERTS | Commented out |
| DQ call in SP_RUN_MONITORING_CYCLE | Commented out |
| DQ delete in SP_CLEANUP_MONITORING_DATA | Commented out |

**To re-enable:** Uncomment the marked sections in `MONITORING_OBSERVABILITY.sql` and redeploy.

---

## 6. Clean-Room Validation Results (March 25, 2026)

**Test Method:** Dropped D_RAW and D_BRONZE completely. Rebuilt from scratch with 4 mock source tables, 4 streams, 4 bronze targets, and 10 sample rows.

### Object Deployment

| Step | Objects | Result |
|:---|:---|:---:|
| CREATE DATABASE D_RAW, D_BRONZE | 2 databases | PASS |
| CREATE SCHEMA (SADB, EHMS, MONITORING) | 4 schemas | PASS |
| CREATE monitoring tables | 5 tables | PASS |
| MERGE pipeline configs | 22 config rows | PASS |
| CREATE stored procedures | 6 SPs | PASS |
| CREATE views | 4 views | PASS |
| CREATE tasks | 2 tasks | PASS |

### Runtime Execution

| Test | Result | Detail |
|:---|:---:|:---|
| `SP_CAPTURE_STREAM_HEALTH()` | PASS | 4 streams ACTIVE (IS_STALE=FALSE, ~336h until stale, HAS_PENDING_DATA=TRUE). 18 gracefully ERROR (no mock objects). |
| `SP_CAPTURE_TASK_HEALTH()` | PASS | 22 rows captured. All unhealthy (expected — no running tasks in test). |
| `SP_GENERATE_ALERTS()` | PASS | 22 TASK_FAILURE alerts. 0 STREAM_STALE (correct). |
| `SP_RUN_MONITORING_CYCLE()` | PASS | Full end-to-end: Stream → Task → Alerts. |
| **Dedup test** (2nd run) | PASS | 1 new alert (re-opened acknowledged). 21 correctly blocked by dedup. |
| `SP_ACKNOWLEDGE_ALERT(1, 'BABU', '...')` | PASS | Alert acknowledged. VW_ACTIVE_ALERTS: 22 → 21. |
| `SP_CLEANUP_MONITORING_DATA(90)` | PASS | Returns counts. 0 deleted (all < 90 days). |

### View Validation

| View | Columns | Rows | Expected | Result |
|:---|:---:|:---:|:---:|:---:|
| `VW_ACTIVE_ALERTS` | 7 | 21 | 21 (22 − 1 acknowledged) | PASS |
| `VW_PIPELINE_HEALTH_DASHBOARD` | 12 | 22 | 22 (1 per pipeline) | PASS |
| `VW_PIPELINE_SUMMARY` | 6 | 1 | 1 (single summary) | PASS |
| `VW_TASK_EXECUTION_HISTORY` | 8 | 0 | 0 (no task runs in test) | PASS |

---

## 7. Remaining Recommendations (Non-Blocking)

### P1 — Should Fix (Post-Deploy Sprint)

| Item | Risk | Effort |
|:---|:---:|:---:|
| Increase TASK_HISTORY `RESULT_LIMIT` from 1000 to 5000 | LOW | 5 min |
| Add FUTURE GRANTS for new objects in MONITORING schema | LOW | 15 min |

### P2 — Nice to Have

| Item | Risk | Effort |
|:---|:---:|:---:|
| Snowflake Alert / email notification for CRITICAL alerts | LOW | 3 hrs |
| Clustering on SNAPSHOT_TMS for snapshot tables at scale | LOW | 30 min |
| Per-table DQ threshold override in config (when DQ re-enabled) | LOW | 2 hrs |

---

## 8. Deployment Checklist for Customer Environment

```
[ ] 1.  Run MONITORING_OBSERVABILITY_PREREQUISITE_GRANTS.sql (as ACCOUNTADMIN)
[ ] 2.  Verify grants:
          SHOW GRANTS TO ROLE "D-SNW-DEVBI1-ETL";
[ ] 3.  Run MONITORING_OBSERVABILITY.sql
[ ] 4.  Verify config:
          SELECT COUNT(*) FROM D_BRONZE.MONITORING.CDC_PIPELINE_CONFIG;
          -- Expect: 22
[ ] 5.  Verify objects:
          SELECT TABLE_NAME, TABLE_TYPE
          FROM D_BRONZE.INFORMATION_SCHEMA.TABLES
          WHERE TABLE_SCHEMA = 'MONITORING' ORDER BY TABLE_TYPE, TABLE_NAME;
          -- Expect: 5 BASE TABLEs + 4 VIEWs
[ ] 6.  Run initial cycle:
          CALL D_BRONZE.MONITORING.SP_RUN_MONITORING_CYCLE();
[ ] 7.  Check dashboard:
          SELECT TABLE_NAME, STREAM_STATUS, TASK_HEALTHY, OVERALL_HEALTH
          FROM D_BRONZE.MONITORING.VW_PIPELINE_HEALTH_DASHBOARD;
[ ] 8.  Check summary:
          SELECT * FROM D_BRONZE.MONITORING.VW_PIPELINE_SUMMARY;
          -- Expect: TOTAL_PIPELINES = 22
[ ] 9.  Check alerts:
          SELECT ALERT_TYPE, ALERT_SEVERITY, COUNT(*)
          FROM D_BRONZE.MONITORING.VW_ACTIVE_ALERTS
          GROUP BY ALERT_TYPE, ALERT_SEVERITY;
[ ] 10. Resume tasks:
          ALTER TASK D_BRONZE.MONITORING.TASK_CDC_MONITORING_CYCLE RESUME;
          ALTER TASK D_BRONZE.MONITORING.TASK_CDC_MONITORING_CLEANUP RESUME;
[ ] 11. Wait 15 min, verify second cycle populates correctly
[ ] 12. Check task history (real-time):
          SELECT * FROM D_BRONZE.MONITORING.VW_TASK_EXECUTION_HISTORY LIMIT 10;
```

---

## 9. Sign-Off

| Field | Value |
|:---|:---|
| **Verdict** | **APPROVED FOR PRODUCTION** |
| **Overall Score** | **8.8 / 10** |
| **Clean-Room Test** | DROP → CREATE → DEPLOY → EXECUTE → VALIDATE — ALL PASSED |
| **Bugs Found & Fixed** | 5 (INFORMATION_SCHEMA.STREAMS, GRANT ALL, EXECUTE IMMEDIATE INTO, single-schema stream, stale guard) |
| **Excluded (Future)** | DQ SP + 2 DQ views + DQ alert — code preserved in script |
| **Active Objects** | 5 tables + 6 SPs + 4 views + 2 tasks + 22 config rows = **17 objects** |
| **Deployment Decision** | Framework is validated and ready for customer environment deployment |

---

*Generated: March 25, 2026 | Clean-room deployment validated on Snowflake account qsb28595*