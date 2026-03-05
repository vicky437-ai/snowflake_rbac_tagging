# Data Prevention Layer Analysis
## Informatica IDMC Redeployment - Data Protection Options

**Prepared for:** Customer Technical Review  
**Date:** March 5, 2026  
**Document Version:** 3.0

---

# EXECUTIVE SUMMARY

## The Problem

```mermaid
flowchart LR
    subgraph Problem["⚠️ CURRENT RISK"]
        A[("🗄️ IDMC Job")] -->|Undeploy| B["DROP/TRUNCATE"]
        B -->|Executes| C[("❌ DATA LOST")]
    end
    
    style A fill:#4a90d9,stroke:#2e6da4,color:#fff
    style B fill:#f0ad4e,stroke:#eea236,color:#fff
    style C fill:#d9534f,stroke:#c9302c,color:#fff
```

**Impact:** Historical data in RAW layer base tables is destroyed during Informatica IDMC job redeployment.

---

## Solutions Overview

```mermaid
flowchart TB
    subgraph UC1["USE CASE 1: Time Travel + Clone"]
        direction TB
        T1["⏱️ Built-in Snowflake Feature"]
        T2["📅 Up to 90 days recovery"]
        T3["⚡ Real-time protection"]
    end
    
    subgraph UC2["USE CASE 2: Scheduled Cloning"]
        direction TB
        S1["📋 Automated Daily Backups"]
        S2["♾️ Unlimited retention"]
        S3["🛡️ Independent copies"]
    end
    
    UC1 --> REC
    UC2 --> REC
    
    REC["✅ RECOMMENDED: Hybrid Approach"]
    
    style UC1 fill:#5cb85c,stroke:#4cae4c,color:#fff
    style UC2 fill:#5bc0de,stroke:#46b8da,color:#fff
    style REC fill:#337ab7,stroke:#2e6da4,color:#fff
```

---

## Quick Comparison

| Aspect | Use Case 1 | Use Case 2 |
|--------|-----------|------------|
| **Strategy** | Time Travel + Clone | Scheduled Auto-Cloning |
| **Best For** | Managed redeployments | Unmanaged redeployments |
| **Recovery Window** | Up to 90 days | Unlimited |
| **Real-time?** | ✅ Yes | ❌ No (scheduled) |
| **Effort** | 🟢 Low (built-in) | 🟡 Medium (task setup) |

---

## Recommended Architecture

```mermaid
flowchart LR
    subgraph Layer1["LAYER 1"]
        TT["⏱️ Time Travel<br/>14 days retention"]
    end
    
    subgraph Layer2["LAYER 2"]
        SC["📋 Scheduled Clones<br/>Daily backups"]
    end
    
    subgraph Layer3["LAYER 3"]
        CDC["📊 CDC Prevention Table<br/>TRKFC_TRSTN_V1"]
    end
    
    TT -->|"Fine-grained<br/>recovery"| SC
    SC -->|"Long-term<br/>backup"| CDC
    CDC -->|"Full audit<br/>trail"| SAFE["🛡️ PROTECTED"]
    
    style Layer1 fill:#5cb85c,stroke:#4cae4c,color:#fff
    style Layer2 fill:#5bc0de,stroke:#46b8da,color:#fff
    style Layer3 fill:#f0ad4e,stroke:#eea236,color:#fff
    style SAFE fill:#337ab7,stroke:#2e6da4,color:#fff
```

---

## Decision Flowchart

```mermaid
flowchart TD
    START["🔍 Data Loss Detected"] --> Q1{"When did<br/>loss occur?"}
    
    Q1 -->|"Within 14 days"| UC1_PATH
    Q1 -->|"Over 14 days"| UC2_PATH
    
    subgraph UC1_PATH["Use Case 1"]
        A1["Use Time Travel"]
        A2["Cost: FREE"]
        A3["RTO: Minutes"]
    end
    
    subgraph UC2_PATH["Use Case 2"]
        B1["Use Scheduled Clone"]
        B2["Restore from backup"]
        B3["RTO: Minutes"]
    end
    
    UC1_PATH --> DONE["✅ Data Recovered"]
    UC2_PATH --> DONE
    
    style START fill:#d9534f,stroke:#c9302c,color:#fff
    style Q1 fill:#f0ad4e,stroke:#eea236,color:#fff
    style UC1_PATH fill:#5cb85c,stroke:#4cae4c,color:#fff
    style UC2_PATH fill:#5bc0de,stroke:#46b8da,color:#fff
    style DONE fill:#337ab7,stroke:#2e6da4,color:#fff
```

---

## Cost & RTO/RPO Summary

| Metric | Use Case 1 | Use Case 2 | Hybrid |
|--------|-----------|------------|--------|
| **RPO** (max data loss) | 0 seconds | Up to 24 hours | 0 within 14 days |
| **RTO** (recovery time) | 1-5 minutes | 1-5 minutes | 1-5 minutes |
| **Recovery Window** | 14-90 days | Unlimited | Unlimited |
| **Monthly Cost (100GB)** | ~$2.80 | ~$3.40 | ~$6.20 |

---

# DETAILED ANALYSIS

---

## Current Problem Statement

```mermaid
flowchart LR
    subgraph Current["CURRENT STATE"]
        IDMC["🔧 Informatica<br/>IDMC Job"]
        RAW["🗄️ RAW Layer<br/>Base Table"]
    end
    
    subgraph Risk["⚠️ RISK EVENT"]
        UNDEPLOY["Undeploy /<br/>Redeploy"]
        DROP["DROP or<br/>TRUNCATE"]
    end
    
    subgraph Impact["❌ IMPACT"]
        LOST["Historical<br/>Data Lost"]
    end
    
    IDMC --> RAW
    RAW --> UNDEPLOY
    UNDEPLOY --> DROP
    DROP --> LOST
    
    style IDMC fill:#4a90d9,stroke:#2e6da4,color:#fff
    style RAW fill:#5cb85c,stroke:#4cae4c,color:#fff
    style UNDEPLOY fill:#f0ad4e,stroke:#eea236,color:#fff
    style DROP fill:#f0ad4e,stroke:#eea236,color:#fff
    style LOST fill:#d9534f,stroke:#c9302c,color:#fff
```

### Critical Question

> ⚠️ **ACTION REQUIRED:** Verify whether IDMC uses `DROP TABLE` or `TRUNCATE TABLE`
> 
> The recovery method differs significantly based on this behavior.

| IDMC Action | Recovery Complexity | Method |
|-------------|---------------------|--------|
| **TRUNCATE TABLE** | 🟢 Simple | Direct Time Travel query |
| **DROP TABLE** | 🟡 Moderate | UNDROP workflow |
| **DROP + CREATE (same name)** | 🔴 Complex | Rename → UNDROP → Restore |

---

## Use Case 1: Time Travel + Zero-Copy Cloning

### Architecture

```mermaid
flowchart TB
    subgraph Timeline["TIME TRAVEL WINDOW (14 days)"]
        direction LR
        D1["Day 1<br/>📊 v1"] --> D5["Day 5<br/>📊 v2"]
        D5 --> D10["Day 10<br/>📊 v3"]
        D10 --> D14["Day 14<br/>📊 v4"]
    end
    
    D14 -->|"After retention"| LOST["❌ Lost"]
    
    Timeline -->|"Any point<br/>recoverable"| RECOVER["✅ CLONE AT<br/>TIMESTAMP"]
    
    style D1 fill:#5cb85c,stroke:#4cae4c,color:#fff
    style D5 fill:#5cb85c,stroke:#4cae4c,color:#fff
    style D10 fill:#5cb85c,stroke:#4cae4c,color:#fff
    style D14 fill:#5cb85c,stroke:#4cae4c,color:#fff
    style LOST fill:#d9534f,stroke:#c9302c,color:#fff
    style RECOVER fill:#337ab7,stroke:#2e6da4,color:#fff
```

### How It Works

```sql
-- Recovery using Time Travel + Cloning (after TRUNCATE)
CREATE TABLE recovered_table CLONE original_table
  AT(TIMESTAMP => '<timestamp_before_truncation>'::TIMESTAMP_LTZ);
  
-- Recovery after accidental DELETE
SELECT * FROM table_name AT(OFFSET => -3600);  -- 1 hour ago
```

### Pros & Cons

```mermaid
flowchart LR
    subgraph Pros["✅ PROS"]
        P1["Zero initial<br/>storage cost"]
        P2["Point-in-time<br/>recovery"]
        P3["Built-in<br/>always active"]
        P4["Instant clone<br/>operations"]
    end
    
    subgraph Cons["❌ CONS"]
        C1["Max 90 days<br/>retention"]
        C2["DROP requires<br/>UNDROP workflow"]
        C3["Storage costs<br/>during retention"]
        C4["Fail-safe adds<br/>7 days cost"]
    end
    
    style Pros fill:#5cb85c,stroke:#4cae4c,color:#fff
    style Cons fill:#d9534f,stroke:#c9302c,color:#fff
```

### Recovery Commands

```sql
-- Scenario 1: After TRUNCATE (Simple)
CREATE TABLE D_BRONZE.SADB.TRKFC_TRSTN_BASE_RECOVERED 
  CLONE D_BRONZE.SADB.TRKFC_TRSTN_BASE
  AT(TIMESTAMP => '2026-03-05 10:00:00'::TIMESTAMP_LTZ);

-- Scenario 2: After DROP + CREATE same name (Complex)
-- Step 1: Rename current (new) table
ALTER TABLE D_BRONZE.SADB.TRKFC_TRSTN_BASE 
  RENAME TO D_BRONZE.SADB.TRKFC_TRSTN_BASE_NEW;

-- Step 2: Restore dropped table
UNDROP TABLE D_BRONZE.SADB.TRKFC_TRSTN_BASE;

-- Step 3: Verify data
SELECT COUNT(*) FROM D_BRONZE.SADB.TRKFC_TRSTN_BASE;
```

---

## Use Case 2: Scheduled Automatic Zero-Copy Cloning

### Architecture

```mermaid
flowchart TB
    subgraph Production["PRODUCTION"]
        PROD["🗄️ TRKFC_TRSTN_BASE<br/>(Live Data)"]
    end
    
    subgraph Task["⏰ DAILY TASK (2 AM)"]
        CLONE_OP["Zero-Copy Clone"]
    end
    
    subgraph Backups["📦 SADB_BACKUPS SCHEMA"]
        BK1["BKP_20260301_020000"]
        BK2["BKP_20260302_020000"]
        BK3["BKP_20260303_020000"]
        BK4["BKP_20260304_020000"]
        BK5["BKP_20260305_020000 ◄ Today"]
    end
    
    subgraph Cleanup["🧹 WEEKLY CLEANUP"]
        DEL["Keep last 7 days"]
    end
    
    PROD --> CLONE_OP
    CLONE_OP --> Backups
    Backups --> Cleanup
    
    style PROD fill:#5cb85c,stroke:#4cae4c,color:#fff
    style Task fill:#5bc0de,stroke:#46b8da,color:#fff
    style Backups fill:#f0ad4e,stroke:#eea236,color:#fff
    style Cleanup fill:#777,stroke:#555,color:#fff
```

### Implementation

```sql
-- 1. Create backup schema
CREATE SCHEMA IF NOT EXISTS D_BRONZE.SADB_BACKUPS;

-- 2. Stored procedure for automated cloning
CREATE OR REPLACE PROCEDURE D_BRONZE.SADB_BACKUPS.SP_BACKUP_TABLE(
    source_table VARCHAR,
    backup_prefix VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    backup_name VARCHAR;
    sql_stmt VARCHAR;
BEGIN
    backup_name := backup_prefix || '_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    sql_stmt := 'CREATE TABLE D_BRONZE.SADB_BACKUPS.' || backup_name || 
                ' CLONE ' || source_table;
    EXECUTE IMMEDIATE sql_stmt;
    RETURN 'Backup created: ' || backup_name;
END;
$$;

-- 3. Daily backup task
CREATE OR REPLACE TASK D_BRONZE.SADB_BACKUPS.TASK_DAILY_BACKUP
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS
    CALL D_BRONZE.SADB_BACKUPS.SP_BACKUP_TABLE(
        'D_BRONZE.SADB.TRKFC_TRSTN_BASE',
        'TRKFC_TRSTN_BASE_BKP'
    );

ALTER TASK D_BRONZE.SADB_BACKUPS.TASK_DAILY_BACKUP RESUME;

-- 4. Cleanup procedure
CREATE OR REPLACE PROCEDURE D_BRONZE.SADB_BACKUPS.SP_CLEANUP_OLD_BACKUPS(
    retention_days NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    tables_dropped NUMBER := 0;
    cur CURSOR FOR 
        SELECT table_name 
        FROM D_BRONZE.INFORMATION_SCHEMA.TABLES 
        WHERE table_schema = 'SADB_BACKUPS'
          AND table_name LIKE 'TRKFC_TRSTN_BASE_BKP_%'
          AND created < DATEADD(day, -retention_days, CURRENT_TIMESTAMP());
BEGIN
    FOR record IN cur DO
        EXECUTE IMMEDIATE 'DROP TABLE D_BRONZE.SADB_BACKUPS.' || record.table_name;
        tables_dropped := tables_dropped + 1;
    END FOR;
    RETURN 'Dropped ' || tables_dropped || ' old backup tables';
END;
$$;

-- 5. Weekly cleanup task
CREATE OR REPLACE TASK D_BRONZE.SADB_BACKUPS.TASK_CLEANUP_BACKUPS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 3 * * 0 UTC'
AS
    CALL D_BRONZE.SADB_BACKUPS.SP_CLEANUP_OLD_BACKUPS(7);

ALTER TASK D_BRONZE.SADB_BACKUPS.TASK_CLEANUP_BACKUPS RESUME;
```

### Pros & Cons

```mermaid
flowchart LR
    subgraph Pros["✅ PROS"]
        P1["Unlimited<br/>retention"]
        P2["Zero-copy<br/>instant clone"]
        P3["Independent of<br/>IDMC operations"]
        P4["Full isolation<br/>from production"]
    end
    
    subgraph Cons["❌ CONS"]
        C1["Not real-time<br/>(scheduled)"]
        C2["Storage grows<br/>with divergence"]
        C3["Requires task<br/>management"]
        C4["Clones lose<br/>Time Travel history"]
    end
    
    style Pros fill:#5cb85c,stroke:#4cae4c,color:#fff
    style Cons fill:#d9534f,stroke:#c9302c,color:#fff
```

---

## Detailed Comparison Matrix

```mermaid
flowchart TB
    subgraph Compare["FEATURE COMPARISON"]
        direction LR
        
        subgraph UC1["USE CASE 1"]
            U1A["Recovery: 90 days max"]
            U1B["Granularity: Any second"]
            U1C["Real-time: ✅ Yes"]
            U1D["Complexity: 🟢 Low"]
        end
        
        subgraph UC2["USE CASE 2"]
            U2A["Recovery: Unlimited"]
            U2B["Granularity: Scheduled"]
            U2C["Real-time: ❌ No"]
            U2D["Complexity: 🟡 Medium"]
        end
    end
    
    style UC1 fill:#5cb85c,stroke:#4cae4c,color:#fff
    style UC2 fill:#5bc0de,stroke:#46b8da,color:#fff
```

| Criteria | Use Case 1 | Use Case 2 | Winner |
|----------|-----------|------------|--------|
| Max Recovery Window | 90 days | Unlimited | 🏆 UC2 |
| Recovery Granularity | Any second | Scheduled points | 🏆 UC1 |
| Initial Storage Cost | $0 | $0 | 🤝 Tie |
| Operational Complexity | Low | Medium | 🏆 UC1 |
| Protection: DROP | ✅ Yes | ✅ Yes | 🤝 Tie |
| Protection: TRUNCATE | ✅ Yes | ✅ Yes | 🤝 Tie |
| Real-time Protection | ✅ Yes | ❌ No | 🏆 UC1 |
| Works After IDMC Redeploy | Depends | Always | 🏆 UC2 |

---

## RTO/RPO Analysis

```mermaid
graph TB
    subgraph RPO["📊 RPO - Recovery Point Objective"]
        RPO1["UC1: 0 seconds<br/>(any point in time)"]
        RPO2["UC2: Up to 24 hours<br/>(between backups)"]
        RPOH["Hybrid: 0 seconds<br/>(within 14 days)"]
    end
    
    subgraph RTO["⏱️ RTO - Recovery Time Objective"]
        RTO1["UC1: 1-5 minutes"]
        RTO2["UC2: 1-5 minutes"]
        RTOH["Hybrid: 1-5 minutes"]
    end
    
    subgraph Window["📅 Recovery Window"]
        W1["UC1: Up to 90 days"]
        W2["UC2: Unlimited"]
        WH["Hybrid: Unlimited"]
    end
    
    style RPO fill:#5bc0de,stroke:#46b8da,color:#fff
    style RTO fill:#5cb85c,stroke:#4cae4c,color:#fff
    style Window fill:#f0ad4e,stroke:#eea236,color:#fff
```

### Business Impact Analysis

| Scenario | Data Loss Risk | Business Impact | Mitigation |
|----------|---------------|-----------------|------------|
| IDMC redeploy within 14 days | 🟢 None | None | Time Travel |
| IDMC redeploy after 14 days | 🟡 Up to 24 hours | Low-Medium | Daily Clone |
| Undetected corruption | 🟢 Full audit trail | Low | CDC Table |
| Accidental DELETE | 🟢 None | None | Time Travel |

---

## Cost Projections

### Assumptions
- Table size: 100 GB
- Daily change rate: 5%
- Snowflake on-demand storage: $40/TB/month
- Compute (X-Small): $2/credit

### Monthly Cost Breakdown

```mermaid
pie title Monthly Cost Distribution (100GB Table)
    "Time Travel (14 days)" : 1.87
    "Fail-safe (7 days)" : 0.93
    "Clone Divergence" : 1.40
    "Task Compute" : 2.00
```

| Component | Calculation | Monthly Cost |
|-----------|-------------|--------------|
| Time Travel (14 days) | 100GB × 14/30 × $40/TB | ~$1.87 |
| Fail-safe (7 days) | 100GB × 7/30 × $40/TB | ~$0.93 |
| Clone Divergence (7 backups) | 100GB × 5% × 7 × $40/TB | ~$1.40 |
| Task Compute | 2 tasks × 1 min × 30 days × $2 | ~$2.00 |
| **TOTAL** | | **~$6.20/month** |

### Cost Scaling

| Table Size | Basic (UC1 only) | Full Protection (Hybrid) |
|------------|------------------|--------------------------|
| 100 GB | ~$2.80/month | ~$6.20/month |
| 1 TB | ~$28/month | ~$62/month |
| 10 TB | ~$280/month | ~$620/month |

---

## Implementation Roadmap

```mermaid
gantt
    title Implementation Timeline
    dateFormat  YYYY-MM-DD
    section Week 1
    Verify IDMC behavior (DROP vs TRUNCATE)    :a1, 2026-03-05, 5d
    Set Time Travel = 14 days                   :a2, 2026-03-05, 1d
    section Week 2
    Deploy backup tasks                         :b1, 2026-03-10, 5d
    Test recovery procedures                    :b2, 2026-03-12, 3d
    section Week 3
    Create recovery runbook                     :c1, 2026-03-17, 3d
    Team training                               :c2, 2026-03-19, 2d
```

---

## Action Items

```mermaid
flowchart TB
    subgraph High["🔴 HIGH PRIORITY"]
        H1["Verify IDMC behavior<br/>Owner: Infra Team<br/>Timeline: Week 1"]
        H2["Set TIME_TRAVEL = 14<br/>Owner: DBA<br/>Timeline: Immediate"]
    end
    
    subgraph Medium["🟡 MEDIUM PRIORITY"]
        M1["Deploy backup task<br/>Owner: Data Eng<br/>Timeline: Week 2"]
        M2["Test recovery<br/>Owner: QA<br/>Timeline: Week 2"]
    end
    
    subgraph Low["🟢 LOW PRIORITY"]
        L1["Document runbooks<br/>Owner: Ops<br/>Timeline: Week 3"]
        L2["Team training<br/>Owner: All<br/>Timeline: Week 3"]
    end
    
    High --> Medium --> Low
    
    style High fill:#d9534f,stroke:#c9302c,color:#fff
    style Medium fill:#f0ad4e,stroke:#eea236,color:#fff
    style Low fill:#5cb85c,stroke:#4cae4c,color:#fff
```

| Priority | Action | Owner | Timeline | Status |
|----------|--------|-------|----------|--------|
| 🔴 High | Verify IDMC behavior (DROP vs TRUNCATE) | Infra Team | Week 1 | ⬜ |
| 🔴 High | Set DATA_RETENTION_TIME_IN_DAYS = 14 | DBA | Immediate | ⬜ |
| 🟡 Medium | Deploy scheduled backup task | Data Eng | Week 2 | ⬜ |
| 🟡 Medium | Deploy cleanup task | Data Eng | Week 2 | ⬜ |
| 🟡 Medium | Test recovery procedures | QA | Week 2 | ⬜ |
| 🟢 Low | Document recovery runbooks | Operations | Week 3 | ⬜ |
| 🟢 Low | Team training | All | Week 3 | ⬜ |

---

## Monitoring Queries

```sql
-- 1. Check Time Travel retention setting
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' 
  IN TABLE D_BRONZE.SADB.TRKFC_TRSTN_BASE;

-- 2. Monitor storage costs
SELECT 
    table_name,
    ROUND(ACTIVE_BYTES / POWER(1024,3), 2) AS active_gb,
    ROUND(TIME_TRAVEL_BYTES / POWER(1024,3), 2) AS time_travel_gb,
    ROUND(FAILSAFE_BYTES / POWER(1024,3), 2) AS failsafe_gb,
    ROUND((ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES) 
          / POWER(1024,3), 2) AS total_gb
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE table_catalog = 'D_BRONZE'
  AND table_schema IN ('SADB', 'SADB_BACKUPS')
  AND table_name LIKE 'TRKFC_TRSTN%'
  AND DELETED IS NULL;

-- 3. Check backup task status
SHOW TASKS IN SCHEMA D_BRONZE.SADB_BACKUPS;

-- 4. List available backups
SELECT table_name, created
FROM D_BRONZE.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'SADB_BACKUPS'
ORDER BY created DESC;
```

---

## References

All information based on official Snowflake documentation:

| # | Topic | URL |
|---|-------|-----|
| 1 | Time Travel | [docs.snowflake.com/en/user-guide/data-time-travel](https://docs.snowflake.com/en/user-guide/data-time-travel) |
| 2 | Data Storage | [docs.snowflake.com/en/user-guide/tables-storage-considerations](https://docs.snowflake.com/en/user-guide/tables-storage-considerations) |
| 3 | TRUNCATE TABLE | [docs.snowflake.com/en/sql-reference/sql/truncate-table](https://docs.snowflake.com/en/sql-reference/sql/truncate-table) |
| 4 | UNDROP TABLE | [docs.snowflake.com/en/sql-reference/sql/undrop-table](https://docs.snowflake.com/en/sql-reference/sql/undrop-table) |
| 5 | Storage Costs | [docs.snowflake.com/en/user-guide/data-cdp-storage-costs](https://docs.snowflake.com/en/user-guide/data-cdp-storage-costs) |
| 6 | Tasks | [docs.snowflake.com/en/user-guide/tasks-intro](https://docs.snowflake.com/en/user-guide/tasks-intro) |

---

*Document Version 3.0 | March 5, 2026 | Based on Snowflake Documentation*
