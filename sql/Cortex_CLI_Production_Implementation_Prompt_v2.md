# CORTEX CLI PROMPT: Production Implementation for Bronze to Silver Curated Layer

## COMPREHENSIVE PROMPT FOR ~100 TABLES IMPLEMENTATION

---

```
You are a Senior Snowflake Solutions Architect and Production Data Engineer with deep expertise in enterprise data platform implementation, CDC patterns, Dynamic Tables, Snowflake 2025 best practices, Azure DevOps CI/CD pipelines, and end-to-end observability. You have successfully deployed production-grade data pipelines for Fortune 500 enterprises.

I am providing you with two reference documents:
1. **Bronze_to_Silver_Curated_Data_Layer_Design_Specification_v1.docx** - Approved design document
2. **Bronze_to_Silver_Architecture_Technical_Review_v1.md** - Technical review with recommendations

Your task is to generate a complete, production-ready implementation framework for approximately 100 tables following the approved design patterns.

---

## ARCHITECTURE CONTEXT - IMPORTANT

### Medallion Architecture Position
This implementation covers the **SILVER CURATED LAYER** - the first transformation layer after Bronze ingestion:

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        MEDALLION ARCHITECTURE OVERVIEW                        │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌─────────────┐      ┌─────────────────────┐      ┌─────────────────────┐ │
│   │   BRONZE    │      │   SILVER CURATED    │      │   GOLD (FUTURE)     │ │
│   │   (Raw)     │ ───► │   (THIS SCOPE)      │ ───► │   Facts/Dims/       │ │
│   │   IDMC      │      │   Clean, Dedupe,    │      │   Business Tables   │ │
│   │   Ingestion │      │   SLA-Enforced      │      │   (Next Phase)      │ │
│   └─────────────┘      └─────────────────────┘      └─────────────────────┘ │
│                                                                              │
│   ▲ We are HERE                                                              │
│   │                                                                          │
│   └── Bronze to Silver = Current Implementation Scope                        │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Design for Future Extensibility
While this implementation focuses STRICTLY on the Silver Curated Layer, design all components with these future requirements in mind:

1. **Silver as Foundation**: Silver CURR_DT tables will be consumed by downstream Fact and Dimension tables
2. **TARGET_LAG = DOWNSTREAM**: Enable cascading refresh for future Gold layer Dynamic Tables
3. **Consistent Audit Columns**: EFFECTIVE_TS, LOAD_TS patterns must be consistent for lineage tracking through Gold
4. **Stable Contracts**: Silver schema stability is critical - downstream Gold layer depends on it
5. **Observability End-to-End**: Monitoring framework must support future Gold layer integration
6. **DevOps Patterns**: CI/CD structure must accommodate future Gold layer deployments

**IMPORTANT**: Do NOT implement Gold layer (Facts/Dimensions/Business tables) in this scope. Only design Silver layer with extensibility in mind.

---

## CRITICAL CONSTRAINTS - READ CAREFULLY

### MANDATORY REQUIREMENTS
1. **NO HALLUCINATIONS**: Use ONLY documented Snowflake 2025 native functionality. Do not invent features, syntax, or capabilities that do not exist.
2. **PRODUCTION-READY CODE**: All generated code must be directly executable in a production Snowflake environment without modification.
3. **FOLLOW APPROVED DESIGN EXACTLY**: Adhere strictly to the patterns defined in the design specification document. Do not deviate from:
   - CDC Pattern: _BASE table → One-time INSERT → Silver BASE table + _LOG table → CDC_DT (INCREMENTAL + IMMUTABLE WHERE) → CURR_DT (UNION ALL + ROW_NUMBER deduplication)
   - Full Load Pattern: Bronze raw table → Dynamic Table (REFRESH_MODE = FULL) → Silver DT
4. **PARAMETERIZED FOR MULTI-ENVIRONMENT**: No hardcoded values. All environment-specific configurations must be parameters.
5. **METADATA-DRIVEN ARCHITECTURE**: Implementation must be driven by configuration metadata tables, not individual scripts per table.
6. **AZURE DEVOPS COMPATIBLE**: All scripts and configurations must be structured for Azure DevOps CI/CD pipelines.
7. **END-TO-END OBSERVABILITY**: Implement comprehensive Snowflake-native observability for monitoring, alerting, and troubleshooting.

### SNOWFLAKE 2025 BEST PRACTICES TO APPLY
Based on the design document and technical review, implement these specific optimizations:
- **IMMUTABLE WHERE** for CDC _LOG tables: `IMMUTABLE WHERE (OP_LAST_REPLICATED < CURRENT_TIMESTAMP() - INTERVAL '1 day')`
- **REFRESH_MODE = INCREMENTAL** for CDC Dynamic Tables
- **REFRESH_MODE = FULL** for Full Load Dynamic Tables
- **TARGET_LAG = DOWNSTREAM** for dependent Dynamic Tables (CURR_DT and downstream)
- **CHANGE_TRACKING = TRUE** on Silver BASE tables
- **AUTO_SUSPEND = 60** for cost optimization
- **CLUSTER BY primary key** for large tables (>1M rows)
- **DATA_RETENTION_TIME_IN_DAYS = 7** for Time Travel

### CONSTRAINTS FROM TECHNICAL REVIEW
Incorporate these validated technical decisions:
1. Dynamic Tables with INCREMENTAL mode CANNOT consume from Streams - use _LOG tables directly
2. CURRENT_TIMESTAMP() is ALLOWED in INCREMENTAL mode for audit columns (LOAD_TS)
3. CURRENT_TIMESTAMP() is ALLOWED in FULL mode without restrictions
4. ROW_NUMBER() deduplication must use: `PARTITION BY {PK} ORDER BY EFFECTIVE_TS DESC, LOAD_TS DESC`
5. COALESCE pattern for DELETE handling: `COALESCE({COLUMN}_NEW, {COLUMN}_OLD)`
6. Soft delete via IS_DELETED flag: `CASE WHEN OP_CODE = 'D' THEN TRUE ELSE FALSE END`

---

## DELIVERABLE 1: METADATA-DRIVEN CONFIGURATION FRAMEWORK

### 1.1 Environment Configuration Table
Create a parameterized environment configuration system:

```sql
-- Requirements:
-- - Support environments: DEV, QA, UAT, PROD
-- - Parameterize: database names, warehouse names, warehouse sizes, TARGET_LAG values, retention days
-- - Include environment-specific email recipients for alerts
-- - Support feature flags (enable/disable DQ checks, alerting, etc.)
-- - Include Azure DevOps service connection references
-- - Include observability configuration (log retention, metric granularity)
```

### 1.2 Table Configuration Metadata
Create metadata tables to drive Dynamic Table generation:

```sql
-- CDC_TABLE_CONFIG: Configuration for CDC pattern tables
-- Required columns:
-- - SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE (Bronze location)
-- - TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE (Silver location)  
-- - PRIMARY_KEY_COLUMNS (comma-separated for composite keys)
-- - BUSINESS_COLUMNS (columns to include in Silver, excluding IDMC metadata)
-- - TARGET_LAG_MINUTES (5 for CDC_DT, 'DOWNSTREAM' for CURR_DT)
-- - IMMUTABLE_WHERE_INTERVAL_DAYS (default 1)
-- - WAREHOUSE_NAME (parameterized reference)
-- - IS_ACTIVE (enable/disable individual tables)
-- - CLUSTER_BY_COLUMNS (optional, for large tables)
-- - DEPLOYMENT_GROUP (for phased rollouts via Azure DevOps)
-- - OBSERVABILITY_TIER (STANDARD, ENHANCED, CRITICAL - determines monitoring depth)

-- FULL_LOAD_TABLE_CONFIG: Configuration for Full Load pattern tables
-- Required columns:
-- - SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE
-- - TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE
-- - BUSINESS_COLUMNS
-- - TARGET_LAG_HOURS (or 'DOWNSTREAM')
-- - WAREHOUSE_NAME
-- - IS_ACTIVE
-- - DEPLOYMENT_GROUP
-- - OBSERVABILITY_TIER
```

### 1.3 Column Mapping Metadata
Create column-level configuration for transformation logic:

```sql
-- COLUMN_MAPPING_CONFIG:
-- - TABLE_CONFIG_ID (FK to CDC_TABLE_CONFIG or FULL_LOAD_TABLE_CONFIG)
-- - SOURCE_COLUMN_NAME
-- - TARGET_COLUMN_NAME
-- - DATA_TYPE
-- - IS_PRIMARY_KEY
-- - IS_NULLABLE
-- - DEFAULT_VALUE
-- - TRANSFORMATION_LOGIC (optional SQL expression)
```

### 1.4 Deployment Tracking Metadata
Create tables to track deployment history for Azure DevOps integration:

```sql
-- DEPLOYMENT_HISTORY: Track all deployments
-- - DEPLOYMENT_ID, DEPLOYMENT_TIMESTAMP
-- - ENVIRONMENT (DEV/QA/UAT/PROD)
-- - AZURE_DEVOPS_BUILD_ID, AZURE_DEVOPS_RELEASE_ID
-- - GIT_COMMIT_SHA, GIT_BRANCH
-- - DEPLOYED_BY (service principal or user)
-- - DEPLOYMENT_STATUS (SUCCESS/FAILED/ROLLBACK)
-- - OBJECTS_DEPLOYED (JSON array of object names)
-- - DEPLOYMENT_DURATION_SECONDS
-- - ERROR_MESSAGE (if failed)

-- OBJECT_VERSION_HISTORY: Track object versions
-- - OBJECT_NAME, OBJECT_TYPE (TABLE, DYNAMIC_TABLE, VIEW, PROCEDURE)
-- - VERSION_NUMBER, DEPLOYED_VERSION_HASH
-- - DEPLOYMENT_ID (FK)
-- - DDL_STATEMENT (stored for rollback capability)
```

---

## DELIVERABLE 2: DDL GENERATOR STORED PROCEDURES

### 2.1 CDC Pattern DDL Generator
Create a stored procedure that generates all DDL for a single CDC table:

```sql
-- Procedure: SP_GENERATE_CDC_TABLE_DDL
-- Input: TABLE_CONFIG_ID, ENVIRONMENT
-- Output: Executable DDL statements for:
--   1. Silver BASE table (with CHANGE_TRACKING = TRUE)
--   2. One-time INSERT statement for BASE load
--   3. CDC_DT Dynamic Table (INCREMENTAL + IMMUTABLE WHERE)
--   4. CURR_DT Dynamic Table (UNION ALL + ROW_NUMBER deduplication)

-- Requirements:
-- - Read configuration from metadata tables
-- - Apply environment-specific parameters
-- - Generate valid Snowflake DDL syntax
-- - Include all audit columns: IS_DELETED, EFFECTIVE_TS, LOAD_TS, RECORD_SOURCE
-- - Apply COALESCE pattern for all columns in CDC_DT
-- - Generate proper CLUSTER BY clause if configured
-- - Include comments with deployment metadata (build ID, timestamp, version)
```

### 2.2 Full Load Pattern DDL Generator
Create a stored procedure that generates DDL for Full Load tables:

```sql
-- Procedure: SP_GENERATE_FULL_LOAD_TABLE_DDL
-- Input: TABLE_CONFIG_ID, ENVIRONMENT
-- Output: Executable DDL for:
--   1. Single Dynamic Table with REFRESH_MODE = FULL
--   2. CURRENT_TIMESTAMP() for LOAD_TS (allowed in FULL mode)

-- Requirements:
-- - Simpler than CDC (single Dynamic Table, no BASE/CDC/CURR separation)
-- - Apply environment-specific parameters
-- - Include LOAD_TS audit column
-- - Include deployment metadata comments
```

### 2.3 Batch DDL Generator
Create a procedure to generate DDL for all configured tables:

```sql
-- Procedure: SP_GENERATE_ALL_TABLE_DDL
-- Input: ENVIRONMENT, PATTERN_TYPE ('CDC', 'FULL_LOAD', 'ALL'), DEPLOYMENT_GROUP (optional)
-- Output: Complete DDL script for all active tables in configuration

-- Requirements:
-- - Iterate through metadata tables
-- - Generate DDL in dependency order (BASE tables first, then CDC_DT, then CURR_DT)
-- - Include CREATE SCHEMA IF NOT EXISTS statements
-- - Include verification queries after each table
-- - Support DEPLOYMENT_GROUP filtering for phased rollouts
-- - Generate deployment manifest for Azure DevOps artifact
```

---

## DELIVERABLE 3: AZURE DEVOPS CI/CD FRAMEWORK

### 3.1 Repository Structure
Define the Git repository structure for Azure DevOps:

```yaml
# Repository structure for Azure DevOps
/snowflake-silver-layer
  /pipelines
    - azure-pipelines.yml           # Main CI/CD pipeline definition
    - azure-pipelines-pr.yml        # Pull request validation pipeline
    - azure-pipelines-release.yml   # Release pipeline with approvals
  /templates
    - deploy-stage-template.yml     # Reusable deployment stage template
    - validation-template.yml       # Reusable validation template
    - rollback-template.yml         # Rollback procedure template
  /scripts
    /infrastructure
      - 01_environment_setup.sql
      - 02_create_metadata_tables.sql
      - 03_create_monitoring_views.sql
      - 04_create_alert_infrastructure.sql
    /configuration
      - 01_environment_config.sql
      - 02_cdc_table_config.sql
      - 03_full_load_table_config.sql
      - 04_column_mappings.sql
      - 05_dq_rules_config.sql
    /stored_procedures
      - 01_ddl_generators.sql
      - 02_deployment_procedures.sql
      - 03_alert_procedures.sql
      - 04_dq_procedures.sql
      - 05_recovery_procedures.sql
      - 06_observability_procedures.sql
    /deployment
      - 01_deploy_all.sql
      - 02_deploy_cdc_tables.sql
      - 03_deploy_full_load_tables.sql
      - 04_rollback.sql
    /operations
      - 01_daily_health_check.sql
      - 02_sla_monitoring.sql
      - 03_cost_monitoring.sql
      - 04_incident_response.sql
    /testing
      - 01_deployment_validation.sql
      - 02_regression_tests.sql
      - 03_performance_baseline.sql
  /config
    - dev.env.json                  # DEV environment variables
    - qa.env.json                   # QA environment variables
    - uat.env.json                  # UAT environment variables
    - prod.env.json                 # PROD environment variables
  /docs
    - technical_design_document.md
    - runbook.md
    - functional_documentation.md
    - data_dictionary.md
    - devops_guide.md
```

### 3.2 Azure DevOps Pipeline Definition
Create the main CI/CD pipeline:

```yaml
# azure-pipelines.yml requirements:
# 
# TRIGGERS:
# - CI trigger on main/develop branches
# - PR trigger for validation
# - Scheduled trigger for drift detection
#
# STAGES:
# 1. Build Stage:
#    - Validate SQL syntax
#    - Run static code analysis (SQLFluff or similar)
#    - Generate deployment artifacts
#    - Version tagging
#
# 2. DEV Deployment Stage:
#    - Auto-deploy on merge to develop
#    - Run deployment validation tests
#    - Update deployment tracking metadata
#
# 3. QA Deployment Stage:
#    - Manual approval gate
#    - Deploy to QA environment
#    - Run regression tests
#    - Performance baseline comparison
#
# 4. UAT Deployment Stage:
#    - Manual approval gate (business stakeholder)
#    - Deploy to UAT environment
#    - Run smoke tests
#    - User acceptance sign-off task
#
# 5. PROD Deployment Stage:
#    - Manual approval gate (change advisory board)
#    - Deployment window enforcement
#    - Blue-green or canary deployment option
#    - Automatic rollback on failure
#    - Post-deployment validation
#    - Update observability dashboards
#
# VARIABLES:
# - Environment-specific Snowflake connection (Azure Key Vault)
# - Service principal credentials
# - Notification settings (Teams/Slack webhooks)
#
# ARTIFACTS:
# - Deployment manifest (JSON)
# - DDL scripts (versioned)
# - Rollback scripts
# - Test results
```

### 3.3 Snowflake Connection Configuration
Create secure connection handling for Azure DevOps:

```yaml
# Requirements for Snowflake connection:
# 
# 1. Azure Key Vault Integration:
#    - Store Snowflake credentials in Key Vault
#    - Service principal authentication
#    - Key rotation support
#
# 2. Connection Variables:
#    - SNOWFLAKE_ACCOUNT
#    - SNOWFLAKE_USER (service account)
#    - SNOWFLAKE_PRIVATE_KEY (from Key Vault)
#    - SNOWFLAKE_WAREHOUSE
#    - SNOWFLAKE_DATABASE
#    - SNOWFLAKE_ROLE
#
# 3. SnowSQL or Snowflake CLI:
#    - Use SnowSQL for script execution
#    - Or Snowflake CLI (snow sql) for modern approach
#    - Connection profile per environment
```

### 3.4 Deployment Stored Procedures
Create procedures for Azure DevOps to call:

```sql
-- SP_EXECUTE_DEPLOYMENT: Main deployment entry point
-- Input: 
--   - ENVIRONMENT
--   - DEPLOYMENT_GROUP (optional, for phased rollouts)
--   - AZURE_DEVOPS_BUILD_ID
--   - AZURE_DEVOPS_RELEASE_ID
--   - GIT_COMMIT_SHA
--   - DEPLOYED_BY
-- Output:
--   - DEPLOYMENT_ID
--   - SUCCESS/FAILURE status
--   - List of deployed objects
--   - Deployment duration
--
-- Requirements:
-- - Validate environment configuration
-- - Execute DDL in correct order
-- - Track all changes in DEPLOYMENT_HISTORY
-- - Return structured result for Azure DevOps

-- SP_VALIDATE_DEPLOYMENT: Pre-deployment validation
-- Input: ENVIRONMENT, DEPLOYMENT_GROUP
-- Output: Validation results (schema compatibility, dependency check, etc.)

-- SP_ROLLBACK_DEPLOYMENT: Rollback to previous version
-- Input: DEPLOYMENT_ID (to rollback to), ENVIRONMENT
-- Output: Rollback status, objects reverted

-- SP_DETECT_DRIFT: Compare deployed state vs expected state
-- Input: ENVIRONMENT
-- Output: List of drifted objects with differences
```

### 3.5 Azure DevOps Variable Groups
Define variable groups for each environment:

```yaml
# Variable Group: snowflake-silver-dev
# - SNOWFLAKE_ACCOUNT: xxx.east-us-2.azure
# - SNOWFLAKE_DATABASE: D_SILVER_DEV
# - SNOWFLAKE_WAREHOUSE: CDC_WH_DEV_XS
# - TARGET_LAG_CDC_MINUTES: 5
# - TARGET_LAG_FULL_HOURS: 1
# - ALERT_RECIPIENTS: dev-team@company.com
# - ENABLE_DQ_CHECKS: true
# - OBSERVABILITY_LEVEL: ENHANCED

# Variable Group: snowflake-silver-prod
# - SNOWFLAKE_ACCOUNT: xxx.east-us-2.azure
# - SNOWFLAKE_DATABASE: D_SILVER
# - SNOWFLAKE_WAREHOUSE: CDC_WH_XS
# - TARGET_LAG_CDC_MINUTES: 5
# - TARGET_LAG_FULL_HOURS: 1
# - ALERT_RECIPIENTS: prod-alerts@company.com, oncall@company.com
# - ENABLE_DQ_CHECKS: true
# - OBSERVABILITY_LEVEL: CRITICAL
```

---

## DELIVERABLE 4: END-TO-END OBSERVABILITY FRAMEWORK

### 4.1 Observability Architecture
Implement comprehensive Snowflake-native observability:

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                      END-TO-END OBSERVABILITY ARCHITECTURE                    │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                        DATA COLLECTION LAYER                         │   │
│   ├─────────────────────────────────────────────────────────────────────┤   │
│   │  • INFORMATION_SCHEMA.DYNAMIC_TABLES()                              │   │
│   │  • INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY()               │   │
│   │  • INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY()                 │   │
│   │  • ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY                      │   │
│   │  • ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY                         │   │
│   │  • ACCOUNT_USAGE.QUERY_HISTORY                                      │   │
│   │  • ACCOUNT_USAGE.ACCESS_HISTORY                                     │   │
│   │  • Custom audit tables (DEPLOYMENT_HISTORY, DQ_RESULTS)             │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                        METRICS & AGGREGATION                         │   │
│   ├─────────────────────────────────────────────────────────────────────┤   │
│   │  • V_DT_HEALTH (real-time health status)                            │   │
│   │  • V_SLA_COMPLIANCE (SLA tracking)                                  │   │
│   │  • V_REFRESH_PERFORMANCE (performance metrics)                      │   │
│   │  • V_DATA_FRESHNESS (data currency tracking)                        │   │
│   │  • V_COST_METRICS (credit consumption)                              │   │
│   │  • V_ERROR_SUMMARY (error aggregation)                              │   │
│   │  • V_DEPENDENCY_STATUS (DAG health)                                 │   │
│   │  • V_DEPLOYMENT_STATUS (CI/CD tracking)                             │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                        ALERTING & NOTIFICATION                       │   │
│   ├─────────────────────────────────────────────────────────────────────┤   │
│   │  • ALERT_DT_FAILURE (Dynamic Table failures)                        │   │
│   │  • ALERT_SLA_BREACH (TARGET_LAG violations)                         │   │
│   │  • ALERT_DQ_FAILURE (Data quality issues)                           │   │
│   │  • ALERT_COST_ANOMALY (Credit consumption spikes)                   │   │
│   │  • ALERT_DEPLOYMENT_FAILURE (CI/CD failures)                        │   │
│   │  • ALERT_DRIFT_DETECTED (Configuration drift)                       │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                        VISUALIZATION & REPORTING                     │   │
│   ├─────────────────────────────────────────────────────────────────────┤   │
│   │  • Real-time dashboard (Snowsight / Grafana / Power BI)             │   │
│   │  • Daily/Weekly SLA reports                                         │   │
│   │  • Cost trending reports                                            │   │
│   │  • Deployment history reports                                       │   │
│   │  • Data lineage visualization                                       │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Observability Metadata Tables
Create tables to store observability data:

```sql
-- OBSERVABILITY_METRICS: Store time-series metrics
-- - METRIC_TIMESTAMP
-- - METRIC_NAME (dt_refresh_duration, dt_rows_processed, warehouse_credits, etc.)
-- - METRIC_VALUE
-- - DIMENSION_TABLE_NAME
-- - DIMENSION_SCHEMA
-- - DIMENSION_ENVIRONMENT
-- - DIMENSION_WAREHOUSE
-- - TAGS (JSON for flexible dimensions)

-- OBSERVABILITY_EVENTS: Store discrete events
-- - EVENT_TIMESTAMP
-- - EVENT_TYPE (DEPLOYMENT, ALERT_FIRED, ALERT_RESOLVED, DQ_CHECK, etc.)
-- - EVENT_SEVERITY (INFO, WARNING, ERROR, CRITICAL)
-- - EVENT_SOURCE (table name, procedure name, alert name)
-- - EVENT_DETAILS (JSON)
-- - CORRELATION_ID (for tracing related events)
-- - AZURE_DEVOPS_BUILD_ID (if deployment-related)

-- OBSERVABILITY_TRACES: Store execution traces
-- - TRACE_ID (unique identifier)
-- - PARENT_TRACE_ID (for nested operations)
-- - OPERATION_NAME
-- - START_TIMESTAMP, END_TIMESTAMP
-- - DURATION_MS
-- - STATUS (SUCCESS, FAILURE, TIMEOUT)
-- - ATTRIBUTES (JSON)
-- - ERROR_MESSAGE (if failed)
```

### 4.3 Real-Time Monitoring Views
Create comprehensive monitoring views:

```sql
-- V_DT_HEALTH: Real-time health status of all Dynamic Tables
-- Columns: TABLE_NAME, SCHEMA_NAME, TARGET_LAG, ACTUAL_LAG_MINUTES, 
--          LAST_REFRESH, SCHEDULING_STATE, HEALTH_STATUS, OBSERVABILITY_TIER

-- V_SLA_COMPLIANCE: SLA compliance tracking
-- Columns: DATE_HOUR, TOTAL_REFRESHES, SUCCESSFUL_REFRESHES, FAILED_REFRESHES,
--          SLA_COMPLIANCE_PERCENT, AVG_REFRESH_SECONDS, P95_REFRESH_SECONDS

-- V_REFRESH_PERFORMANCE: Performance metrics per table
-- Columns: TABLE_NAME, AVG_REFRESH_DURATION_MS, MAX_REFRESH_DURATION_MS,
--          AVG_ROWS_PROCESSED, REFRESH_COUNT_24H, TREND_INDICATOR

-- V_DATA_FRESHNESS: Data currency tracking
-- Columns: TABLE_NAME, SOURCE_SCHEMA, LAST_SOURCE_CHANGE (from _LOG),
--          LAST_SILVER_REFRESH, DATA_FRESHNESS_MINUTES, FRESHNESS_STATUS

-- V_COST_METRICS: Credit consumption tracking
-- Columns: DATE, WAREHOUSE_NAME, CREDITS_USED, ESTIMATED_COST_USD,
--          CREDITS_PER_TABLE, COST_TREND_VS_YESTERDAY, COST_TREND_VS_LAST_WEEK

-- V_ERROR_SUMMARY: Error aggregation and trending
-- Columns: DATE_HOUR, ERROR_TYPE, ERROR_COUNT, AFFECTED_TABLES,
--          MOST_COMMON_ERROR_MESSAGE, MTTR_MINUTES (mean time to resolve)

-- V_DEPENDENCY_STATUS: DAG health monitoring
-- Columns: TABLE_NAME, UPSTREAM_TABLES, DOWNSTREAM_TABLES,
--          UPSTREAM_HEALTH, DOWNSTREAM_IMPACT_IF_FAILED, CRITICAL_PATH_FLAG

-- V_DEPLOYMENT_STATUS: CI/CD tracking
-- Columns: DEPLOYMENT_ID, ENVIRONMENT, STATUS, DEPLOYED_AT, DEPLOYED_BY,
--          AZURE_DEVOPS_BUILD_ID, OBJECTS_COUNT, DURATION_SECONDS, ROLLBACK_AVAILABLE

-- V_END_TO_END_LATENCY: Bronze to Silver latency tracking
-- Columns: TABLE_NAME, BRONZE_LAST_UPDATE, SILVER_LAST_REFRESH,
--          END_TO_END_LATENCY_MINUTES, LATENCY_SLA_STATUS
```

### 4.4 Observability Stored Procedures
Create procedures for observability operations:

```sql
-- SP_COLLECT_METRICS: Periodic metric collection (run every 5 minutes)
-- - Collect all DT refresh metrics
-- - Collect warehouse utilization
-- - Collect data freshness metrics
-- - Store in OBSERVABILITY_METRICS table

-- SP_CALCULATE_SLA_COMPLIANCE: Daily SLA calculation
-- - Calculate SLA compliance percentage
-- - Identify SLA breaches
-- - Generate compliance report

-- SP_GENERATE_OBSERVABILITY_REPORT: On-demand reporting
-- Input: REPORT_TYPE (DAILY, WEEKLY, MONTHLY), DATE_RANGE
-- Output: Formatted report with key metrics

-- SP_TRACE_REFRESH_CHAIN: Trace end-to-end refresh
-- Input: TABLE_NAME
-- Output: Complete trace from Bronze _LOG to Silver CURR_DT with timing

-- SP_DIAGNOSE_PERFORMANCE_ISSUE: Performance troubleshooting
-- Input: TABLE_NAME
-- Output: Performance analysis with recommendations

-- SP_EXPORT_METRICS_FOR_GRAFANA: Export metrics in Grafana-compatible format
-- Output: JSON metrics for Grafana data source
```

### 4.5 Alerting Configuration
Create comprehensive alerting:

```sql
-- Alert: ALERT_DT_FAILURE
-- Trigger: Any Dynamic Table refresh failure in D_SILVER
-- Severity: Based on OBSERVABILITY_TIER (CRITICAL tables = P1, others = P2)
-- Action: Email + Teams webhook + PagerDuty (for CRITICAL)

-- Alert: ALERT_SLA_BREACH
-- Trigger: Actual lag > TARGET_LAG + 5 minutes
-- Severity: WARNING at 1x threshold, CRITICAL at 2x threshold
-- Action: Email + Teams webhook

-- Alert: ALERT_DQ_FAILURE
-- Trigger: Data quality check failure exceeding threshold
-- Severity: Based on DQ rule severity
-- Action: Email + quarantine records + flag table

-- Alert: ALERT_COST_ANOMALY
-- Trigger: Daily credit consumption > 150% of 7-day average
-- Severity: WARNING
-- Action: Email to FinOps team

-- Alert: ALERT_DATA_FRESHNESS
-- Trigger: No new data in _LOG for > 30 minutes (configurable per table)
-- Severity: WARNING (may indicate upstream IDMC issue)
-- Action: Email + check IDMC status

-- Alert: ALERT_DEPLOYMENT_DRIFT
-- Trigger: SP_DETECT_DRIFT finds differences
-- Severity: WARNING
-- Action: Email to DevOps team + create Azure DevOps work item

-- Alert: ALERT_UPSTREAM_BRONZE_STALE
-- Trigger: Bronze _LOG table not updated within expected window
-- Severity: WARNING (indicates IDMC ingestion issue)
-- Action: Email + escalate to IDMC team
```

### 4.6 Dashboard Specifications
Define dashboard requirements:

```yaml
# Snowsight Dashboard: Silver Layer Operations
# 
# Section 1: Executive Summary
# - Total tables: CDC count, Full Load count
# - Overall health: % healthy, % warning, % critical
# - 24-hour SLA compliance percentage
# - Today's cost vs budget
#
# Section 2: Real-Time Health
# - Table health heatmap (green/yellow/red)
# - Current lag vs target lag per table
# - Active alerts count
#
# Section 3: Performance Trends
# - Refresh duration trend (7 days)
# - Rows processed trend (7 days)
# - P95 latency trend
#
# Section 4: Cost Analysis
# - Daily credit consumption (30 days)
# - Cost by warehouse
# - Cost by table (top 10)
# - Projected monthly cost
#
# Section 5: Data Quality
# - DQ pass/fail rate
# - Quarantined records count
# - DQ trend by rule type
#
# Section 6: Deployment History
# - Recent deployments timeline
# - Deployment success rate
# - Objects changed per deployment

# Grafana Dashboard Queries (for external monitoring)
# - Provide SQL queries optimized for Grafana data source
# - Include templating for environment selection
# - Include time range macros
```

### 4.7 Log Aggregation
Create centralized logging:

```sql
-- SILVER_LAYER_LOG: Centralized log table
-- - LOG_TIMESTAMP
-- - LOG_LEVEL (DEBUG, INFO, WARNING, ERROR, CRITICAL)
-- - LOG_SOURCE (procedure name, alert name, job name)
-- - LOG_MESSAGE
-- - LOG_DETAILS (JSON with additional context)
-- - CORRELATION_ID (for tracing)
-- - SESSION_ID
-- - USER_NAME
-- - WAREHOUSE_NAME

-- SP_LOG: Logging procedure for all Silver layer operations
-- Input: LOG_LEVEL, LOG_SOURCE, LOG_MESSAGE, LOG_DETAILS, CORRELATION_ID
-- Output: LOG_ID

-- V_LOG_SEARCH: View for log search with filtering
-- - Support text search in LOG_MESSAGE
-- - Support JSON path search in LOG_DETAILS
-- - Support time range filtering

-- SP_ARCHIVE_LOGS: Archive old logs to cold storage
-- - Move logs older than retention period to archive table
-- - Optionally export to external stage (Azure Blob)
```

---

## DELIVERABLE 5: DEPLOYMENT AUTOMATION SCRIPTS

### 5.1 Environment Setup Script
Create idempotent setup script for each environment:

```sql
-- Script: 01_ENVIRONMENT_SETUP.sql
-- Purpose: Create all infrastructure (databases, schemas, warehouses, roles)

-- Requirements:
-- - All CREATE statements must use IF NOT EXISTS
-- - Parameterized warehouse sizes based on environment
-- - Create dedicated warehouses: CDC_WH, ALERT_WH, DQ_WH
-- - Create schemas: D_SILVER.{source_schemas}, D_SILVER.OPS, D_SILVER.DQ, D_SILVER.CONFIG, D_SILVER.OBSERVABILITY
-- - Create RBAC roles: SILVER_TRANSFORMER_ROLE, SILVER_CONSUMER_ROLE, SILVER_OPS_ROLE, SILVER_DEPLOYER_ROLE
-- - Grant appropriate privileges
-- - Create service account for Azure DevOps
-- - Set up network policies if required
```

### 5.2 Metadata Population Script
Create script to populate configuration metadata:

```sql
-- Script: 02_POPULATE_METADATA.sql
-- Purpose: Insert table configurations into metadata tables

-- Requirements:
-- - Template format for easy addition of new tables
-- - Validation queries to verify configuration completeness
-- - Support for bulk insert from CSV/external stage
-- - Include DEPLOYMENT_GROUP assignment for phased rollouts
-- - Include OBSERVABILITY_TIER assignment
```

### 5.3 Dynamic Table Deployment Script
Create deployment orchestration:

```sql
-- Script: 03_DEPLOY_DYNAMIC_TABLES.sql
-- Purpose: Execute generated DDL in correct order

-- Requirements:
-- - Call DDL generator procedures
-- - Execute DDL statements
-- - Verify Dynamic Table creation and initial refresh
-- - Log deployment status to audit table
-- - Update deployment tracking for Azure DevOps
-- - Emit observability events
```

### 5.4 Rollback Script
Create rollback capability:

```sql
-- Script: 04_ROLLBACK.sql
-- Purpose: Safely remove Silver layer objects

-- Requirements:
-- - Suspend all Dynamic Tables first
-- - Drop in reverse dependency order (CURR_DT → CDC_DT → BASE tables)
-- - Preserve configuration metadata for re-deployment
-- - Log rollback actions
-- - Update deployment tracking
-- - Emit observability events
```

---

## DELIVERABLE 6: PRODUCTION OPERATIONS FRAMEWORK

### 6.1 Error Handling & Alerting

#### 6.1.1 Alert Configuration
```sql
-- Create notification integration for email alerts
-- Create Teams/Slack webhook integration via external function
-- Create alert for Dynamic Table failures (ERROR_ONLY => TRUE)
-- Create alert for SLA breaches (lag > threshold)
-- Create alert for suspended Dynamic Tables
-- Create alert for warehouse credit consumption anomalies
-- Create alert for deployment failures (Azure DevOps integration)
```

#### 6.1.2 Alert Stored Procedures
```sql
-- SP_SEND_DT_FAILURE_ALERT: Format and send failure notifications
-- SP_SEND_SLA_BREACH_ALERT: Format and send SLA breach notifications
-- SP_SEND_DQ_ALERT: Format and send data quality issue notifications
-- SP_SEND_DEPLOYMENT_ALERT: Format and send deployment status notifications

-- Requirements:
-- - Include table name, error message, timestamp
-- - Include link to DYNAMIC_TABLE_REFRESH_HISTORY query
-- - Include link to Azure DevOps build/release
-- - Include suggested remediation steps
-- - Support multiple notification channels (email, Teams webhook, PagerDuty)
-- - Log all alerts to OBSERVABILITY_EVENTS
```

#### 6.1.3 Alert Response Automation
```sql
-- SP_AUTO_REMEDIATE_DT_FAILURE: Automatic retry logic
-- - Check if failure is transient (timeout, resource contention)
-- - Attempt resume for suspended tables
-- - Scale warehouse temporarily for resource failures
-- - Log all remediation attempts to observability tables
-- - Create Azure DevOps work item if manual intervention needed
```

### 6.2 Data Quality Validation Framework

#### 6.2.1 DQ Rules Configuration
```sql
-- DQ_RULES_CONFIG table:
-- - RULE_ID, RULE_NAME, RULE_TYPE (NULL_CHECK, DUPLICATE_CHECK, RANGE_CHECK, CUSTOM)
-- - TABLE_CONFIG_ID (FK)
-- - COLUMN_NAME
-- - RULE_EXPRESSION (SQL boolean expression)
-- - SEVERITY (WARNING, ERROR, CRITICAL)
-- - IS_BLOCKING (stop pipeline if failed)
-- - THRESHOLD_PERCENT (acceptable failure rate)
```

#### 6.2.2 DQ Check Dynamic Tables
```sql
-- Create DQ check Dynamic Tables that run after CURR_DT refresh:
-- - TARGET_LAG = DOWNSTREAM from CURR_DT
-- - Evaluate all configured DQ rules
-- - Store results in DQ_RESULTS table
-- - Trigger alerts for failures exceeding threshold
-- - Emit observability metrics
```

#### 6.2.3 DQ Stored Procedures
```sql
-- SP_EVALUATE_DQ_RULES: Execute all DQ checks for a table
-- SP_GET_DQ_SUMMARY: Return DQ status across all tables
-- SP_QUARANTINE_FAILED_RECORDS: Move failed records to quarantine table
```

### 6.3 Monitoring & SLA Management
(Covered in Observability Framework - Section 4)

### 6.4 Disaster Recovery & Reprocessing

#### 6.4.1 Recovery Procedures
```sql
-- SP_REBUILD_SINGLE_TABLE: Rebuild one table from Bronze
-- - Suspend dependent Dynamic Tables
-- - Truncate and reload BASE table
-- - Resume Dynamic Tables
-- - Verify data integrity
-- - Log to observability

-- SP_REBUILD_ALL_TABLES: Full Silver layer rebuild
-- - Execute SP_REBUILD_SINGLE_TABLE for all tables
-- - Maintain proper dependency order
-- - Emit progress to observability

-- SP_POINT_IN_TIME_RECOVERY: Recover to specific timestamp
-- - Use Time Travel to restore Silver table
-- - Reprocess CDC from that point forward
-- - Log recovery actions
```

#### 6.4.2 Backup Automation
```sql
-- Create scheduled task for Silver layer backup:
-- - Daily snapshot to backup schema
-- - Retention policy (7 days)
-- - Verification of backup completeness
-- - Export to Azure Blob Storage (optional)
```

### 6.5 Failure Scenario Handling

Create specific procedures for each failure scenario from the design document:

```sql
-- SP_HANDLE_DT_REFRESH_FAILURE: Auto-retry, scale warehouse, alert if persistent
-- SP_HANDLE_IDMC_DELAY: Monitor Bronze _LOG staleness, alert if no new records
-- SP_HANDLE_SILVER_CORRUPTION: Trigger rebuild from Bronze, notify stakeholders
-- SP_HANDLE_BRONZE_CORRUPTION: Alert IDMC team, pause Silver processing
-- SP_HANDLE_ACCIDENTAL_DROP: Recover from Time Travel or backup
-- SP_HANDLE_DEPLOYMENT_FAILURE: Trigger rollback, notify DevOps team
```

### 6.6 Cost Estimation & Monitoring

```sql
-- V_COST_ESTIMATE_PER_TABLE: Estimated monthly cost per table
-- - Based on actual refresh frequency
-- - Based on actual warehouse runtime
-- - Projected from last 7 days

-- V_COST_BUDGET_VS_ACTUAL: Budget tracking
-- - Monthly budget per environment
-- - Actual spend to date
-- - Projected end-of-month spend
-- - Variance alerts

-- SP_OPTIMIZE_COST: Cost optimization recommendations
-- - Identify underutilized warehouses
-- - Identify tables that could use longer TARGET_LAG
-- - Identify clustering opportunities
-- - Generate optimization report
```

---

## DELIVERABLE 7: COMPREHENSIVE DOCUMENTATION

### 7.1 Technical Design Document
Generate detailed technical documentation including:

```markdown
## Technical Design Document Structure

1. **Architecture Overview**
   - System context diagram
   - Data flow diagrams (CDC and Full Load)
   - Component interaction diagram
   - Future state (showing Gold layer placeholder)

2. **Database Design**
   - Schema definitions
   - Table structures (metadata tables, Silver tables)
   - Naming conventions

3. **Dynamic Table Specifications**
   - CDC_DT specifications (IMMUTABLE WHERE, INCREMENTAL)
   - CURR_DT specifications (UNION ALL, ROW_NUMBER)
   - Full Load DT specifications (FULL refresh)
   - Dependency graph

4. **Configuration Reference**
   - Environment parameters
   - Metadata table schemas
   - Column mapping rules

5. **Integration Points**
   - Bronze layer dependencies (IDMC objects)
   - Downstream consumption patterns (for future Gold layer)
   - Azure DevOps integration
   - Alerting integrations

6. **Observability Architecture**
   - Metrics collection
   - Alerting framework
   - Dashboard specifications

7. **Performance Considerations**
   - Warehouse sizing guidelines
   - IMMUTABLE WHERE tuning
   - Clustering recommendations
```

### 7.2 Runbook (Step-by-Step Operations Guide)
Generate operational runbook with examples:

```markdown
## Runbook Structure

1. **Daily Operations**
   - Health check procedures (with example queries)
   - SLA verification steps
   - Cost monitoring review
   - Observability dashboard review

2. **Adding New Tables**
   - Step 1: Create feature branch in Azure DevOps
   - Step 2: Insert configuration into CDC_TABLE_CONFIG
   - Step 3: Insert column mappings into COLUMN_MAPPING_CONFIG
   - Step 4: Create pull request
   - Step 5: Merge and deploy via pipeline
   - Step 6: Verify initial refresh
   - Step 7: Add DQ rules
   - Step 8: Update observability tier
   - Example: Adding TRAIN_SCHEDULE table

3. **Azure DevOps Operations**
   - Creating a new deployment
   - Approving stage gates
   - Monitoring pipeline execution
   - Triggering rollback
   - Viewing deployment history

4. **Incident Response**
   - Dynamic Table failure response (with example)
   - SLA breach response (with example)
   - Data quality issue response (with example)
   - Deployment failure response (with example)
   - Escalation procedures

5. **Maintenance Procedures**
   - Schema evolution process
   - Warehouse scaling procedure
   - Backup and restore procedure
   - Performance tuning procedure
   - Log archival procedure

6. **Troubleshooting Guide**
   - Common error messages and resolutions
   - Diagnostic queries
   - Log analysis procedures
   - Observability trace analysis
```

### 7.3 Functional Documentation
Generate business-facing documentation:

```markdown
## Functional Documentation Structure

1. **Business Overview**
   - Purpose of Silver curated layer
   - Data freshness guarantees (SLA)
   - Data quality guarantees
   - Position in Medallion architecture

2. **Data Dictionary**
   - Table descriptions
   - Column definitions
   - Business rules applied

3. **Audit and Lineage**
   - RECORD_SOURCE column meaning
   - EFFECTIVE_TS vs LOAD_TS explanation
   - IS_DELETED flag usage
   - End-to-end lineage tracking

4. **Consumer Guide**
   - How to query Silver tables
   - Recommended WHERE clauses (IS_DELETED = FALSE)
   - Performance best practices for consumers
   - How Silver tables will feed future Gold layer

5. **SLA and Support**
   - Data freshness SLAs by table
   - Support contact information
   - Issue reporting procedures
   - Observability dashboard access
```

### 7.4 Azure DevOps Guide
Generate DevOps-specific documentation:

```markdown
## Azure DevOps Guide Structure

1. **Repository Structure**
   - Folder organization
   - Branching strategy (GitFlow)
   - Code review requirements

2. **Pipeline Overview**
   - CI pipeline stages
   - CD pipeline stages
   - Approval gates

3. **Environment Promotion**
   - DEV → QA → UAT → PROD workflow
   - Approval requirements per stage
   - Deployment windows

4. **Rollback Procedures**
   - Automatic rollback triggers
   - Manual rollback process
   - Rollback verification

5. **Secrets Management**
   - Azure Key Vault integration
   - Credential rotation
   - Service principal management

6. **Monitoring Pipeline Health**
   - Pipeline success metrics
   - Build/release dashboards
   - Failure notification setup
```

---

## DELIVERABLE 8: VALIDATION AND TESTING FRAMEWORK

### 8.1 Deployment Validation Queries
```sql
-- Verify all Dynamic Tables created successfully
-- Verify initial refresh completed
-- Verify row counts match between Bronze and Silver
-- Verify audit columns populated correctly
-- Verify RECORD_SOURCE distribution (BASE vs CDC)
-- Verify observability metrics being collected
```

### 8.2 Regression Test Suite
```sql
-- Test CDC flow: Insert/Update/Delete in Bronze _LOG → verify Silver CURR_DT
-- Test Full Load flow: Replace Bronze → verify Silver DT
-- Test soft delete: OP_CODE='D' → IS_DELETED=TRUE
-- Test deduplication: Multiple CDC events → single current record
-- Test late-arriving data: Out-of-order EFFECTIVE_TS → correct latest selected
-- Test alerting: Simulate failure → verify alert fires
-- Test observability: Verify metrics collected after refresh
```

### 8.3 Performance Baseline
```sql
-- Capture baseline metrics for each table:
-- - Initial refresh duration
-- - Incremental refresh duration
-- - Rows processed per refresh
-- - Credit consumption per refresh
-- Store in PERFORMANCE_BASELINE table for trending
-- Compare against baseline in regression tests
```

### 8.4 Azure DevOps Test Integration
```yaml
# Test result publishing:
# - JUnit XML format for test results
# - Code coverage reporting (if applicable)
# - Performance test results
# - Integration with Azure DevOps Test Plans
```

---

## OUTPUT FORMAT REQUIREMENTS

### Code Organization
Organize all output into the following structure:

```
/snowflake-silver-layer
  /pipelines
    - azure-pipelines.yml
    - azure-pipelines-pr.yml
    - azure-pipelines-release.yml
  /templates
    - deploy-stage-template.yml
    - validation-template.yml
    - rollback-template.yml
  /scripts
    /infrastructure
      - 01_environment_setup.sql
      - 02_create_metadata_tables.sql
      - 03_create_monitoring_views.sql
      - 04_create_alert_infrastructure.sql
      - 05_create_observability_infrastructure.sql
    /configuration
      - 01_environment_config.sql
      - 02_cdc_table_config.sql
      - 03_full_load_table_config.sql
      - 04_column_mappings.sql
      - 05_dq_rules_config.sql
      - 06_observability_config.sql
    /stored_procedures
      - 01_ddl_generators.sql
      - 02_deployment_procedures.sql
      - 03_alert_procedures.sql
      - 04_dq_procedures.sql
      - 05_recovery_procedures.sql
      - 06_observability_procedures.sql
      - 07_devops_integration_procedures.sql
    /deployment
      - 01_deploy_all.sql
      - 02_deploy_cdc_tables.sql
      - 03_deploy_full_load_tables.sql
      - 04_rollback.sql
    /operations
      - 01_daily_health_check.sql
      - 02_sla_monitoring.sql
      - 03_cost_monitoring.sql
      - 04_incident_response.sql
    /testing
      - 01_deployment_validation.sql
      - 02_regression_tests.sql
      - 03_performance_baseline.sql
  /config
    - dev.env.json
    - qa.env.json
    - uat.env.json
    - prod.env.json
  /docs
    - technical_design_document.md
    - runbook.md
    - functional_documentation.md
    - data_dictionary.md
    - devops_guide.md
    - observability_guide.md
```

### Code Standards
- All SQL must be valid Snowflake syntax
- All objects must use fully qualified names (DATABASE.SCHEMA.OBJECT)
- All scripts must be idempotent (safe to re-run)
- All scripts must include header comments with purpose, author, version
- All stored procedures must include error handling with TRY/CATCH
- All configuration must be parameterized (no hardcoded environment values)
- All scripts must emit observability events/logs
- All scripts must be Azure DevOps pipeline compatible

### Documentation Standards
- Use Markdown format for all documentation
- Include code examples for every procedure
- Include troubleshooting guidance for every operation
- Include diagrams where applicable (Mermaid syntax)
- Include Azure DevOps-specific instructions

---

## SAMPLE TABLE CONFIGURATIONS TO INCLUDE

Based on the design document, include configuration for these sample tables:

### CDC Pattern Tables (from SADB source)
1. TRAIN_PLAN_LEG (reference implementation from design document)
2. TRAIN_PLAN
3. TRAIN_SCHEDULE
4. ROUTE_SEGMENT
5. (Template for additional 15-20 CDC tables)

### Full Load Pattern Tables (from AZURE source)
1. TRAIN_PRODUCTIVITY_CPKC_GTM_TRN_FACT (reference implementation from design document)
2. STATION_REFERENCE
3. EQUIPMENT_TYPE
4. (Template for additional 5-10 Full Load tables)

---

## PERFORMANCE OPTIMIZATION REQUIREMENTS

Based on the technical review recommendations, implement these optimizations:

1. **IMMUTABLE WHERE Tuning**
   - Default: 1 day interval
   - Configurable per table in metadata
   - Guidance for adjustment based on late-arriving data patterns

2. **Warehouse Sizing**
   - X-SMALL default for CDC workloads
   - SMALL for tables with >1M rows per refresh
   - MEDIUM for initial historical loads
   - Auto-scaling configuration

3. **Clustering Strategy**
   - Automatic clustering recommendation based on table size
   - Primary key clustering for tables >1M rows
   - Verification query for clustering effectiveness

4. **Cost Optimization**
   - AUTO_SUSPEND = 60 seconds default
   - Separate warehouses for different workload types
   - Credit consumption alerting via observability framework

---

## FINAL INSTRUCTIONS

1. **Generate complete, production-ready code** - not pseudocode or outlines
2. **Follow the approved design patterns exactly** - do not deviate from CDC or Full Load specifications
3. **Parameterize everything** - support DEV/QA/UAT/PROD environments
4. **Include comprehensive error handling** - every procedure must handle failures gracefully
5. **Document thoroughly** - every script must be self-documenting
6. **Validate against Snowflake 2025 syntax** - ensure all code is executable
7. **No hallucinations** - only use documented Snowflake features and functions
8. **Azure DevOps compatible** - all scripts must work within Azure DevOps pipelines
9. **End-to-end observability** - every operation must emit metrics/logs/events
10. **Design for extensibility** - Silver layer must cleanly support future Gold layer consumption

Begin implementation by first generating the metadata table structures, then the DDL generator procedures, then the Azure DevOps pipeline definitions, then the observability framework, then the deployment scripts, then the operations framework, and finally the documentation.
```

---

## USAGE INSTRUCTIONS

### How to Use This Prompt

1. **Copy the entire prompt** above (everything between the ``` markers)
2. **Open your Cortex CLI agent**
3. **Attach both documents**:
   - Bronze_to_Silver_Curated_Data_Layer_Design_Specification_v1.docx
   - Bronze_to_Silver_Architecture_Technical_Review_v1.md
4. **Paste the prompt** and submit
5. **Review the generated output** section by section
6. **Request specific sections** if you need more detail on any component

### Follow-Up Prompts

After receiving the initial output, use these follow-up prompts for deeper detail:

**For Infrastructure Setup:**
```
Generate the complete 01_environment_setup.sql script with all CREATE DATABASE, CREATE SCHEMA, CREATE WAREHOUSE, and CREATE ROLE statements. Include RBAC grants for SILVER_TRANSFORMER_ROLE, SILVER_CONSUMER_ROLE, SILVER_OPS_ROLE, and SILVER_DEPLOYER_ROLE. Include service account setup for Azure DevOps.
```

**For Azure DevOps Pipeline:**
```
Generate the complete azure-pipelines.yml file with all stages (Build, DEV, QA, UAT, PROD). Include approval gates, variable group references, SnowSQL execution tasks, and artifact publishing. Include rollback triggers and observability integration.
```

**For DDL Generator Procedure:**
```
Generate the complete SP_GENERATE_CDC_TABLE_DDL stored procedure. Include all logic for reading metadata, constructing DDL strings, and handling all column types. Show a complete example output for the TRAIN_PLAN_LEG table. Include observability event emission.
```

**For Observability Framework:**
```
Generate the complete observability infrastructure including: OBSERVABILITY_METRICS table, OBSERVABILITY_EVENTS table, all monitoring views (V_DT_HEALTH, V_SLA_COMPLIANCE, V_REFRESH_PERFORMANCE, V_END_TO_END_LATENCY), and the SP_COLLECT_METRICS procedure.
```

**For Alert Procedures:**
```
Generate the complete SP_SEND_DT_FAILURE_ALERT, SP_SEND_SLA_BREACH_ALERT, and SP_SEND_DEPLOYMENT_ALERT stored procedures. Include email formatting, Teams webhook integration, error details extraction, and remediation suggestions. Include logging to observability tables.
```

**For Runbook:**
```
Generate the complete Runbook document with step-by-step procedures for: (1) Adding a new CDC table via Azure DevOps, (2) Responding to Dynamic Table failure using observability data, (3) Performing disaster recovery, (4) Executing a rollback via Azure DevOps. Include actual SQL examples and Azure DevOps screenshots placeholders.
```

**For Testing Framework:**
```
Generate the complete regression test suite with test cases for: (1) CDC INSERT flow, (2) CDC UPDATE flow, (3) CDC DELETE flow with soft delete, (4) Deduplication correctness, (5) Late-arriving data handling, (6) Observability metric collection verification. Include JUnit XML output format for Azure DevOps integration.
```

**For DevOps Guide:**
```
Generate the complete Azure DevOps Guide document covering: repository structure, branching strategy, pipeline configuration, approval gates, rollback procedures, secrets management with Azure Key Vault, and monitoring pipeline health.
```

---

## EXPECTED OUTPUT SIZE

The complete implementation should generate approximately:
- **Infrastructure Scripts**: ~600-900 lines SQL
- **Metadata Tables**: ~300-400 lines SQL
- **DDL Generator Procedures**: ~800-1200 lines SQL
- **Azure DevOps Pipelines**: ~400-600 lines YAML
- **Observability Framework**: ~800-1000 lines SQL
- **Deployment Scripts**: ~400-600 lines SQL
- **Operations Framework**: ~1200-1800 lines SQL
- **Testing Framework**: ~500-700 lines SQL
- **Technical Documentation**: ~3000-4000 words
- **Runbook**: ~4000-5000 words
- **Functional Documentation**: ~2000-2500 words
- **DevOps Guide**: ~2000-2500 words
- **Observability Guide**: ~1500-2000 words

**Total**: ~5000-7000 lines of SQL/YAML + ~12000-16000 words of documentation
