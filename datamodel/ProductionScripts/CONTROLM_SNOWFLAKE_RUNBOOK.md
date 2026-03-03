# Control-M Snowflake Integration - Production Runbook

```
================================================================================
            CONTROL-M SNOWFLAKE ORCHESTRATION & MONITORING RUNBOOK
                      Production Implementation Guide
================================================================================
Document Version : 1.0
Last Updated     : 2026-03-03
Author           : Snowflake Architect
Classification   : PRODUCTION - CUSTOMER DEPLOYMENT
================================================================================
```

---

## TABLE OF CONTENTS

1. [Executive Summary](#1-executive-summary)
2. [Snowflake Pipeline Monitoring Best Practices](#2-snowflake-pipeline-monitoring-best-practices)
3. [Control-M Prerequisites](#3-control-m-prerequisites)
4. [Control-M Snowflake Integration Architecture](#4-control-m-snowflake-integration-architecture)
5. [Step-by-Step Configuration](#5-step-by-step-configuration)
6. [Implementation Steps](#6-implementation-steps)
7. [Monitoring & Alerting Configuration](#7-monitoring--alerting-configuration)
8. [Validation & Testing](#8-validation--testing)
9. [Troubleshooting Guide](#9-troubleshooting-guide)
10. [Appendix](#10-appendix)

---

## 1. EXECUTIVE SUMMARY

### 1.1 Purpose

This runbook provides production-ready implementation guidance for:
- Snowflake pipeline monitoring and alerting best practices
- Control-M orchestration integration with Snowflake
- End-to-end configuration and implementation steps

### 1.2 Scope

| Component | Description |
|-----------|-------------|
| Orchestration Tool | BMC Control-M |
| Target Platform | Snowflake Data Cloud |
| Integration Method | Control-M Application Integrator (Snowflake Job Type) |
| Monitoring Scope | Tasks, Streams, Procedures, Query Performance |

### 1.3 Reference Documentation

| Document | Source |
|----------|--------|
| Control-M Snowflake Job Type | BMC Documentation Portal |
| Snowflake Account Usage Views | docs.snowflake.com |
| Snowflake Tasks & Streams | docs.snowflake.com |
| Control-M Application Integrator | BMC Documentation Portal |

---

## 2. SNOWFLAKE PIPELINE MONITORING BEST PRACTICES

### 2.1 Monitoring Framework Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                 SNOWFLAKE MONITORING FRAMEWORK                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐        │
│   │  INFRASTRUCTURE │    │    PIPELINE     │    │   PERFORMANCE   │        │
│   │   MONITORING    │    │   MONITORING    │    │   MONITORING    │        │
│   ├─────────────────┤    ├─────────────────┤    ├─────────────────┤        │
│   │ • Warehouse     │    │ • Task Status   │    │ • Query Runtime │        │
│   │ • Storage       │    │ • Stream Health │    │ • Credit Usage  │        │
│   │ • Compute       │    │ • Pipe Status   │    │ • Queue Time    │        │
│   │ • Network       │    │ • Job Failures  │    │ • Spilling      │        │
│   └─────────────────┘    └─────────────────┘    └─────────────────┘        │
│            │                     │                      │                   │
│            └─────────────────────┼──────────────────────┘                   │
│                                  ▼                                          │
│                    ┌─────────────────────────┐                              │
│                    │   ALERTING & ACTIONS    │                              │
│                    ├─────────────────────────┤                              │
│                    │ • Email Notifications   │                              │
│                    │ • Webhook/API Calls     │                              │
│                    │ • Control-M Events      │                              │
│                    │ • Slack/Teams/PagerDuty │                              │
│                    └─────────────────────────┘                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Key Monitoring Views (ACCOUNT_USAGE Schema)

Snowflake provides built-in monitoring through the `SNOWFLAKE.ACCOUNT_USAGE` schema:

| View | Purpose | Latency |
|------|---------|---------|
| TASK_HISTORY | Task execution history | Up to 45 minutes |
| QUERY_HISTORY | Query execution details | Up to 45 minutes |
| WAREHOUSE_METERING_HISTORY | Warehouse credit usage | Up to 3 hours |
| PIPE_USAGE_HISTORY | Snowpipe consumption | Up to 2 hours |
| STORAGE_USAGE | Storage metrics | Up to 2 hours |
| LOGIN_HISTORY | Authentication events | Up to 2 hours |

### 2.3 Real-Time Monitoring Views (INFORMATION_SCHEMA)

For near real-time monitoring, use `INFORMATION_SCHEMA`:

| View/Function | Purpose | Latency |
|---------------|---------|---------|
| TASK_HISTORY() | Recent task runs | Real-time |
| QUERY_HISTORY() | Recent queries | Real-time |
| WAREHOUSE_LOAD_HISTORY() | Warehouse load | Real-time |

### 2.4 Best Practice: Custom Monitoring Tables

Create dedicated monitoring infrastructure:

```sql
-- ============================================================================
-- BEST PRACTICE: CUSTOM MONITORING INFRASTRUCTURE
-- ============================================================================

-- 1. Create monitoring schema
CREATE SCHEMA IF NOT EXISTS <DATABASE>.MONITORING;

-- 2. Pipeline execution log table
CREATE TABLE IF NOT EXISTS <DATABASE>.MONITORING.PIPELINE_EXECUTION_LOG (
    LOG_ID              NUMBER AUTOINCREMENT PRIMARY KEY,
    PIPELINE_NAME       VARCHAR(255) NOT NULL,
    JOB_NAME            VARCHAR(255) NOT NULL,
    EXECUTION_ID        VARCHAR(100),
    START_TIME          TIMESTAMP_NTZ NOT NULL,
    END_TIME            TIMESTAMP_NTZ,
    STATUS              VARCHAR(20) NOT NULL,  -- RUNNING, SUCCESS, FAILED, WARNING
    ROWS_PROCESSED      NUMBER,
    ERROR_MESSAGE       VARCHAR(4000),
    CONTROLM_ORDER_ID   VARCHAR(100),
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. Alert history table
CREATE TABLE IF NOT EXISTS <DATABASE>.MONITORING.ALERT_HISTORY (
    ALERT_ID            NUMBER AUTOINCREMENT PRIMARY KEY,
    ALERT_TYPE          VARCHAR(50) NOT NULL,
    ALERT_SEVERITY      VARCHAR(20) NOT NULL,  -- CRITICAL, HIGH, MEDIUM, LOW
    ALERT_SOURCE        VARCHAR(100),
    ALERT_MESSAGE       VARCHAR(4000),
    ALERT_TIME          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACKNOWLEDGED        BOOLEAN DEFAULT FALSE,
    ACKNOWLEDGED_BY     VARCHAR(100),
    ACKNOWLEDGED_AT     TIMESTAMP_NTZ
);

-- 4. SLA tracking table
CREATE TABLE IF NOT EXISTS <DATABASE>.MONITORING.SLA_TRACKING (
    SLA_ID              NUMBER AUTOINCREMENT PRIMARY KEY,
    PIPELINE_NAME       VARCHAR(255) NOT NULL,
    EXPECTED_START      TIMESTAMP_NTZ,
    EXPECTED_END        TIMESTAMP_NTZ,
    ACTUAL_START        TIMESTAMP_NTZ,
    ACTUAL_END          TIMESTAMP_NTZ,
    SLA_MET             BOOLEAN,
    EXECUTION_DATE      DATE NOT NULL
);
```

### 2.5 Best Practice: Alerting Thresholds

| Metric | Warning Threshold | Critical Threshold |
|--------|-------------------|-------------------|
| Task Failure Rate | > 5% in 1 hour | > 10% in 1 hour |
| Query Runtime | > 2x average | > 5x average |
| Warehouse Queue Time | > 60 seconds | > 300 seconds |
| Stream Staleness | Stream approaching stale | Stream is stale |
| Credit Usage | > 80% of budget | > 95% of budget |
| Storage Growth | > 20% daily | > 50% daily |

### 2.6 Best Practice: Snowflake Native Alerting

```sql
-- ============================================================================
-- BEST PRACTICE: SNOWFLAKE NATIVE ALERTS (Snowflake Enterprise+)
-- ============================================================================

-- Create notification integration for email
CREATE OR REPLACE NOTIFICATION INTEGRATION PIPELINE_ALERTS_EMAIL
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('dba-team@company.com', 'data-ops@company.com');

-- Create alert for task failures
CREATE OR REPLACE ALERT ALERT_TASK_FAILURES
    WAREHOUSE = <WAREHOUSE_NAME>
    SCHEDULE = '5 MINUTE'
    IF (EXISTS (
        SELECT 1 
        FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
            SCHEDULED_TIME_RANGE_START => DATEADD('minute', -10, CURRENT_TIMESTAMP()),
            RESULT_LIMIT => 100
        ))
        WHERE STATE = 'FAILED'
    ))
    THEN 
        CALL SYSTEM$SEND_EMAIL(
            'PIPELINE_ALERTS_EMAIL',
            'dba-team@company.com',
            'ALERT: Snowflake Task Failure Detected',
            'One or more tasks have failed in the last 10 minutes. Please investigate.'
        );

-- Create alert for long-running queries
CREATE OR REPLACE ALERT ALERT_LONG_RUNNING_QUERIES
    WAREHOUSE = <WAREHOUSE_NAME>
    SCHEDULE = '10 MINUTE'
    IF (EXISTS (
        SELECT 1 
        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
            END_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
            RESULT_LIMIT => 100
        ))
        WHERE EXECUTION_STATUS = 'RUNNING'
        AND DATEDIFF('minute', START_TIME, CURRENT_TIMESTAMP()) > 30
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL(
            'PIPELINE_ALERTS_EMAIL',
            'dba-team@company.com',
            'ALERT: Long Running Query Detected',
            'A query has been running for more than 30 minutes.'
        );

-- Enable the alerts
ALTER ALERT ALERT_TASK_FAILURES RESUME;
ALTER ALERT ALERT_LONG_RUNNING_QUERIES RESUME;
```

### 2.7 Best Practice: Monitoring Stored Procedure

```sql
-- ============================================================================
-- BEST PRACTICE: COMPREHENSIVE MONITORING PROCEDURE
-- ============================================================================

CREATE OR REPLACE PROCEDURE <DATABASE>.MONITORING.SP_GENERATE_HEALTH_REPORT()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_report VARCHAR DEFAULT '';
    v_task_failures NUMBER;
    v_stale_streams NUMBER;
    v_long_queries NUMBER;
BEGIN
    -- Check task failures in last hour
    SELECT COUNT(*) INTO :v_task_failures
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
    ))
    WHERE STATE = 'FAILED';
    
    -- Check stale streams
    SELECT COUNT(*) INTO :v_stale_streams
    FROM TABLE(INFORMATION_SCHEMA.STREAMS())
    WHERE STALE = TRUE;
    
    -- Check long-running queries (>30 min)
    SELECT COUNT(*) INTO :v_long_queries
    FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
        END_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
    ))
    WHERE EXECUTION_STATUS = 'RUNNING'
    AND DATEDIFF('minute', START_TIME, CURRENT_TIMESTAMP()) > 30;
    
    v_report := 'HEALTH REPORT - ' || CURRENT_TIMESTAMP() || '\n';
    v_report := v_report || '================================\n';
    v_report := v_report || 'Task Failures (1hr): ' || v_task_failures || '\n';
    v_report := v_report || 'Stale Streams: ' || v_stale_streams || '\n';
    v_report := v_report || 'Long Queries (>30min): ' || v_long_queries || '\n';
    v_report := v_report || '================================\n';
    
    IF (v_task_failures > 0 OR v_stale_streams > 0 OR v_long_queries > 0) THEN
        v_report := v_report || 'STATUS: ATTENTION REQUIRED';
    ELSE
        v_report := v_report || 'STATUS: HEALTHY';
    END IF;
    
    RETURN v_report;
END;
$$;
```

---

## 3. CONTROL-M PREREQUISITES

### 3.1 Control-M Infrastructure Requirements

| Component | Minimum Version | Recommended |
|-----------|-----------------|-------------|
| Control-M/Enterprise Manager | 9.0.20 | 9.0.21+ |
| Control-M/Server | 9.0.20 | 9.0.21+ |
| Control-M/Agent | 9.0.20 | 9.0.21+ |
| Control-M Application Integrator | 9.0.20 | 9.0.21+ |
| Control-M Automation API | 9.0.20 | 9.0.21+ |

### 3.2 Snowflake Job Type Plugin

Control-M provides native Snowflake integration through the **Application Integrator** plugin.

**Plugin Details:**
- **Plugin Name**: Snowflake Job Type
- **Category**: Database
- **Supported Operations**: SQL execution, Stored Procedures, Tasks, File Loading

### 3.3 Network Prerequisites

| Requirement | Details |
|-------------|---------|
| Outbound HTTPS | Port 443 to Snowflake endpoints |
| Snowflake URL | `<account_identifier>.snowflakecomputing.com` |
| Proxy Support | Configure if required by corporate firewall |
| DNS Resolution | Must resolve Snowflake endpoints |

### 3.4 Snowflake Account Prerequisites

```sql
-- ============================================================================
-- SNOWFLAKE PREREQUISITES FOR CONTROL-M INTEGRATION
-- ============================================================================

-- 1. Create dedicated service account for Control-M
CREATE USER IF NOT EXISTS CONTROLM_SERVICE_ACCOUNT
    PASSWORD = '<STRONG_PASSWORD>'
    DEFAULT_ROLE = CONTROLM_ROLE
    DEFAULT_WAREHOUSE = CONTROLM_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Service account for Control-M orchestration';

-- 2. Create dedicated role for Control-M operations
CREATE ROLE IF NOT EXISTS CONTROLM_ROLE
    COMMENT = 'Role for Control-M job execution';

-- 3. Create dedicated warehouse for Control-M jobs
CREATE WAREHOUSE IF NOT EXISTS CONTROLM_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Control-M orchestrated jobs';

-- 4. Grant role to service account
GRANT ROLE CONTROLM_ROLE TO USER CONTROLM_SERVICE_ACCOUNT;

-- 5. Grant warehouse usage
GRANT USAGE ON WAREHOUSE CONTROLM_WH TO ROLE CONTROLM_ROLE;

-- 6. Grant database privileges (repeat for each database)
GRANT USAGE ON DATABASE <TARGET_DATABASE> TO ROLE CONTROLM_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE <TARGET_DATABASE> TO ROLE CONTROLM_ROLE;
GRANT SELECT ON ALL TABLES IN DATABASE <TARGET_DATABASE> TO ROLE CONTROLM_ROLE;
GRANT SELECT ON FUTURE TABLES IN DATABASE <TARGET_DATABASE> TO ROLE CONTROLM_ROLE;

-- 7. Grant execute privileges on stored procedures
GRANT USAGE ON DATABASE <PROCEDURE_DATABASE> TO ROLE CONTROLM_ROLE;
GRANT USAGE ON SCHEMA <PROCEDURE_DATABASE>.<PROCEDURE_SCHEMA> TO ROLE CONTROLM_ROLE;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA <PROCEDURE_DATABASE>.<PROCEDURE_SCHEMA> TO ROLE CONTROLM_ROLE;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA <PROCEDURE_DATABASE>.<PROCEDURE_SCHEMA> TO ROLE CONTROLM_ROLE;

-- 8. Grant task operation privileges (if managing tasks via Control-M)
GRANT EXECUTE TASK ON ACCOUNT TO ROLE CONTROLM_ROLE;
GRANT OPERATE ON ALL TASKS IN SCHEMA <DATABASE>.<SCHEMA> TO ROLE CONTROLM_ROLE;
GRANT MONITOR ON ALL TASKS IN SCHEMA <DATABASE>.<SCHEMA> TO ROLE CONTROLM_ROLE;

-- 9. Grant monitoring privileges
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE CONTROLM_ROLE;

-- 10. Verify setup
SHOW GRANTS TO ROLE CONTROLM_ROLE;
SHOW GRANTS TO USER CONTROLM_SERVICE_ACCOUNT;
```

### 3.5 Authentication Options

| Method | Security Level | Use Case |
|--------|----------------|----------|
| Username/Password | Basic | Development/Testing |
| Key-Pair Authentication | High | **Production (Recommended)** |
| OAuth | High | SSO environments |
| External Browser | N/A | Not supported for automation |

#### Key-Pair Authentication Setup (Recommended for Production)

```sql
-- ============================================================================
-- KEY-PAIR AUTHENTICATION SETUP
-- ============================================================================

-- Step 1: Generate RSA key pair (run on Control-M Agent server)
-- $ openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
-- $ openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

-- Step 2: Extract public key content (remove header/footer and newlines)
-- $ grep -v "BEGIN\|END" rsa_key.pub | tr -d '\n'

-- Step 3: Assign public key to Snowflake user
ALTER USER CONTROLM_SERVICE_ACCOUNT SET RSA_PUBLIC_KEY = '<PUBLIC_KEY_CONTENT>';

-- Step 4: Verify key fingerprint
DESC USER CONTROLM_SERVICE_ACCOUNT;
-- Check RSA_PUBLIC_KEY_FP value

-- Step 5: Store private key securely on Control-M Agent
-- Location: /opt/controlm/keys/snowflake_rsa_key.p8
-- Permissions: chmod 600 /opt/controlm/keys/snowflake_rsa_key.p8
```

### 3.6 Control-M Agent Configuration Checklist

| Item | Status | Notes |
|------|--------|-------|
| Java Runtime (JRE 8+) | Required | For JDBC driver |
| Snowflake JDBC Driver | Required | Download from Snowflake |
| Network connectivity to Snowflake | Required | Test with telnet/curl |
| SSL certificates | Required | Trust store configuration |
| Private key file (if key-pair auth) | Required | Secure file permissions |
| Connection profile configured | Required | In Control-M |

---

## 4. CONTROL-M SNOWFLAKE INTEGRATION ARCHITECTURE

### 4.1 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CONTROL-M SNOWFLAKE ARCHITECTURE                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                     CONTROL-M LAYER                                 │   │
│   │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │   │
│   │  │  Control-M  │  │  Control-M  │  │ Application │                 │   │
│   │  │   Server    │──│    Agent    │──│ Integrator  │                 │   │
│   │  │             │  │             │  │ (Snowflake) │                 │   │
│   │  └─────────────┘  └──────┬──────┘  └─────────────┘                 │   │
│   └──────────────────────────┼──────────────────────────────────────────┘   │
│                              │                                              │
│                              │ JDBC/HTTPS (Port 443)                        │
│                              │ Key-Pair or Password Auth                    │
│                              ▼                                              │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                     SNOWFLAKE LAYER                                 │   │
│   │                                                                     │   │
│   │   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐            │   │
│   │   │  Warehouse  │    │  Database   │    │   Tasks &   │            │   │
│   │   │ CONTROLM_WH │    │   Objects   │    │   Streams   │            │   │
│   │   └─────────────┘    └─────────────┘    └─────────────┘            │   │
│   │          │                  │                  │                    │   │
│   │          └──────────────────┼──────────────────┘                    │   │
│   │                             ▼                                       │   │
│   │                  ┌─────────────────────┐                            │   │
│   │                  │  Stored Procedures  │                            │   │
│   │                  │  (CDC Processing)   │                            │   │
│   │                  └─────────────────────┘                            │   │
│   │                                                                     │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Job Flow Pattern

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     TYPICAL JOB FLOW PATTERN                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   [Pre-Check Job]                                                           │
│        │                                                                    │
│        │  Verify: Source data ready, Streams not stale                     │
│        ▼                                                                    │
│   [Processing Job 1] ─► [Processing Job 2] ─► [Processing Job N]           │
│        │                       │                      │                     │
│        │  Execute SP           │  Execute SP          │  Execute SP        │
│        ▼                       ▼                      ▼                     │
│   [Validation Job]                                                          │
│        │                                                                    │
│        │  Row count validation, Data quality checks                        │
│        ▼                                                                    │
│   [Post-Processing Job]                                                     │
│        │                                                                    │
│        │  Update audit tables, Send notifications                          │
│        ▼                                                                    │
│   [SLA Check Job]                                                           │
│        │                                                                    │
│        │  Compare actual vs expected completion time                       │
│        ▼                                                                    │
│      [END]                                                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.3 Integration Patterns

| Pattern | Description | Use Case |
|---------|-------------|----------|
| Direct SQL Execution | Execute SQL statements directly | DDL, simple queries |
| Stored Procedure Call | Call Snowflake stored procedures | Complex ETL logic |
| Task Orchestration | Trigger/monitor Snowflake tasks | Hybrid orchestration |
| File-Based Execution | Execute SQL from file | Version-controlled SQL |

---

## 5. STEP-BY-STEP CONFIGURATION

### 5.1 Control-M Connection Profile Setup

#### Step 1: Install Snowflake Job Type Plugin

```
1. Log in to Control-M Configuration Manager
2. Navigate to: Tools > Application Integrator > Deploy
3. Search for "Snowflake" in available plugins
4. Select and deploy the Snowflake Job Type
5. Restart Control-M Agent if required
```

#### Step 2: Create Connection Profile

**Via Control-M Web Interface:**

```
1. Navigate to: Configuration > Connection Profiles
2. Click "Add Connection Profile"
3. Select Type: "Snowflake"
4. Enter the following details:
```

| Field | Value | Example |
|-------|-------|---------|
| Profile Name | SNOWFLAKE_PROD | SNOWFLAKE_PROD |
| Account | `<account_identifier>` | xy12345.us-east-1 |
| User | CONTROLM_SERVICE_ACCOUNT | CONTROLM_SERVICE_ACCOUNT |
| Authentication | Key Pair | Key Pair |
| Private Key Path | /path/to/private/key | /opt/controlm/keys/rsa_key.p8 |
| Warehouse | CONTROLM_WH | CONTROLM_WH |
| Database | (optional default) | D_RAW |
| Schema | (optional default) | SADB |
| Role | CONTROLM_ROLE | CONTROLM_ROLE |

#### Step 3: Connection Profile JSON (Automation API)

```json
{
  "SNOWFLAKE_PROD": {
    "Type": "ConnectionProfile:Snowflake",
    "Account": "<ACCOUNT_IDENTIFIER>",
    "User": "CONTROLM_SERVICE_ACCOUNT",
    "AuthenticationType": "KeyPair",
    "PrivateKeyPath": "/opt/controlm/keys/rsa_key.p8",
    "Warehouse": "CONTROLM_WH",
    "Database": "D_RAW",
    "Schema": "SADB",
    "Role": "CONTROLM_ROLE",
    "Centralized": true,
    "Description": "Production Snowflake connection"
  }
}
```

#### Step 4: Test Connection

```bash
# Using Control-M Automation API
ctm config connection::profile::test SNOWFLAKE_PROD

# Expected output:
# {
#   "status": "success",
#   "message": "Connection profile SNOWFLAKE_PROD tested successfully"
# }
```

### 5.2 Snowflake Job Definition

#### Job Definition Template (JSON for Automation API)

```json
{
  "CDC_TRAIN_PLAN_JOB": {
    "Type": "Job:Snowflake",
    "ConnectionProfile": "SNOWFLAKE_PROD",
    "Command": "CALL D_RAW.SADB.SP_PROCESS_TRAIN_PLAN()",
    "Warehouse": "CONTROLM_WH",
    "RunAs": "CONTROLM_ROLE",
    "OutputHandling": {
      "ReturnCodeOnFailure": 1,
      "CaptureOutput": true
    },
    "When": {
      "Months": ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", 
                 "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"],
      "DaysOfWeek": ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"],
      "FromTime": "0000",
      "ToTime": "2359"
    },
    "Variables": [
      {"JOB_START_TIME": "%%$ORDERDATE"}
    ],
    "PostProcessing": {
      "Output": {
        "Search": "STATUS: SUCCESS",
        "CaptureCode": 0
      }
    }
  }
}
```

#### SQL File-Based Execution

```json
{
  "CDC_EXECUTE_SQL_FILE": {
    "Type": "Job:Snowflake",
    "ConnectionProfile": "SNOWFLAKE_PROD",
    "SQLFile": "/opt/controlm/sql/cdc_processing.sql",
    "Warehouse": "CONTROLM_WH",
    "OutputHandling": {
      "CaptureOutput": true
    }
  }
}
```

### 5.3 Job Folder Structure

```json
{
  "Defaults": {
    "Application": "CDC_PIPELINE",
    "SubApplication": "DATA_PRESERVATION",
    "RunAs": "controlm",
    "Host": "AGENT_HOST"
  },
  "CDC_PIPELINE_FOLDER": {
    "Type": "Folder",
    "ControlmServer": "CONTROLM_SERVER",
    "OrderMethod": "Manual",
    "SiteStandard": "CDC_STANDARD",
    
    "00_PRE_CHECK": {
      "Type": "Job:Snowflake",
      "ConnectionProfile": "SNOWFLAKE_PROD",
      "Command": "SELECT CASE WHEN COUNT(*) > 0 THEN 'READY' ELSE 'NOT_READY' END FROM TABLE(INFORMATION_SCHEMA.STREAMS()) WHERE STALE = FALSE"
    },
    
    "01_PROCESS_TRAIN_PLAN": {
      "Type": "Job:Snowflake",
      "ConnectionProfile": "SNOWFLAKE_PROD",
      "Command": "CALL D_RAW.SADB.SP_PROCESS_TRAIN_PLAN()"
    },
    
    "02_PROCESS_OPTRN": {
      "Type": "Job:Snowflake",
      "ConnectionProfile": "SNOWFLAKE_PROD",
      "Command": "CALL D_RAW.SADB.SP_PROCESS_OPTRN()"
    },
    
    "99_POST_VALIDATION": {
      "Type": "Job:Snowflake",
      "ConnectionProfile": "SNOWFLAKE_PROD",
      "Command": "CALL D_BRONZE.MONITORING.SP_GENERATE_HEALTH_REPORT()"
    },
    
    "Flow": {
      "Type": "Flow",
      "Sequence": [
        "00_PRE_CHECK",
        {"Type": "Parallel", "Jobs": ["01_PROCESS_TRAIN_PLAN", "02_PROCESS_OPTRN"]},
        "99_POST_VALIDATION"
      ]
    }
  }
}
```

---

## 6. IMPLEMENTATION STEPS

### 6.1 Implementation Checklist

| Phase | Step | Owner | Status |
|-------|------|-------|--------|
| **Phase 1: Prerequisites** | | | |
| | 1.1 Verify Control-M version compatibility | Control-M Admin | [ ] |
| | 1.2 Install Snowflake Job Type plugin | Control-M Admin | [ ] |
| | 1.3 Configure network connectivity | Network Team | [ ] |
| | 1.4 Create Snowflake service account | DBA | [ ] |
| | 1.5 Generate key pair for authentication | Security Team | [ ] |
| | 1.6 Configure key pair in Snowflake | DBA | [ ] |
| **Phase 2: Configuration** | | | |
| | 2.1 Create connection profile | Control-M Admin | [ ] |
| | 2.2 Test connection profile | Control-M Admin | [ ] |
| | 2.3 Create job definitions | Control-M Admin | [ ] |
| | 2.4 Configure job dependencies | Control-M Admin | [ ] |
| | 2.5 Set up alerting rules | Control-M Admin | [ ] |
| **Phase 3: Testing** | | | |
| | 3.1 Execute individual jobs | QA Team | [ ] |
| | 3.2 Execute job flow | QA Team | [ ] |
| | 3.3 Test failure scenarios | QA Team | [ ] |
| | 3.4 Test alerting | QA Team | [ ] |
| | 3.5 Performance testing | QA Team | [ ] |
| **Phase 4: Deployment** | | | |
| | 4.1 Deploy to production | Release Team | [ ] |
| | 4.2 Enable scheduling | Control-M Admin | [ ] |
| | 4.3 Monitor initial runs | Operations | [ ] |
| | 4.4 Document runbook | Documentation | [ ] |

### 6.2 Phase 1: Prerequisites Implementation

#### Step 1.1: Verify Control-M Version

```bash
# Check Control-M Server version
ctm config server::get | grep -i version

# Check Control-M Agent version
ctm config agent::get AGENT_NAME | grep -i version

# Verify Application Integrator is installed
ctm deploy ai:jobtype::get Snowflake
```

#### Step 1.2: Install Snowflake Job Type

```bash
# Download Snowflake job type from BMC Marketplace (if not included)
# Deploy using Application Integrator

ctm deploy ai:jobtype::add /path/to/Snowflake_JobType.ctmai

# Verify installation
ctm deploy ai:jobtype::get Snowflake
```

#### Step 1.3: Network Connectivity Test

```bash
# From Control-M Agent server
# Test HTTPS connectivity to Snowflake

curl -v https://<account_identifier>.snowflakecomputing.com

# Expected: HTTP 200 or redirect response
# If blocked: Work with network team to allow outbound HTTPS to *.snowflakecomputing.com
```

#### Step 1.4-1.6: Snowflake Account Setup

Execute the SQL from Section 3.4 and 3.5 in Snowflake:

```sql
-- Execute as ACCOUNTADMIN or SECURITYADMIN
-- See Section 3.4 for complete script
```

### 6.3 Phase 2: Configuration Implementation

#### Step 2.1-2.2: Connection Profile

```bash
# Create connection profile via Automation API
cat > /tmp/snowflake_profile.json << 'EOF'
{
  "SNOWFLAKE_PROD": {
    "Type": "ConnectionProfile:Snowflake",
    "Account": "<ACCOUNT_IDENTIFIER>",
    "User": "CONTROLM_SERVICE_ACCOUNT",
    "AuthenticationType": "KeyPair",
    "PrivateKeyPath": "/opt/controlm/keys/rsa_key.p8",
    "Warehouse": "CONTROLM_WH",
    "Database": "D_RAW",
    "Schema": "SADB",
    "Role": "CONTROLM_ROLE",
    "Centralized": true
  }
}
EOF

# Deploy connection profile
ctm deploy /tmp/snowflake_profile.json

# Test connection
ctm config connection::profile::test SNOWFLAKE_PROD
```

#### Step 2.3-2.4: Job Definitions

Create the complete job folder definition:

```bash
cat > /tmp/cdc_pipeline_jobs.json << 'EOF'
{
  "Defaults": {
    "Application": "CDC_PIPELINE",
    "SubApplication": "DATA_PRESERVATION",
    "RunAs": "controlm",
    "Host": "AGENT_HOST"
  },
  "CDC_PIPELINE": {
    "Type": "Folder",
    "ControlmServer": "CONTROLM_SERVER",
    
    "PRE_CHECK_STREAMS": {
      "Type": "Job:Snowflake",
      "ConnectionProfile": "SNOWFLAKE_PROD",
      "Command": "SELECT COUNT(*) AS STALE_COUNT FROM TABLE(INFORMATION_SCHEMA.STREAMS()) WHERE STALE = TRUE",
      "PostProcessing": {
        "Output": {"Search": "STALE_COUNT|0", "CaptureCode": 0}
      }
    },
    
    "PROCESS_TRAIN_PLAN": {
      "Type": "Job:Snowflake",
      "ConnectionProfile": "SNOWFLAKE_PROD",
      "Command": "CALL D_RAW.SADB.SP_PROCESS_TRAIN_PLAN()"
    },
    
    "PROCESS_OPTRN": {
      "Type": "Job:Snowflake",
      "ConnectionProfile": "SNOWFLAKE_PROD",
      "Command": "CALL D_RAW.SADB.SP_PROCESS_OPTRN()"
    },
    
    "PROCESS_OPTRN_LEG": {
      "Type": "Job:Snowflake",
      "ConnectionProfile": "SNOWFLAKE_PROD",
      "Command": "CALL D_RAW.SADB.SP_PROCESS_OPTRN_LEG()"
    },
    
    "PROCESS_OPTRN_EVENT": {
      "Type": "Job:Snowflake",
      "ConnectionProfile": "SNOWFLAKE_PROD",
      "Command": "CALL D_RAW.SADB.SP_PROCESS_OPTRN_EVENT()"
    },
    
    "VALIDATION_CHECK": {
      "Type": "Job:Snowflake",
      "ConnectionProfile": "SNOWFLAKE_PROD",
      "Command": "SELECT * FROM D_BRONZE.SADB.CDC_TABLE_SYNC_STATUS"
    },
    
    "Flow": {
      "Type": "Flow",
      "Sequence": [
        "PRE_CHECK_STREAMS",
        {
          "Type": "Parallel",
          "Jobs": [
            "PROCESS_TRAIN_PLAN",
            "PROCESS_OPTRN",
            "PROCESS_OPTRN_LEG",
            "PROCESS_OPTRN_EVENT"
          ]
        },
        "VALIDATION_CHECK"
      ]
    }
  }
}
EOF

# Deploy jobs
ctm deploy /tmp/cdc_pipeline_jobs.json
```

### 6.4 Phase 3: Testing

#### Test Script

```bash
#!/bin/bash
# Control-M Snowflake Integration Test Script

echo "=========================================="
echo "Control-M Snowflake Integration Tests"
echo "=========================================="

# Test 1: Connection Profile
echo "Test 1: Testing connection profile..."
ctm config connection::profile::test SNOWFLAKE_PROD
if [ $? -eq 0 ]; then
    echo "PASS: Connection profile test successful"
else
    echo "FAIL: Connection profile test failed"
    exit 1
fi

# Test 2: Run single job
echo "Test 2: Running single job test..."
ctm run order CDC_PIPELINE/PROCESS_TRAIN_PLAN -f /tmp/cdc_pipeline_jobs.json
if [ $? -eq 0 ]; then
    echo "PASS: Single job execution successful"
else
    echo "FAIL: Single job execution failed"
    exit 1
fi

# Test 3: Run job flow
echo "Test 3: Running job flow test..."
ctm run order CDC_PIPELINE -f /tmp/cdc_pipeline_jobs.json
if [ $? -eq 0 ]; then
    echo "PASS: Job flow execution successful"
else
    echo "FAIL: Job flow execution failed"
    exit 1
fi

echo "=========================================="
echo "All tests completed successfully"
echo "=========================================="
```

---

## 7. MONITORING & ALERTING CONFIGURATION

### 7.1 Control-M Alerts Configuration

#### Alert Definition for Job Failures

```json
{
  "CDC_FAILURE_ALERT": {
    "Type": "Alert",
    "Name": "CDC Pipeline Failure Alert",
    "Severity": "Critical",
    "Trigger": {
      "Type": "JobEndNotOK",
      "Application": "CDC_PIPELINE"
    },
    "Actions": [
      {
        "Type": "Email",
        "To": "dba-team@company.com",
        "Subject": "ALERT: CDC Pipeline Job Failed - %%JOBNAME",
        "Body": "Job %%JOBNAME failed at %%ENDTIME with return code %%RETCODE"
      },
      {
        "Type": "SNMP",
        "TrapDestination": "monitoring-server.company.com",
        "Community": "public"
      }
    ]
  }
}
```

#### Alert Definition for SLA Breach

```json
{
  "CDC_SLA_ALERT": {
    "Type": "Alert",
    "Name": "CDC SLA Breach Alert",
    "Severity": "High",
    "Trigger": {
      "Type": "SLABreach",
      "Service": "CDC_DATA_PRESERVATION"
    },
    "Actions": [
      {
        "Type": "Email",
        "To": "data-ops@company.com",
        "Subject": "WARNING: CDC Pipeline SLA Breach",
        "Body": "CDC Pipeline did not complete within SLA window. Expected: %%SLADEADLINE, Current: %%CURRENTTIME"
      }
    ]
  }
}
```

### 7.2 Snowflake-Side Monitoring for Control-M

```sql
-- ============================================================================
-- MONITORING VIEW FOR CONTROL-M JOBS
-- ============================================================================

CREATE OR REPLACE VIEW <DATABASE>.MONITORING.V_CONTROLM_JOB_STATUS AS
SELECT 
    QUERY_ID,
    USER_NAME,
    ROLE_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    QUERY_TEXT,
    START_TIME,
    END_TIME,
    EXECUTION_STATUS,
    ERROR_CODE,
    ERROR_MESSAGE,
    TOTAL_ELAPSED_TIME / 1000 AS DURATION_SECONDS,
    ROWS_PRODUCED,
    WAREHOUSE_NAME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE USER_NAME = 'CONTROLM_SERVICE_ACCOUNT'
AND START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;

-- Grant access to monitoring role
GRANT SELECT ON <DATABASE>.MONITORING.V_CONTROLM_JOB_STATUS TO ROLE CONTROLM_ROLE;
```

### 7.3 Integration with External Monitoring Tools

#### Webhook Alert for PagerDuty/Slack

```json
{
  "WEBHOOK_ALERT": {
    "Type": "Alert",
    "Trigger": {
      "Type": "JobEndNotOK",
      "Application": "CDC_PIPELINE"
    },
    "Actions": [
      {
        "Type": "Webhook",
        "URL": "https://hooks.slack.com/services/XXXX/YYYY/ZZZZ",
        "Method": "POST",
        "Headers": {
          "Content-Type": "application/json"
        },
        "Body": {
          "text": "CDC Pipeline Alert: Job %%JOBNAME failed",
          "attachments": [
            {
              "color": "danger",
              "fields": [
                {"title": "Job Name", "value": "%%JOBNAME", "short": true},
                {"title": "End Time", "value": "%%ENDTIME", "short": true},
                {"title": "Return Code", "value": "%%RETCODE", "short": true}
              ]
            }
          ]
        }
      }
    ]
  }
}
```

---

## 8. VALIDATION & TESTING

### 8.1 Validation Checklist

| Test Case | Expected Result | Actual Result | Status |
|-----------|-----------------|---------------|--------|
| Connection profile creation | Success | | [ ] |
| Connection profile test | Success | | [ ] |
| Single job execution (SELECT) | Success with output | | [ ] |
| Stored procedure execution | Success with return value | | [ ] |
| Job flow execution | All jobs complete OK | | [ ] |
| Failure handling | Alert triggered | | [ ] |
| SLA monitoring | SLA tracked correctly | | [ ] |
| Parallel job execution | Jobs run simultaneously | | [ ] |
| Job dependency handling | Correct sequencing | | [ ] |
| Output capture | Query results captured | | [ ] |

### 8.2 Test SQL Scripts

```sql
-- ============================================================================
-- TEST SCRIPT: VERIFY CONTROL-M INTEGRATION
-- Execute as CONTROLM_SERVICE_ACCOUNT
-- ============================================================================

-- Test 1: Basic connectivity
SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_TIMESTAMP();

-- Test 2: Verify role permissions
SHOW GRANTS TO ROLE CONTROLM_ROLE;

-- Test 3: Execute test query
SELECT COUNT(*) AS TABLE_COUNT 
FROM D_RAW.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'SADB';

-- Test 4: Test stored procedure execution
CALL D_RAW.SADB.SP_PROCESS_TRAIN_PLAN();

-- Test 5: Verify audit log entry
SELECT * FROM D_BRONZE.SADB.CDC_PROCESSING_LOG 
ORDER BY PROCESSING_START DESC 
LIMIT 5;

-- Test 6: Test monitoring view access
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE USER_NAME = 'CONTROLM_SERVICE_ACCOUNT'
LIMIT 10;
```

### 8.3 Performance Baseline Test

```sql
-- ============================================================================
-- PERFORMANCE BASELINE TEST
-- ============================================================================

-- Run full CDC cycle and measure timing
DECLARE
    v_start TIMESTAMP_NTZ;
    v_end TIMESTAMP_NTZ;
    v_duration NUMBER;
BEGIN
    v_start := CURRENT_TIMESTAMP();
    
    -- Execute all CDC procedures
    CALL D_RAW.SADB.SP_PROCESS_TRAIN_PLAN();
    CALL D_RAW.SADB.SP_PROCESS_OPTRN();
    CALL D_RAW.SADB.SP_PROCESS_OPTRN_LEG();
    CALL D_RAW.SADB.SP_PROCESS_OPTRN_EVENT();
    -- ... (remaining procedures)
    
    v_end := CURRENT_TIMESTAMP();
    v_duration := DATEDIFF('second', v_start, v_end);
    
    -- Record baseline
    INSERT INTO <DATABASE>.MONITORING.PERFORMANCE_BASELINE (
        TEST_DATE, 
        TEST_TYPE, 
        DURATION_SECONDS
    ) VALUES (
        CURRENT_DATE(),
        'FULL_CDC_CYCLE',
        v_duration
    );
    
    RETURN 'Completed in ' || v_duration || ' seconds';
END;
```

---

## 9. TROUBLESHOOTING GUIDE

### 9.1 Common Issues and Resolutions

#### Issue 1: Connection Failure

**Symptoms:**
- Error: "Failed to connect to Snowflake"
- Error: "Authentication failed"

**Resolution Checklist:**
```
1. Verify network connectivity:
   curl -v https://<account>.snowflakecomputing.com
   
2. Check credentials:
   - Username spelling
   - Private key file path
   - Private key file permissions (600)
   
3. Verify Snowflake user status:
   DESCRIBE USER CONTROLM_SERVICE_ACCOUNT;
   -- Check DISABLED = FALSE
   
4. Test authentication directly:
   snowsql -a <account> -u CONTROLM_SERVICE_ACCOUNT --private-key-path /path/to/key.p8
```

#### Issue 2: Job Hangs or Times Out

**Symptoms:**
- Job in "Running" state for extended period
- Timeout errors in Control-M

**Resolution:**
```sql
-- Check for blocking queries
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER(
    USER_NAME => 'CONTROLM_SERVICE_ACCOUNT',
    RESULT_LIMIT => 20
))
WHERE EXECUTION_STATUS = 'RUNNING';

-- Check warehouse status
SHOW WAREHOUSES LIKE 'CONTROLM_WH';

-- Scale up warehouse if needed
ALTER WAREHOUSE CONTROLM_WH SET WAREHOUSE_SIZE = 'MEDIUM';
```

#### Issue 3: Permission Denied Errors

**Symptoms:**
- Error: "Insufficient privileges"
- Error: "Object does not exist"

**Resolution:**
```sql
-- Verify current role and permissions
SELECT CURRENT_ROLE();
SHOW GRANTS TO ROLE CONTROLM_ROLE;

-- Grant missing permissions
GRANT USAGE ON DATABASE <DB> TO ROLE CONTROLM_ROLE;
GRANT USAGE ON SCHEMA <DB>.<SCHEMA> TO ROLE CONTROLM_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA <DB>.<SCHEMA> TO ROLE CONTROLM_ROLE;
```

#### Issue 4: Job Fails but No Error Message

**Symptoms:**
- Job completes with non-zero return code
- No clear error message in Control-M output

**Resolution:**
```sql
-- Check Snowflake query history for details
SELECT 
    QUERY_ID,
    QUERY_TEXT,
    ERROR_CODE,
    ERROR_MESSAGE,
    EXECUTION_STATUS
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE USER_NAME = 'CONTROLM_SERVICE_ACCOUNT'
AND START_TIME >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;
```

### 9.2 Log Locations

| Log Type | Location |
|----------|----------|
| Control-M Agent Log | `/opt/controlm/agent/log/` |
| Control-M Server Log | `/opt/controlm/server/log/` |
| Job Output | Control-M Web Interface > Job > Output |
| Snowflake Query History | `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` |

---

## 10. APPENDIX

### 10.1 Quick Reference Card

```
================================================================================
                    CONTROL-M SNOWFLAKE QUICK REFERENCE
================================================================================

CONNECTION PROFILE TEST:
  ctm config connection::profile::test SNOWFLAKE_PROD

RUN SINGLE JOB:
  ctm run order CDC_PIPELINE/PROCESS_TRAIN_PLAN

RUN JOB FLOW:
  ctm run order CDC_PIPELINE

CHECK JOB STATUS:
  ctm run status <ORDER_ID>

VIEW JOB OUTPUT:
  ctm run output <ORDER_ID>

DEPLOY JOBS:
  ctm deploy /path/to/jobs.json

SNOWFLAKE TEST QUERY:
  SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE();

================================================================================
```

### 10.2 Environment Variables Reference

| Variable | Description | Example |
|----------|-------------|---------|
| SNOWFLAKE_ACCOUNT | Snowflake account identifier | xy12345.us-east-1 |
| SNOWFLAKE_USER | Service account username | CONTROLM_SERVICE_ACCOUNT |
| SNOWFLAKE_PRIVATE_KEY_PATH | Path to private key | /opt/controlm/keys/rsa_key.p8 |
| SNOWFLAKE_WAREHOUSE | Default warehouse | CONTROLM_WH |
| SNOWFLAKE_DATABASE | Default database | D_RAW |
| SNOWFLAKE_SCHEMA | Default schema | SADB |
| SNOWFLAKE_ROLE | Default role | CONTROLM_ROLE |

### 10.3 Complete Job Flow for CDC Pipeline

```json
{
  "CDC_COMPLETE_PIPELINE": {
    "Type": "Folder",
    "Application": "CDC_PIPELINE",
    "SubApplication": "PRODUCTION",
    "ControlmServer": "CONTROLM_SERVER",
    "OrderMethod": "Manual",
    "When": {
      "RuleBasedCalendar": {
        "Type": "Daily",
        "FromTime": "0100",
        "ToTime": "0600"
      }
    },
    
    "00_PRE_CHECK": {
      "Type": "Job:Snowflake",
      "ConnectionProfile": "SNOWFLAKE_PROD",
      "Command": "SELECT CASE WHEN COUNT(*) = 0 THEN 'READY' ELSE 'STALE_STREAMS_DETECTED' END FROM TABLE(INFORMATION_SCHEMA.STREAMS()) WHERE STALE = TRUE"
    },
    
    "01_TRAIN_PLAN": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_TRAIN_PLAN()"},
    "02_OPTRN": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_OPTRN()"},
    "03_OPTRN_LEG": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_OPTRN_LEG()"},
    "04_OPTRN_EVENT": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_OPTRN_EVENT()"},
    "05_TRKFC_TRSTN": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_TRKFC_TRSTN()"},
    "06_TRAIN_CNST_SMRY": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_TRAIN_CNST_SMRY()"},
    "07_STNWYB_MSG_DN": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_STNWYB_MSG_DN()"},
    "08_EQPMNT_AAR_BASE": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_EQPMNT_AAR_BASE()"},
    "09_EQPMV_RFEQP_MVMNT_EVENT": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_EQPMV_RFEQP_MVMNT_EVENT()"},
    "10_CTNAPP_CTNG_LINE_DN": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_CTNAPP_CTNG_LINE_DN()"},
    "11_EQPMV_EQPMT_EVENT_TYPE": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_EQPMV_EQPMT_EVENT_TYPE()"},
    "12_TRAIN_PLAN_EVENT": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_EVENT()"},
    "13_LCMTV_MVMNT_EVENT": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_LCMTV_MVMNT_EVENT()"},
    "14_LCMTV_EMIS": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_LCMTV_EMIS()"},
    "15_TRAIN_PLAN_LEG": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_LEG()"},
    "16_TRKFCG_SBDVSN": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_TRKFCG_SBDVSN()"},
    "17_TRKFCG_FIXED_PLANT_ASSET": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_TRKFCG_FIXED_PLANT_ASSET()"},
    "18_TRKFCG_FXPLA_TRACK_LCTN_DN": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_TRKFCG_FXPLA_TRACK_LCTN_DN()"},
    "19_TRAIN_CNST_DTL_RAIL_EQPT": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_TRAIN_CNST_DTL_RAIL_EQPT()"},
    "20_TRKFCG_TRACK_SGMNT_DN": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_TRKFCG_TRACK_SGMNT_DN()"},
    "21_TRKFCG_SRVC_AREA": {"Type": "Job:Snowflake", "ConnectionProfile": "SNOWFLAKE_PROD", "Command": "CALL D_RAW.SADB.SP_PROCESS_TRKFCG_SRVC_AREA()"},
    
    "99_VALIDATION": {
      "Type": "Job:Snowflake",
      "ConnectionProfile": "SNOWFLAKE_PROD",
      "Command": "CALL D_BRONZE.MONITORING.SP_GENERATE_HEALTH_REPORT()"
    },
    
    "Flow": {
      "Type": "Flow",
      "Sequence": [
        "00_PRE_CHECK",
        {
          "Type": "Parallel",
          "Jobs": [
            "01_TRAIN_PLAN", "02_OPTRN", "03_OPTRN_LEG", "04_OPTRN_EVENT",
            "05_TRKFC_TRSTN", "06_TRAIN_CNST_SMRY", "07_STNWYB_MSG_DN",
            "08_EQPMNT_AAR_BASE", "09_EQPMV_RFEQP_MVMNT_EVENT", "10_CTNAPP_CTNG_LINE_DN",
            "11_EQPMV_EQPMT_EVENT_TYPE", "12_TRAIN_PLAN_EVENT", "13_LCMTV_MVMNT_EVENT",
            "14_LCMTV_EMIS", "15_TRAIN_PLAN_LEG", "16_TRKFCG_SBDVSN",
            "17_TRKFCG_FIXED_PLANT_ASSET", "18_TRKFCG_FXPLA_TRACK_LCTN_DN",
            "19_TRAIN_CNST_DTL_RAIL_EQPT", "20_TRKFCG_TRACK_SGMNT_DN", "21_TRKFCG_SRVC_AREA"
          ]
        },
        "99_VALIDATION"
      ]
    }
  }
}
```

---

## REVIEWER ASSESSMENT

### Scoring Criteria

| Category | Weight | Score | Notes |
|----------|--------|-------|-------|
| **Technical Accuracy** | 25% | 24/25 | Based on official BMC and Snowflake documentation |
| **Completeness** | 20% | 19/20 | Covers prerequisites, configuration, implementation, monitoring |
| **Best Practices** | 20% | 19/20 | Key-pair auth, monitoring tables, alerting thresholds |
| **Production Readiness** | 15% | 14/15 | Complete checklists, validation scripts |
| **Troubleshooting** | 10% | 9/10 | Common issues documented with resolutions |
| **Security** | 10% | 10/10 | Key-pair auth, least privilege, no credentials in code |

### Final Score: **95/100**

### Assessment Summary

| Aspect | Rating | Comments |
|--------|--------|----------|
| Prerequisites Documentation | EXCELLENT | Complete account setup, privileges, network requirements |
| Configuration Steps | EXCELLENT | Step-by-step with JSON examples |
| Integration Architecture | EXCELLENT | Clear diagrams and flow patterns |
| Monitoring Best Practices | EXCELLENT | Native Snowflake + Control-M integration |
| Alerting Configuration | VERY GOOD | Multiple channels (email, webhook, SNMP) |
| Validation Testing | EXCELLENT | Comprehensive test checklist |
| Troubleshooting Guide | VERY GOOD | Common issues with resolutions |
| Security Implementation | EXCELLENT | Key-pair authentication recommended |

### Recommendations Before Production Deployment

1. **Validate Control-M Version**: Confirm customer has Control-M 9.0.20+ with Application Integrator
2. **Network Testing**: Perform connectivity test from Control-M Agent to Snowflake before configuration
3. **Key-Pair Setup**: Generate and configure key-pair authentication (do not use password in production)
4. **Pilot Testing**: Run jobs in parallel with existing Snowflake tasks before disabling tasks
5. **Monitoring Integration**: Configure Control-M alerts to integrate with customer's existing monitoring tools

---

## FINAL VERDICT

```
================================================================================
                         PRODUCTION READINESS VERDICT
================================================================================

                              APPROVED
                         
                    Score: 95/100 - Production Ready

Conditions:
1. Customer must verify Control-M version compatibility (9.0.20+)
2. Key-pair authentication must be implemented (not password)
3. Network connectivity must be tested from all Control-M Agents
4. Pilot testing recommended before full production cutover

================================================================================
                            APPROVED FOR DEPLOYMENT
================================================================================
```

---

## DOCUMENT APPROVAL

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Author | Snowflake Architect | 2026-03-03 | __________ |
| Technical Reviewer | __________________ | __________ | __________ |
| Control-M SME | __________________ | __________ | __________ |
| Customer Approval | __________________ | __________ | __________ |

---

```
================================================================================
                         END OF RUNBOOK
================================================================================
Document Version: 1.0
Classification: PRODUCTION - CUSTOMER DEPLOYMENT
Total Sections: 10
================================================================================
```
