# Snowflake Naming Convention Standards
## Production-Ready Implementation Guide

**Document Version:** 2.0  
**Last Updated:** October 2025  
**Purpose:** Establish consistent, scalable naming conventions for Snowflake data platform

---

## Table of Contents

1. [Introduction](#introduction)
2. [Core Principles](#core-principles)
3. [Account Strategy Decision Guide](#account-strategy-decision-guide)
4. [Identifier Rules and Constraints](#identifier-rules-and-constraints)
5. [Account-Level Object Conventions](#account-level-object-conventions)
6. [Database-Level Object Conventions](#database-level-object-conventions)
7. [Schema Architecture and Layers](#schema-architecture-and-layers)
8. [Column Naming Standards](#column-naming-standards)
9. [Object Tagging Conventions](#object-tagging-conventions)
10. [Multi-Account vs Single-Account Strategies](#multi-account-vs-single-account-strategies)
11. [Implementation Examples](#implementation-examples)
12. [Compliance Checklist](#compliance-checklist)

---

## Introduction

### Why Naming Conventions Matter

> "There are only two hard things in Computer Science: cache invalidation and naming things." — Phil Karlton

Consistent naming conventions are critical for:
- **Maintainability**: Easy identification of object purpose and ownership
- **Scalability**: Systematic organization as data platform grows
- **Collaboration**: Reduced ambiguity across teams
- **Automation**: Predictable patterns enable CI/CD integration
- **Governance**: Clear audit trails and access control
- **Troubleshooting**: Rapid identification during incident response

### Scope

This document covers naming conventions for all Snowflake objects across development, testing, staging, and production environments. It provides guidance for both **multi-account** and **single-account** architectures.

---

## Core Principles

### Golden Rules

1. **UPPERCASE with UNDERSCORES**: Use all uppercase letters with underscores as separators. This aligns with Snowflake's default behavior, as unquoted identifiers are case-insensitive and stored as uppercase. Adhering to this standard prevents confusion and potential errors related to case sensitivity.
2. **Descriptive but Concise**: Names should be self-documenting but not verbose
3. **Consistent Structure**: Follow established patterns across all objects
4. **Avoid Abbreviations**: Use full words unless standard abbreviations are well-known
5. **Singular Nouns**: Use singular form for entity names (CUSTOMER not CUSTOMERS)
6. **No Special Characters**: Stick to alphanumeric and underscores only
7. **Environment Awareness**: Include environment prefixes ONLY if using single-account architecture
8. **Layer Identification**: Schema names should indicate data processing layer

### Formatting Standards

```sql
-- CORRECT Examples (Multi-Account)
SALES_RAW_DB
FINANCE_ANALYST_ROLE
STG_CUSTOMER_ORDER

-- CORRECT Examples (Single-Account)
PROD_SALES_RAW_DB
PROD_FINANCE_ANALYST_ROLE
STG_CUSTOMER_ORDER

-- INCORRECT Examples
dev-sales-raw-db          -- hyphens not allowed
prodFinanceAnalystRole    -- camelCase not preferred
stg_customers_orders      -- plural forms
```

---

## Account Strategy Decision Guide

### Decision Flowchart

```
┌─────────────────────────────────────────────────────────┐
│  Do you need complete environment isolation?            │
└────────────────┬────────────────────────────────────────┘
                 │
        ┌────────┴────────┐
        │ YES             │ NO
        │                 │
        ▼                 ▼
┌───────────────┐  ┌──────────────────┐
│ Multi-Account │  │ Single-Account   │
│ Architecture  │  │ Architecture     │
└───────┬───────┘  └────────┬─────────┘
        │                   │
        ▼                   ▼
┌───────────────────────────────────────────────────┐
│ Naming: NO environment prefixes                   │
│ Example: SALES_RAW_DB                            │
│ Pros: Simpler names, identical across envs       │
└───────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────┐
│ Naming: WITH environment prefixes                 │
│ Example: PROD_SALES_RAW_DB, DEV_SALES_RAW_DB     │
│ Pros: Single pane of glass, shared resources     │
└───────────────────────────────────────────────────┘
```

### Quick Decision Matrix

| Factor | Multi-Account ⭐ | Single-Account |
|--------|------------------|----------------|
| **Production Readiness** | ⭐⭐⭐⭐⭐ Recommended | ⭐⭐⭐ Acceptable |
| **Security Isolation** | ⭐⭐⭐⭐⭐ Complete | ⭐⭐ Role-based only |
| **Cost Tracking** | ⭐⭐⭐⭐⭐ Per-account | ⭐⭐⭐ Tags required |
| **Complexity** | ⭐⭐⭐ Higher | ⭐⭐⭐⭐⭐ Lower |
| **CI/CD Simplicity** | ⭐⭐⭐⭐⭐ Identical code | ⭐⭐⭐ Requires substitution |
| **Blast Radius** | ⭐⭐⭐⭐⭐ Contained | ⭐⭐ Shared |
| **Compliance** | ⭐⭐⭐⭐⭐ Easier | ⭐⭐⭐ More complex |

### Choose Multi-Account If:
✅ You're in a production/enterprise environment  
✅ Security and compliance are critical (SOC2, HIPAA, PCI-DSS)  
✅ You need complete isolation between environments  
✅ You want independent billing and cost tracking  
✅ You have multiple teams managing different environments  
✅ You want to prevent dev/test from affecting production  

### Choose Single-Account If:
✅ You're a startup or small team  
✅ Budget is constrained  
✅ You need frequent cross-environment queries  
✅ Your data governance requirements are simple  
✅ You have a small team managing all environments  

---

## Identifier Rules and Constraints

### Snowflake System Requirements

**Unquoted Identifiers** (Recommended):
- Must start with a letter (A-Z) or underscore (_)
- Can contain letters, numbers, underscores, and dollar signs
- Maximum 255 characters
- Case-insensitive (stored as UPPERCASE)
- Cannot be reserved keywords

**Quoted Identifiers** (Use Sparingly):
- Enclosed in double quotes: "My Identifier"
- Case-sensitive
- Can contain spaces and special characters
- Should be avoided for production objects

**IMPORTANT**: Avoiding quoted identifiers is critical because they are case-sensitive, which can lead to subtle and hard-to-debug errors where "mytable" and "MyTable" are treated as two different objects. This creates maintenance nightmares and can cause production incidents when queries fail due to incorrect casing.

### Reserved Keywords to Avoid

Common reserved words that **cannot** be used as unquoted identifiers:
- SQL Keywords: SELECT, FROM, WHERE, JOIN, TABLE, VIEW
- Snowflake-Specific: ACCOUNT, DATABASE, WAREHOUSE, STAGE, PIPE
- Data Types: NUMBER, VARCHAR, BOOLEAN, DATE, TIMESTAMP
- Full list: [Snowflake Reserved Keywords Documentation](https://docs.snowflake.com/en/sql-reference/reserved-keywords)

**Workaround**: If you must use a reserved word, enclose it in double quotes (e.g., "ORDER") but this requires consistent quoting in all references.

### Character Limit Guidelines

| Object Type | Recommended Max | System Max |
|-------------|----------------|------------|
| Database    | 30 characters  | 255        |
| Schema      | 30 characters  | 255        |
| Table       | 40 characters  | 255        |
| Column      | 30 characters  | 255        |
| Role        | 40 characters  | 255        |

---

## Account-Level Object Conventions

### Environment Prefixes

**IMPORTANT**: Use environment prefixes **ONLY** for Single-Account architecture.

| Environment | Prefix | Description | Multi-Account | Single-Account |
|-------------|--------|-------------|---------------|----------------|
| Development | DEV    | Development and experimentation | ❌ Not Used | ✅ Required |
| System Integration Testing | SIT | Integration testing | ❌ Not Used | ✅ Required |
| User Acceptance Testing | UAT | Pre-production validation | ❌ Not Used | ✅ Required |
| Quality Assurance | QA | Quality testing | ❌ Not Used | ✅ Required |
| Staging | STG | Production-like pre-release | ❌ Not Used | ✅ Required |
| Production | PROD   | Live production | ❌ Not Used | ✅ Required |
| Sandbox | SBX    | Isolated sandbox for POCs | ❌ Not Used | ✅ Required |

### User Accounts

#### Multi-Account Architecture:

**Format**: `[EMAIL]` or `[PROJECT]_[APP_CODE]_USER`

```sql
-- Human Users
CREATE USER "jane.smith@company.com" ...

-- Service Accounts (NO environment prefix)
CREATE USER SALES_FIVETRAN_USER ...
CREATE USER MARKETING_AIRFLOW_USER ...
CREATE USER ANALYTICS_DBT_USER ...
```

#### Single-Account Architecture:

**Format**: `[EMAIL]` or `[ENV]_[PROJECT]_[APP_CODE]_USER`

```sql
-- Human Users
CREATE USER "jane.smith@company.com" ...

-- Service Accounts (WITH environment prefix)
CREATE USER PROD_SALES_FIVETRAN_USER ...
CREATE USER DEV_MARKETING_AIRFLOW_USER ...
CREATE USER STG_ANALYTICS_DBT_USER ...
```

### Roles

#### Multi-Account Architecture:

**Format**: `[PROJECT]_[ROLE_NAME]_ROLE`

```sql
-- Functional Roles (NO environment prefix)
CREATE ROLE SALES_ANALYST_ROLE;
CREATE ROLE FINANCE_DEVELOPER_ROLE;
CREATE ROLE MARKETING_READER_ROLE;
CREATE ROLE ETL_EXECUTOR_ROLE;
```

#### Single-Account Architecture:

**Format**: `[ENV]_[PROJECT]_[ROLE_NAME]_ROLE` (for service roles)  
**Format**: `[PROJECT]_[ROLE_NAME]_ROLE` (for user roles)

```sql
-- Functional Roles (generally no prefix)
CREATE ROLE SALES_ANALYST_ROLE;
CREATE ROLE FINANCE_DEVELOPER_ROLE;

-- Service Roles (WITH environment prefix)
CREATE ROLE PROD_ETL_EXECUTOR_ROLE;
CREATE ROLE DEV_DBT_TRANSFORMER_ROLE;
```

### Virtual Warehouses

#### Multi-Account Architecture:

**Format**: `[PROJECT]_[PURPOSE]_[SIZE]_WH`

```sql
-- NO environment prefix needed
CREATE WAREHOUSE SALES_LOAD_L_WH ...
CREATE WAREHOUSE ANALYTICS_TRANSFORM_M_WH ...
CREATE WAREHOUSE FINANCE_QUERY_XL_WH ...
CREATE WAREHOUSE MARKETING_ADHOC_S_WH ...
```

#### Single-Account Architecture:

**Format**: `[ENV]_[PROJECT]_[PURPOSE]_[SIZE]_WH`

```sql
-- WITH environment prefix
CREATE WAREHOUSE PROD_SALES_LOAD_L_WH ...
CREATE WAREHOUSE DEV_ANALYTICS_TRANSFORM_M_WH ...
CREATE WAREHOUSE PROD_FINANCE_QUERY_XL_WH ...
CREATE WAREHOUSE STG_MARKETING_ADHOC_S_WH ...
```

### Databases

#### Multi-Account Architecture:

**Format**: `[PROJECT]_[DATA_LAYER]_DB`

```sql
-- NO environment prefix needed
CREATE DATABASE SALES_RAW_DB;
CREATE DATABASE SALES_STAGING_DB;
CREATE DATABASE SALES_CORE_DB;
CREATE DATABASE ANALYTICS_MART_DB;
CREATE DATABASE FINANCE_ARCHIVE_DB;
```

#### Single-Account Architecture:

**Format**: `[ENV]_[PROJECT]_[DATA_LAYER]_DB`

```sql
-- WITH environment prefix
CREATE DATABASE PROD_SALES_RAW_DB;
CREATE DATABASE PROD_SALES_STAGING_DB;
CREATE DATABASE PROD_SALES_CORE_DB;
CREATE DATABASE DEV_ANALYTICS_MART_DB;
CREATE DATABASE PROD_FINANCE_ARCHIVE_DB;
```

### Account Naming Convention (Multi-Account Only)

**Format**: `[COMPANY]_[ENVIRONMENT]` or `[COMPANY]_[ENVIRONMENT]_[REGION]`

**Examples**:
```
acme_prod
acme_dev
acme_staging
acme_uat

-- With region
acme_prod_us_east
acme_prod_eu_west
acme_dev_us_east
```

### Resource Monitors

**Format**: `[ENV]_[SCOPE]_[LIMIT]_MONITOR` (Single-Account)  
**Format**: `[SCOPE]_[LIMIT]_MONITOR` (Multi-Account)

**Examples**:
```sql
-- Multi-Account
CREATE RESOURCE MONITOR ACCOUNT_DAILY_1000_MONITOR ...

-- Single-Account
CREATE RESOURCE MONITOR PROD_ACCOUNT_DAILY_1000_MONITOR ...
CREATE RESOURCE MONITOR DEV_WAREHOUSE_MONTHLY_500_MONITOR ...
```

### Storage Integrations

**Format**: `[PROVIDER]_[REGION]_[PURPOSE]_INTEGRATION`

**Examples**:
```sql
CREATE STORAGE INTEGRATION AWS_US_EAST_S3_INTEGRATION ...
CREATE STORAGE INTEGRATION AZURE_WEST_BLOB_INTEGRATION ...
CREATE STORAGE INTEGRATION GCP_CENTRAL_GCS_INTEGRATION ...
```

---

## Database-Level Object Conventions

Database-level objects (schemas, tables, views) typically don't require environment prefixes since they exist within environment-specific databases.

### Schemas

**Format**: `[LAYER_PREFIX]_[SOURCE_SYSTEM]` or `[LAYER_PREFIX]`

#### Layer Prefixes

| Prefix | Layer Name | Purpose | Example |
|--------|-----------|---------|---------|
| LND | Landing | Raw ingested data, no transformation | LND_SALESFORCE |
| RAW | Raw | Unprocessed source data | RAW_HUBSPOT |
| STG | Staging | Initial cleansing and typing | STG_CUSTOMER |
| INT | Integration | Combined and conformed data | INT_ORDER_PROCESSING |
| CORE | Core/Enterprise | Conformed dimensional model | CORE_DIM |
| MART | Data Mart | Business-specific aggregates | MART_SALES |
| RPT | Reporting | Report-ready views | RPT_EXECUTIVE |
| WRK | Workbench | Sandbox/scratch area | WRK_DATA_SCIENCE |
| UTIL | Utility | Helper functions and procedures | UTIL_ADMIN |
| ARCH | Archive | Historical/compliance data | ARCH_2024 |

**Examples** (Same for both Multi-Account and Single-Account):
```sql
-- Source-Specific Landing Schemas
CREATE SCHEMA SALES_RAW_DB.LND_SALESFORCE;
CREATE SCHEMA SALES_RAW_DB.LND_NETSUITE;

-- Processing Layer Schemas
CREATE SCHEMA SALES_STAGING_DB.STG_CUSTOMER;
CREATE SCHEMA SALES_STAGING_DB.STG_PRODUCT;

-- Data Warehouse Core Schemas
CREATE SCHEMA SALES_CORE_DB.CORE_DIM;
CREATE SCHEMA SALES_CORE_DB.CORE_FACT;

-- Mart Schemas
CREATE SCHEMA ANALYTICS_MART_DB.MART_SALES;
CREATE SCHEMA ANALYTICS_MART_DB.MART_FINANCE;
```

### Tables

**Format**: `[TABLE_NAME]` or `[PREFIX_]TABLE_NAME`

**Prefixes** (Optional but Recommended):

| Prefix | Type | Example |
|--------|------|---------|
| FACT_  | Fact Table | FACT_SALES, FACT_ORDERS |
| DIM_   | Dimension Table | DIM_CUSTOMER, DIM_PRODUCT |
| BRG_   | Bridge Table | BRG_PRODUCT_CATEGORY |
| TMP_   | Temporary Table | TMP_STAGING_LOAD |
| HIST_  | Historical Snapshot | HIST_CUSTOMER_DAILY |
| AGG_   | Aggregate Table | AGG_MONTHLY_REVENUE |
| REF_   | Reference/Lookup | REF_COUNTRY_CODES |

**Examples**:
```sql
-- Dimension Tables
CREATE TABLE CORE_DIM.DIM_CUSTOMER ...
CREATE TABLE CORE_DIM.DIM_DATE ...
CREATE TABLE CORE_DIM.DIM_PRODUCT ...

-- Fact Tables
CREATE TABLE CORE_FACT.FACT_ORDER ...
CREATE TABLE CORE_FACT.FACT_TRANSACTION ...

-- Staging Tables (no prefix needed)
CREATE TABLE STG_CUSTOMER.CUSTOMER ...
CREATE TABLE STG_CUSTOMER.CUSTOMER_ADDRESS ...

-- Reference Tables
CREATE TABLE UTIL.REF_TIMEZONE ...
CREATE TABLE UTIL.REF_CURRENCY_CONVERSION ...
```

### Views, File Formats, Stages, Pipes, Streams, Tasks, Stored Procedures, UDFs, Sequences

All database-level objects follow the same conventions regardless of account architecture. See the main document sections for detailed examples.

---

## Schema Architecture and Layers

### Recommended Multi-Layer Architecture

```
[ENV_]DOMAIN_RAW_DB
├── LND_SALESFORCE          -- Raw API/file dumps
├── LND_HUBSPOT
└── LND_NETSUITE

[ENV_]DOMAIN_STAGING_DB
├── STG_CUSTOMER            -- Cleaned, typed data
├── STG_PRODUCT
└── STG_ORDER

[ENV_]DOMAIN_CORE_DB
├── CORE_DIM                -- Dimension tables
├── CORE_FACT               -- Fact tables
└── INT_BUSINESS_RULES      -- Integration/business logic

[ENV_]DOMAIN_MART_DB
├── MART_SALES              -- Sales analytics
├── MART_FINANCE            -- Finance reporting
└── MART_OPERATIONS         -- Operational dashboards

[ENV_]DOMAIN_UTILITY_DB
├── UTIL_ADMIN              -- Admin procedures
├── WRK_DATA_SCIENCE        -- Sandbox area
└── ARCH_HISTORICAL         -- Archive data
```

**Note**: `[ENV_]` prefix only used in Single-Account architecture.

---

## Column Naming Standards

### General Column Naming Rules

1. **Use lowercase or UPPERCASE consistently** (UPPERCASE recommended for consistency with table names)
2. **Avoid abbreviations** unless industry-standard (ID, NUM, AMT)
3. **Be descriptive** but avoid excessive length
4. **Use underscores** to separate words
5. **Include data type hints** for dates and flags

### Standard Column Suffixes

| Suffix | Meaning | Example |
|--------|---------|---------|
| _ID    | Identifier/Primary Key | CUSTOMER_ID, ORDER_ID |
| _KEY   | Surrogate Key | CUSTOMER_KEY |
| _CODE  | Classification Code | PRODUCT_CODE, STATUS_CODE |
| _NAME  | Full Name | CUSTOMER_NAME, PRODUCT_NAME |
| _DESC  | Description | PRODUCT_DESC |
| _DATE  | Date Only | ORDER_DATE, CREATED_DATE |
| _DATETIME | Date and Time | ORDER_DATETIME, UPDATED_DATETIME |
| _TIMESTAMP | Timestamp | LOAD_TIMESTAMP, MODIFIED_TIMESTAMP |
| _FLAG  | Boolean/Indicator | ACTIVE_FLAG, IS_DELETED_FLAG |
| _IND   | Indicator | SUCCESS_IND |
| _CNT   | Count | ORDER_LINE_CNT |
| _AMT   | Amount/Money | ORDER_AMT, TAX_AMT |
| _QTY   | Quantity | ORDER_QTY |
| _PCT   | Percentage | DISCOUNT_PCT, TAX_PCT |
| _NUM   | Number | INVOICE_NUM, SEQUENCE_NUM |
| _ADDR  | Address | BILLING_ADDR, SHIPPING_ADDR |

### Standard Audit Columns

**Basic ETL Audit Columns** (for LND/RAW/STG layers):
```sql
CREATED_BY          VARCHAR(100),
CREATED_DATETIME    TIMESTAMP_NTZ,
UPDATED_BY          VARCHAR(100),
UPDATED_DATETIME    TIMESTAMP_NTZ,
LOAD_TIMESTAMP      TIMESTAMP_NTZ,
SOURCE_SYSTEM       VARCHAR(50)
```

**Kimball-Style Audit Columns** (for CORE_DIM layer):
```sql
EFFECTIVE_DATE      DATE,
EXPIRATION_DATE     DATE,
CURRENT_FLAG        BOOLEAN,
ROW_VERSION         INTEGER,
ETL_BATCH_ID        VARCHAR(50),
ETL_INSERT_DATETIME TIMESTAMP_NTZ,
ETL_UPDATE_DATETIME TIMESTAMP_NTZ
```

---

## Object Tagging Conventions

Snowflake's tagging feature allows you to apply key-value metadata to objects for governance, cost tracking, discovery, and compliance.

### Recommended Standard Tags

| Tag Name | Description | Example Values | Required |
|----------|-------------|----------------|----------|
| PROJECT | Business project or domain | 'SALES_ANALYTICS', 'CUSTOMER_360' | Recommended |
| COST_CENTER | Internal department code | 'CC_4510', 'FINANCE', 'MARKETING' | Recommended |
| DATA_SENSITIVITY | Data classification level | 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'PII' | **Required** |
| OWNER_EMAIL | Team or individual responsible | 'data.engineering@company.com' | Recommended |
| STATUS | Lifecycle status | 'ACTIVE', 'DEPRECATED', 'ARCHIVED' | Recommended |
| ENVIRONMENT | Environment designation | 'PRODUCTION', 'DEVELOPMENT', 'STAGING' | **Multi-Account Only** |

### Creating and Applying Tags

```sql
-- Create tags in a central schema
USE SCHEMA GOVERNANCE.TAGS;

CREATE TAG IF NOT EXISTS DATA_SENSITIVITY
  COMMENT = 'Classification level for data security';

CREATE TAG IF NOT EXISTS ENVIRONMENT
  COMMENT = 'Environment designation (Multi-Account only)';

-- Apply tags to a database (Multi-Account)
ALTER DATABASE SALES_RAW_DB SET TAG
  PROJECT = 'SALES_ANALYTICS',
  ENVIRONMENT = 'PRODUCTION',
  OWNER_EMAIL = 'sales.engineering@company.com';

-- Apply tags to a table
ALTER TABLE CORE_DIM.DIM_CUSTOMER SET TAG
  DATA_SENSITIVITY = 'PII',
  PROJECT = 'CUSTOMER_360',
  STATUS = 'ACTIVE';
```

---

## Multi-Account vs Single-Account Strategies

### Comparison Summary

| Aspect | Multi-Account ⭐ | Single-Account |
|--------|------------------|----------------|
| **Object Naming** | Simpler (no ENV prefix) | More complex (ENV prefix required) |
| **Code Promotion** | Identical SQL everywhere | Requires string substitution |
| **Security** | Complete isolation | Role-based isolation |
| **Cost Tracking** | Per-account billing | Tag-based allocation |
| **Management Overhead** | Higher (multiple accounts) | Lower (one account) |
| **Blast Radius** | Contained per environment | Shared across environments |
| **CI/CD Complexity** | Lower (no name changes) | Higher (environment substitution) |
| **Compliance** | Easier to audit | Requires more controls |
| **Resource Sharing** | Not possible | Possible |
| **Cross-Env Queries** | Not possible directly | Easy |

### Multi-Account Implementation Example

```
Production Account (acme_prod):
├── SALES_RAW_DB
├── SALES_CORE_DB
├── SALES_ANALYST_ROLE
└── SALES_LOAD_L_WH

Development Account (acme_dev):
├── SALES_RAW_DB          (Same name!)
├── SALES_CORE_DB         (Same name!)
├── SALES_ANALYST_ROLE    (Same name!)
└── SALES_LOAD_L_WH       (Same name!)
```

**CI/CD Script** (works identically in all environments):
```sql
-- No environment-specific logic needed!
CREATE OR REPLACE DATABASE SALES_RAW_DB;
CREATE OR REPLACE WAREHOUSE SALES_LOAD_L_WH;
CREATE OR REPLACE ROLE SALES_ANALYST_ROLE;
```

### Single-Account Implementation Example

```
Unified Account (acme_unified):
├── PROD_SALES_RAW_DB
├── PROD_SALES_CORE_DB
├── DEV_SALES_RAW_DB
├── DEV_SALES_CORE_DB
├── PROD_SALES_ANALYST_ROLE
├── DEV_SALES_ANALYST_ROLE
└── PROD_SALES_LOAD_L_WH
```

**CI/CD Script** (requires environment substitution):
```sql
-- Requires variable substitution
CREATE OR REPLACE DATABASE ${ENV}_SALES_RAW_DB;
CREATE OR REPLACE WAREHOUSE ${ENV}_SALES_LOAD_L_WH;
CREATE OR REPLACE ROLE ${ENV}_SALES_ANALYST_ROLE;
```

---

## Implementation Examples

### Complete Setup: Multi-Account Architecture

```sql
-- ============================================
-- PRODUCTION ACCOUNT SETUP (Multi-Account)
-- Account: acme-prod
-- ============================================

-- 1. Create Roles (NO environment prefix)
CREATE ROLE IF NOT EXISTS SALES_ADMIN_ROLE;
CREATE ROLE IF NOT EXISTS SALES_DEVELOPER_ROLE;
CREATE ROLE IF NOT EXISTS SALES_ANALYST_ROLE;

-- 2. Create Service Users (NO environment prefix)
CREATE USER IF NOT EXISTS SALES_FIVETRAN_USER 
  PASSWORD = 'STRONG_PASSWORD'
  DEFAULT_ROLE = SALES_DEVELOPER_ROLE;

-- 3. Create Warehouses (NO environment prefix)
CREATE WAREHOUSE IF NOT EXISTS SALES_LOAD_L_WH WITH
  WAREHOUSE_SIZE = 'LARGE'
  AUTO_SUSPEND = 300;

-- 4. Create Databases (NO environment prefix)
CREATE DATABASE IF NOT EXISTS SALES_RAW_DB;
CREATE DATABASE IF NOT EXISTS SALES_STAGING_DB;
CREATE DATABASE IF NOT EXISTS SALES_CORE_DB;

-- Tag with environment
ALTER DATABASE SALES_RAW_DB SET TAG ENVIRONMENT = 'PRODUCTION';
```

### Complete Setup: Single-Account Architecture

```sql
-- ============================================
-- SINGLE ACCOUNT SETUP
-- Account: acme-unified
-- ============================================

-- 1. Create Roles
CREATE ROLE IF NOT EXISTS SALES_ADMIN_ROLE;
CREATE ROLE IF NOT EXISTS PROD_ETL_EXECUTOR_ROLE;
CREATE ROLE IF NOT EXISTS DEV_ETL_EXECUTOR_ROLE;

-- 2. Create Service Users (WITH environment prefix)
CREATE USER IF NOT EXISTS PROD_SALES_FIVETRAN_USER 
  PASSWORD = 'STRONG_PASSWORD'
  DEFAULT_ROLE = PROD_ETL_EXECUTOR_ROLE;

CREATE USER IF NOT EXISTS DEV_SALES_FIVETRAN_USER 
  PASSWORD = 'STRONG_PASSWORD'
  DEFAULT_ROLE = DEV_ETL_EXECUTOR_ROLE;

-- 3. Create Warehouses (WITH environment prefix)
CREATE WAREHOUSE IF NOT EXISTS PROD_SALES_LOAD_L_WH WITH
  WAREHOUSE_SIZE = 'LARGE'
  AUTO_SUSPEND = 300;

CREATE WAREHOUSE IF NOT EXISTS DEV_SALES_LOAD_M_WH WITH
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 300;

-- 4. Create Databases (WITH environment prefix)
CREATE DATABASE IF NOT EXISTS PROD_SALES_RAW_DB;
CREATE DATABASE IF NOT EXISTS PROD_SALES_STAGING_DB;
CREATE DATABASE IF NOT EXISTS DEV_SALES_RAW_DB;
CREATE DATABASE IF NOT EXISTS DEV_SALES_STAGING_DB;
```

---

## Compliance Checklist

### Pre-Deployment Review

Use this checklist before creating any Snowflake objects in production:

#### Architecture Validation
- [ ] Account strategy selected (Multi-Account vs Single-Account)
- [ ] Naming convention approach confirmed (with or without ENV prefix)
- [ ] All stakeholders aligned on approach

#### Naming Validation
- [ ] Object name follows established convention for its type
- [ ] Environment prefix included ONLY if using single-account
- [ ] Name is in UPPERCASE with underscores
- [ ] No reserved keywords used as unquoted identifiers
- [ ] Name is descriptive and self-documenting
- [ ] No abbreviations unless industry-standard
- [ ] Length is under recommended maximum
- [ ] Name doesn't conflict with existing objects

#### Technical Validation
- [ ] DDL uses `IF NOT EXISTS` clause for CI/CD compatibility
- [ ] Appropriate role specified as creator/owner
- [ ] Required permissions granted
- [ ] Warehouse size appropriate for workload
- [ ] Resource monitor configured (for warehouses)
- [ ] Retention period set appropriately
- [ ] Clustering keys defined if needed
- [ ] Comments added for documentation

#### Schema Design
- [ ] Tables placed in correct layer schema
- [ ] Audit columns included in all tables
- [ ] Primary keys defined
- [ ] Foreign keys documented (via comments)
- [ ] Data types appropriate and consistent
- [ ] NULL/NOT NULL specified
- [ ] Default values set where appropriate

#### Security & Governance
- [ ] Role-based access control configured
- [ ] Data masking policies applied to PII
- [ ] Row access policies defined if needed
- [ ] Object tagging for classification
- [ ] Network policies reviewed
- [ ] Service account vs user account appropriate

#### Tagging & Classification
- [ ] Required tags applied (DATA_SENSITIVITY, PROJECT, OWNER_EMAIL)
- [ ] ENVIRONMENT tag applied (Multi-Account architecture only)
- [ ] PII and sensitive columns tagged appropriately
- [ ] Tag-based policies configured where needed
- [ ] Cost allocation tags assigned (COST_CENTER)
- [ ] Compliance tags added as applicable

#### Documentation
- [ ] Object purpose documented via COMMENT
- [ ] Lineage documented for data flow
- [ ] Dependencies mapped
- [ ] Contact/owner identified
- [ ] Change log updated

---

## Glossary

**Account Object**: Objects that exist at account level (databases, roles, warehouses)

**Database Object**: Objects that exist within databases (tables, views, schemas)

**Fully-Qualified Name**: Complete path to object: `DATABASE.SCHEMA.OBJECT`

**Identifier**: Name given to a Snowflake object

**Multi-Account Architecture**: Separate Snowflake accounts for each environment (recommended)

**Quoted Identifier**: Object name enclosed in double quotes (case-sensitive)

**Reserved Keyword**: SQL keyword that cannot be used as unquoted identifier

**Schema Layer**: Logical grouping indicating data processing stage

**Single-Account Architecture**: One Snowflake account with environment prefixes for all objects

**Surrogate Key**: System-generated unique identifier

**Tag**: Key-value metadata for object classification and governance

**Unquoted Identifier**: Object name without quotes (case-insensitive, stored as UPPERCASE)

---

## Additional Resources

- [Snowflake Documentation - Identifiers](https://docs.snowflake.com/en/sql-reference/identifiers)
- [Snowflake Reserved Keywords](https://docs.snowflake.com/en/sql-reference/reserved-keywords)
- [Snowflake Object Tagging](https://docs.snowflake.com/en/user-guide/object-tagging)
- [Snowflake Tag-Based Masking](https://docs.snowflake.com/en/user-guide/tag-based-masking-policies)
- [Snowflake Multi-Account Best Practices](https://docs.snowflake.com/)
- [Data Vault 2.0 Naming Standards](http://datavaultalliance.com/)
- [Kimball Dimensional Modeling](https://www.kimballgroup.com/)

---

## Document Change Log

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-10-09 | Venkannababu Thatavarthi | Initial release |
| 2.0 | 2025-10-11 | Venkannababu Thatavarthi | Added multi-account architecture guidance, decision flowchart, and separated conventions |

---

**END OF DOCUMENT**