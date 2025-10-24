# Enterprise Data Mapping Document Template
## Multi-Source to Snowflake Integration Project

---

## Document Control

| Field | Value |
|-------|-------|
| **Document Name** | Data Mapping Specification |
| **Project Name** | [Your Project Name] |
| **Version** | 1.0 |
| **Created Date** | [Date] |
| **Last Updated** | [Date] |
| **Author** | [Name] |
| **Reviewer** | [Name] |
| **Approver** | [Name] |
| **Status** | Draft / In Review / Approved |

### Version History
| Version | Date | Author | Description of Changes |
|---------|------|--------|----------------------|
| 0.1 | [Date] | [Name] | Initial draft |
| 0.2 | [Date] | [Name] | Added source system details |
| 1.0 | [Date] | [Name] | Final approved version |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Project Overview](#2-project-overview)
3. [Source Systems Overview](#3-source-systems-overview)
4. [Target Snowflake Architecture](#4-target-snowflake-architecture)
5. [Source-to-Target Mapping Specifications](#5-source-to-target-mapping-specifications)
6. [Data Relationships & Keys](#6-data-relationships--keys)
7. [Transformation Rules](#7-transformation-rules)
8. [Data Quality Rules](#8-data-quality-rules)
9. [Expected Values & Reference Data](#9-expected-values--reference-data)
10. [Data Lineage](#10-data-lineage)
11. [Dependencies & Prerequisites](#11-dependencies--prerequisites)
12. [Appendix](#12-appendix)

---

## 1. Executive Summary

### 1.1 Purpose
[Describe the purpose of this mapping document]

### 1.2 Scope
- **In Scope**: [List what is included]
- **Out of Scope**: [List what is excluded]

### 1.3 Business Objectives
- [Objective 1]
- [Objective 2]
- [Objective 3]

### 1.4 Key Stakeholders
| Role | Name | Contact | Responsibility |
|------|------|---------|---------------|
| Business Owner | | | |
| Technical Lead | | | |
| Data Architect | | | |
| Data Engineer | | | |

---

## 2. Project Overview

### 2.1 High-Level Data Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Oracle    │────▶│             │     │             │
├─────────────┤     │             │     │             │
│ SQL Server  │────▶│  Snowflake  │────▶│Presentation │
├─────────────┤     │   (Bronze/  │     │    Layer    │
│     AWS     │────▶│   Silver)   │     │   (Gold)    │
└─────────────┘     └─────────────┘     └─────────────┘
```

### 2.2 Data Volume Estimates
| Source System | Table Count | Total Records | Data Size | Update Frequency |
|--------------|-------------|---------------|-----------|-----------------|
| Oracle | | | | |
| SQL Server | | | | |
| AWS | | | | |

### 2.3 Key Deliverables
- [ ] Mapped [X] source tables to Snowflake
- [ ] Defined [Y] transformation rules
- [ ] Established [Z] data quality checks

---

## 3. Source Systems Overview

### 3.1 Oracle Database

| Parameter | Value |
|-----------|-------|
| **Server/Host** | [hostname] |
| **Database Name** | [DB name] |
| **Schema(s)** | [Schema names] |
| **Version** | [Oracle version] |
| **Connection Type** | JDBC/ODBC |
| **Authentication** | Service Account / OAuth |
| **Data Extraction Method** | Full Load / Incremental / CDC |
| **Extraction Frequency** | Daily / Hourly / Real-time |
| **Time Zone** | [Time zone] |
| **Character Set** | UTF-8 / Other |

#### Oracle Source Tables
| Schema | Table Name | Description | Row Count | Primary Key | Update Frequency |
|--------|------------|-------------|-----------|-------------|-----------------|
| | | | | | |
| | | | | | |

### 3.2 SQL Server Database

| Parameter | Value |
|-----------|-------|
| **Server/Host** | [hostname] |
| **Database Name** | [DB name] |
| **Schema(s)** | [Schema names] |
| **Version** | [SQL Server version] |
| **Connection Type** | JDBC/ODBC |
| **Authentication** | Windows Auth / SQL Auth |
| **Data Extraction Method** | Full Load / Incremental / CDC |
| **Extraction Frequency** | Daily / Hourly / Real-time |
| **Time Zone** | [Time zone] |
| **Collation** | [Collation setting] |

#### SQL Server Source Tables
| Schema | Table Name | Description | Row Count | Primary Key | Update Frequency |
|--------|------------|-------------|-----------|-------------|-----------------|
| | | | | | |
| | | | | | |

### 3.3 AWS Data Sources

| Parameter | Value |
|-----------|-------|
| **AWS Service** | S3 / RDS / DynamoDB / Redshift |
| **Region** | [AWS Region] |
| **Bucket/Database** | [Name] |
| **File Format** | JSON / CSV / Parquet / Avro |
| **Authentication** | IAM Role / Access Keys |
| **Data Extraction Method** | Full Load / Incremental |
| **Extraction Frequency** | Daily / Hourly / Real-time |
| **Encryption** | At-rest / In-transit |

#### AWS Source Objects
| Service | Object/Table Name | Description | Size/Row Count | Key Fields | Update Frequency |
|---------|------------------|-------------|----------------|------------|-----------------|
| | | | | | |
| | | | | | |

---

## 4. Target Snowflake Architecture

### 4.1 Snowflake Configuration

| Parameter | Value |
|-----------|-------|
| **Account** | [Account identifier] |
| **Region** | [Cloud region] |
| **Database(s)** | RAW_DB, ANALYTICS_DB |
| **Warehouse(s)** | ETL_WH (Size: XS/S/M/L) |
| **Role(s)** | ETL_ROLE, ANALYST_ROLE |
| **File Format** | [Default file format] |

### 4.2 Target Data Architecture

```
SNOWFLAKE
│
├── RAW_DB (Bronze Layer)
│   ├── ORACLE_SCHEMA
│   ├── SQLSERVER_SCHEMA
│   └── AWS_SCHEMA
│
├── ANALYTICS_DB
│   ├── STAGING (Silver Layer)
│   │   ├── STG_CUSTOMER
│   │   ├── STG_PRODUCT
│   │   └── STG_TRANSACTION
│   │
│   ├── INTERMEDIATE
│   │   ├── INT_CUSTOMER_360
│   │   └── INT_PRODUCT_METRICS
│   │
│   └── PRESENTATION (Gold Layer)
│       ├── DIM_CUSTOMER
│       ├── DIM_PRODUCT
│       ├── DIM_DATE
│       └── FACT_SALES
```

### 4.3 Naming Conventions

| Object Type | Pattern | Example |
|------------|---------|---------|
| Raw Tables | {SOURCE}_{TABLE_NAME}_RAW | ORACLE_CUSTOMERS_RAW |
| Staging Tables | STG_{DOMAIN}_{ENTITY} | STG_CRM_CUSTOMER |
| Dimension Tables | DIM_{ENTITY} | DIM_CUSTOMER |
| Fact Tables | FACT_{PROCESS} | FACT_SALES |
| Views | V_{PURPOSE}_{ENTITY} | V_REPORT_REVENUE |

---

## 5. Source-to-Target Mapping Specifications

### 5.1 Mapping Template

#### Mapping ID: [MAP_001]
#### Mapping Name: [Customer Data Integration]
#### Source System: [Oracle/SQL Server/AWS]
#### Target Table: [DIM_CUSTOMER]

##### 5.1.1 Column-Level Mapping

| # | Source Column | Source Data Type | Target Column | Target Data Type | Transformation Logic | Mandatory | Default Value | Comments |
|---|--------------|------------------|---------------|------------------|---------------------|-----------|---------------|----------|
| 1 | CUST_ID | NUMBER(10) | CUSTOMER_ID | NUMBER(38,0) | CAST AS NUMBER | Y | - | Primary Key |
| 2 | CUST_NAME | VARCHAR2(100) | CUSTOMER_NAME | VARCHAR(100) | UPPER(TRIM()) | Y | 'UNKNOWN' | Standardize to uppercase |
| 3 | CUST_EMAIL | VARCHAR2(255) | EMAIL_ADDRESS | VARCHAR(255) | LOWER(TRIM()) | N | NULL | Standardize to lowercase |
| 4 | CREATED_DT | DATE | CREATED_DATE | TIMESTAMP_NTZ | TO_TIMESTAMP_NTZ() | Y | CURRENT_TIMESTAMP() | Convert to Snowflake timestamp |
| 5 | CUST_STATUS | CHAR(1) | CUSTOMER_STATUS | VARCHAR(20) | CASE WHEN 'A' THEN 'ACTIVE' WHEN 'I' THEN 'INACTIVE' ELSE 'UNKNOWN' END | Y | 'UNKNOWN' | Status code expansion |
| 6 | - | - | ETL_LOAD_DATE | TIMESTAMP_NTZ | CURRENT_TIMESTAMP() | Y | - | Audit column |
| 7 | - | - | ETL_UPDATE_DATE | TIMESTAMP_NTZ | CURRENT_TIMESTAMP() | Y | - | Audit column |

##### 5.1.2 Source SQL Query
```sql
SELECT 
    CUST_ID,
    CUST_NAME,
    CUST_EMAIL,
    CREATED_DT,
    CUST_STATUS
FROM ORACLE_SCHEMA.CUSTOMERS
WHERE LAST_UPDATED >= :last_extract_date
  AND CUST_STATUS IN ('A', 'I')
```

##### 5.1.3 Target Table DDL
```sql
CREATE TABLE IF NOT EXISTS ANALYTICS_DB.PRESENTATION.DIM_CUSTOMER (
    CUSTOMER_ID         NUMBER(38,0) NOT NULL,
    CUSTOMER_NAME       VARCHAR(100) NOT NULL,
    EMAIL_ADDRESS       VARCHAR(255),
    CREATED_DATE        TIMESTAMP_NTZ NOT NULL,
    CUSTOMER_STATUS     VARCHAR(20) NOT NULL,
    ETL_LOAD_DATE      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ETL_UPDATE_DATE    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DIM_CUSTOMER PRIMARY KEY (CUSTOMER_ID)
);
```

### 5.2 Complex Mapping Example

#### Mapping ID: [MAP_002]
#### Mapping Name: [Sales Transaction Fact]
#### Source System: [Multiple - Oracle + SQL Server]
#### Target Table: [FACT_SALES]

##### 5.2.1 Multi-Source Join Mapping

| # | Source Table | Source Column | Join Condition | Target Column | Transformation | Comments |
|---|--------------|---------------|----------------|---------------|----------------|----------|
| 1 | ORACLE.ORDERS | ORDER_ID | Primary | ORDER_ID | Direct | Primary grain |
| 2 | ORACLE.ORDERS | CUSTOMER_ID | - | CUSTOMER_KEY | Surrogate key lookup | FK to DIM_CUSTOMER |
| 3 | ORACLE.ORDERS | ORDER_DATE | - | ORDER_DATE_KEY | Date dimension lookup | FK to DIM_DATE |
| 4 | SQLSERVER.ORDER_ITEMS | PRODUCT_ID | ON O.ORDER_ID = OI.ORDER_ID | PRODUCT_KEY | Surrogate key lookup | FK to DIM_PRODUCT |
| 5 | SQLSERVER.ORDER_ITEMS | QUANTITY | - | QUANTITY | SUM(QUANTITY) | Aggregate at order level |
| 6 | SQLSERVER.ORDER_ITEMS | UNIT_PRICE | - | UNIT_PRICE | Direct | - |
| 7 | SQLSERVER.ORDER_ITEMS | DISCOUNT | - | DISCOUNT_AMOUNT | QUANTITY * UNIT_PRICE * DISCOUNT | Calculate discount |
| 8 | Calculated | - | - | TOTAL_AMOUNT | (QUANTITY * UNIT_PRICE) - DISCOUNT_AMOUNT | Derived field |

##### 5.2.2 Source Join Query
```sql
SELECT 
    o.ORDER_ID,
    o.CUSTOMER_ID,
    o.ORDER_DATE,
    oi.PRODUCT_ID,
    oi.QUANTITY,
    oi.UNIT_PRICE,
    oi.DISCOUNT,
    (oi.QUANTITY * oi.UNIT_PRICE * (1 - oi.DISCOUNT)) AS NET_AMOUNT
FROM ORACLE_SCHEMA.ORDERS o
INNER JOIN SQLSERVER_SCHEMA.ORDER_ITEMS oi
    ON o.ORDER_ID = oi.ORDER_ID
WHERE o.ORDER_DATE >= :start_date
```

---

## 6. Data Relationships & Keys

### 6.1 Entity Relationship Diagram

```
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│  DIM_CUSTOMER   │         │   FACT_SALES    │         │  DIM_PRODUCT    │
├─────────────────┤    1:M  ├─────────────────┤   M:1   ├─────────────────┤
│ CUSTOMER_KEY PK │◄────────│ CUSTOMER_KEY FK │────────►│ PRODUCT_KEY PK  │
│ CUSTOMER_ID     │         │ PRODUCT_KEY FK  │         │ PRODUCT_ID      │
│ CUSTOMER_NAME   │         │ DATE_KEY FK     │         │ PRODUCT_NAME    │
└─────────────────┘         │ ORDER_ID        │         └─────────────────┘
                           │ QUANTITY        │                  
                           │ AMOUNT          │                  
                           └────────┬────────┘                  
                                   │ M:1                        
                                   ▼                            
                           ┌─────────────────┐                  
                           │   DIM_DATE      │                  
                           ├─────────────────┤                  
                           │ DATE_KEY PK     │                  
                           │ CALENDAR_DATE   │                  
                           │ FISCAL_PERIOD   │                  
                           └─────────────────┘                  
```

### 6.2 Key Mapping

| Target Table | Key Type | Key Column(s) | Source | Generation Method | Comments |
|--------------|----------|---------------|--------|-------------------|----------|
| DIM_CUSTOMER | Primary Key | CUSTOMER_KEY | - | IDENTITY/SEQUENCE | Surrogate key |
| DIM_CUSTOMER | Natural Key | CUSTOMER_ID | Oracle.CUSTOMERS.CUST_ID | Direct mapping | Business key |
| DIM_CUSTOMER | Alternate Key | EMAIL_ADDRESS | Oracle.CUSTOMERS.CUST_EMAIL | Direct mapping | Unique constraint |
| FACT_SALES | Primary Key | ORDER_ID, PRODUCT_ID | Composite | Direct mapping | Grain key |
| FACT_SALES | Foreign Key | CUSTOMER_KEY | DIM_CUSTOMER | Lookup on CUSTOMER_ID | - |
| FACT_SALES | Foreign Key | PRODUCT_KEY | DIM_PRODUCT | Lookup on PRODUCT_ID | - |
| FACT_SALES | Foreign Key | DATE_KEY | DIM_DATE | Lookup on ORDER_DATE | - |

### 6.3 Referential Integrity Rules

| Relationship | Parent Table | Child Table | Join Keys | Cardinality | Enforcement |
|--------------|--------------|-------------|-----------|-------------|-------------|
| Customer Orders | DIM_CUSTOMER | FACT_SALES | CUSTOMER_KEY | 1:M | Foreign Key |
| Product Sales | DIM_PRODUCT | FACT_SALES | PRODUCT_KEY | 1:M | Foreign Key |
| Order Date | DIM_DATE | FACT_SALES | DATE_KEY | 1:M | Foreign Key |

---

## 7. Transformation Rules

### 7.1 Business Transformation Rules

| Rule ID | Rule Name | Source Field(s) | Target Field | Logic | Example |
|---------|-----------|-----------------|--------------|-------|---------|
| TR_001 | Customer Segmentation | TOTAL_PURCHASES, LAST_PURCHASE_DATE | CUSTOMER_SEGMENT | IF TOTAL_PURCHASES > 100 AND LAST_PURCHASE < 30 DAYS THEN 'PLATINUM' ELSIF... | Input: 150, 2024-01-15<br>Output: 'PLATINUM' |
| TR_002 | Revenue Calculation | QUANTITY, UNIT_PRICE, DISCOUNT, TAX_RATE | TOTAL_REVENUE | (QUANTITY * UNIT_PRICE * (1-DISCOUNT)) * (1+TAX_RATE) | Input: 10, $100, 10%, 8%<br>Output: $972 |
| TR_003 | Date Standardization | Various date formats | STANDARD_DATE | Convert to YYYY-MM-DD HH:MI:SS | Input: '01/15/24'<br>Output: '2024-01-15 00:00:00' |
| TR_004 | Phone Formatting | PHONE_NUMBER | FORMATTED_PHONE | Remove special chars, format as (XXX) XXX-XXXX | Input: '1234567890'<br>Output: '(123) 456-7890' |
| TR_005 | Email Validation | EMAIL | VALID_EMAIL | Regex validation + domain check | Input: 'test@email'<br>Output: NULL |

### 7.2 Data Type Conversions

| Source System | Source Type | Target Type | Conversion Rule | Null Handling |
|---------------|-------------|-------------|-----------------|---------------|
| Oracle | NUMBER(10,2) | DECIMAL(12,2) | Direct cast | NULL → NULL |
| Oracle | VARCHAR2(4000) | VARCHAR(4000) | Direct cast | NULL → NULL |
| Oracle | DATE | TIMESTAMP_NTZ | TO_TIMESTAMP_NTZ() | NULL → '1900-01-01' |
| SQL Server | DATETIME | TIMESTAMP_NTZ | CONVERT_TIMEZONE('UTC') | NULL → NULL |
| SQL Server | NVARCHAR(MAX) | VARCHAR(16777216) | CAST + TRIM | NULL → Empty String |
| AWS JSON | String | VARCHAR | JSON_EXTRACT_PATH_TEXT() | NULL → 'N/A' |

### 7.3 Aggregation Rules

| Aggregation ID | Source Table | Grouping Columns | Aggregation Logic | Target Table |
|----------------|--------------|------------------|-------------------|--------------|
| AGG_001 | ORDER_ITEMS | CUSTOMER_ID, PRODUCT_ID | SUM(QUANTITY), SUM(AMOUNT) | CUSTOMER_PRODUCT_SUMMARY |
| AGG_002 | TRANSACTIONS | CUSTOMER_ID, DATE | COUNT(*), SUM(AMOUNT), AVG(AMOUNT) | DAILY_CUSTOMER_METRICS |

---

## 8. Data Quality Rules

### 8.1 Field-Level Quality Rules

| Rule ID | Table.Column | Rule Type | Rule Logic | Error Handling | Severity |
|---------|--------------|-----------|------------|----------------|----------|
| DQ_001 | DIM_CUSTOMER.CUSTOMER_ID | NOT NULL | IS NOT NULL | Reject record | Critical |
| DQ_002 | DIM_CUSTOMER.EMAIL_ADDRESS | FORMAT | REGEXP_LIKE('^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') | Set to NULL | Warning |
| DQ_003 | DIM_CUSTOMER.AGE | RANGE | BETWEEN 0 AND 120 | Set to default (0) | Warning |
| DQ_004 | FACT_SALES.QUANTITY | POSITIVE | > 0 | Reject record | Error |
| DQ_005 | FACT_SALES.UNIT_PRICE | REASONABLE | BETWEEN 0.01 AND 999999.99 | Flag for review | Warning |
| DQ_006 | DIM_PRODUCT.PRODUCT_CODE | UNIQUE | COUNT(PRODUCT_CODE) = 1 | Reject duplicate | Critical |
| DQ_007 | DIM_DATE.CALENDAR_DATE | VALID_DATE | IS_DATE(CALENDAR_DATE) | Reject record | Critical |

### 8.2 Cross-Field Validation Rules

| Rule ID | Rule Name | Validation Logic | Error Action |
|---------|-----------|------------------|--------------|
| XF_001 | Start/End Date Logic | START_DATE <= END_DATE | Reject record |
| XF_002 | Discount Validation | DISCOUNT_AMOUNT <= TOTAL_AMOUNT | Set discount to 0 |
| XF_003 | Address Completeness | IF COUNTRY = 'USA' THEN STATE IS NOT NULL | Flag for review |

### 8.3 Referential Integrity Checks

| Check ID | Child Table.Column | Parent Table.Column | Check Type | Action on Failure |
|----------|-------------------|---------------------|------------|-------------------|
| RI_001 | FACT_SALES.CUSTOMER_KEY | DIM_CUSTOMER.CUSTOMER_KEY | Lookup | Create dummy record (-1) |
| RI_002 | FACT_SALES.PRODUCT_KEY | DIM_PRODUCT.PRODUCT_KEY | Lookup | Create dummy record (-1) |
| RI_003 | FACT_SALES.DATE_KEY | DIM_DATE.DATE_KEY | Lookup | Use default date (19000101) |

### 8.4 Record-Level Quality Metrics

| Metric | Formula | Threshold | Alert Level |
|--------|---------|-----------|-------------|
| Completeness | (Non-NULL values / Total values) * 100 | > 95% | Warning if < 95% |
| Uniqueness | (Unique values / Total values) * 100 | = 100% for keys | Error if < 100% |
| Validity | (Valid values / Total values) * 100 | > 98% | Warning if < 98% |
| Consistency | (Consistent values / Total values) * 100 | > 99% | Error if < 99% |

---

## 9. Expected Values & Reference Data

### 9.1 Code Value Mappings

#### Status Codes
| Source System | Source Value | Source Description | Target Value | Target Description |
|--------------|--------------|-------------------|--------------|-------------------|
| Oracle | A | Active | ACTIVE | Customer is active |
| Oracle | I | Inactive | INACTIVE | Customer is inactive |
| Oracle | P | Pending | PENDING | Customer pending approval |
| SQL Server | 1 | Active | ACTIVE | Customer is active |
| SQL Server | 0 | Inactive | INACTIVE | Customer is inactive |
| SQL Server | NULL | Unknown | UNKNOWN | Status not defined |

#### Country Codes
| Source Value | Target Value | ISO Code | Description |
|--------------|--------------|----------|-------------|
| US | United States | USA | United States of America |
| UK | United Kingdom | GBR | United Kingdom |
| CA | Canada | CAN | Canada |

### 9.2 Business Rule Constants

| Constant Name | Value | Description | Usage |
|---------------|-------|-------------|-------|
| MIN_ORDER_AMOUNT | 0.01 | Minimum valid order amount | Validation |
| MAX_ORDER_AMOUNT | 999999.99 | Maximum valid order amount | Validation |
| DEFAULT_TAX_RATE | 0.08 | Default tax rate if not specified | Calculation |
| CURRENCY_CODE | USD | Default currency | Standardization |
| FISCAL_YEAR_START | 04-01 | Fiscal year start month-day | Date calculation |

### 9.3 Allowed Values

| Field | Allowed Values | Case Sensitive | Null Allowed |
|-------|----------------|----------------|--------------|
| CUSTOMER_SEGMENT | ['PLATINUM', 'GOLD', 'SILVER', 'BRONZE'] | No | No |
| PAYMENT_METHOD | ['CREDIT_CARD', 'DEBIT_CARD', 'PAYPAL', 'BANK_TRANSFER', 'CASH'] | No | Yes |
| ORDER_STATUS | ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED'] | No | No |
| PRODUCT_CATEGORY | [Reference lookup table PRODUCT_CATEGORY_REF] | Yes | No |

---

## 10. Data Lineage

### 10.1 End-to-End Data Flow

```
Source System          Bronze Layer           Silver Layer          Gold Layer
─────────────         ──────────────         ──────────────        ────────────
Oracle.CUSTOMERS  →   CUSTOMERS_RAW     →    STG_CUSTOMER     →   DIM_CUSTOMER
Oracle.ORDERS     →   ORDERS_RAW        →    STG_ORDER        →   ┐
                                                                   ├→ FACT_SALES
SQL.ORDER_ITEMS   →   ORDER_ITEMS_RAW   →    STG_ORDER_ITEM   →   ┘

AWS.products.json →   PRODUCTS_RAW      →    STG_PRODUCT      →   DIM_PRODUCT
```

### 10.2 Column-Level Lineage

| Target Column | Target Table | Source Columns | Source Tables | Transformation |
|---------------|--------------|----------------|---------------|---------------|
| CUSTOMER_KEY | DIM_CUSTOMER | - | - | Generated sequence |
| CUSTOMER_ID | DIM_CUSTOMER | CUST_ID | Oracle.CUSTOMERS | Direct |
| CUSTOMER_NAME | DIM_CUSTOMER | FIRST_NAME, LAST_NAME | Oracle.CUSTOMERS | CONCAT(FIRST_NAME, ' ', LAST_NAME) |
| TOTAL_REVENUE | FACT_SALES | QUANTITY, UNIT_PRICE, DISCOUNT | SQL.ORDER_ITEMS | SUM(QUANTITY * UNIT_PRICE * (1-DISCOUNT)) |

---

## 11. Dependencies & Prerequisites

### 11.1 Load Dependencies

```
Level 1 (Independent):
  ├── DIM_DATE
  ├── DIM_GEOGRAPHY
  └── REFERENCE_TABLES

Level 2 (Dependent on Level 1):
  ├── DIM_CUSTOMER (depends on DIM_GEOGRAPHY)
  └── DIM_PRODUCT

Level 3 (Dependent on Level 2):
  └── FACT_SALES (depends on all dimensions)
```

### 11.2 Load Sequence

| Sequence | Table Name | Dependencies | Load Type | Estimated Runtime |
|----------|------------|--------------|-----------|-------------------|
| 1 | DIM_DATE | None | Full | 5 min |
| 2 | DIM_GEOGRAPHY | None | Full | 10 min |
| 3 | DIM_CUSTOMER | DIM_GEOGRAPHY | Incremental | 15 min |
| 4 | DIM_PRODUCT | None | Full | 10 min |
| 5 | FACT_SALES | All Dimensions | Incremental | 30 min |

### 11.3 Prerequisites

#### Technical Prerequisites
- [ ] Snowflake account provisioned
- [ ] Network connectivity established
- [ ] Service accounts created
- [ ] Database and schemas created
- [ ] Appropriate privileges granted

#### Data Prerequisites
- [ ] Source system access confirmed
- [ ] Historical data available
- [ ] Reference data loaded
- [ ] Data quality baseline established

---

## 12. Appendix

### 12.1 Sample Data

#### Source Sample (Oracle.CUSTOMERS)
```
CUST_ID | CUST_NAME    | CUST_EMAIL           | CREATED_DT  | CUST_STATUS
--------|--------------|----------------------|-------------|-------------
1001    | John Smith   | jsmith@email.com     | 2024-01-15  | A
1002    | Jane Doe     | jdoe@email.com       | 2024-01-16  | A
1003    | Bob Johnson  | bjohnson@email.com   | 2024-01-17  | I
```

#### Target Sample (DIM_CUSTOMER)
```
CUSTOMER_KEY | CUSTOMER_ID | CUSTOMER_NAME | EMAIL_ADDRESS        | CUSTOMER_STATUS | ETL_LOAD_DATE
-------------|-------------|---------------|----------------------|-----------------|---------------
1            | 1001        | JOHN SMITH    | jsmith@email.com     | ACTIVE          | 2024-01-20
2            | 1002        | JANE DOE      | jdoe@email.com       | ACTIVE          | 2024-01-20
3            | 1003        | BOB JOHNSON   | bjohnson@email.com   | INACTIVE        | 2024-01-20
```

### 12.2 SQL Templates

#### Incremental Load Template
```sql
MERGE INTO target_table tgt
USING (
    SELECT * FROM source_table
    WHERE last_modified >= :last_extract_date
) src
ON tgt.business_key = src.business_key
WHEN MATCHED AND src.hash_value != tgt.hash_value THEN
    UPDATE SET
        tgt.column1 = src.column1,
        tgt.column2 = src.column2,
        tgt.etl_update_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (columns...)
    VALUES (src.columns...);
```

#### SCD Type 2 Template
```sql
-- Close existing record
UPDATE dim_table
SET end_date = CURRENT_DATE() - 1,
    current_flag = 'N'
WHERE business_key IN (
    SELECT business_key
    FROM staging_table
    WHERE change_detected = 'Y'
)
AND current_flag = 'Y';

-- Insert new version
INSERT INTO dim_table
SELECT 
    NEXTVAL('dim_seq'),
    staging_columns,
    CURRENT_DATE() AS start_date,
    '9999-12-31' AS end_date,
    'Y' AS current_flag
FROM staging_table
WHERE change_detected = 'Y';
```

### 12.3 Data Type Mapping Reference

| Oracle | SQL Server | AWS/JSON | Snowflake | Notes |
|--------|------------|----------|-----------|-------|
| NUMBER(p,s) | DECIMAL(p,s) | number | NUMBER(p,s) | Preserve precision |
| VARCHAR2(n) | VARCHAR(n) | string | VARCHAR(n) | Max 16MB in Snowflake |
| DATE | DATETIME | string | TIMESTAMP_NTZ | No timezone |
| CLOB | VARCHAR(MAX) | string | VARCHAR | Max 16MB |
| BLOB | VARBINARY(MAX) | base64 | BINARY | Base64 decode required |
| CHAR(n) | CHAR(n) | string | CHAR(n) | Fixed length |

### 12.4 Troubleshooting Guide

| Issue | Possible Cause | Resolution |
|-------|----------------|------------|
| Duplicate records | Missing unique constraint | Add deduplication logic |
| NULL in mandatory field | Source data quality | Add default value |
| Foreign key violation | Load order incorrect | Check dependency sequence |
| Performance degradation | Missing indexes/clustering | Add appropriate keys |
| Data type mismatch | Incorrect mapping | Review type conversion |

### 12.5 Glossary

| Term | Definition |
|------|------------|
| **ETL** | Extract, Transform, Load - Process of moving data |
| **CDC** | Change Data Capture - Tracking data changes |
| **SCD** | Slowly Changing Dimension - Historical tracking |
| **Surrogate Key** | System-generated unique identifier |
| **Natural Key** | Business meaningful identifier |
| **Grain** | Level of detail in a fact table |
| **Cardinality** | Relationship between tables (1:1, 1:M, M:M) |

### 12.6 Contact Information

| Role | Name | Email | Phone |
|------|------|-------|-------|
| Project Manager | | | |
| Data Architect | | | |
| Source System SME - Oracle | | | |
| Source System SME - SQL Server | | | |
| Source System SME - AWS | | | |
| Snowflake Administrator | | | |

### 12.7 Sign-Off

| Role | Name | Signature | Date |
|------|------|-----------|------|
| Business Owner | | | |
| Technical Lead | | | |
| Data Architect | | | |
| QA Lead | | | |

---

## Document Change Log

| Date | Version | Author | Change Description |
|------|---------|--------|--------------------|
| | | | |
| | | | |

---

**END OF DOCUMENT**

---

### Notes:
- This is a living document and should be updated as the project evolves
- All SQL examples should be validated in the target environment
- Performance metrics should be captured during testing
- Data quality thresholds should be agreed upon with business stakeholders