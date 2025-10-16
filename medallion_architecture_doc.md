# Medallion Architecture in Snowflake
## A Complete Guide to Building Modern Data Platforms

**Prepared by:** Venkannababu Thatavarthi  
**Date:** October 15, 2025  
**Version:** 1.0

---

## Executive Summary

In today's data-driven world, organizations need a structured approach to handle the increasing volume, variety, and velocity of data. The Medallion Architecture provides exactly that—a proven framework that transforms raw, chaotic data into clean, reliable, analytics-ready datasets.

Think of Medallion Architecture as a multi-stage water filtration system. Just as water goes through progressively finer filters to become drinking-quality, your data flows through Bronze, Silver, and Gold layers, getting cleaner and more refined at each stage. By the time data reaches the Gold layer, it's ready for critical business decisions.

In this document, I'll walk you through how we implement this architecture in Snowflake, showing you exactly what each layer looks like, why it matters, and how it benefits your organization.

---

## Table of Contents

1. [What is Medallion Architecture?](#what-is-medallion-architecture)
2. [Why Use Medallion Architecture?](#why-use-medallion-architecture)
3. [Architecture Overview](#architecture-overview)
4. [Bronze Layer - Raw Data Landing Zone](#bronze-layer)
5. [Silver Layer - Cleaned and Standardized Data](#silver-layer)
6. [Gold Layer - Business-Ready Analytics](#gold-layer)
7. [Platinum Layer - Advanced Analytics (Optional)](#platinum-layer)
8. [Semantic Layer - Business User Interface](#semantic-layer)
9. [Implementation in Snowflake](#implementation-in-snowflake)
10. [Real-World Use Cases](#real-world-use-cases)
11. [Best Practices](#best-practices)

---

## What is Medallion Architecture?

Medallion Architecture is a data design pattern that organizes data into layers—each representing a different level of data quality and refinement. The name comes from the progression through Bronze, Silver, and Gold tiers, much like Olympic medals representing increasing value.

### The Core Concept

Instead of loading data once and transforming it all at the same time (traditional ETL), Medallion Architecture follows an incremental, progressive approach:

1. **Land everything first** - Capture all raw data without transformation
2. **Clean progressively** - Apply transformations in stages, not all at once
3. **Optimize for purpose** - Each layer serves specific use cases

This approach aligns with modern **ELT (Extract, Load, Transform)** methodology, where raw data is loaded first and transformed within the data platform using its computational power.

### Why "Medallion"?

The metaphor is quite fitting:
- **Bronze** = Raw material, unprocessed but valuable
- **Silver** = Refined and standardized, ready for various uses
- **Gold** = Polished and purpose-built for premium experiences

Each layer builds upon the previous one, adding value through progressive refinement.

---

## Why Use Medallion Architecture?

### Traditional Challenges We Solve

Over the years, I've seen organizations struggle with these common problems:

**Problem 1: "We lost the raw data"**
- Transformed data at ingestion
- Can't reprocess with new business logic
- No historical audit trail

**Problem 2: "Everything takes forever to load"**
- Complex transformations block data ingestion
- Can't get data to analysts quickly
- Transformation failures delay everything

**Problem 3: "Different teams see different numbers"**
- No single source of truth
- Each team transforms data differently
- Reconciliation nightmares

**Problem 4: "We can't trace where this number came from"**
- No data lineage
- Can't debug issues
- Compliance and audit problems

### How Medallion Architecture Helps

The Medallion approach solves these issues through clear separation of concerns:

✅ **Always have raw data** - Bronze layer preserves everything  
✅ **Fast data ingestion** - Load first, transform later  
✅ **Single source of truth** - Silver layer standardizes across sources  
✅ **Clear data lineage** - Track transformations layer by layer  
✅ **Flexible reprocessing** - Can rebuild Silver and Gold anytime  
✅ **Incremental processing** - Only process changed data  
✅ **Multiple consumer support** - Different teams use different layers  

### Business Benefits

From a business perspective, here's what this means:

- **Faster time to insights** - Analysts get data sooner
- **Better data quality** - Issues caught and fixed systematically
- **Lower costs** - Process only what changed, not everything
- **Easier compliance** - Full audit trail from source to report
- **Greater agility** - Change business logic without re-ingesting data
- **Reduced risk** - Always have raw data to fall back on

---

## Architecture Overview

Here's how the Medallion Architecture looks in Snowflake:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE IN SNOWFLAKE               │
│                         Data Flow Diagram                           │
└─────────────────────────────────────────────────────────────────────┘

SOURCE SYSTEMS                    SNOWFLAKE DATA PLATFORM
═══════════════                   ═══════════════════════

┌──────────────┐                  ┌────────────────────────────────┐
│   CRM        │                  │     BRONZE LAYER               │
│  (Salesforce)│────┐             │   (Raw Data Landing)           │
└──────────────┘    │             │                                │
                    ├────────────>│  ┌──────────────────────────┐  │
┌──────────────┐    │             │  │ bronze_crm               │  │
│   ERP        │    │             │  │ - VARIANT columns        │  │
│  (SAP)       │────┤             │  │ - Full history           │  │
└──────────────┘    │             │  │ - Append-only            │  │
                    │             │  └──────────────────────────┘  │
┌──────────────┐    │             │                                │
│  Web APIs    │────┤             │  ┌──────────────────────────┐  │
└──────────────┘    │             │  │ bronze_transactions      │  │
                    │             │  │ - JSON/Parquet           │  │
┌──────────────┐    │             │  │ - No transformations     │  │
│  Log Files   │────┘             │  └──────────────────────────┘  │
└──────────────┘                  └──────────────┬─────────────────┘
                                                 │
                                                 │ Clean, Dedupe,
                                                 │ Standardize
                                                 v
                                  ┌────────────────────────────────┐
                                  │     SILVER LAYER               │
                                  │   (Cleansed & Standardized)    │
                                  │                                │
                                  │  ┌──────────────────────────┐  │
                                  │  │ silver_customers         │  │
                                  │  │ - Deduplicated           │  │
                                  │  │ - Validated              │  │
                                  │  │ - Type-converted         │  │
                                  │  └──────────────────────────┘  │
                                  │                                │
                                  │  ┌──────────────────────────┐  │
                                  │  │ silver_transactions      │  │
                                  │  │ - Joined entities        │  │
                                  │  │ - Business keys          │  │
                                  │  └──────────────────────────┘  │
                                  └──────────────┬─────────────────┘
                                                 │
                                                 │ Aggregate,
                                                 │ Enrich, Optimize
                                                 v
                                  ┌────────────────────────────────┐
                                  │     GOLD LAYER                 │
                                  │   (Business-Ready Analytics)   │
                                  │                                │
                                  │  ┌──────────────────────────┐  │
                                  │  │ gold_customer_360        │  │
                                  │  │ - Pre-aggregated         │  │
                                  │  │ - Clustered              │  │
                                  │  │ - RBAC applied           │  │
                                  │  └──────────────────────────┘  │
                                  │                                │
                                  │  ┌──────────────────────────┐  │
                                  │  │ gold_monthly_revenue     │  │
                                  │  │ - Fact tables            │  │
                                  │  │ - Dimension tables       │  │
                                  │  └──────────────────────────┘  │
                                  └──────────────┬─────────────────┘
                                                 │
                                                 └────────────────┐
                                                                  │
CONSUMPTION LAYER                                                 │
═════════════════                                                 │
                                                                  │
┌──────────────┐     ┌──────────────┐     ┌──────────────┐      │
│   Tableau    │◄────┤   Power BI   │◄────┤  Looker      │◄─────┘
└──────────────┘     └──────────────┘     └──────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ Data Science │     │   ML Models  │     │  Ad-hoc SQL  │
└──────────────┘     └──────────────┘     └──────────────┘
```

### Layer Comparison

| Layer | Purpose | Data State | Users | Update Frequency |
|-------|---------|------------|-------|------------------|
| **Bronze** | Raw data landing | Unprocessed, as-is | Data Engineers | Real-time / Batch |
| **Silver** | Cleansed data | Deduplicated, validated | Data Engineers, Analysts | Hourly / Daily |
| **Gold** | Business metrics | Aggregated, optimized | Business Users, BI Tools | Daily / Weekly |
| **Platinum** | Advanced analytics | ML features, predictions | Data Scientists | On-demand |

---

## Bronze Layer - Raw Data Landing Zone

The Bronze layer is your digital archive. Think of it as a safety deposit box where you store everything exactly as it arrived, without judgment or modification.

### Purpose and Characteristics

**What Bronze Does:**
- Captures all data from source systems "as-is"
- Preserves complete history for auditability
- Enables reprocessing without re-extracting from sources
- Provides data lineage foundation
- Supports both batch and streaming ingestion

**Key Principles:**
1. **Append-only** - Never delete, only insert
2. **Schema-flexible** - Accept any structure
3. **Full fidelity** - Preserve all source columns
4. **Metadata-rich** - Track when and how data arrived

### Snowflake Implementation

In Snowflake, the Bronze layer typically uses:
- **VARIANT data type** for semi-structured data (JSON, XML, Avro)
- **Regular columns** for structured data
- **Metadata columns** for ingestion tracking
- **Time-travel** for historical queries
- **External stages** for S3/Azure/GCS integration

### Example 1: Bronze Table for CRM Data

```sql
-- Create Bronze database and schema
CREATE DATABASE IF NOT EXISTS bronze_db;
CREATE SCHEMA IF NOT EXISTS bronze_db.crm;

-- Bronze table for customer data from Salesforce
CREATE OR REPLACE TABLE bronze_db.crm.customers (
    -- Source data stored as JSON
    raw_data VARIANT,
    
    -- Metadata columns
    source_system VARCHAR(50) DEFAULT 'salesforce',
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    file_name VARCHAR(500),
    file_row_number NUMBER,
    
    -- Technical columns
    _bronze_id NUMBER AUTOINCREMENT,
    _is_deleted BOOLEAN DEFAULT FALSE
);

-- Load data from S3 stage
COPY INTO bronze_db.crm.customers (raw_data, file_name, file_row_number)
FROM (
    SELECT 
        $1,                    -- Full JSON object
        METADATA$FILENAME,     -- Source file name
        METADATA$FILE_ROW_NUMBER
    FROM @my_s3_stage/crm/customers/
)
FILE_FORMAT = (TYPE = 'JSON')
PATTERN = '.*customers.*json'
ON_ERROR = CONTINUE;
```

### Example 2: Bronze Table for Transactional Data

```sql
-- Bronze table for e-commerce transactions
CREATE OR REPLACE TABLE bronze_db.transactions.orders (
    -- Structured approach for high-volume data
    order_id VARCHAR(100),
    customer_id VARCHAR(100),
    order_date TIMESTAMP_NTZ,
    order_amount NUMBER(18,2),
    currency VARCHAR(3),
    order_status VARCHAR(50),
    
    -- Raw JSON for additional fields
    additional_data VARIANT,
    
    -- Metadata
    source_system VARCHAR(50),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    batch_id VARCHAR(100),
    
    -- Technical
    _bronze_id NUMBER AUTOINCREMENT PRIMARY KEY
);

-- Create stream for change tracking
CREATE STREAM bronze_db.transactions.orders_stream 
    ON TABLE bronze_db.transactions.orders
    APPEND_ONLY = TRUE;
```

### Example 3: Bronze Table for API Data

```sql
-- Bronze table for REST API responses
CREATE OR REPLACE TABLE bronze_db.api.weather_data (
    api_response VARIANT,
    api_endpoint VARCHAR(500),
    http_status_code NUMBER,
    response_time_ms NUMBER,
    request_timestamp TIMESTAMP_NTZ,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Load from API using Snowpipe (continuous ingestion)
CREATE OR REPLACE PIPE bronze_db.api.weather_pipe
AUTO_INGEST = TRUE
AS
COPY INTO bronze_db.api.weather_data
FROM @my_api_stage/weather/
FILE_FORMAT = (TYPE = 'JSON');
```

### Bronze Layer Best Practices

**DO's:**
✅ Store complete source payloads  
✅ Add ingestion timestamps  
✅ Include source system identifiers  
✅ Use VARIANT for flexible schema  
✅ Enable time-travel (default 1 day, extend to 90 for important data)  
✅ Document source system mappings  

**DON'Ts:**
❌ Transform data in Bronze  
❌ Delete historical records  
❌ Apply business rules  
❌ Join multiple sources  
❌ Filter out "bad" data  

### Data Retention

```sql
-- Set data retention for Bronze tables (90 days for audit)
ALTER TABLE bronze_db.crm.customers 
    SET DATA_RETENTION_TIME_IN_DAYS = 90;

-- Enable fail-safe for critical data
-- (automatic 7-day recovery period after retention expires)
```

---

## Silver Layer - Cleaned and Standardized Data

The Silver layer is where data gets its first real makeover. This is where we clean, validate, deduplicate, and standardize data into a consistent enterprise format.

### Purpose and Characteristics

**What Silver Does:**
- Removes duplicates and invalid records
- Converts data types and formats
- Standardizes naming conventions
- Validates against business rules
- Joins related entities
- Creates surrogate keys
- Implements slowly changing dimensions (SCD)

**The "Just Enough" Principle:**

In Silver, we apply *just enough* transformation to make data usable for multiple downstream purposes. We're not yet applying business-specific logic—that comes in Gold. Think of Silver as your "enterprise data warehouse" view.

### Snowflake Implementation

```sql
-- Create Silver database and schema
CREATE DATABASE IF NOT EXISTS silver_db;
CREATE SCHEMA IF NOT EXISTS silver_db.crm;
```

### Example 1: Silver Customer Table

```sql
-- Silver table for cleaned customer data
CREATE OR REPLACE TABLE silver_db.crm.customers (
    -- Surrogate key
    customer_key NUMBER AUTOINCREMENT PRIMARY KEY,
    
    -- Business key from source
    customer_id VARCHAR(100) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    
    -- Cleansed attributes
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    
    -- Standardized address
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country_code VARCHAR(3),
    
    -- Derived attributes
    full_name VARCHAR(255) GENERATED ALWAYS AS (
        CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, ''))
    ),
    
    -- SCD Type 2 columns
    valid_from TIMESTAMP_NTZ NOT NULL,
    valid_to TIMESTAMP_NTZ,
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _bronze_id NUMBER,
    
    -- Data quality flags
    data_quality_score NUMBER(3,2),
    has_missing_email BOOLEAN,
    has_missing_phone BOOLEAN
);

-- Create unique constraint on business key + current record
CREATE UNIQUE INDEX idx_customer_unique 
    ON silver_db.crm.customers(customer_id, source_system, is_current)
    WHERE is_current = TRUE;
```

### Example 2: Transform Bronze to Silver

```sql
-- Transformation logic: Bronze → Silver
CREATE OR REPLACE TASK silver_db.crm.transform_customers
    WAREHOUSE = transform_wh
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('bronze_db.crm.customers_stream')
AS
INSERT INTO silver_db.crm.customers (
    customer_id,
    source_system,
    first_name,
    last_name,
    email,
    phone,
    address_line1,
    city,
    state,
    postal_code,
    country_code,
    valid_from,
    _bronze_id,
    data_quality_score,
    has_missing_email,
    has_missing_phone
)
SELECT
    -- Extract from JSON
    raw_data:id::VARCHAR AS customer_id,
    source_system,
    
    -- Clean and standardize names
    INITCAP(TRIM(raw_data:first_name::VARCHAR)) AS first_name,
    INITCAP(TRIM(raw_data:last_name::VARCHAR)) AS last_name,
    
    -- Validate and clean email
    CASE 
        WHEN raw_data:email::VARCHAR REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
        THEN LOWER(TRIM(raw_data:email::VARCHAR))
        ELSE NULL
    END AS email,
    
    -- Standardize phone number
    REGEXP_REPLACE(raw_data:phone::VARCHAR, '[^0-9]', '') AS phone,
    
    -- Clean address
    TRIM(raw_data:address:street::VARCHAR) AS address_line1,
    INITCAP(TRIM(raw_data:address:city::VARCHAR)) AS city,
    UPPER(TRIM(raw_data:address:state::VARCHAR)) AS state,
    TRIM(raw_data:address:zip::VARCHAR) AS postal_code,
    COALESCE(raw_data:address:country::VARCHAR, 'US') AS country_code,
    
    -- SCD tracking
    ingestion_timestamp AS valid_from,
    
    -- Bronze reference
    _bronze_id,
    
    -- Calculate data quality score (0-1)
    (
        CASE WHEN raw_data:email IS NOT NULL THEN 0.25 ELSE 0 END +
        CASE WHEN raw_data:phone IS NOT NULL THEN 0.25 ELSE 0 END +
        CASE WHEN raw_data:address IS NOT NULL THEN 0.25 ELSE 0 END +
        CASE WHEN raw_data:first_name IS NOT NULL AND raw_data:last_name IS NOT NULL THEN 0.25 ELSE 0 END
    ) AS data_quality_score,
    
    -- Quality flags
    raw_data:email IS NULL AS has_missing_email,
    raw_data:phone IS NULL AS has_missing_phone

FROM bronze_db.crm.customers_stream
WHERE raw_data:id IS NOT NULL  -- Filter out invalid records
  AND _is_deleted = FALSE;

-- Resume the task
ALTER TASK silver_db.crm.transform_customers RESUME;
```

### Example 3: Deduplication Logic

```sql
-- Handle duplicates (keep most recent record)
MERGE INTO silver_db.crm.customers AS target
USING (
    SELECT 
        customer_id,
        source_system,
        first_name,
        last_name,
        email,
        -- ... other columns
        valid_from,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, source_system 
            ORDER BY valid_from DESC
        ) AS row_num
    FROM silver_db.crm.customers_staging
) AS source
ON target.customer_id = source.customer_id
   AND target.source_system = source.source_system
   AND target.is_current = TRUE
   AND source.row_num = 1
WHEN MATCHED AND (
    target.email != source.email OR
    target.phone != source.phone
    -- ... other change detection
) THEN UPDATE SET
    valid_to = source.valid_from,
    is_current = FALSE
WHEN NOT MATCHED THEN INSERT (
    customer_id,
    source_system,
    first_name,
    -- ... all columns
)
VALUES (
    source.customer_id,
    source.source_system,
    source.first_name,
    -- ... all values
);
```

### Silver Layer Transformations

Common transformations in Silver:

1. **Data Type Conversion**
```sql
-- Convert string dates to proper timestamps
TRY_TO_TIMESTAMP(date_string, 'YYYY-MM-DD HH24:MI:SS')

-- Convert string numbers to numeric
TRY_TO_NUMBER(amount_string, 18, 2)
```

2. **Data Cleansing**
```sql
-- Remove special characters
REGEXP_REPLACE(column, '[^A-Za-z0-9 ]', '')

-- Standardize phone numbers
REGEXP_REPLACE(phone, '[^0-9]', '')

-- Trim whitespace
TRIM(column)
```

3. **Data Validation**
```sql
-- Email validation
CASE 
    WHEN email REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
    THEN email
    ELSE NULL
END

-- Date range validation
CASE
    WHEN order_date BETWEEN '2020-01-01' AND CURRENT_DATE()
    THEN order_date
    ELSE NULL
END
```

4. **Standardization**
```sql
-- Country code standardization
CASE 
    WHEN country IN ('USA', 'United States', 'US') THEN 'US'
    WHEN country IN ('UK', 'United Kingdom', 'GB') THEN 'GB'
    ELSE country
END

-- State abbreviation
CASE 
    WHEN state = 'California' THEN 'CA'
    WHEN state = 'New York' THEN 'NY'
    ELSE state
END
```

### Silver Layer Best Practices

**DO's:**
✅ Apply data quality rules  
✅ Create surrogate keys  
✅ Implement SCD Type 2 for history  
✅ Standardize across sources  
✅ Document transformation logic  
✅ Track data lineage  
✅ Add data quality scores  

**DON'Ts:**
❌ Apply business-specific aggregations  
❌ Create department-specific views  
❌ Implement complex business rules  
❌ Over-normalize data  

---

## Gold Layer - Business-Ready Analytics

The Gold layer is where data becomes truly valuable for business users. This is the consumption layer—optimized, aggregated, and packaged for specific business needs.

### Purpose and Characteristics

**What Gold Does:**
- Creates business-specific data marts
- Implements star/snowflake schemas
- Pre-calculates common metrics
- Applies business logic and rules
- Optimizes for query performance
- Implements security and access controls

**Key Difference from Silver:**

While Silver is enterprise-wide and generalized, Gold is purpose-built. Each Gold table/view serves a specific business function or department.

### Snowflake Implementation

```sql
-- Create Gold database and schemas (organized by business domain)
CREATE DATABASE IF NOT EXISTS gold_db;
CREATE SCHEMA IF NOT EXISTS gold_db.sales;
CREATE SCHEMA IF NOT EXISTS gold_db.marketing;
CREATE SCHEMA IF NOT EXISTS gold_db.finance;
CREATE SCHEMA IF NOT EXISTS gold_db.executive;
```

### Example 1: Customer 360 View

```sql
-- Gold table: Complete customer view
CREATE OR REPLACE TABLE gold_db.marketing.customer_360 (
    customer_key NUMBER PRIMARY KEY,
    customer_id VARCHAR(100),
    
    -- Customer profile
    full_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(20),
    customer_segment VARCHAR(50),
    
    -- Customer value metrics
    lifetime_value NUMBER(18,2),
    total_orders NUMBER,
    total_revenue NUMBER(18,2),
    average_order_value NUMBER(18,2),
    
    -- Behavioral metrics
    first_order_date DATE,
    last_order_date DATE,
    days_since_last_order NUMBER,
    order_frequency_days NUMBER,
    
    -- Engagement scores
    engagement_score NUMBER(3,2),
    churn_risk_score NUMBER(3,2),
    
    -- RFM analysis
    recency_score NUMBER,
    frequency_score NUMBER,
    monetary_score NUMBER,
    rfm_segment VARCHAR(20),
    
    -- Demographics
    age_group VARCHAR(20),
    location_city VARCHAR(100),
    location_state VARCHAR(50),
    
    -- Preferences
    preferred_category VARCHAR(100),
    preferred_channel VARCHAR(50),
    
    -- Status
    is_active BOOLEAN,
    customer_status VARCHAR(50),
    
    -- Metadata
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (customer_segment, location_state);

-- Populate Customer 360
CREATE OR REPLACE TASK gold_db.marketing.refresh_customer_360
    WAREHOUSE = analytics_wh
    SCHEDULE = 'USING CRON 0 2 * * * America/Los_Angeles'  -- Daily at 2 AM
AS
INSERT OVERWRITE gold_db.marketing.customer_360
SELECT
    c.customer_key,
    c.customer_id,
    c.full_name,
    c.email,
    c.phone,
    
    -- Segment customers based on behavior
    CASE
        WHEN total_revenue >= 10000 THEN 'VIP'
        WHEN total_revenue >= 5000 THEN 'High Value'
        WHEN total_revenue >= 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment,
    
    -- Calculate customer metrics
    COALESCE(SUM(o.order_amount), 0) AS lifetime_value,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(o.order_amount), 0) AS total_revenue,
    COALESCE(AVG(o.order_amount), 0) AS average_order_value,
    
    -- Temporal metrics
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date,
    DATEDIFF(day, MAX(o.order_date), CURRENT_DATE()) AS days_since_last_order,
    CASE 
        WHEN COUNT(DISTINCT o.order_id) > 1 
        THEN DATEDIFF(day, MIN(o.order_date), MAX(o.order_date)) / COUNT(DISTINCT o.order_id)
        ELSE NULL
    END AS order_frequency_days,
    
    -- Engagement score (0-1)
    LEAST(1, (
        CASE WHEN DATEDIFF(day, MAX(o.order_date), CURRENT_DATE()) <= 30 THEN 0.4 ELSE 0 END +
        CASE WHEN COUNT(DISTINCT o.order_id) >= 5 THEN 0.3 ELSE COUNT(DISTINCT o.order_id) * 0.06 END +
        CASE WHEN COALESCE(SUM(o.order_amount), 0) >= 1000 THEN 0.3 ELSE COALESCE(SUM(o.order_amount), 0) / 1000 * 0.3 END
    )) AS engagement_score,
    
    -- Churn risk (higher = more risk)
    CASE
        WHEN DATEDIFF(day, MAX(o.order_date), CURRENT_DATE()) > 180 THEN 0.9
        WHEN DATEDIFF(day, MAX(o.order_date), CURRENT_DATE()) > 90 THEN 0.6
        WHEN DATEDIFF(day, MAX(o.order_date), CURRENT_DATE()) > 60 THEN 0.3
        ELSE 0.1
    END AS churn_risk_score,
    
    -- RFM scores (1-5 scale)
    NTILE(5) OVER (ORDER BY DATEDIFF(day, MAX(o.order_date), CURRENT_DATE()) DESC) AS recency_score,
    NTILE(5) OVER (ORDER BY COUNT(DISTINCT o.order_id)) AS frequency_score,
    NTILE(5) OVER (ORDER BY COALESCE(SUM(o.order_amount), 0)) AS monetary_score,
    
    -- RFM segment
    CASE
        WHEN recency_score >= 4 AND frequency_score >= 4 THEN 'Champions'
        WHEN recency_score >= 3 AND frequency_score >= 3 THEN 'Loyal'
        WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'Promising'
        WHEN recency_score <= 2 AND frequency_score >= 4 THEN 'At Risk'
        WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Lost'
        ELSE 'Regular'
    END AS rfm_segment,
    
    -- Demographics
    CASE
        WHEN DATEDIFF(year, c.birth_date, CURRENT_DATE()) < 25 THEN '18-24'
        WHEN DATEDIFF(year, c.birth_date, CURRENT_DATE()) < 35 THEN '25-34'
        WHEN DATEDIFF(year, c.birth_date, CURRENT_DATE()) < 45 THEN '35-44'
        WHEN DATEDIFF(year, c.birth_date, CURRENT_DATE()) < 55 THEN '45-54'
        WHEN DATEDIFF(year, c.birth_date, CURRENT_DATE()) < 65 THEN '55-64'
        ELSE '65+'
    END AS age_group,
    
    c.city AS location_city,
    c.state AS location_state,
    
    -- Preferences (most frequent)
    MODE(p.category) AS preferred_category,
    MODE(o.order_channel) AS preferred_channel,
    
    -- Status
    DATEDIFF(day, MAX(o.order_date), CURRENT_DATE()) <= 90 AS is_active,
    CASE
        WHEN COUNT(o.order_id) = 0 THEN 'Never Purchased'
        WHEN DATEDIFF(day, MAX(o.order_date), CURRENT_DATE()) > 180 THEN 'Inactive'
        WHEN DATEDIFF(day, MAX(o.order_date), CURRENT_DATE()) > 90 THEN 'At Risk'
        ELSE 'Active'
    END AS customer_status,
    
    CURRENT_TIMESTAMP() AS last_updated

FROM silver_db.crm.customers c
LEFT JOIN silver_db.transactions.orders o 
    ON c.customer_id = o.customer_id AND o.is_current = TRUE
LEFT JOIN silver_db.product.products p
    ON o.product_id = p.product_id
WHERE c.is_current = TRUE
GROUP BY 
    c.customer_key, c.customer_id, c.full_name, c.email, c.phone,
    c.birth_date, c.city, c.state;
```

### Example 2: Sales Dashboard Fact Table

```sql
-- Gold fact table: Daily sales metrics
CREATE OR REPLACE TABLE gold_db.sales.fact_daily_sales (
    -- Date dimension
    sale_date DATE NOT NULL,
    year NUMBER,
    quarter NUMBER,
    month NUMBER,
    day_of_week VARCHAR(10),
    
    -- Dimension keys
    customer_key NUMBER,
    product_key NUMBER,
    store_key NUMBER,
    
    -- Measures
    order_count NUMBER,
    total_revenue NUMBER(18,2),
    total_cost NUMBER(18,2),
    total_profit NUMBER(18,2),
    avg_order_value NUMBER(18,2),
    units_sold NUMBER,
    
    -- Calculated metrics
    profit_margin NUMBER(5,2),
    revenue_per_customer NUMBER(18,2),
    
    PRIMARY KEY (sale_date, customer_key, product_key, store_key)
)
CLUSTER BY (sale_date);

-- Create materialized view for real-time aggregation
CREATE OR REPLACE MATERIALIZED VIEW gold_db.sales.mv_current_day_sales AS
SELECT
    CURRENT_DATE() AS sale_date,
    customer_key,
    product_key,
    store_key,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(order_amount) AS total_revenue,
    SUM(cost_amount) AS total_cost,
    SUM(order_amount - cost_amount) AS total_profit,
    AVG(order_amount) AS avg_order_value,
    SUM(quantity) AS units_sold
FROM silver_db.transactions.orders
WHERE order_date = CURRENT_DATE()
GROUP BY customer_key, product_key, store_key;
```

### Example 3: Executive Dashboard

```sql
-- Gold view: Executive KPIs
CREATE OR REPLACE SECURE VIEW gold_db.executive.v_executive_kpis AS
WITH monthly_metrics AS (
    SELECT
        DATE_TRUNC('month', sale_date) AS month,
        SUM(total_revenue) AS monthly_revenue,
        SUM(total_profit) AS monthly_profit,
        SUM(order_count) AS monthly_orders,
        COUNT(DISTINCT customer_key) AS active_customers
    FROM gold_db.sales.fact_daily_sales
    WHERE sale_date >= DATEADD(month, -12, CURRENT_DATE())
    GROUP BY DATE_TRUNC('month', sale_date)
)
SELECT
    month,
    monthly_revenue,
    monthly_profit,
    monthly_orders,
    active_customers,
    
    -- Growth metrics
    LAG(monthly_revenue) OVER (ORDER BY month) AS prev_month_revenue,
    (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY month)) / 
        LAG(monthly_revenue) OVER (ORDER BY month) * 100 AS revenue_growth_pct,
    
    -- Year-over-year
    LAG(monthly_revenue, 12) OVER (ORDER BY month) AS same_month_last_year,
    (monthly_revenue - LAG(monthly_revenue, 12) OVER (ORDER BY month)) / 
        LAG(monthly_revenue, 12) OVER (ORDER BY month) * 100 AS yoy_growth_pct,
    
    -- Customer metrics
    monthly_revenue / NULLIF(active_customers, 0) AS revenue_per_customer,
    monthly_profit / NULLIF(monthly_revenue, 0) * 100 AS profit_margin_pct

FROM monthly_metrics
ORDER BY month DESC;

-- Grant access to executives only
GRANT SELECT ON gold_db.executive.v_executive_kpis TO ROLE executive;
```

### Gold Layer Best Practices

**DO's:**
✅ Organize by business domain (sales, marketing, finance)  
✅ Pre-aggregate common queries  
✅ Use clustering keys for performance  
✅ Implement star/snowflake schemas  
✅ Apply Row-Level Security  
✅ Create secure views for sensitive data  
✅ Use materialized views for complex calculations  

**DON'Ts:**
❌ Store raw or uncleaned data  
❌ Duplicate Silver layer logic  
❌ Create overly complex joins at query time  
❌ Skip performance optimization  

---

## Platinum Layer - Advanced Analytics (Optional)

The Platinum layer is an optional extension for organizations with advanced analytics and machine learning needs. This layer goes beyond standard BI reporting to support predictive modeling, AI features, and real-time decisioning.

### Purpose

**Platinum is for:**
- Machine learning feature stores
- Real-time prediction scoring
- Complex feature engineering
- Model training datasets
- A/B testing frameworks

### Example: ML Feature Store

```sql
CREATE DATABASE IF NOT EXISTS platinum_db;
CREATE SCHEMA IF NOT EXISTS platinum_db.ml_features;

-- Platinum table: Customer churn prediction features
CREATE OR REPLACE TABLE platinum_db.ml_features.customer_churn_features (
    customer_key NUMBER PRIMARY KEY,
    
    -- Temporal features
    days_since_first_purchase NUMBER,
    days_since_last_purchase NUMBER,
    purchase_frequency NUMBER(10,2),
    
    -- Behavioral features
    total_purchases NUMBER,
    total_revenue NUMBER(18,2),
    avg_order_value NUMBER(18,2),
    std_order_value NUMBER(18,2),
    
    -- Engagement features
    email_open_rate NUMBER(5,2),
    email_click_rate NUMBER(5,2),
    support_tickets_count NUMBER,
    
    -- Product affinity features
    distinct_categories_purchased NUMBER,
    most_purchased_category VARCHAR(100),
    category_diversity_score NUMBER(3,2),
    
    -- Target variable
    churned BOOLEAN,
    churn_probability NUMBER(3,2),
    
    -- Metadata
    feature_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    model_version VARCHAR(20)
);
```

---

## Semantic Layer - Business User Interface

The Semantic layer sits on top of Gold (and sometimes Platinum) to provide a business-friendly interface. It translates technical database concepts into business terminology.

### Purpose

Instead of users writing SQL against tables, they interact with business-friendly metrics and dimensions through BI tools.

### Example: Semantic Views

```sql
CREATE SCHEMA IF NOT EXISTS gold_db.semantic;

-- Semantic view: Business-friendly sales metrics
CREATE OR REPLACE VIEW gold_db.semantic.v_sales_metrics AS
SELECT
    sale_date AS "Date",
    c.full_name AS "Customer Name",
    c.customer_segment AS "Customer Segment",
    p.product_name AS "Product Name",
    p.category AS "Product Category",
    s.store_name AS "Store Name",
    s.region AS "Store Region",
    
    -- Business metrics (with friendly names)
    f.total_revenue AS "Total Revenue",
    f.total_profit AS "Total Profit",
    f.profit_margin AS "Profit Margin %",
    f.order_count AS "Number of Orders",
    f.units_sold AS "Units Sold",
    f.avg_order_value AS "Average Order Value"
    
FROM gold_db.sales.fact_daily_sales f
JOIN silver_db.crm.customers c ON f.customer_key = c.customer_key
JOIN silver_db.product.products p ON f.product_key = p.product_key
JOIN silver_db.location.stores s ON f.store_key = s.store_key
WHERE c.is_current = TRUE;

-- Grant to business users
GRANT SELECT ON gold_db.semantic.v_sales_metrics TO ROLE business_analyst;
```

---

## Implementation in Snowflake

### Complete Implementation Steps

#### Step 1: Database Structure Setup

```sql
-- Create all databases
CREATE DATABASE bronze_db COMMENT = 'Raw data landing zone';
CREATE DATABASE silver_db COMMENT = 'Cleansed and standardized data';
CREATE DATABASE gold_db COMMENT = 'Business-ready analytics layer';

-- Create schemas within each database (organized by source/domain)
-- Bronze: Organized by source system
CREATE SCHEMA bronze_db.salesforce;
CREATE SCHEMA bronze_db.sap;
CREATE SCHEMA bronze_db.web_api;

-- Silver: Organized by entity type
CREATE SCHEMA silver_db.customer;
CREATE SCHEMA silver_db.product;
CREATE SCHEMA silver_db.transactions;

-- Gold: Organized by business function
CREATE SCHEMA gold_db.sales;
CREATE SCHEMA gold_db.marketing;
CREATE SCHEMA gold_db.finance;
```

#### Step 2: Warehouse Configuration

```sql
-- Create dedicated warehouses for each layer
CREATE WAREHOUSE bronze_wh
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for Bronze layer ingestion';

CREATE WAREHOUSE silver_wh
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for Silver layer transformations';

CREATE WAREHOUSE gold_wh
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 600
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for Gold layer analytics';
```

#### Step 3: Implement Incremental Processing

```sql
-- Enable change tracking on Bronze tables
ALTER TABLE bronze_db.salesforce.customers 
    SET CHANGE_TRACKING = TRUE;

-- Create streams for incremental processing
CREATE STREAM silver_db.customer.bronze_customers_stream
    ON TABLE bronze_db.salesforce.customers
    APPEND_ONLY = FALSE;

-- Create scheduled tasks for automated transformation
CREATE TASK silver_db.customer.process_bronze_to_silver
    WAREHOUSE = silver_wh
    SCHEDULE = '15 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('silver_db.customer.bronze_customers_stream')
AS
    -- transformation SQL here
    INSERT INTO silver_db.customer.customers (...)
    SELECT ... FROM silver_db.customer.bronze_customers_stream;

ALTER TASK silver_db.customer.process_bronze_to_silver RESUME;
```

#### Step 4: Implement Data Governance

```sql
-- Apply tagging for data classification
ALTER TABLE silver_db.customer.customers 
    SET TAG governance.tags.data_sensitivity = 'CONFIDENTIAL',
        governance.tags.data_domain = 'CUSTOMER';

-- Apply masking policies to PII columns
ALTER TABLE silver_db.customer.customers
    MODIFY COLUMN email SET TAG governance.tags.pii_email = 'true',
    MODIFY COLUMN phone SET TAG governance.tags.pii_phone = 'true';

-- Implement Row-Level Security
CREATE ROW ACCESS POLICY gold_db.policies.regional_access AS (region VARCHAR) 
RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'EXECUTIVE') THEN TRUE
        WHEN CURRENT_ROLE() = 'SALES_MANAGER_WEST' AND region = 'WEST' THEN TRUE
        WHEN CURRENT_ROLE() = 'SALES_MANAGER_EAST' AND region = 'EAST' THEN TRUE
        ELSE FALSE
    END;

ALTER TABLE gold_db.sales.fact_daily_sales
    ADD ROW ACCESS POLICY gold_db.policies.regional_access ON (store_region);
```

---

## Real-World Use Cases

### Use Case 1: E-Commerce Platform

**Scenario:** Large online retailer with multiple data sources

**Bronze Layer:**
- Raw order data from e-commerce platform (JSON)
- Customer clickstream data (Parquet)
- Inventory updates from ERP (CSV)
- Payment transactions from payment gateway (JSON)

**Silver Layer:**
- Deduplicated customer profiles
- Validated order transactions
- Standardized product catalog
- Reconciled inventory levels

**Gold Layer:**
- Customer 360 view for marketing
- Daily sales dashboard for executives
- Inventory forecasting for supply chain
- Product recommendation features for website

**Result:** 
- 60% faster data availability for analysts
- Single source of truth for customer data
- Real-time inventory visibility
- Personalized product recommendations

### Use Case 2: Financial Services

**Scenario:** Bank with regulatory compliance requirements

**Bronze Layer:**
- Transaction logs from core banking system
- Customer data from CRM
- Credit bureau data feeds
- ATM/branch transaction logs

**Silver Layer:**
- Consolidated customer accounts
- Validated transactions with fraud flags
- Credit risk metrics
- Regulatory reporting datasets

**Gold Layer:**
- Customer risk profiles
- Anti-money laundering (AML) dashboards
- Regulatory compliance reports
- Customer lifetime value analysis

**Result:**
- Full audit trail from raw data to reports
- Faster regulatory reporting (days to hours)
- Improved fraud detection
- Better customer risk assessment

### Use Case 3: Healthcare Analytics

**Scenario:** Hospital system with patient care focus

**Bronze Layer:**
- Electronic Health Records (HL7/FHIR)
- Claims data from insurance
- Lab results (various formats)
- Patient monitoring devices (IoT data)

**Silver Layer:**
- De-identified patient records
- Standardized diagnosis codes (ICD-10)
- Reconciled claims and treatments
- Validated lab results

**Gold Layer:**
- Patient outcomes dashboards
- Treatment effectiveness analysis
- Cost per patient analysis
- Readmission risk predictions

**Result:**
- HIPAA-compliant data storage
- Improved patient outcomes through analytics
- Reduced readmission rates
- Better resource allocation

---

## Best Practices

### 1. Data Quality Management

```sql
-- Add data quality checks in Silver layer
CREATE OR REPLACE PROCEDURE silver_db.utils.validate_customer_data()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Check for duplicates
    INSERT INTO silver_db.quality.validation_log
    SELECT 
        CURRENT_TIMESTAMP() AS check_time,
        'customer' AS table_name,
        'duplicate_check' AS check_type,
        COUNT(*) AS issue_count
    FROM (
        SELECT customer_id, source_system, COUNT(*) as cnt
        FROM silver_db.customer.customers
        WHERE is_current = TRUE
        GROUP BY customer_id, source_system
        HAVING COUNT(*) > 1
    );
    
    -- Check for missing emails
    INSERT INTO silver_db.quality.validation_log
    SELECT 
        CURRENT_TIMESTAMP(),
        'customer',
        'missing_email',
        COUNT(*)
    FROM silver_db.customer.customers
    WHERE is_current = TRUE AND email IS NULL;
    
    RETURN 'Validation complete';
END;
$$;
```

### 2. Cost Optimization

- **Use clustering keys** on large Gold tables
- **Implement data retention policies** on Bronze (e.g., keep 90 days)
- **Use materialized views** judiciously (they consume storage)
- **Right-size warehouses** for each layer
- **Schedule tasks during off-peak hours**

```sql
-- Set retention policy
ALTER TABLE bronze_db.salesforce.customers 
    SET DATA_RETENTION_TIME_IN_DAYS = 7;  -- Bronze: short retention

ALTER TABLE silver_db.customer.customers 
    SET DATA_RETENTION_TIME_IN_DAYS = 30; -- Silver: medium retention

ALTER TABLE gold_db.sales.fact_daily_sales 
    SET DATA_RETENTION_TIME_IN_DAYS = 90; -- Gold: longer retention
```

### 3. Performance Optimization

```sql
-- Cluster large Gold tables
ALTER TABLE gold_db.sales.fact_daily_sales 
    CLUSTER BY (sale_date, customer_key);

-- Use search optimization for frequent lookups
ALTER TABLE gold_db.marketing.customer_360 
    ADD SEARCH OPTIMIZATION ON EQUALITY(customer_id, email);

-- Create materialized views for complex aggregations
CREATE MATERIALIZED VIEW gold_db.sales.mv_monthly_metrics AS
SELECT
    DATE_TRUNC('month', sale_date) AS month,
    customer_key,
    SUM(total_revenue) AS monthly_revenue,
    COUNT(DISTINCT order_id) AS order_count
FROM gold_db.sales.fact_daily_sales
GROUP BY DATE_TRUNC('month', sale_date), customer_key;
```

### 4. Monitoring and Observability

```sql
-- Create monitoring views
CREATE OR REPLACE VIEW admin.v_pipeline_health AS
SELECT
    CURRENT_TIMESTAMP() AS check_time,
    'bronze_to_silver' AS pipeline,
    DATEDIFF(minute, MAX(ingestion_timestamp), CURRENT_TIMESTAMP()) AS minutes_since_last_load
FROM bronze_db.salesforce.customers
UNION ALL
SELECT
    CURRENT_TIMESTAMP(),
    'silver_to_gold',
    DATEDIFF(minute, MAX(updated_timestamp), CURRENT_TIMESTAMP())
FROM silver_db.customer.customers;

-- Alert if data is stale
CREATE ALERT admin.alert_stale_data
    WAREHOUSE = admin_wh
    SCHEDULE = '30 MINUTE'
    IF (EXISTS (
        SELECT 1 FROM admin.v_pipeline_health
        WHERE minutes_since_last_load > 120
    ))
    THEN CALL SYSTEM$SEND_EMAIL(...);
```

### 5. Documentation Standards

Maintain comprehensive documentation:

```sql
-- Document tables with comments
COMMENT ON TABLE bronze_db.salesforce.customers IS 
    'Raw customer data from Salesforce CRM. 
     Ingested via API every 15 minutes.
     Contains PII - handle according to privacy policy.
     Source: Salesforce REST API v52.0';

COMMENT ON COLUMN silver_db.customer.customers.data_quality_score IS
    'Data quality score (0-1) based on completeness of email, phone, address, and name fields.
     Score >= 0.75 indicates high quality record.
     Calculated in transformation: bronze_to_silver_customers';
```

---

## Summary

Medallion Architecture provides a proven framework for organizing data in Snowflake, delivering:

✅ **Clear Data Lineage** - Track data from source to report  
✅ **Improved Data Quality** - Progressive refinement catches issues early  
✅ **Faster Time to Value** - Get data to analysts quickly  
✅ **Flexibility** - Reprocess without re-ingesting  
✅ **Cost Efficiency** - Process only what changed  
✅ **Better Governance** - Consistent standards across organization  
✅ **Scalability** - Grows with your data needs  

### Layer Recap

| Layer | Purpose | Who Uses It | Update Frequency |
|-------|---------|-------------|------------------|
| **Bronze** | Raw data archive | Data Engineers | Real-time/Batch |
| **Silver** | Enterprise data warehouse | Data Engineers, Analysts | Hourly/Daily |
| **Gold** | Business analytics | Business Users, BI Tools | Daily/Weekly |
| **Platinum** | Advanced analytics | Data Scientists | On-demand |
| **Semantic** | Business interface | All business users | Real-time views |

### Getting Started

Start your Medallion implementation in Snowflake:

1. **Week 1:** Set up database structure and ingestion to Bronze
2. **Week 2-3:** Build Silver transformation pipelines
3. **Week 4:** Create first Gold marts for priority use cases
4. **Week 5-8:** Expand coverage and optimize performance
5. **Ongoing:** Monitor, refine, and expand

The beauty of Medallion Architecture is that you can start small with one source system and one use case, then expand iteratively as you prove value.

---

**Questions or need help with implementation?**  
Contact: Venkannababu Thatavarthi  
Email: venkannababu.t@company.com

---

**Document Version:** 1.0  
**Last Updated:** October 15, 2025  
**Status:** Production Ready for Customer Presentation