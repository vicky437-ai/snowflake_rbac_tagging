# Snowflake Security & Data Protection Guide

## Part 2: Data Protection, Sharing, Governance & Best Practices

**Version:** 2025.1  
**Scope:** Multi-Customer Educational Documentation  
**Editions Covered:** Standard, Enterprise, Business Critical

---

# Section 7: Data Storage & Protection

## 7.1 Micro-Partitions

### Overview
Micro-partitions are Snowflake's fundamental storage unit. Data is automatically organized into immutable, compressed columnar files (50-500 MB each). This architecture enables Time Travel, Fail-safe, zero-copy cloning, and efficient query pruning without manual partitioning.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Micro-Partitions | ✓ | ✓ | ✓ |
| Automatic Clustering | ✓ | ✓ | ✓ |
| Query Pruning | ✓ | ✓ | ✓ |
| Columnar Compression | ✓ | ✓ | ✓ |

### Micro-Partition Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    MICRO-PARTITION STRUCTURE                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  TABLE: SALES_DATA                                              │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    MICRO-PARTITIONS                      │    │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐    │    │
│  │  │   MP-1   │ │   MP-2   │ │   MP-3   │ │   MP-4   │    │    │
│  │  │ 50-500MB │ │ 50-500MB │ │ 50-500MB │ │ 50-500MB │    │    │
│  │  │ Compressed│ │ Compressed│ │ Compressed│ │ Compressed│   │    │
│  │  │ Encrypted │ │ Encrypted │ │ Encrypted │ │ Encrypted │   │    │
│  │  │ Immutable │ │ Immutable │ │ Immutable │ │ Immutable │   │    │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  EACH MICRO-PARTITION CONTAINS:                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐            │    │
│  │  │ COL_A  │ │ COL_B  │ │ COL_C  │ │ COL_D  │  Columns   │    │
│  │  │ values │ │ values │ │ values │ │ values │  stored    │    │
│  │  │        │ │        │ │        │ │        │  together  │    │
│  │  └────────┘ └────────┘ └────────┘ └────────┘            │    │
│  │                                                          │    │
│  │  METADATA (for pruning):                                 │    │
│  │  • MIN/MAX values per column                             │    │
│  │  • NULL count                                            │    │
│  │  • Distinct value count                                  │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  BENEFITS:                                                      │
│  • Automatic organization (no manual partitioning)              │
│  • Efficient compression (columnar, similar values together)    │
│  • Fast pruning (metadata eliminates irrelevant partitions)     │
│  • Foundation for Time Travel (immutable = keep old versions)   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Key Benefits

| Benefit | Description |
|---------|-------------|
| **No Manual Partitioning** | Data automatically organized |
| **Efficient Compression** | Columnar storage, 4-8x compression |
| **Query Pruning** | Metadata used to skip irrelevant data |
| **Immutability** | Enables Time Travel and consistency |
| **Parallel Processing** | Each partition processed independently |

### SQL Examples

```sql
-- View table clustering information
SELECT SYSTEM$CLUSTERING_INFORMATION('MY_DATABASE.MY_SCHEMA.MY_TABLE');

-- View table storage metrics
SELECT 
  TABLE_NAME,
  ROW_COUNT,
  BYTES,
  ACTIVE_BYTES,
  TIME_TRAVEL_BYTES,
  FAILSAFE_BYTES,
  CLUSTERING_KEY
FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE TABLE_SCHEMA = 'MY_SCHEMA';
```

### Why This Matters
Micro-partitions eliminate manual partition management, provide automatic performance optimization, and enable unique Snowflake features like Time Travel and zero-copy cloning.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions

---

## 7.2 Time Travel

### Overview
Time Travel enables querying, cloning, or restoring data from any point within the retention period. Access historical versions of tables after updates, deletes, or drops. Retention ranges from 0-1 day (Standard) to 0-90 days (Enterprise+).

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Time Travel (1 day max) | ✓ | - | - |
| Time Travel (90 days max) | - | ✓ | ✓ |
| UNDROP capability | ✓ | ✓ | ✓ |
| Clone from past | ✓ | ✓ | ✓ |

### Retention Periods

| Edition | Default | Maximum |
|---------|---------|---------|
| Standard | 1 day | 1 day |
| Enterprise | 1 day | 90 days |
| Business Critical | 1 day | 90 days |

### Time Travel Methods

```
┌─────────────────────────────────────────────────────────────────┐
│                     TIME TRAVEL METHODS                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  METHOD 1: AT (exact point in time)                             │
│  ─────────────────────────────────────                          │
│  SELECT * FROM table AT(TIMESTAMP => '2025-01-10 14:30:00')     │
│  SELECT * FROM table AT(OFFSET => -3600)  -- 1 hour ago         │
│  SELECT * FROM table AT(STATEMENT => 'query-id')                │
│                                                                  │
│  METHOD 2: BEFORE (just before point)                           │
│  ────────────────────────────────────────                       │
│  SELECT * FROM table BEFORE(TIMESTAMP => '2025-01-10 14:30:00') │
│  SELECT * FROM table BEFORE(STATEMENT => 'query-id')            │
│                                                                  │
│  TIME TRAVEL TIMELINE:                                          │
│                                                                  │
│  Past ◄──────────────────────────────────────────────► Present  │
│                                                                  │
│  ┌─────┐    ┌─────┐    ┌─────┐    ┌─────┐    ┌─────┐           │
│  │Day-7│    │Day-5│    │Day-3│    │Day-1│    │Today│           │
│  └─────┘    └─────┘    └─────┘    └─────┘    └─────┘           │
│     │          │          │          │          │               │
│     └──────────┴──────────┴──────────┴──────────┘               │
│              Time Travel Period (Configurable)                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### SQL Examples

```sql
-- Query data at specific timestamp
SELECT * 
FROM MY_DATABASE.MY_SCHEMA.MY_TABLE 
AT(TIMESTAMP => '2025-01-10 14:30:00'::TIMESTAMP_LTZ);

-- Query data from 1 hour ago
SELECT * 
FROM MY_DATABASE.MY_SCHEMA.MY_TABLE 
AT(OFFSET => -3600);

-- Query data before a specific query was executed
SELECT * 
FROM MY_DATABASE.MY_SCHEMA.MY_TABLE 
BEFORE(STATEMENT => '01abc123-0000-0000-0000-000000000000');

-- Set retention period for table (Enterprise+)
ALTER TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE 
  SET DATA_RETENTION_TIME_IN_DAYS = 30;

-- Set retention period for schema
ALTER SCHEMA MY_DATABASE.MY_SCHEMA 
  SET DATA_RETENTION_TIME_IN_DAYS = 14;

-- Set retention period for database
ALTER DATABASE MY_DATABASE 
  SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- View current retention setting
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE MY_TABLE;

-- UNDROP: Restore dropped table
DROP TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE;
-- Oops! Bring it back:
UNDROP TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE;

-- UNDROP: Restore dropped schema
DROP SCHEMA MY_DATABASE.MY_SCHEMA;
UNDROP SCHEMA MY_DATABASE.MY_SCHEMA;

-- UNDROP: Restore dropped database
DROP DATABASE MY_DATABASE;
UNDROP DATABASE MY_DATABASE;

-- Clone table from historical point
CREATE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE_BACKUP 
  CLONE MY_DATABASE.MY_SCHEMA.MY_TABLE
  AT(TIMESTAMP => '2025-01-10 14:30:00'::TIMESTAMP_LTZ);

-- Restore table to previous state
CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE 
  CLONE MY_DATABASE.MY_SCHEMA.MY_TABLE
  AT(OFFSET => -86400);  -- Restore to 24 hours ago

-- View Time Travel storage costs
SELECT 
  TABLE_NAME,
  ACTIVE_BYTES / POWER(1024, 3) AS ACTIVE_GB,
  TIME_TRAVEL_BYTES / POWER(1024, 3) AS TIME_TRAVEL_GB,
  FAILSAFE_BYTES / POWER(1024, 3) AS FAILSAFE_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE TABLE_SCHEMA = 'MY_SCHEMA'
ORDER BY TIME_TRAVEL_BYTES DESC;
```

### Best Practices

1. **Set appropriate retention** - Balance protection vs storage cost
2. **Use longer retention for critical tables** - Production data
3. **Shorter retention for staging** - Transient data
4. **Clone before major changes** - Safety net for migrations
5. **Monitor Time Travel storage** - Avoid unexpected costs

### Why This Matters
Time Travel provides instant data recovery without backups, enables point-in-time analysis, and supports compliance requirements for data versioning.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/data-time-travel

---

## 7.3 Fail-safe

### Overview
Fail-safe provides an additional 7-day recovery period AFTER Time Travel expires. This is a disaster recovery feature managed exclusively by Snowflake Support. Customers cannot access Fail-safe data directly.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Fail-safe (7 days) | ✓ | ✓ | ✓ |
| Snowflake Support Recovery | ✓ | ✓ | ✓ |

### Time Travel + Fail-safe Timeline

```
┌─────────────────────────────────────────────────────────────────┐
│              DATA PROTECTION TIMELINE                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Data Modified/Deleted                                          │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    TIME TRAVEL PERIOD                     │   │
│  │              (0-90 days, configurable)                    │   │
│  │                                                           │   │
│  │  • Customer can query historical data                     │   │
│  │  • Customer can UNDROP objects                            │   │
│  │  • Customer can clone from past                           │   │
│  │  • Self-service recovery                                  │   │
│  └──────────────────────────────────────────────────────────┘   │
│         │                                                        │
│         ▼ Time Travel expires                                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    FAIL-SAFE PERIOD                       │   │
│  │                      (7 days, fixed)                      │   │
│  │                                                           │   │
│  │  • Customer CANNOT access directly                        │   │
│  │  • Recovery via Snowflake Support ONLY                    │   │
│  │  • Best-effort disaster recovery                          │   │
│  │  • Data still protected in storage                        │   │
│  └──────────────────────────────────────────────────────────┘   │
│         │                                                        │
│         ▼ Fail-safe expires                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                 DATA PERMANENTLY DELETED                  │   │
│  │                   (No recovery possible)                  │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Tables Without Fail-safe

| Table Type | Time Travel | Fail-safe | Use Case |
|------------|:-----------:|:---------:|----------|
| Permanent | ✓ | ✓ | Production data |
| Transient | ✓ | ✗ | Staging, ETL |
| Temporary | ✓ | ✗ | Session-specific |

### SQL Examples

```sql
-- Create transient table (no Fail-safe, reduced costs)
CREATE TRANSIENT TABLE MY_DATABASE.MY_SCHEMA.STAGING_DATA (
  ID INT,
  DATA VARCHAR
);

-- Create temporary table (session-only, no Fail-safe)
CREATE TEMPORARY TABLE SESSION_TEMP (
  ID INT,
  DATA VARCHAR
);

-- View Fail-safe storage usage
SELECT 
  TABLE_NAME,
  FAILSAFE_BYTES / POWER(1024, 3) AS FAILSAFE_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE FAILSAFE_BYTES > 0
ORDER BY FAILSAFE_BYTES DESC;

-- Convert permanent table to transient (drop Fail-safe)
-- Note: Must recreate table
CREATE TRANSIENT TABLE MY_TABLE_NEW CLONE MY_TABLE;
DROP TABLE MY_TABLE;
ALTER TABLE MY_TABLE_NEW RENAME TO MY_TABLE;
```

### Why This Matters
Fail-safe is the last line of defense against data loss. Even after Time Travel expires, Snowflake can recover data for 7 additional days in disaster scenarios.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/data-failsafe

---

## 7.4 Cloning (Zero-Copy Clone)

### Overview
Cloning creates instant copies of databases, schemas, or tables without duplicating data. Clones share underlying micro-partitions with the source until data diverges. This enables rapid environment provisioning and testing.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Zero-Copy Cloning | ✓ | ✓ | ✓ |
| Clone with Time Travel | ✓ | ✓ | ✓ |
| Clone with COPY GRANTS | ✓ | ✓ | ✓ |

### Zero-Copy Clone Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    ZERO-COPY CLONE PROCESS                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  STEP 1: BEFORE CLONE                                           │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  SOURCE TABLE                                            │    │
│  │  ┌──────┐ ┌──────┐ ┌──────┐                             │    │
│  │  │ MP-1 │ │ MP-2 │ │ MP-3 │  (Micro-partitions)         │    │
│  │  └──────┘ └──────┘ └──────┘                             │    │
│  │     │        │        │                                  │    │
│  │     └────────┴────────┴───► Storage: 3 units            │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  STEP 2: IMMEDIATELY AFTER CLONE (metadata only, instant)       │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  SOURCE TABLE          CLONE TABLE                       │    │
│  │  ┌──────┐ ┌──────┐    ┌──────┐ ┌──────┐                 │    │
│  │  │ MP-1 │ │ MP-2 │    │ MP-1 │ │ MP-2 │  (Shared!)      │    │
│  │  └──┬───┘ └──┬───┘    └──┬───┘ └──┬───┘                 │    │
│  │     │        │           │        │                      │    │
│  │     └────────┴───────────┴────────┘                      │    │
│  │                    │                                     │    │
│  │                    ▼                                     │    │
│  │         ┌──────┐ ┌──────┐ ┌──────┐                      │    │
│  │         │ MP-1 │ │ MP-2 │ │ MP-3 │  Storage: Still 3!   │    │
│  │         └──────┘ └──────┘ └──────┘                      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  STEP 3: AFTER MODIFICATIONS TO CLONE                           │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  SOURCE TABLE          CLONE TABLE                       │    │
│  │  ┌──────┐ ┌──────┐    ┌──────┐ ┌──────┐ ┌──────┐        │    │
│  │  │ MP-1 │ │ MP-2 │    │ MP-1 │ │ MP-2 │ │MP-NEW│        │    │
│  │  └──┬───┘ └──┬───┘    └──┬───┘ └──┬───┘ └──┬───┘        │    │
│  │     │        │           │        │        │             │    │
│  │  (Shared)  (Shared)   (Shared) (Shared)  (New!)         │    │
│  │                                                          │    │
│  │         Storage: 3 original + 1 new = 4 units           │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  KEY POINTS:                                                    │
│  • Clone is instant (metadata operation)                        │
│  • No storage until data diverges                               │
│  • Source and clone are independent after creation              │
│  • Policies (masking, RAP) apply to clones                      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### SQL Examples

```sql
-- Clone database
CREATE DATABASE MY_DEV_DATABASE CLONE MY_PROD_DATABASE;

-- Clone schema
CREATE SCHEMA MY_DATABASE.DEV_SCHEMA CLONE MY_DATABASE.PROD_SCHEMA;

-- Clone table
CREATE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE_CLONE 
  CLONE MY_DATABASE.MY_SCHEMA.MY_TABLE;

-- Clone with Time Travel (point-in-time)
CREATE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE_BACKUP 
  CLONE MY_DATABASE.MY_SCHEMA.MY_TABLE
  AT(TIMESTAMP => '2025-01-10 14:30:00'::TIMESTAMP_LTZ);

-- Clone database with COPY GRANTS (preserve permissions)
CREATE DATABASE MY_DEV_DATABASE 
  CLONE MY_PROD_DATABASE
  COPY GRANTS;

-- Clone to different schema
CREATE TABLE MY_DATABASE.BACKUP_SCHEMA.DAILY_BACKUP 
  CLONE MY_DATABASE.PROD_SCHEMA.CRITICAL_TABLE;

-- View clone relationships (no direct query, but can check metadata)
SELECT 
  TABLE_CATALOG,
  TABLE_SCHEMA,
  TABLE_NAME,
  CREATED,
  COMMENT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE '%CLONE%';

-- Drop clone (doesn't affect source)
DROP TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE_CLONE;
```

### Use Cases

| Use Case | Description |
|----------|-------------|
| **Dev/Test Environments** | Instant production copies for testing |
| **Data Backups** | Point-in-time snapshots |
| **A/B Testing** | Compare different data transformations |
| **Migration Testing** | Test schema changes on clone |
| **Disaster Recovery** | Quick recovery from Time Travel |

### Best Practices

1. **Use for dev/test provisioning** - Instant, no storage cost initially
2. **Clone before migrations** - Safety net for schema changes
3. **Use COPY GRANTS carefully** - Security implications
4. **Clean up old clones** - Storage accumulates with divergence
5. **Document clone purposes** - Track what clones are for

### Why This Matters
Zero-copy cloning eliminates the time and cost of data duplication, enabling rapid environment creation, safe testing, and instant backups.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/object-clone

---

# Section 8: Data Sharing & Replication

## 8.1 Secure Data Sharing

### Overview
Secure Data Sharing enables live data sharing between Snowflake accounts without copying data. Consumers see real-time data while providers maintain full control. No data movement, no ETL, no egress costs.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Direct Shares | ✓ | ✓ | ✓ |
| Reader Accounts | ✓ | ✓ | ✓ |
| Data Exchange | ✓ | ✓ | ✓ |
| Listings | ✓ | ✓ | ✓ |

### Sharing Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SECURE DATA SHARING                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  PROVIDER ACCOUNT                    CONSUMER ACCOUNT            │
│  ┌─────────────────────┐            ┌─────────────────────┐     │
│  │                     │            │                     │     │
│  │  ┌───────────────┐  │            │  ┌───────────────┐  │     │
│  │  │   DATABASE    │  │            │  │   DATABASE    │  │     │
│  │  │  ┌─────────┐  │  │   SHARE    │  │  (from share) │  │     │
│  │  │  │ TABLE   │──┼──┼───────────►│  │  ┌─────────┐  │  │     │
│  │  │  └─────────┘  │  │   (No      │  │  │ TABLE   │  │  │     │
│  │  │  ┌─────────┐  │  │   copy!)   │  │  │ (live)  │  │  │     │
│  │  │  │ VIEW    │──┼──┼───────────►│  │  └─────────┘  │  │     │
│  │  │  └─────────┘  │  │            │  │               │  │     │
│  │  └───────────────┘  │            │  └───────────────┘  │     │
│  │                     │            │                     │     │
│  │  Provider controls: │            │  Consumer gets:     │     │
│  │  • What to share    │            │  • Real-time data   │     │
│  │  • Who can access   │            │  • Read-only access │     │
│  │  • Revoke anytime   │            │  • No storage cost  │     │
│  │                     │            │                     │     │
│  └─────────────────────┘            └─────────────────────┘     │
│                                                                  │
│  SECURITY FEATURES:                                             │
│  • Masking policies travel with shared data                     │
│  • Row access policies travel with shared data                  │
│  • Secure views supported                                       │
│  • Consumer role determines policy application                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### SQL Examples

```sql
-- PROVIDER: Create share
CREATE SHARE MY_DATA_SHARE
  COMMENT = 'Shared sales data for partners';

-- PROVIDER: Grant privileges on database
GRANT USAGE ON DATABASE SHARED_DB TO SHARE MY_DATA_SHARE;

-- PROVIDER: Grant privileges on schema
GRANT USAGE ON SCHEMA SHARED_DB.SHARED_SCHEMA TO SHARE MY_DATA_SHARE;

-- PROVIDER: Grant SELECT on tables/views
GRANT SELECT ON TABLE SHARED_DB.SHARED_SCHEMA.SALES_DATA TO SHARE MY_DATA_SHARE;
GRANT SELECT ON VIEW SHARED_DB.SHARED_SCHEMA.SALES_SUMMARY TO SHARE MY_DATA_SHARE;

-- PROVIDER: Add consumer accounts
ALTER SHARE MY_DATA_SHARE ADD ACCOUNTS = ABC12345, XYZ67890;

-- PROVIDER: View share details
SHOW SHARES;
DESCRIBE SHARE MY_DATA_SHARE;

-- PROVIDER: View consumers
SHOW GRANTS TO SHARE MY_DATA_SHARE;

-- PROVIDER: Revoke access
ALTER SHARE MY_DATA_SHARE REMOVE ACCOUNTS = ABC12345;

-- CONSUMER: Create database from share
CREATE DATABASE PARTNER_DATA FROM SHARE PROVIDER_ACCOUNT.MY_DATA_SHARE;

-- CONSUMER: Grant access to roles
GRANT IMPORTED PRIVILEGES ON DATABASE PARTNER_DATA TO ROLE ANALYST;

-- CONSUMER: Query shared data
SELECT * FROM PARTNER_DATA.SHARED_SCHEMA.SALES_DATA;

-- PROVIDER: Create reader account (for non-Snowflake consumers)
CREATE MANAGED ACCOUNT READER_ACCT
  ADMIN_NAME = 'reader_admin'
  ADMIN_PASSWORD = 'SecurePassword123!'
  TYPE = READER;

-- PROVIDER: Share to reader account
ALTER SHARE MY_DATA_SHARE ADD ACCOUNTS = READER_ACCT;
```

### Best Practices

1. **Use secure views** - Control what data is visible
2. **Apply masking policies** - Policies travel with shares
3. **Document share purposes** - Use COMMENT field
4. **Regular access reviews** - Audit who has access
5. **Use reader accounts** - For non-Snowflake consumers
6. **Monitor share usage** - Track consumer queries

### Why This Matters
Secure Data Sharing eliminates data pipelines for data distribution, ensures real-time access, and maintains security controls across organizational boundaries.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/data-sharing-intro

---

## 8.2 Snowflake Marketplace & Data Exchange

### Overview
Snowflake Marketplace provides public data listings from third-party providers. Data Exchange enables private marketplaces for controlled data sharing within organizations or with trusted partners.

### Components

| Component | Description | Access |
|-----------|-------------|--------|
| **Marketplace** | Public data listings | Anyone |
| **Data Exchange** | Private marketplace | Invited only |
| **Listings** | Published datasets | Per listing |

### SQL Examples

```sql
-- View available shares from Marketplace
SHOW SHARES IN APPLICATION PACKAGE;

-- Create database from Marketplace listing
-- (Done through Snowsight UI, then grant access)
GRANT IMPORTED PRIVILEGES ON DATABASE MARKETPLACE_DATA TO ROLE ANALYST;

-- Query Marketplace data
SELECT * FROM MARKETPLACE_DATA.SCHEMA.TABLE LIMIT 100;
```

### Documentation Reference
https://docs.snowflake.com/en/user-guide/data-exchange

---

## 8.3 Replication

### Overview
Replication copies databases and account objects across regions and cloud platforms. Supports disaster recovery, data locality requirements, and global data distribution. Includes failover/failback capabilities for business continuity.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Database Replication | ✓ | ✓ | ✓ |
| Account Replication | - | - | ✓ |
| Failover/Failback | - | - | ✓ |
| Cross-Cloud Replication | ✓ | ✓ | ✓ |

### Replication Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   DATABASE REPLICATION                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  PRIMARY ACCOUNT                   SECONDARY ACCOUNT             │
│  (US-WEST-2)                       (EU-WEST-1)                   │
│  ┌─────────────────────┐          ┌─────────────────────┐       │
│  │                     │          │                     │       │
│  │  ┌───────────────┐  │ Replicate│  ┌───────────────┐  │       │
│  │  │   DATABASE    │  │─────────►│  │   DATABASE    │  │       │
│  │  │   (Primary)   │  │          │  │  (Secondary)  │  │       │
│  │  │               │  │          │  │  (Read-Only)  │  │       │
│  │  │  Read/Write   │  │          │  │               │  │       │
│  │  └───────────────┘  │          │  └───────────────┘  │       │
│  │                     │          │                     │       │
│  └─────────────────────┘          └─────────────────────┘       │
│                                                                  │
│  FAILOVER (Business Critical):                                  │
│  ┌─────────────────────┐          ┌─────────────────────┐       │
│  │  PRIMARY ───────────┼──────────┼───► SECONDARY       │       │
│  │  (Down)             │  Promote │     (Now Primary)   │       │
│  └─────────────────────┘          └─────────────────────┘       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### SQL Examples

```sql
-- Enable replication for database (on primary)
ALTER DATABASE MY_DATABASE ENABLE REPLICATION TO ACCOUNTS 
  ORG1.SECONDARY_ACCOUNT;

-- Create secondary database (on secondary account)
CREATE DATABASE MY_DATABASE AS REPLICA OF 
  ORG1.PRIMARY_ACCOUNT.MY_DATABASE;

-- Refresh secondary database
ALTER DATABASE MY_DATABASE REFRESH;

-- Set up automatic refresh (use tasks)
CREATE TASK REFRESH_REPLICA_DB
  WAREHOUSE = REPLICATION_WH
  SCHEDULE = '60 MINUTE'
AS
ALTER DATABASE MY_DATABASE REFRESH;

ALTER TASK REFRESH_REPLICA_DB RESUME;

-- Create replication group (Business Critical)
CREATE REPLICATION GROUP MY_REPL_GROUP
  OBJECT_TYPES = DATABASES, ROLES, USERS
  ALLOWED_DATABASES = MY_DATABASE
  ALLOWED_ACCOUNTS = ORG1.SECONDARY_ACCOUNT
  REPLICATION_SCHEDULE = '10 MINUTE';

-- Monitor replication status
SHOW REPLICATION DATABASES;

SELECT 
  DATABASE_NAME,
  IS_PRIMARY,
  PRIMARY_ACCOUNT_LOCATOR,
  REPLICATION_ALLOWED_TO_ACCOUNTS,
  BYTES_REPLICATED
FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_USAGE_HISTORY;

-- Failover to secondary (Business Critical)
-- On secondary account:
ALTER DATABASE MY_DATABASE PRIMARY;

-- Failback to original primary
-- On original primary account:
ALTER DATABASE MY_DATABASE PRIMARY;
```

### Best Practices

1. **Plan for data locality** - Replicate to regions near users
2. **Test failover regularly** - Ensure DR procedures work
3. **Monitor replication lag** - Alert on excessive lag
4. **Use replication groups** - Coordinate related objects
5. **Document DR procedures** - Runbooks for failover

### Why This Matters
Replication enables disaster recovery, meets data residency requirements, and reduces latency for global users. Business-critical workloads require failover capability.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/database-replication-intro

---

# Section 9: Governance & Compliance

## 9.1 Object Tagging

### Overview
Object tagging attaches key-value metadata to Snowflake objects for classification, governance, and policy application. Tags enable centralized management of sensitive data across the organization.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Object Tagging | - | ✓ | ✓ |
| Tag Propagation | - | ✓ | ✓ |
| Tag-Based Masking | - | ✓ | ✓ |

### Taggable Objects

| Level | Objects |
|-------|---------|
| Account | Warehouses, Users, Roles |
| Database | Databases |
| Schema | Schemas, Tables, Views, Columns |
| Column | Table columns, View columns |

### SQL Examples

```sql
-- Create tag with allowed values
CREATE TAG SENSITIVITY_LEVEL
  ALLOWED_VALUES = ('PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED')
  COMMENT = 'Data sensitivity classification';

CREATE TAG DATA_DOMAIN
  ALLOWED_VALUES = ('FINANCE', 'HR', 'SALES', 'MARKETING', 'ENGINEERING')
  COMMENT = 'Business domain classification';

CREATE TAG PII
  ALLOWED_VALUES = ('TRUE', 'FALSE')
  COMMENT = 'Contains personally identifiable information';

-- Apply tag to database
ALTER DATABASE MY_DATABASE SET TAG SENSITIVITY_LEVEL = 'CONFIDENTIAL';

-- Apply tag to table
ALTER TABLE MY_DATABASE.MY_SCHEMA.EMPLOYEES 
  SET TAG DATA_DOMAIN = 'HR';

-- Apply tag to column
ALTER TABLE MY_DATABASE.MY_SCHEMA.EMPLOYEES 
  MODIFY COLUMN SSN SET TAG PII = 'TRUE';

ALTER TABLE MY_DATABASE.MY_SCHEMA.EMPLOYEES 
  MODIFY COLUMN EMAIL SET TAG PII = 'TRUE';

-- View tags on object
SELECT SYSTEM$GET_TAG('SENSITIVITY_LEVEL', 'MY_DATABASE', 'DATABASE');

-- Query all tagged objects
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES(
  'SENSITIVITY_LEVEL',
  'TAG'
));

-- Find all columns tagged as PII
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
  'PII',
  'TAG'
))
WHERE TAG_VALUE = 'TRUE';

-- Remove tag from object
ALTER TABLE MY_DATABASE.MY_SCHEMA.EMPLOYEES 
  UNSET TAG DATA_DOMAIN;

-- Drop tag
DROP TAG SENSITIVITY_LEVEL;

-- Tag-based masking policy
CREATE MASKING POLICY PII_MASK AS (val STRING) 
  RETURNS STRING ->
  CASE
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('PII') = 'TRUE' 
      AND CURRENT_ROLE() NOT IN ('DATA_ADMIN', 'COMPLIANCE')
    THEN '***MASKED***'
    ELSE val
  END;

-- Associate masking policy with tag
ALTER TAG PII SET MASKING POLICY PII_MASK;
```

### Best Practices

1. **Define tag taxonomy** - Standard tags across organization
2. **Use ALLOWED_VALUES** - Enforce consistency
3. **Tag at creation** - Include in DDL standards
4. **Combine with masking** - Tag-based policy application
5. **Regular tag audits** - Ensure accuracy

### Why This Matters
Tags enable data discovery, drive policy application, and support compliance reporting. Essential for large-scale data governance.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/object-tagging

---

## 9.2 Data Classification

### Overview
Data Classification automatically detects and classifies sensitive data in columns. Built-in classifiers identify PII (names, emails, SSNs, etc.) and recommend appropriate tags for governance.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Data Classification | - | ✓ | ✓ |
| SYSTEM$CLASSIFY | - | ✓ | ✓ |
| Auto-Tagging | - | ✓ | ✓ |

### Classification Categories

| Category | Type | Examples |
|----------|------|----------|
| **SEMANTIC** | Data type | Email, Phone, SSN, Name, Address |
| **PRIVACY** | Sensitivity | Sensitive, Highly Sensitive, Confidential |

### SQL Examples

```sql
-- Classify a single table
SELECT SYSTEM$CLASSIFY('MY_DATABASE.MY_SCHEMA.CUSTOMERS', {'auto_tag': true});

-- Classify entire schema
CALL SNOWFLAKE.DATA_PRIVACY.CLASSIFY_SCHEMA(
  'MY_DATABASE.MY_SCHEMA',
  {'auto_tag': true}
);

-- View classification results
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
  'SNOWFLAKE.CORE.SEMANTIC_CATEGORY',
  'TAG'
));

-- View privacy category tags
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
  'SNOWFLAKE.CORE.PRIVACY_CATEGORY',
  'TAG'
));

-- Query classification recommendations (without auto-tagging)
SELECT SYSTEM$CLASSIFY('MY_DATABASE.MY_SCHEMA.EMPLOYEES');
-- Returns JSON with recommendations

-- Sample classification output:
-- {
--   "columns": [
--     {"column_name": "EMAIL", "semantic_category": "EMAIL", "privacy_category": "IDENTIFIER"},
--     {"column_name": "SSN", "semantic_category": "US_SSN", "privacy_category": "IDENTIFIER"},
--     {"column_name": "PHONE", "semantic_category": "PHONE_NUMBER", "privacy_category": "IDENTIFIER"}
--   ]
-- }
```

### Best Practices

1. **Run classification on new tables** - Include in data onboarding
2. **Review before auto-tagging** - Validate recommendations
3. **Combine with masking policies** - Automate protection
4. **Regular re-classification** - Catch schema changes
5. **Document exceptions** - Track manual overrides

### Why This Matters
Data classification automates the discovery of sensitive data, reducing manual effort and improving compliance coverage.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/governance-classify

---

## 9.3 Access History

### Overview
Access History tracks all data access including columns queried, base tables accessed through views, and policy applications. Provides complete audit trail for compliance (GDPR, CCPA, HIPAA).

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Access History | - | ✓ | ✓ |
| Column-Level Tracking | - | ✓ | ✓ |
| 365-Day Retention | - | ✓ | ✓ |

### What's Tracked

| Information | Description |
|-------------|-------------|
| Query ID | Unique identifier |
| User | Who executed query |
| Role | Active role |
| Columns Accessed | Specific columns read |
| Base Objects | Underlying tables (through views) |
| Direct vs Indirect | How access occurred |
| Policies Applied | Masking/RAP policies triggered |

### SQL Examples

```sql
-- View recent data access
SELECT 
  QUERY_ID,
  QUERY_START_TIME,
  USER_NAME,
  ROLE_NAME,
  DIRECT_OBJECTS_ACCESSED,
  BASE_OBJECTS_ACCESSED,
  OBJECTS_MODIFIED
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE QUERY_START_TIME > DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY QUERY_START_TIME DESC
LIMIT 100;

-- Find who accessed a specific table
SELECT 
  USER_NAME,
  ROLE_NAME,
  QUERY_START_TIME,
  QUERY_ID
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
  LATERAL FLATTEN(BASE_OBJECTS_ACCESSED) f
WHERE f.value:objectName::STRING = 'MY_DATABASE.MY_SCHEMA.SENSITIVE_TABLE'
  AND QUERY_START_TIME > DATEADD(day, -30, CURRENT_TIMESTAMP())
ORDER BY QUERY_START_TIME DESC;

-- Find who accessed specific columns
SELECT 
  ah.USER_NAME,
  ah.ROLE_NAME,
  ah.QUERY_START_TIME,
  col.value:columnName::STRING AS COLUMN_NAME
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
  LATERAL FLATTEN(BASE_OBJECTS_ACCESSED) obj,
  LATERAL FLATTEN(obj.value:columns) col
WHERE obj.value:objectName::STRING = 'MY_DATABASE.MY_SCHEMA.EMPLOYEES'
  AND col.value:columnName::STRING IN ('SSN', 'SALARY')
  AND ah.QUERY_START_TIME > DATEADD(day, -30, CURRENT_TIMESTAMP())
ORDER BY ah.QUERY_START_TIME DESC;

-- Audit access to PII columns
SELECT 
  ah.USER_NAME,
  ah.ROLE_NAME,
  COUNT(*) AS ACCESS_COUNT,
  MIN(ah.QUERY_START_TIME) AS FIRST_ACCESS,
  MAX(ah.QUERY_START_TIME) AS LAST_ACCESS
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
  LATERAL FLATTEN(BASE_OBJECTS_ACCESSED) obj,
  LATERAL FLATTEN(obj.value:columns) col,
  TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('PII', 'TAG')) tr
WHERE obj.value:objectName::STRING = tr.OBJECT_DATABASE || '.' || tr.OBJECT_SCHEMA || '.' || tr.OBJECT_NAME
  AND col.value:columnName::STRING = tr.COLUMN_NAME
  AND tr.TAG_VALUE = 'TRUE'
  AND ah.QUERY_START_TIME > DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY ah.USER_NAME, ah.ROLE_NAME
ORDER BY ACCESS_COUNT DESC;

-- Track access to shared data
SELECT 
  USER_NAME,
  ROLE_NAME,
  QUERY_START_TIME,
  obj.value:objectName::STRING AS OBJECT_ACCESSED
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
  LATERAL FLATTEN(DIRECT_OBJECTS_ACCESSED) obj
WHERE obj.value:objectDomain::STRING = 'Table'
  AND QUERY_START_TIME > DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY QUERY_START_TIME DESC;
```

### Best Practices

1. **Regular access reviews** - Monthly or quarterly audits
2. **Alert on sensitive data access** - Proactive monitoring
3. **Combine with tagging** - Track PII access specifically
4. **Retain query details** - For investigation if needed
5. **Automate compliance reports** - Scheduled queries

### Why This Matters
Access History provides complete visibility into data access for compliance audits, security investigations, and privacy regulations.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/access-history

---

## 9.4 Audit Logging

### Overview
Snowflake maintains comprehensive audit logs in the ACCOUNT_USAGE schema with 365-day retention. Logs cover authentication, queries, administrative actions, and security events.

### Key ACCOUNT_USAGE Views

| View | Content |
|------|---------|
| LOGIN_HISTORY | Authentication attempts (success/failure) |
| QUERY_HISTORY | All executed queries |
| SESSIONS | Session information |
| GRANTS_TO_ROLES | Role privilege grants |
| GRANTS_TO_USERS | User privilege grants |
| USERS | User accounts |
| ROLES | Role definitions |
| WAREHOUSE_EVENTS_HISTORY | Warehouse operations |

### SQL Examples

```sql
-- Monitor failed login attempts
SELECT 
  USER_NAME,
  CLIENT_IP,
  REPORTED_CLIENT_TYPE,
  ERROR_CODE,
  ERROR_MESSAGE,
  EVENT_TIMESTAMP
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE IS_SUCCESS = 'NO'
  AND EVENT_TIMESTAMP > DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY EVENT_TIMESTAMP DESC;

-- Find brute force attempts (multiple failures from same IP)
SELECT 
  CLIENT_IP,
  COUNT(*) AS FAILED_ATTEMPTS,
  COUNT(DISTINCT USER_NAME) AS UNIQUE_USERS_ATTEMPTED,
  MIN(EVENT_TIMESTAMP) AS FIRST_ATTEMPT,
  MAX(EVENT_TIMESTAMP) AS LAST_ATTEMPT
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE IS_SUCCESS = 'NO'
  AND EVENT_TIMESTAMP > DATEADD(hour, -24, CURRENT_TIMESTAMP())
GROUP BY CLIENT_IP
HAVING COUNT(*) > 5
ORDER BY FAILED_ATTEMPTS DESC;

-- Audit privilege grants
SELECT 
  CREATED_ON,
  MODIFIED_ON,
  PRIVILEGE,
  GRANTED_ON,
  NAME AS OBJECT_NAME,
  GRANTED_TO,
  GRANTEE_NAME,
  GRANT_OPTION,
  GRANTED_BY
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE CREATED_ON > DATEADD(day, -30, CURRENT_TIMESTAMP())
ORDER BY CREATED_ON DESC;

-- Track ACCOUNTADMIN activity
SELECT 
  USER_NAME,
  ROLE_NAME,
  QUERY_TYPE,
  QUERY_TEXT,
  START_TIME,
  END_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE ROLE_NAME = 'ACCOUNTADMIN'
  AND START_TIME > DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;

-- Monitor DDL changes
SELECT 
  USER_NAME,
  ROLE_NAME,
  QUERY_TYPE,
  QUERY_TEXT,
  START_TIME,
  DATABASE_NAME,
  SCHEMA_NAME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TYPE IN ('CREATE_TABLE', 'DROP_TABLE', 'ALTER_TABLE',
                     'CREATE_VIEW', 'DROP_VIEW', 'ALTER_VIEW',
                     'CREATE_ROLE', 'DROP_ROLE', 'GRANT', 'REVOKE')
  AND START_TIME > DATEADD(day, -30, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;

-- User account changes
SELECT 
  NAME,
  CREATED_ON,
  DELETED_ON,
  DISABLED,
  DEFAULT_ROLE,
  LAST_SUCCESS_LOGIN
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
ORDER BY CREATED_ON DESC;
```

### Best Practices

1. **Set up alerting** - Notify on security events
2. **Export to SIEM** - Integration with security tools
3. **Regular audit reviews** - Weekly security checks
4. **Document retention** - 365 days in ACCOUNT_USAGE
5. **Automate compliance reports** - Scheduled dashboards

### Documentation Reference
https://docs.snowflake.com/en/sql-reference/account-usage

---

## 9.5 Compliance Certifications

### Overview
Snowflake maintains industry-leading compliance certifications across all editions, with enhanced certifications for Business Critical edition.

### Certifications by Edition

| Certification | Standard | Enterprise | Business Critical |
|---------------|:--------:|:----------:|:-----------------:|
| SOC 1 Type II | ✓ | ✓ | ✓ |
| SOC 2 Type II | ✓ | ✓ | ✓ |
| ISO 27001 | ✓ | ✓ | ✓ |
| ISO 27017 | ✓ | ✓ | ✓ |
| ISO 27018 | ✓ | ✓ | ✓ |
| HIPAA | - | - | ✓ |
| HITRUST | - | - | ✓ |
| PCI-DSS | - | - | ✓ |
| FedRAMP Moderate | - | - | ✓ |
| StateRAMP | - | - | ✓ |
| IRAP (Australia) | - | - | ✓ |

### Key Compliance Features

| Requirement | Snowflake Feature |
|-------------|-------------------|
| Data Encryption | AES-256 at rest, TLS 1.2+ in transit |
| Access Control | RBAC, Row/Column security |
| Audit Logging | 365-day retention, comprehensive tracking |
| Key Management | Tri-Secret Secure (Business Critical) |
| Network Security | Network policies, PrivateLink |
| Data Residency | Multi-region deployment, replication |

### Documentation Reference
https://docs.snowflake.com/en/user-guide/security-compliance

---

# Section 10: Best Practices Summary

## Priority 1: Critical (Implement Immediately)

| # | Practice | Why It Matters |
|---|----------|----------------|
| 1 | **Enforce MFA for ACCOUNTADMIN** | Protects highest-privilege account from credential theft |
| 2 | **Use custom roles for daily work** | Principle of least privilege; never use ACCOUNTADMIN routinely |
| 3 | **Create role hierarchy to SYSADMIN** | Enables administrative oversight of all objects |
| 4 | **Configure network policies** (Enterprise+) | Restricts access to trusted networks only |
| 5 | **Enable Access History tracking** (Enterprise+) | Required for compliance audits and security investigations |

## Priority 2: Recommended (Implement Soon)

| # | Practice | Why It Matters |
|---|----------|----------------|
| 6 | **Implement federated authentication (SSO)** | Centralizes identity, enables immediate revocation |
| 7 | **Use key pair auth for service accounts** | Eliminates password exposure in automation |
| 8 | **Apply masking policies to PII** (Enterprise+) | Protects sensitive data, enables least-privilege access |
| 9 | **Classify and tag sensitive data** (Enterprise+) | Enables governance, supports tag-based policies |
| 10 | **Set appropriate Time Travel retention** | Balance data protection with storage costs |

## Priority 3: Advanced (Implement for Maturity)

| # | Practice | Why It Matters |
|---|----------|----------------|
| 11 | **Implement row access policies** | Strong data isolation without application changes |
| 12 | **Use secure views for sensitive data** | Hides logic, prevents information leakage |
| 13 | **Configure database replication for DR** | Business continuity, disaster recovery |
| 14 | **Implement tag-based masking** | Centralized, scalable data protection |
| 15 | **Regular access reviews via ACCOUNT_USAGE** | Detect anomalies, maintain compliance |

## Anti-Patterns to Avoid

| Anti-Pattern | Risk | Correct Approach |
|--------------|------|------------------|
| Using ACCOUNTADMIN daily | Privilege escalation, audit confusion | Create functional roles |
| Granting privileges to users | Management complexity | Grant to roles only |
| No Future Grants | Manual grant maintenance | Use Future Grants for automation |
| Overly permissive network policies | Increased attack surface | Restrict to known IPs |
| Not rotating service account keys | Credential exposure | Annual rotation minimum |
| Ignoring Time Travel storage | Unexpected costs | Monitor and set appropriate retention |
| SELECT ANY TABLE without justification | Over-privileged access | Grant specific table access |
| Sharing without masking policies | Data exposure to consumers | Apply policies before sharing |

---

# Edition Comparison Summary

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| **Access Control** |
| RBAC & DAC | ✓ | ✓ | ✓ |
| Users & Roles | ✓ | ✓ | ✓ |
| Database Roles | ✓ | ✓ | ✓ |
| Future Grants | ✓ | ✓ | ✓ |
| **Authentication** |
| MFA | ✓ | ✓ | ✓ |
| SSO/SAML | ✓ | ✓ | ✓ |
| Key Pair Auth | ✓ | ✓ | ✓ |
| OAuth | ✓ | ✓ | ✓ |
| SCIM | ✓ | ✓ | ✓ |
| **Network Security** |
| Network Policies | - | ✓ | ✓ |
| Session Policies | ✓ | ✓ | ✓ |
| PrivateLink | - | - | ✓ |
| **Encryption** |
| Encryption at Rest | ✓ | ✓ | ✓ |
| Encryption in Transit | ✓ | ✓ | ✓ |
| Periodic Rekeying | - | ✓ | ✓ |
| Tri-Secret Secure | - | - | ✓ |
| **Column/Row Security** |
| Dynamic Data Masking | - | ✓ | ✓ |
| Row Access Policies | - | ✓ | ✓ |
| Secure Views | ✓ | ✓ | ✓ |
| **Data Protection** |
| Time Travel (1 day) | ✓ | - | - |
| Time Travel (90 days) | - | ✓ | ✓ |
| Fail-safe (7 days) | ✓ | ✓ | ✓ |
| Zero-Copy Cloning | ✓ | ✓ | ✓ |
| **Sharing & Replication** |
| Secure Data Sharing | ✓ | ✓ | ✓ |
| Database Replication | ✓ | ✓ | ✓ |
| Account Replication | - | - | ✓ |
| Failover/Failback | - | - | ✓ |
| **Governance** |
| Object Tagging | - | ✓ | ✓ |
| Data Classification | - | ✓ | ✓ |
| Access History | - | ✓ | ✓ |
| Audit Logging | ✓ | ✓ | ✓ |
| **Compliance** |
| SOC 1/2 Type II | ✓ | ✓ | ✓ |
| ISO 27001 | ✓ | ✓ | ✓ |
| HIPAA | - | - | ✓ |
| PCI-DSS | - | - | ✓ |
| FedRAMP Moderate | - | - | ✓ |

---

# Documentation Reference Summary

| Topic | URL |
|-------|-----|
| Security Overview | https://docs.snowflake.com/en/user-guide/security |
| Access Control | https://docs.snowflake.com/en/user-guide/security-access-control-overview |
| Privileges | https://docs.snowflake.com/en/user-guide/security-access-control-privileges |
| Authentication | https://docs.snowflake.com/en/user-guide/authentication |
| MFA | https://docs.snowflake.com/en/user-guide/security-mfa |
| SSO/SAML | https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-overview |
| Key Pair Auth | https://docs.snowflake.com/en/user-guide/key-pair-auth |
| OAuth | https://docs.snowflake.com/en/user-guide/oauth-intro |
| SCIM | https://docs.snowflake.com/en/user-guide/scim |
| Network Policies | https://docs.snowflake.com/en/user-guide/network-policies |
| PrivateLink | https://docs.snowflake.com/en/user-guide/admin-security-privatelink |
| Encryption | https://docs.snowflake.com/en/user-guide/security-encryption |
| Tri-Secret Secure | https://docs.snowflake.com/en/user-guide/security-encryption-manage |
| Data Masking | https://docs.snowflake.com/en/user-guide/security-column-ddm-intro |
| Row Access Policies | https://docs.snowflake.com/en/user-guide/security-row-intro |
| Secure Views | https://docs.snowflake.com/en/user-guide/views-secure |
| Time Travel | https://docs.snowflake.com/en/user-guide/data-time-travel |
| Fail-safe | https://docs.snowflake.com/en/user-guide/data-failsafe |
| Cloning | https://docs.snowflake.com/en/user-guide/object-clone |
| Data Sharing | https://docs.snowflake.com/en/user-guide/data-sharing-intro |
| Replication | https://docs.snowflake.com/en/user-guide/database-replication-intro |
| Object Tagging | https://docs.snowflake.com/en/user-guide/object-tagging |
| Data Classification | https://docs.snowflake.com/en/user-guide/governance-classify |
| Access History | https://docs.snowflake.com/en/user-guide/access-history |
| Account Usage | https://docs.snowflake.com/en/sql-reference/account-usage |
| Compliance | https://docs.snowflake.com/en/user-guide/security-compliance |

---

# Document Information

**Complete Coverage:**
- Part 1 (Sections 1-6): Access Control, Authentication, Network Security, Encryption, Column/Row Security
- Part 2 (Sections 7-10): Data Protection, Sharing, Governance, Best Practices

**Document Version:** 2025.1  
**Based On:** Official Snowflake Documentation (docs.snowflake.com)  
**Validation Date:** January 2025  
**Suitable For:** Multi-customer presentations, training, security assessments
