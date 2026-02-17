# Snowflake Streams
## Complete Customer Documentation & Best Practices Guide

---

# Executive Summary

## What is a Snowflake Stream?

```mermaid
flowchart LR
    subgraph Definition["STREAM = CHANGE DATA CAPTURE (CDC)"]
        A[Source Table] --> B[Stream Object]
        B --> C[Track Changes:<br/>INSERT, UPDATE, DELETE]
        C --> D[Consume Changes<br/>in DML Transaction]
    end
    
    style B fill:#4169E1,stroke:#000080,color:#fff
```

**Key Concept:** A stream is like a **bookmark** in a book - it marks a point in time and shows you what changed since that bookmark was placed.

### What a Stream IS:
- ✅ A **metadata object** that tracks changes (CDC) to a source table
- ✅ A **pointer/offset** to a specific point in the table's transaction history
- ✅ A **change table** showing what changed between two points in time
- ✅ **Lightweight** - stores only offset, not actual data

### What a Stream is NOT:
- ❌ NOT a copy of the table data
- ❌ NOT a trigger or stored procedure
- ❌ NOT real-time streaming (it's batch-oriented)
- ❌ NOT a materialized view

---

# How Streams Work

## The Offset Concept

```mermaid
flowchart LR
    subgraph Timeline["TABLE VERSION TIMELINE"]
        V1[v1] --> V2[v2] --> V3[v3] --> V4[v4] --> V5[v5]
    end
    
    subgraph Stream["STREAM OFFSET"]
        OFFSET["Stream Offset<br/>(between v2 & v3)"]
    end
    
    OFFSET -.->|Points to| V2
    
    subgraph Query["WHEN QUERIED"]
        RESULT["Returns changes<br/>v3 → v5"]
    end
    
    style OFFSET fill:#FFD700,stroke:#B8860B
    style RESULT fill:#90EE90,stroke:#228B22
```

**Key Points:**
1. Stream stores an **offset** (position) in the table's version history
2. When queried, returns all changes **from offset to current version**
3. Offset advances **ONLY** when stream is consumed in a DML transaction

---

## Stream Metadata Columns

```mermaid
classDiagram
    class StreamRecord {
        +ALL_TABLE_COLUMNS : (original columns)
        +METADATA$ACTION : VARCHAR
        +METADATA$ISUPDATE : BOOLEAN
        +METADATA$ROW_ID : VARCHAR
    }
    
    note for StreamRecord "METADATA$ACTION: INSERT or DELETE\nMETADATA$ISUPDATE: TRUE if part of UPDATE\nMETADATA$ROW_ID: Unique row identifier"
```

| Column | Values | Description |
|--------|--------|-------------|
| `METADATA$ACTION` | `INSERT` or `DELETE` | The DML operation type |
| `METADATA$ISUPDATE` | `TRUE` / `FALSE` | Was this part of an UPDATE? |
| `METADATA$ROW_ID` | Unique ID | Immutable row identifier |

### How Updates are Represented

```mermaid
flowchart TD
    subgraph Update["UPDATE Statement"]
        U["UPDATE table SET col = 'new'<br/>WHERE id = 1"]
    end
    
    subgraph Stream["Stream Records (2 rows)"]
        D["Row 1: DELETE + ISUPDATE=TRUE<br/>(old value)"]
        I["Row 2: INSERT + ISUPDATE=TRUE<br/>(new value)"]
    end
    
    Update --> D
    Update --> I
    
    style D fill:#FF6B6B,stroke:#C0392B
    style I fill:#90EE90,stroke:#228B22
```

**Important:** An UPDATE appears as a **DELETE + INSERT pair** with `METADATA$ISUPDATE = TRUE`

---

## Stream Types

```mermaid
flowchart TB
    subgraph Types["THREE STREAM TYPES"]
        STD["STANDARD<br/>(Default)"]
        APP["APPEND-ONLY"]
        INS["INSERT-ONLY"]
    end
    
    subgraph Standard["STANDARD STREAM"]
        S1[Tracks INSERT]
        S2[Tracks UPDATE]
        S3[Tracks DELETE]
        S4[Tracks TRUNCATE]
        S5[Computes Net Delta]
    end
    
    subgraph AppendOnly["APPEND-ONLY STREAM"]
        A1[Tracks INSERT only]
        A2[Ignores UPDATE]
        A3[Ignores DELETE]
        A4[More Performant]
    end
    
    subgraph InsertOnly["INSERT-ONLY STREAM"]
        I1[External Tables only]
        I2[Iceberg Tables only]
        I3[Tracks new files only]
    end
    
    STD --> Standard
    APP --> AppendOnly
    INS --> InsertOnly
    
    style STD fill:#4169E1,color:#fff
    style APP fill:#9370DB,color:#fff
    style INS fill:#20B2AA,color:#fff
```

### Stream Type Comparison

| Feature | Standard | Append-Only | Insert-Only |
|---------|----------|-------------|-------------|
| **Tracks INSERT** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Tracks UPDATE** | ✅ Yes | ❌ No | ❌ No |
| **Tracks DELETE** | ✅ Yes | ❌ No | ❌ No |
| **Tracks TRUNCATE** | ✅ Yes | ❌ No | ❌ No |
| **Net Delta** | ✅ Yes | ❌ No | ❌ No |
| **Performance** | Standard | ⚡ Better | ⚡ Better |
| **Source Objects** | Tables, Views | Tables, Views | External/Iceberg |
| **Geospatial Data** | ❌ No | ✅ Yes | ✅ Yes |

---

## Supported Source Objects

```mermaid
flowchart TB
    subgraph Sources["STREAM SOURCE OBJECTS"]
        subgraph Supported["✅ SUPPORTED"]
            T1[Standard Tables]
            T2[Views]
            T3[Secure Views]
            T4[Directory Tables]
            T5[Dynamic Tables]
            T6[Iceberg Tables]
            T7[External Tables]
            T8[Event Tables]
            T9[Shared Tables]
        end
        
        subgraph NotSupported["❌ NOT SUPPORTED"]
            N1[Materialized Views]
            N2[Temporary Tables]
        end
    end
    
    style Supported fill:#90EE90
    style NotSupported fill:#FF6B6B
```

---

# Stream Data Flow

## Basic CDC Pipeline

```mermaid
sequenceDiagram
    participant SRC as Source Table
    participant STR as Stream
    participant TGT as Target Table
    participant TSK as Task (Optional)
    
    Note over SRC: Initial State
    SRC->>STR: CREATE STREAM ON TABLE
    Note over STR: Offset = Current Version
    
    Note over SRC: DML Operations
    SRC->>SRC: INSERT/UPDATE/DELETE
    
    Note over STR: Stream Tracks Changes
    STR-->>STR: Records change metadata
    
    TSK->>STR: Check SYSTEM$STREAM_HAS_DATA()
    
    alt Has Data
        TSK->>STR: SELECT * FROM stream
        TSK->>TGT: INSERT INTO target<br/>SELECT * FROM stream
        Note over STR: Offset Advances
    else No Data
        TSK-->>TSK: Skip execution
    end
```

---

## When Does the Offset Advance?

```mermaid
flowchart TD
    subgraph Advance["OFFSET ADVANCES ✅"]
        A1[INSERT INTO ... SELECT FROM stream]
        A2[UPDATE ... FROM stream]
        A3[DELETE ... FROM stream]
        A4[MERGE ... USING stream]
        A5[CREATE TABLE AS SELECT FROM stream]
        A6[COPY INTO location FROM stream]
    end
    
    subgraph NoAdvance["OFFSET DOES NOT ADVANCE ❌"]
        N1[SELECT * FROM stream]
        N2[Query within explicit transaction<br/>without COMMIT]
    end
    
    style Advance fill:#90EE90
    style NoAdvance fill:#FF6B6B
```

**Critical Rule:** The offset advances **ONLY** when the stream is consumed in a **DML transaction that commits successfully**.

---

## Net Delta Behavior (Standard Streams)

```mermaid
flowchart LR
    subgraph Operations["OPERATIONS BETWEEN OFFSETS"]
        O1["INSERT row id=1"]
        O2["UPDATE row id=1"]
        O3["DELETE row id=1"]
    end
    
    subgraph NetDelta["NET DELTA RESULT"]
        R1["Nothing<br/>(row appeared and disappeared)"]
    end
    
    O1 --> O2 --> O3 --> R1
    
    style R1 fill:#FFD700,stroke:#B8860B
```

**Standard streams compute the NET change:**
- Row inserted then deleted = **No record** (net effect is nothing)
- Row inserted then updated = **One INSERT** with final value
- Row updated multiple times = **One UPDATE** with final value

---

# Streams with Tasks

## Recommended Architecture

```mermaid
flowchart TB
    subgraph Pipeline["CDC PIPELINE WITH TASKS"]
        SRC[(Source Table)]
        STR[Stream]
        TSK[Task<br/>WHEN SYSTEM$STREAM_HAS_DATA]
        TGT[(Target Table)]
    end
    
    SRC --> STR
    STR --> TSK
    TSK --> TGT
    
    subgraph TaskDef["TASK DEFINITION"]
        DEF["CREATE TASK my_task<br/>WAREHOUSE = wh<br/>SCHEDULE = '5 MINUTE'<br/>WHEN SYSTEM$STREAM_HAS_DATA('stream')<br/>AS<br/>INSERT INTO target SELECT * FROM stream"]
    end
    
    style TSK fill:#4169E1,color:#fff
```

## Task with Stream Example

```sql
-- 1. Create source table
CREATE TABLE src_orders (
    order_id NUMBER,
    customer_id NUMBER,
    amount NUMBER,
    created_at TIMESTAMP
);

-- 2. Create stream on source
CREATE STREAM orders_stream ON TABLE src_orders;

-- 3. Create target table
CREATE TABLE tgt_orders_history (
    order_id NUMBER,
    customer_id NUMBER,
    amount NUMBER,
    action VARCHAR,
    processed_at TIMESTAMP
);

-- 4. Create task to process stream
CREATE TASK process_orders_task
    WAREHOUSE = compute_wh
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
    INSERT INTO tgt_orders_history
    SELECT 
        order_id,
        customer_id,
        amount,
        METADATA$ACTION,
        CURRENT_TIMESTAMP()
    FROM orders_stream;

-- 5. Resume task
ALTER TASK process_orders_task RESUME;
```

---

# Staleness and Data Retention

## Stream Staleness Concept

```mermaid
flowchart LR
    subgraph Timeline["DATA RETENTION TIMELINE"]
        T1["Day 1<br/>Stream Created"]
        T2["Day 7<br/>"]
        T3["Day 14<br/>Default Extension"]
        T4["Day 15+<br/>⚠️ STALE RISK"]
    end
    
    T1 --> T2 --> T3 --> T4
    
    subgraph Retention["DATA_RETENTION_TIME_IN_DAYS"]
        R1["Default: 1 day"]
        R2["Extended: up to 14 days<br/>(for streams)"]
    end
    
    style T4 fill:#FF6B6B,stroke:#C0392B
```

## What Causes Staleness?

```mermaid
flowchart TD
    subgraph Causes["STREAM BECOMES STALE WHEN:"]
        C1["Offset falls outside<br/>data retention period"]
        C2["Source table is<br/>recreated (CREATE OR REPLACE)"]
        C3["Underlying table for view<br/>is dropped/recreated"]
        C4["Database/Schema cloned<br/>(unconsumed records lost)"]
    end
    
    subgraph Prevention["HOW TO PREVENT:"]
        P1["Consume stream regularly<br/>(before STALE_AFTER)"]
        P2["Increase<br/>MAX_DATA_EXTENSION_TIME_IN_DAYS"]
        P3["Call SYSTEM$STREAM_HAS_DATA()<br/>periodically"]
    end
    
    style Causes fill:#FF6B6B
    style Prevention fill:#90EE90
```

## Staleness Prevention

| Parameter | Default | Max | Description |
|-----------|---------|-----|-------------|
| `DATA_RETENTION_TIME_IN_DAYS` | 1 | 90 (Enterprise) | Table's Time Travel retention |
| `MAX_DATA_EXTENSION_TIME_IN_DAYS` | 14 | 90 | Extended retention for streams |

**Best Practice:** Consume streams before `STALE_AFTER` timestamp:
```sql
-- Check stream staleness
DESCRIBE STREAM my_stream;
-- Look at STALE_AFTER column
```

---

# When to Use Streams

## ✅ USE Streams When:

```mermaid
flowchart TB
    subgraph UseStreams["✅ IDEAL USE CASES"]
        U1["CDC Pipelines<br/>Track changes to staging tables"]
        U2["Incremental ETL<br/>Process only new/changed data"]
        U3["Audit Logging<br/>Capture all DML operations"]
        U4["Data Synchronization<br/>Keep tables in sync"]
        U5["Event Processing<br/>React to data changes"]
        U6["SCD Type 2<br/>Track historical changes"]
    end
    
    style UseStreams fill:#90EE90
```

### Ideal Scenarios:

| Scenario | Why Streams Work Well |
|----------|----------------------|
| **Staging → Production ETL** | Process only changed rows efficiently |
| **Data Warehouse Updates** | Incremental loads reduce compute costs |
| **Audit/Compliance** | Complete history of all changes |
| **Real-time Dashboards** | Near real-time data freshness |
| **Microservices Integration** | Track changes for downstream systems |

---

## ❌ DO NOT Use Streams When:

```mermaid
flowchart TB
    subgraph DontUse["❌ AVOID STREAMS FOR"]
        D1["Full Table Loads<br/>No benefit over direct query"]
        D2["Tables with No Updates<br/>Append-only pattern better"]
        D3["Geospatial Data<br/>Standard streams don't support"]
        D4["Materialized Views<br/>Not supported"]
        D5["Complex Aggregations<br/>Views with GROUP BY not supported"]
        D6["Very High Frequency<br/>Consider Dynamic Tables instead"]
    end
    
    style DontUse fill:#FF6B6B
```

### When to Use Alternatives:

| Scenario | Better Alternative |
|----------|-------------------|
| **Continuous transformation** | Dynamic Tables |
| **Real-time streaming** | Snowpipe Streaming |
| **Complex CDC logic** | Stored Procedures with CHANGES clause |
| **One-time migration** | Direct INSERT/SELECT |
| **Tables with only INSERTs** | Append-only stream or direct query |

---

# Streams on Views

## Supported View Operations

```mermaid
flowchart TB
    subgraph Supported["✅ SUPPORTED ON VIEWS"]
        S1[Projections<br/>SELECT columns]
        S2[Filters<br/>WHERE clause]
        S3[Inner Joins]
        S4[Cross Joins]
        S5[UNION ALL]
        S6[Nested Views]
    end
    
    subgraph NotSupported["❌ NOT SUPPORTED ON VIEWS"]
        N1[GROUP BY]
        N2[QUALIFY]
        N3[DISTINCT]
        N4[LIMIT]
        N5[Correlated Subqueries]
        N6[Subqueries not in FROM]
    end
    
    style Supported fill:#90EE90
    style NotSupported fill:#FF6B6B
```

## View Stream Requirements

```sql
-- Step 1: Enable change tracking on underlying tables
ALTER TABLE orders SET CHANGE_TRACKING = TRUE;
ALTER TABLE customers SET CHANGE_TRACKING = TRUE;

-- Step 2: Create view with supported operations
CREATE VIEW orders_with_customers AS
SELECT o.*, c.customer_name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id;

-- Step 3: Enable change tracking on view
ALTER VIEW orders_with_customers SET CHANGE_TRACKING = TRUE;

-- Step 4: Create stream on view
CREATE STREAM orders_customers_stream ON VIEW orders_with_customers;
```

---

# Edge Cases and Gotchas

## Critical Edge Cases

```mermaid
flowchart TB
    subgraph EdgeCases["⚠️ EDGE CASES TO KNOW"]
        E1["Transaction Isolation<br/>Stream locked during DML"]
        E2["Multiple Consumers<br/>Each needs own stream"]
        E3["Schema Changes<br/>May break stream queries"]
        E4["Cloning<br/>Unconsumed records lost"]
        E5["Time Travel<br/>Not available on stream data"]
        E6["Geospatial<br/>Use append-only streams"]
    end
    
    style EdgeCases fill:#FFD700
```

## Detailed Edge Cases

### 1. Multiple Consumers Need Separate Streams

```mermaid
flowchart LR
    subgraph Problem["❌ PROBLEM: Single Stream"]
        SRC1[(Table)]
        STR1[Stream]
        C1[Consumer 1]
        C2[Consumer 2]
        
        SRC1 --> STR1
        STR1 --> C1
        STR1 -.->|Offset advanced<br/>Data lost for C2| C2
    end
    
    subgraph Solution["✅ SOLUTION: Multiple Streams"]
        SRC2[(Table)]
        STR2[Stream 1]
        STR3[Stream 2]
        C3[Consumer 1]
        C4[Consumer 2]
        
        SRC2 --> STR2
        SRC2 --> STR3
        STR2 --> C3
        STR3 --> C4
    end
```

**Rule:** Create one stream per consumer to avoid data loss.

### 2. Transaction Isolation (Repeatable Read)

```mermaid
sequenceDiagram
    participant T1 as Transaction 1
    participant STR as Stream
    participant T2 as Transaction 2
    
    T1->>T1: BEGIN
    T1->>STR: SELECT * FROM stream
    Note over STR: Returns changes v1→v3
    
    T2->>T2: INSERT INTO source_table
    Note over STR: New changes recorded
    
    T1->>STR: SELECT * FROM stream (again)
    Note over STR: Still returns v1→v3<br/>(same as first query)
    
    T1->>T1: COMMIT
    Note over STR: Offset advances to v3
    
    T2->>STR: SELECT * FROM stream
    Note over STR: Returns changes v3→v4
```

**Rule:** Within a transaction, stream queries return the same data (repeatable read).

### 3. Schema Changes Can Break Streams

```sql
-- ❌ This can cause issues:
ALTER TABLE source_table ADD COLUMN new_col VARCHAR NOT NULL;

-- Stream may fail if historical data has NULL for new_col
-- because stream enforces current schema constraints

-- ✅ Better approach:
ALTER TABLE source_table ADD COLUMN new_col VARCHAR;  -- Allow NULL
```

### 4. CREATE OR REPLACE Makes Streams Stale

```sql
-- ❌ This makes all streams on the table STALE:
CREATE OR REPLACE TABLE my_table (...);

-- The table history is reset, stream offset becomes invalid

-- ✅ Use ALTER TABLE instead when possible
```

### 5. TRUNCATE Behavior

```mermaid
flowchart LR
    subgraph Standard["STANDARD STREAM"]
        S1["TRUNCATE recorded as<br/>DELETE for all rows"]
    end
    
    subgraph AppendOnly["APPEND-ONLY STREAM"]
        A1["TRUNCATE ignored<br/>Only INSERTs tracked"]
    end
```

---

# CHANGES Clause (Alternative to Streams)

## When to Use CHANGES vs Streams

```mermaid
flowchart TB
    subgraph Streams["USE STREAMS WHEN:"]
        S1[Need persistent offset]
        S2[Automated pipeline with Tasks]
        S3[Multiple consumers]
        S4[Regular consumption pattern]
    end
    
    subgraph Changes["USE CHANGES CLAUSE WHEN:"]
        C1[Ad-hoc queries]
        C2[Don't want to create object]
        C3[Query specific time range]
        C4[Multiple time ranges needed]
    end
    
    style Streams fill:#4169E1,color:#fff
    style Changes fill:#9370DB,color:#fff
```

## CHANGES Clause Example

```sql
-- Enable change tracking first
ALTER TABLE my_table SET CHANGE_TRACKING = TRUE;

-- Query changes between two points in time
SELECT *
FROM my_table
CHANGES(INFORMATION => DEFAULT)
AT(TIMESTAMP => '2024-01-01 00:00:00'::TIMESTAMP)
END(TIMESTAMP => '2024-01-02 00:00:00'::TIMESTAMP);

-- Query changes from a specific offset
SELECT *
FROM my_table
CHANGES(INFORMATION => APPEND_ONLY)
AT(OFFSET => -60*60);  -- Last hour
```

**Key Difference:** CHANGES clause does NOT advance any offset - it's read-only.

---

# Best Practices Summary

## Do's and Don'ts

```mermaid
flowchart TB
    subgraph Dos["✅ DO"]
        D1[Create separate stream per consumer]
        D2[Consume streams regularly]
        D3[Use SYSTEM$STREAM_HAS_DATA in tasks]
        D4[Monitor STALE_AFTER timestamp]
        D5[Use append-only for insert-heavy tables]
        D6[Enable change tracking before creating stream]
    end
    
    subgraph Donts["❌ DON'T"]
        N1[Share streams between consumers]
        N2[Let streams go stale]
        N3[Use CREATE OR REPLACE on source tables]
        N4[Use standard streams for geospatial data]
        N5[Assume stream stores data]
        N6[Query stream without DML to advance offset]
    end
    
    style Dos fill:#90EE90
    style Donts fill:#FF6B6B
```

---

# Quick Reference

## Stream Commands

```sql
-- Create standard stream
CREATE STREAM my_stream ON TABLE my_table;

-- Create append-only stream
CREATE STREAM my_stream ON TABLE my_table APPEND_ONLY = TRUE;

-- Create stream on view
CREATE STREAM my_stream ON VIEW my_view;

-- Check stream status
DESCRIBE STREAM my_stream;
SHOW STREAMS;

-- Check if stream has data
SELECT SYSTEM$STREAM_HAS_DATA('my_stream');

-- Consume stream (advances offset)
INSERT INTO target SELECT * FROM my_stream;

-- Reset stream offset (recreate)
CREATE OR REPLACE STREAM my_stream ON TABLE my_table;

-- Manual offset reset without data
INSERT INTO temp_table SELECT * FROM my_stream WHERE 1=0;
```

## Required Privileges

| Object | Privilege | Notes |
|--------|-----------|-------|
| Database | USAGE | |
| Schema | USAGE | |
| Stream | SELECT | To query stream |
| Source Table/View | SELECT | Stream source |

---

# Summary: Stream Decision Tree

```mermaid
flowchart TD
    START([Need to track table changes?]) --> Q1{Need CDC?}
    
    Q1 -->|Yes| Q2{Automated pipeline?}
    Q1 -->|No| ALT1[Use direct queries]
    
    Q2 -->|Yes| Q3{Multiple consumers?}
    Q2 -->|No| ALT2[Consider CHANGES clause]
    
    Q3 -->|Yes| MULTI[Create separate stream<br/>for each consumer]
    Q3 -->|No| SINGLE[Create single stream]
    
    MULTI --> Q4{Insert-only workload?}
    SINGLE --> Q4
    
    Q4 -->|Yes| APPEND[Use APPEND_ONLY stream]
    Q4 -->|No| Q5{Geospatial data?}
    
    Q5 -->|Yes| APPEND
    Q5 -->|No| STD[Use STANDARD stream]
    
    APPEND --> TASK[Pair with Task<br/>using SYSTEM$STREAM_HAS_DATA]
    STD --> TASK
    
    style START fill:#4169E1,color:#fff
    style TASK fill:#90EE90,stroke:#228B22
```

---

# References

- [Introduction to Streams](https://docs.snowflake.com/en/user-guide/streams-intro)
- [CREATE STREAM](https://docs.snowflake.com/en/sql-reference/sql/create-stream)
- [Streams and Tasks](https://docs.snowflake.com/en/user-guide/data-pipelines-intro)
- [CHANGES Clause](https://docs.snowflake.com/en/sql-reference/constructs/changes)

---

*Document Version: 1.0*  
*Based on: Snowflake Official Documentation*  
*Last Updated: February 2026*
