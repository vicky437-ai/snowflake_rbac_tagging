/*
================================================================================
CDC DATA PRESERVATION - PARAMETER CONFIGURATION
================================================================================
Purpose      : Set environment-specific parameters for CDC deployment
Version      : 1.0 (Production)
Last Updated : 2026-02-23
================================================================================

INSTRUCTIONS:
1. Review and modify the parameter values below for your environment
2. Execute this script FIRST before any other deployment scripts
3. These session variables will be used by subsequent scripts

================================================================================
*/

-- =============================================================================
-- ENVIRONMENT PARAMETERS - MODIFY THESE FOR YOUR ENVIRONMENT
-- =============================================================================

-- Source Environment (where _BASE tables exist)
SET SOURCE_DATABASE = 'D_RAW';
SET SOURCE_SCHEMA = 'SADB';

-- Target Environment (where CDC preserved tables will be created)
SET TARGET_DATABASE = 'D_BRONZE';
SET TARGET_SCHEMA = 'SADB';

-- Warehouse Configuration
SET CDC_WAREHOUSE = 'INFA_INGEST_WH';

-- Task Schedule (CRON or interval format)
-- Examples: '5 MINUTE', '10 MINUTE', '1 HOUR', 'USING CRON 0 */5 * * * UTC'
SET TASK_SCHEDULE = '5 MINUTE';

-- Data Retention Configuration
SET DATA_RETENTION_DAYS = 45;
SET MAX_EXTENSION_DAYS = 15;

-- =============================================================================
-- VERIFY PARAMETERS
-- =============================================================================
SELECT 
    $SOURCE_DATABASE AS SOURCE_DATABASE,
    $SOURCE_SCHEMA AS SOURCE_SCHEMA,
    $TARGET_DATABASE AS TARGET_DATABASE,
    $TARGET_SCHEMA AS TARGET_SCHEMA,
    $CDC_WAREHOUSE AS CDC_WAREHOUSE,
    $TASK_SCHEDULE AS TASK_SCHEDULE,
    $DATA_RETENTION_DAYS AS DATA_RETENTION_DAYS,
    $MAX_EXTENSION_DAYS AS MAX_EXTENSION_DAYS;

-- =============================================================================
-- SET CONTEXT
-- =============================================================================
USE WAREHOUSE IDENTIFIER($CDC_WAREHOUSE);

-- Verify source database exists
SELECT COUNT(*) AS SOURCE_DB_EXISTS 
FROM INFORMATION_SCHEMA.DATABASES 
WHERE DATABASE_NAME = $SOURCE_DATABASE;

-- Verify target database exists
SELECT COUNT(*) AS TARGET_DB_EXISTS 
FROM INFORMATION_SCHEMA.DATABASES 
WHERE DATABASE_NAME = $TARGET_DATABASE;

SHOW PARAMETERS LIKE '%' IN SESSION;

/*
================================================================================
PARAMETER REFERENCE
================================================================================

SOURCE_DATABASE    : Database containing the source _BASE tables
                     (e.g., D_RAW, RAW_DB, SOURCE_DB)

SOURCE_SCHEMA      : Schema containing the source _BASE tables
                     (e.g., SADB, PUBLIC, RAW_SCHEMA)

TARGET_DATABASE    : Database where CDC preserved tables will be created
                     (e.g., D_BRONZE, BRONZE_DB, TARGET_DB)

TARGET_SCHEMA      : Schema where CDC preserved tables will be created
                     (e.g., SADB, PUBLIC, BRONZE_SCHEMA)

CDC_WAREHOUSE      : Warehouse to use for CDC processing tasks
                     Recommended size: SMALL or MEDIUM
                     (e.g., INFA_INGEST_WH, CDC_WH, ETL_WH)

TASK_SCHEDULE      : How often tasks should check for changes
                     - '5 MINUTE' = Every 5 minutes
                     - '1 HOUR' = Every hour
                     - 'USING CRON 0 */5 * * * UTC' = Cron expression

DATA_RETENTION_DAYS: Snowflake Time Travel retention period
                     Default: 45 days (Standard edition supports up to 1 day,
                     Enterprise supports up to 90 days)

MAX_EXTENSION_DAYS : Additional days beyond DATA_RETENTION_DAYS
                     Total retention = DATA_RETENTION_DAYS + MAX_EXTENSION_DAYS

================================================================================
*/
