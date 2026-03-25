/*
================================================================================
PREREQUISITE GRANTS FOR CDC MONITORING AND OBSERVABILITY FRAMEWORK v4.1
================================================================================
Run this script BEFORE deploying MONITORING_OBSERVABILITY.sql
Must be executed by ACCOUNTADMIN or SECURITYADMIN role

Role: D-SNW-DEVBI1-ETL (CDC pipeline execution role)

CHANGES FROM V4.0:
    - Replaced GRANT ALL ON SCHEMA D_BRONZE.MONITORING with least-privilege grants
      (USAGE, CREATE TABLE, CREATE VIEW, CREATE PROCEDURE, CREATE TASK)
    - Replaced GRANT ALL ON objects with specific SELECT/INSERT/UPDATE/DELETE
    - Added SECURITY REVIEW note on IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE
    - Corrected Section 11 comments: INFORMATION_SCHEMA.STREAMS does not exist
    - Retained INFORMATION_SCHEMA view grants for metadata (COLUMNS, TABLES, etc.)

CHANGES FROM V3.0 (carried forward):
    - Updated for 22 tables (removed legacy OPTRN references)
    - SNOWFLAKE IMPORTED PRIVILEGES retained for historical queries
================================================================================
*/

-- =============================================================================
-- SECTION 1: DATABASE-LEVEL GRANTS
-- =============================================================================
GRANT USAGE ON DATABASE D_RAW TO ROLE "D-SNW-DEVBI1-ETL";
GRANT USAGE ON DATABASE D_BRONZE TO ROLE "D-SNW-DEVBI1-ETL";

-- =============================================================================
-- SECTION 2: SCHEMA-LEVEL GRANTS
-- =============================================================================
GRANT USAGE ON SCHEMA D_RAW.SADB TO ROLE "D-SNW-DEVBI1-ETL";
GRANT USAGE ON SCHEMA D_BRONZE.SADB TO ROLE "D-SNW-DEVBI1-ETL";
-- v4.1 FIX: Replaced GRANT ALL (gave DROP, ALTER, etc.) with least-privilege grants
GRANT USAGE ON SCHEMA D_BRONZE.MONITORING TO ROLE "D-SNW-DEVBI1-ETL";
GRANT CREATE TABLE ON SCHEMA D_BRONZE.MONITORING TO ROLE "D-SNW-DEVBI1-ETL";
GRANT CREATE VIEW ON SCHEMA D_BRONZE.MONITORING TO ROLE "D-SNW-DEVBI1-ETL";
GRANT CREATE PROCEDURE ON SCHEMA D_BRONZE.MONITORING TO ROLE "D-SNW-DEVBI1-ETL";
GRANT CREATE TASK ON SCHEMA D_BRONZE.MONITORING TO ROLE "D-SNW-DEVBI1-ETL";
GRANT CREATE SCHEMA ON DATABASE D_BRONZE TO ROLE "D-SNW-DEVBI1-ETL";

-- =============================================================================
-- SECTION 3: D_RAW.SADB SOURCE TABLE GRANTS (22 _BASE source tables)
-- =============================================================================
GRANT SELECT ON TABLE D_RAW.SADB.EQPMNT_AAR_BASE_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.EQPMV_RFEQP_MVMNT_EVENT_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.LCMTV_EMIS_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.STNWYB_MSG_DN_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRAIN_CNST_DTL_RAIL_EQPT_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRAIN_CNST_SMRY_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRAIN_OPTRN_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRAIN_OPTRN_LEG_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRAIN_PLAN_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRAIN_PLAN_EVENT_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRAIN_PLAN_LEG_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRAIN_TYPE_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRAIN_KIND_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRKFCG_FIXED_PLANT_ASSET_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRKFCG_FXPLA_TRACK_LCTN_DN_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRKFCG_SBDVSN_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRKFCG_SRVC_AREA_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRKFCG_TRACK_SGMNT_DN_BASE TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON TABLE D_RAW.SADB.TRKFC_TRSTN_BASE TO ROLE "D-SNW-DEVBI1-ETL";

-- =============================================================================
-- SECTION 4: D_BRONZE.SADB TARGET TABLE GRANTS (22 target tables)
-- =============================================================================
GRANT SELECT ON ALL TABLES IN SCHEMA D_BRONZE.SADB TO ROLE "D-SNW-DEVBI1-ETL";
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA D_BRONZE.SADB TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON FUTURE TABLES IN SCHEMA D_BRONZE.SADB TO ROLE "D-SNW-DEVBI1-ETL";

-- =============================================================================
-- SECTION 5: D_BRONZE.MONITORING OBJECT-LEVEL GRANTS (least-privilege)
-- =============================================================================
-- v4.1 FIX: Replaced GRANT ALL with specific privileges needed by ETL role.
-- ETL needs SELECT/INSERT/UPDATE/DELETE on tables, SELECT on views, USAGE on procedures.
-- No DROP, ALTER, TRUNCATE — those are reserved for schema owners.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA D_BRONZE.MONITORING TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON ALL VIEWS IN SCHEMA D_BRONZE.MONITORING TO ROLE "D-SNW-DEVBI1-ETL";
GRANT USAGE ON ALL PROCEDURES IN SCHEMA D_BRONZE.MONITORING TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA D_BRONZE.MONITORING TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON FUTURE VIEWS IN SCHEMA D_BRONZE.MONITORING TO ROLE "D-SNW-DEVBI1-ETL";
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA D_BRONZE.MONITORING TO ROLE "D-SNW-DEVBI1-ETL";

-- =============================================================================
-- SECTION 6: STREAM GRANTS (22 streams in D_RAW.SADB)
-- =============================================================================
GRANT SELECT ON ALL STREAMS IN SCHEMA D_RAW.SADB TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON FUTURE STREAMS IN SCHEMA D_RAW.SADB TO ROLE "D-SNW-DEVBI1-ETL";

-- =============================================================================
-- SECTION 7: TASK GRANTS
-- =============================================================================
GRANT OPERATE, MONITOR ON ALL TASKS IN SCHEMA D_RAW.SADB TO ROLE "D-SNW-DEVBI1-ETL";
GRANT OPERATE, MONITOR ON ALL TASKS IN SCHEMA D_BRONZE.MONITORING TO ROLE "D-SNW-DEVBI1-ETL";
GRANT EXECUTE TASK ON ACCOUNT TO ROLE "D-SNW-DEVBI1-ETL";

-- =============================================================================
-- SECTION 8: PROCEDURE EXECUTION GRANTS
-- =============================================================================
GRANT USAGE ON ALL PROCEDURES IN SCHEMA D_RAW.SADB TO ROLE "D-SNW-DEVBI1-ETL";
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA D_RAW.SADB TO ROLE "D-SNW-DEVBI1-ETL";

-- =============================================================================
-- SECTION 9: WAREHOUSE GRANT
-- =============================================================================
GRANT USAGE ON WAREHOUSE INFA_INGEST_WH TO ROLE "D-SNW-DEVBI1-ETL";

-- =============================================================================
-- SECTION 10: ACCOUNT_USAGE ACCESS (HISTORICAL TASK/QUERY MONITORING)
-- =============================================================================
-- v4.1 NOTE: IMPORTED PRIVILEGES exposes ALL ACCOUNT_USAGE views (login history,
-- billing, etc.) to the ETL role. This is retained because:
--   - SP_CAPTURE_TASK_HEALTH uses TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) [no IMPORTED PRIVILEGES needed]
--   - VW_TASK_EXECUTION_HISTORY uses TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) [no IMPORTED PRIVILEGES needed]
--   - Backup/historical queries may reference SNOWFLAKE.ACCOUNT_USAGE
-- SECURITY REVIEW: If your organization does NOT need ACCOUNT_USAGE access for
-- the ETL role, comment out or remove the following line to reduce exposure.
-- The monitoring framework will still function without it.
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE "D-SNW-DEVBI1-ETL";

-- =============================================================================
-- SECTION 11: INFORMATION_SCHEMA ACCESS
-- =============================================================================
-- v4.1 FIX: Removed D_RAW.INFORMATION_SCHEMA.STREAMS grants — that view does
-- NOT exist in Snowflake. Stream metadata is obtained via SHOW STREAMS command.
-- INFORMATION_SCHEMA.TASK_HISTORY() is a table function (not a view), accessible
-- to any role with MONITOR/OPERATE on tasks — no additional grants needed.
-- Retained: INFORMATION_SCHEMA view access for metadata queries (e.g., COLUMNS, TABLES).
GRANT SELECT ON ALL VIEWS IN SCHEMA D_RAW.INFORMATION_SCHEMA TO ROLE "D-SNW-DEVBI1-ETL";
GRANT SELECT ON ALL VIEWS IN SCHEMA D_BRONZE.INFORMATION_SCHEMA TO ROLE "D-SNW-DEVBI1-ETL";

-- =============================================================================
-- VERIFICATION
-- =============================================================================
-- USE ROLE "D-SNW-DEVBI1-ETL";
-- SHOW GRANTS TO ROLE "D-SNW-DEVBI1-ETL";
-- SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();