-- ============================================================================
-- Script: 06_monitoring/audit_queries.sql
-- Purpose: Comprehensive monitoring and audit queries for RBAC and tagging
-- Author: Venkannababu Thatavarthi
-- Date: October 13, 2025
-- Version: 1.0
-- ============================================================================
-- Prerequisites: Access to SNOWFLAKE.ACCOUNT_USAGE schema
-- Execution Time: Varies by query
-- ============================================================================

USE ROLE governance_admin;
USE SCHEMA governance.metadata;

-- ============================================================================
-- SECTION 1: Role Assignment Monitoring
-- ============================================================================

-- Query 1: All active user-role assignments
SELECT 
    grantee_name AS user_name,
    name AS role_name,
    granted_on AS assignment_date,
    granted_by,
    DATEDIFF(day, granted_on, CURRENT_TIMESTAMP()) AS days_assigned
FROM snowflake.account_usage.grants_to_users
WHERE granted_to = 'USER'
  AND deleted_on IS NULL
ORDER BY granted_on DESC;

-- Query 2: Users with multiple roles
SELECT 
    grantee_name AS user_name,
    COUNT(DISTINCT name) AS role_count,
    LISTAGG(DISTINCT name, ', ') WITHIN GROUP (ORDER BY name) AS roles
FROM snowflake.account_usage.grants_to_users
WHERE granted_to = 'USER'
  AND deleted_on IS NULL
GROUP BY grantee_name
HAVING COUNT(DISTINCT name) > 1
ORDER BY role_count DESC;

-- Query 3: Users with ACCOUNTADMIN role (CRITICAL MONITORING)
SELECT 
    grantee_name AS user_name,
    granted_on AS assignment_date,
    granted_by,
    DATEDIFF(day, granted_on, CURRENT_TIMESTAMP()) AS days_with_admin
FROM snowflake.account_usage.grants_to_users
WHERE name = 'ACCOUNTADMIN'
  AND deleted_on IS NULL
ORDER BY granted_on;

-- Query 4: Recent role assignments (last 7 days)
SELECT 
    grantee_name AS user_name,
    name AS role_name,
    granted_on,
    granted_by,
    DATEDIFF(hour, granted_on, CURRENT_TIMESTAMP()) AS hours_ago
FROM snowflake.account_usage.grants_to_users
WHERE granted_to = 'USER'
  AND granted_on >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND deleted_on IS NULL
ORDER BY granted_on DESC;

-- Query 5: Orphaned roles (roles not granted to any user or role)
SELECT 
    r.name AS orphaned_role,
    r.created_on,
    r.owner,
    r.comment
FROM snowflake.account_usage.roles r
WHERE r.deleted_on IS NULL
  AND r.name NOT LIKE 'SNOWFLAKE%'
  AND NOT EXISTS (
      SELECT 1 
      FROM snowflake.account_usage.grants_to_roles g
      WHERE g.name = r.name 
        AND g.deleted_on IS NULL
  )
  AND NOT EXISTS (
      SELECT 1
      FROM snowflake.account_usage.grants_to_users u
      WHERE u.name = r.name
        AND u.deleted_on IS NULL
  )
ORDER BY r.created_on DESC;

-- ============================================================================
-- SECTION 2: Privilege Monitoring
-- ============================================================================

-- Query 6: All privileges granted to roles
SELECT 
    grantee_name AS role_name,
    privilege,
    granted_on AS object_type,
    name AS object_name,
    grant_option,
    granted_by,
    granted_on AS grant_date
FROM snowflake.account_usage.grants_to_roles
WHERE deleted_on IS NULL
  AND grantee_name NOT LIKE 'SNOWFLAKE%'
ORDER BY grantee_name, granted_on, privilege;

-- Query 7: Roles with OWNERSHIP privileges
SELECT 
    grantee_name AS role_name,
    granted_on AS object_type,
    name AS object_name,
    granted_on AS ownership_date
FROM snowflake.account_usage.grants_to_roles
WHERE privilege = 'OWNERSHIP'
  AND deleted_on IS NULL
ORDER BY grantee_name, granted_on;

-- Query 8: Roles with dangerous privileges (CRITICAL MONITORING)
SELECT 
    grantee_name AS role_name,
    privilege,
    granted_on AS object_type,
    name AS object_name,
    granted_by,
    granted_on AS grant_date
FROM snowflake.account_usage.grants_to_roles
WHERE deleted_on IS NULL
  AND privilege IN ('MANAGE GRANTS', 'CREATE ROLE', 'CREATE USER', 'OWNERSHIP')
  AND grantee_name NOT IN ('ACCOUNTADMIN', 'SECURITYADMIN', 'USERADMIN', 'SYSADMIN')
ORDER BY granted_on DESC;

-- Query 9: Future grants summary
SELECT 
    grantee_name AS role_name,
    grant_on AS future_object_type,
    name AS location,
    COUNT(*) AS future_grant_count,
    LISTAGG(DISTINCT privilege, ', ') AS privileges
FROM snowflake.account_usage.grants_to_roles
WHERE deleted_on IS NULL
  AND name LIKE '%FUTURE%'
GROUP BY grantee_name, grant_on, name
ORDER BY grantee_name;

-- Query 10: Privilege changes in last 30 days
SELECT 
    grantee_name AS role_name,
    privilege,
    granted_on AS object_type,
    name AS object_name,
    granted_on AS change_date,
    granted_by,
    CASE 
        WHEN deleted_on IS NULL THEN 'GRANTED'
        ELSE 'REVOKED'
    END AS action
FROM snowflake.account_usage.grants_to_roles
WHERE (granted_on >= DATEADD(day, -30, CURRENT_TIMESTAMP())
   OR deleted_on >= DATEADD(day, -30, CURRENT_TIMESTAMP()))
  AND grantee_name NOT LIKE 'SNOWFLAKE%'
ORDER BY COALESCE(deleted_on, granted_on) DESC;

-- ============================================================================
-- SECTION 3: Tag Coverage Monitoring
-- ============================================================================

-- Query 11: Tag coverage by database
SELECT 
    object_database,
    COUNT(DISTINCT object_name) AS tagged_objects,
    COUNT(DISTINCT CASE WHEN object_type = 'TABLE' THEN object_name END) AS tagged_tables,
    COUNT(DISTINCT CASE WHEN object_type = 'COLUMN' THEN object_name END) AS tagged_columns,
    COUNT(DISTINCT tag_name) AS unique_tags
FROM snowflake.account_usage.tag_references
WHERE tag_database = 'GOVERNANCE'
  AND tag_schema = 'TAGS'
  AND deleted IS NULL
GROUP BY object_database
ORDER BY tagged_objects DESC;

-- Query 12: Most commonly used tags
SELECT 
    tag_name,
    COUNT(DISTINCT object_name) AS usage_count,
    COUNT(DISTINCT object_database) AS databases_used,
    LISTAGG(DISTINCT tag_value, ', ') AS common_values
FROM snowflake.account_usage.tag_references
WHERE tag_database = 'GOVERNANCE'
  AND tag_schema = 'TAGS'
  AND deleted IS NULL
GROUP BY tag_name
ORDER BY usage_count DESC;

-- Query 13: Untagged tables with high row counts
SELECT 
    t.table_catalog AS database_name,
    t.table_schema,
    t.table_name,
    t.row_count,
    t.bytes,
    t.created AS table_created,
    DATEDIFF(day, t.created, CURRENT_TIMESTAMP()) AS days_old
FROM snowflake.account_usage.tables t
WHERE t.deleted IS NULL
  AND t.table_schema NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
  AND t.table_catalog NOT IN ('SNOWFLAKE', 'GOVERNANCE')
  AND NOT EXISTS (
      SELECT 1 
      FROM snowflake.account_usage.tag_references tr
      WHERE tr.object_database = t.table_catalog
        AND tr.object_schema = t.table_schema
        AND tr.object_name = t.table_name
        AND tr.tag_database = 'GOVERNANCE'
        AND tr.deleted IS NULL
  )
ORDER BY t.row_count DESC NULLS LAST
LIMIT 100;

-- Query 14: PII columns inventory
SELECT 
    tr.object_database,
    tr.object_schema,
    tr.object_name AS table_name,
    tr.column_name,
    tr.tag_value AS pii_type,
    tr.tag_created,
    c.data_type
FROM snowflake.account_usage.tag_references tr
JOIN snowflake.account_usage.columns c
    ON tr.object_database = c.table_catalog
    AND tr.object_schema = c.table_schema
    AND tr.object_name = c.table_name
    AND tr.column_name = c.column_name
WHERE tr.tag_name LIKE 'PII_%'
  AND tr.deleted IS NULL
  AND c.deleted IS NULL
ORDER BY tr.object_database, tr.object_schema, tr.object_name, tr.column_name;

-- Query 15: Tables with sensitivity tags
SELECT 
    object_database,
    object_schema,
    object_name,
    tag_value AS sensitivity_level,
    tag_created
FROM snowflake.account_usage.tag_references
WHERE tag_name = 'DATA_SENSITIVITY'
  AND deleted IS NULL
ORDER BY 
    CASE tag_value
        WHEN 'RESTRICTED' THEN 1
        WHEN 'CONFIDENTIAL' THEN 2
        WHEN 'INTERNAL' THEN 3
        WHEN 'PUBLIC' THEN 4
    END,
    object_database, object_schema, object_name;

-- ============================================================================
-- SECTION 4: Masking Policy Monitoring
-- ============================================================================

-- Query 16: Masking policies and their associations
SELECT 
    pr.policy_name,
    pr.policy_kind,
    COUNT(DISTINCT pr.ref_database_name || '.' || pr.ref_schema_name || '.' || pr.ref_entity_name) AS protected_objects,
    pr.policy_owner,
    p.created AS policy_created
FROM snowflake.account_usage.policy_references pr
JOIN snowflake.account_usage.masking_policies p
    ON pr.policy_name = p.name
    AND pr.policy_db = p.database_name
    AND pr.policy_schema = p.schema_name
WHERE pr.deleted IS NULL
  AND p.deleted IS NULL
  AND pr.policy_kind = 'MASKING_POLICY'
GROUP BY pr.policy_name, pr.policy_kind, pr.policy_owner, p.created
ORDER BY protected_objects DESC;

-- Query 17: Columns without masking policies (potential PII)
SELECT 
    c.table_catalog AS database_name,
    c.table_schema,
    c.table_name,
    c.column_name,
    c.data_type,
    CASE 
        WHEN c.column_name ILIKE '%email%' THEN 'Likely PII - Email'
        WHEN c.column_name ILIKE '%phone%' THEN 'Likely PII - Phone'
        WHEN c.column_name ILIKE '%ssn%' THEN 'Likely PII - SSN'
        WHEN c.column_name ILIKE '%credit%card%' THEN 'Likely PII - Credit Card'
        WHEN c.column_name ILIKE '%address%' THEN 'Likely PII - Address'
        WHEN c.column_name ILIKE '%name%' AND c.column_name NOT ILIKE '%file%' THEN 'Likely PII - Name'
        ELSE 'Review Needed'
    END AS pii_likelihood
FROM snowflake.account_usage.columns c
WHERE c.deleted IS NULL
  AND c.table_schema NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
  AND c.table_catalog NOT IN ('SNOWFLAKE', 'GOVERNANCE')
  AND (
      c.column_name ILIKE '%email%'
      OR c.column_name ILIKE '%phone%'
      OR c.column_name ILIKE '%ssn%'
      OR c.column_name ILIKE '%credit%card%'
      OR c.column_name ILIKE '%address%'
      OR (c.column_name ILIKE '%name%' AND c.column_name NOT ILIKE '%file%')
  )
  AND NOT EXISTS (
      SELECT 1 
      FROM snowflake.account_usage.policy_references pr
      WHERE pr.ref_database_name = c.table_catalog
        AND pr.ref_schema_name = c.table_schema
        AND pr.ref_entity_name = c.table_name
        AND pr.ref_column_name = c.column_name
        AND pr.policy_kind = 'MASKING_POLICY'
        AND pr.deleted IS NULL
  )
ORDER BY pii_likelihood, c.table_catalog, c.table_schema, c.table_name;

-- ============================================================================
-- SECTION 5: Access Pattern Analysis
-- ============================================================================

-- Query 18: Most active users (by query count)
SELECT 
    user_name,
    COUNT(*) AS query_count,
    COUNT(DISTINCT database_name) AS databases_accessed,
    COUNT(DISTINCT CASE WHEN error_code IS NOT NULL THEN query_id END) AS failed_queries,
    MIN(start_time) AS first_query,
    MAX(start_time) AS last_query
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND user_name NOT LIKE 'SNOWFLAKE%'
GROUP BY user_name
ORDER BY query_count DESC
LIMIT 50;

-- Query 19: Failed access attempts (potential security issues)
SELECT 
    user_name,
    role_name,
    database_name,
    schema_name,
    query_text,
    error_code,
    error_message,
    start_time
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND error_code IN ('1063', '3001', '3003', '3011') -- Access control errors
ORDER BY start_time DESC
LIMIT 100;

-- Query 20: Unusual access patterns (users accessing sensitive databases)
SELECT 
    qh.user_name,
    qh.role_name,
    qh.database_name,
    COUNT(*) AS query_count,
    MIN(qh.start_time) AS first_access,
    MAX(qh.start_time) AS last_access
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
  AND qh.database_name IN ('FINANCE_DB', 'HR_DB') -- Sensitive databases
  AND qh.user_name NOT LIKE 'SNOWFLAKE%'
GROUP BY qh.user_name, qh.role_name, qh.database_name
ORDER BY query_count DESC;

-- ============================================================================
-- SECTION 6: Compliance Reporting
-- ============================================================================

-- Query 21: GDPR compliance - all EU data with PII tags
SELECT 
    tr.object_database,
    tr.object_schema,
    tr.object_name,
    tr.column_name,
    tr.tag_value AS pii_type,
    tr2.tag_value AS data_residency
FROM snowflake.account_usage.tag_references tr
LEFT JOIN snowflake.account_usage.tag_references tr2
    ON tr.object_database = tr2.object_database
    AND tr.object_schema = tr2.object_schema
    AND tr.object_name = tr2.object_name
    AND tr2.tag_name = 'DATA_RESIDENCY'
    AND tr2.deleted IS NULL
WHERE tr.tag_name LIKE 'PII_%'
  AND tr.deleted IS NULL
  AND (tr2.tag_value = 'EU' OR tr2.tag_value IS NULL)
ORDER BY tr.object_database, tr.object_schema, tr.object_name;

-- Query 22: SOX compliance - financial data audit
SELECT 
    tr.object_database,
    tr.object_schema,
    tr.object_name,
    tr.tag_value AS compliance_scope,
    COUNT(DISTINCT c.column_name) AS column_count,
    t.row_count
FROM snowflake.account_usage.tag_references tr
JOIN snowflake.account_usage.tables t
    ON tr.object_database = t.table_catalog
    AND tr.object_schema = t.table_schema
    AND tr.object_name = t.table_name
LEFT JOIN snowflake.account_usage.columns c
    ON t.table_catalog = c.table_catalog
    AND t.table_schema = c.table_schema
    AND t.table_name = c.table_name
WHERE tr.tag_name = 'COMPLIANCE_SCOPE'
  AND tr.tag_value = 'SOX'
  AND tr.deleted IS NULL
  AND t.deleted IS NULL
GROUP BY tr.object_database, tr.object_schema, tr.object_name, tr.tag_value, t.row_count
ORDER BY t.row_count DESC NULLS LAST;

-- Query 23: Data retention compliance
SELECT 
    object_database,
    object_schema,
    object_name,
    tag_value AS retention_period,
    tag_created,
    DATEDIFF(day, tag_created, CURRENT_TIMESTAMP()) AS days_since_tagged
FROM snowflake.account_usage.tag_references
WHERE tag_name = 'RETENTION_PERIOD'
  AND deleted IS NULL
ORDER BY 
    CASE tag_value
        WHEN '30_DAYS' THEN 1
        WHEN '90_DAYS' THEN 2
        WHEN '1_YEAR' THEN 3
        WHEN '3_YEARS' THEN 4
        WHEN '7_YEARS' THEN 5
        WHEN '10_YEARS' THEN 6
        WHEN 'PERMANENT' THEN 7
    END;

-- ============================================================================
-- SECTION 7: Security Alerts
-- ============================================================================

-- Query 24: Recent role creations (potential unauthorized changes)
SELECT 
    name AS role_name,
    created_on,
    owner,
    comment,
    DATEDIFF(hour, created_on, CURRENT_TIMESTAMP()) AS hours_since_creation
FROM snowflake.account_usage.roles
WHERE created_on >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND name NOT LIKE 'SNOWFLAKE%'
ORDER BY created_on DESC;

-- Query 25: ACCOUNTADMIN usage (CRITICAL MONITORING)
SELECT 
    user_name,
    role_name,
    COUNT(*) AS query_count,
    MIN(start_time) AS first_usage,
    MAX(start_time) AS last_usage,
    COUNT(DISTINCT DATE(start_time)) AS days_active
FROM snowflake.account_usage.query_history
WHERE role_name = 'ACCOUNTADMIN'
  AND start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY user_name, role_name
ORDER BY query_count DESC;

-- Query 26: Privilege escalation attempts
SELECT 
    query_id,
    user_name,
    role_name,
    query_text,
    execution_status,
    error_code,
    error_message,
    start_time
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND (
      query_text ILIKE '%GRANT%ACCOUNTADMIN%'
      OR query_text ILIKE '%GRANT%SECURITYADMIN%'
      OR query_text ILIKE '%CREATE ROLE%'
      OR query_text ILIKE '%MANAGE GRANTS%'
  )
  AND role_name NOT IN ('ACCOUNTADMIN', 'SECURITYADMIN', 'USERADMIN')
ORDER BY start_time DESC;

-- ============================================================================
-- SECTION 8: Performance and Usage Metrics
-- ============================================================================

-- Query 27: Role usage statistics
SELECT 
    role_name,
    COUNT(DISTINCT user_name) AS unique_users,
    COUNT(*) AS total_queries,
    SUM(total_elapsed_time) / 1000 AS total_execution_seconds,
    AVG(total_elapsed_time) / 1000 AS avg_execution_seconds
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
  AND role_name NOT LIKE 'SNOWFLAKE%'
GROUP BY role_name
ORDER BY total_queries DESC
LIMIT 50;

-- Query 28: Database access frequency
SELECT 
    database_name,
    COUNT(DISTINCT user_name) AS unique_users,
    COUNT(*) AS query_count,
    SUM(bytes_scanned) / POWER(1024, 3) AS gb_scanned
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND database_name IS NOT NULL
  AND database_name NOT IN ('SNOWFLAKE')
GROUP BY database_name
ORDER BY query_count DESC;

-- ============================================================================
-- SECTION 9: Summary Dashboards
-- ============================================================================

-- Query 29: Governance health check summary
SELECT 
    'Total Users' AS metric,
    COUNT(DISTINCT name) AS value
FROM snowflake.account_usage.users
WHERE deleted_on IS NULL

UNION ALL

SELECT 
    'Total Custom Roles',
    COUNT(DISTINCT name)
FROM snowflake.account_usage.roles
WHERE deleted_on IS NULL
  AND name NOT LIKE 'SNOWFLAKE%'

UNION ALL

SELECT 
    'Total Tagged Objects',
    COUNT(DISTINCT object_database || '.' || object_schema || '.' || object_name)
FROM snowflake.account_usage.tag_references
WHERE tag_database = 'GOVERNANCE'
  AND deleted IS NULL

UNION ALL

SELECT 
    'Active Masking Policies',
    COUNT(DISTINCT name)
FROM snowflake.account_usage.masking_policies
WHERE deleted IS NULL

UNION ALL

SELECT 
    'Tables Without Tags',
    COUNT(DISTINCT t.table_catalog || '.' || t.table_schema || '.' || t.table_name)
FROM snowflake.account_usage.tables t
WHERE t.deleted IS NULL
  AND t.table_schema NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
  AND NOT EXISTS (
      SELECT 1 FROM snowflake.account_usage.tag_references tr
      WHERE tr.object_database = t.table_catalog
        AND tr.object_schema = t.table_schema
        AND tr.object_name = t.table_name
        AND tr.tag_database = 'GOVERNANCE'
        AND tr.deleted IS NULL
  )

UNION ALL

SELECT 
    'Users with ACCOUNTADMIN',
    COUNT(DISTINCT grantee_name)
FROM snowflake.account_usage.grants_to_users
WHERE name = 'ACCOUNTADMIN'
  AND deleted_on IS NULL;

-- ============================================================================
-- SECTION 10: Automated Reporting Procedures
-- ============================================================================

-- Create procedure for daily security report
CREATE OR REPLACE PROCEDURE sp_daily_security_report()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    report VARCHAR DEFAULT '';
BEGIN
    -- This procedure would compile key security metrics
    -- In production, this would send email/notification
    report := 'Daily Security Report for ' || CURRENT_DATE() || CHR(10);
    report := report || '===========================================' || CHR(10);
    
    -- Add report sections here
    report := report || 'ACCOUNTADMIN usage: Check query 25' || CHR(10);
    report := report || 'Failed access attempts: Check query 19' || CHR(10);
    report := report || 'Recent role changes: Check query 24' || CHR(10);
    
    RETURN report;
END;
$$;

SELECT '
=============================================================================
MONITORING AND AUDIT QUERIES READY
=============================================================================

Query Categories Available:
1. Role Assignment Monitoring (Queries 1-5)
2. Privilege Monitoring (Queries 6-10)
3. Tag Coverage Monitoring (Queries 11-15)
4. Masking Policy Monitoring (Queries 16-17)
5. Access Pattern Analysis (Queries 18-20)
6. Compliance Reporting (Queries 21-23)
7. Security Alerts (Queries 24-26)
8. Performance Metrics (Queries 27-28)
9. Summary Dashboards (Query 29)

Critical Monitoring Queries:
- Query 3: Users with ACCOUNTADMIN role
- Query 8: Roles with dangerous privileges
- Query 25: ACCOUNTADMIN usage tracking
- Query 26: Privilege escalation attempts

Recommended Monitoring Schedule:
- Real-time: Query 26 (escalation attempts)
- Hourly: Queries 3, 8, 24 (critical changes)
- Daily: Queries 19, 25 (access patterns, admin usage)
- Weekly: Queries 1, 6, 11, 16 (regular audits)
- Monthly: Queries 21-23 (compliance reports)

For automated alerts, schedule these queries using tasks or external tools.

For questions, contact: security-team@company.com
=============================================================================
' AS summary;

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
