-- ============================================================================
-- Script: 05_grants/grant_database_privileges.sql
-- Purpose: Grant object privileges to access roles
-- Author: Venkannababu Thatavarthi
-- Date: October 13, 2025
-- Version: 1.0
-- ============================================================================
-- Prerequisites: SECURITYADMIN role or MANAGE GRANTS privilege
-- Execution Time: ~10 minutes
-- ============================================================================
-- IMPORTANT: Customize this script based on your actual databases and schemas
-- Replace example database names with your actual database names
-- ============================================================================

USE ROLE securityadmin;

-- ============================================================================
-- SECTION 1: Grant Privileges on Customer Database
-- ============================================================================
-- Assumes: customer_db database exists
-- ============================================================================

-- Grant USAGE privilege on database
GRANT USAGE ON DATABASE customer_db TO ROLE customer_db_usage;
GRANT USAGE ON DATABASE customer_db TO ROLE customer_db_read;
GRANT USAGE ON DATABASE customer_db TO ROLE customer_db_read_write;
GRANT USAGE ON DATABASE customer_db TO ROLE customer_db_admin;

-- Grant USAGE on all schemas
GRANT USAGE ON ALL SCHEMAS IN DATABASE customer_db TO ROLE customer_db_read;
GRANT USAGE ON ALL SCHEMAS IN DATABASE customer_db TO ROLE customer_db_read_write;
GRANT USAGE ON ALL SCHEMAS IN DATABASE customer_db TO ROLE customer_db_admin;

-- Grant USAGE on future schemas
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE customer_db TO ROLE customer_db_read;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE customer_db TO ROLE customer_db_read_write;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE customer_db TO ROLE customer_db_admin;

-- Grant SELECT on all tables (READ role)
GRANT SELECT ON ALL TABLES IN DATABASE customer_db TO ROLE customer_db_read;
GRANT SELECT ON ALL VIEWS IN DATABASE customer_db TO ROLE customer_db_read;
GRANT SELECT ON FUTURE TABLES IN DATABASE customer_db TO ROLE customer_db_read;
GRANT SELECT ON FUTURE VIEWS IN DATABASE customer_db TO ROLE customer_db_read;

-- Grant DML privileges (READ_WRITE role)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE customer_db TO ROLE customer_db_read_write;
GRANT SELECT ON ALL VIEWS IN DATABASE customer_db TO ROLE customer_db_read_write;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE customer_db TO ROLE customer_db_read_write;
GRANT SELECT ON FUTURE VIEWS IN DATABASE customer_db TO ROLE customer_db_read_write;

-- Grant admin privileges (ADMIN role)
GRANT ALL PRIVILEGES ON DATABASE customer_db TO ROLE customer_db_admin;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE customer_db TO ROLE customer_db_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE customer_db TO ROLE customer_db_admin;
GRANT ALL PRIVILEGES ON ALL VIEWS IN DATABASE customer_db TO ROLE customer_db_admin;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE customer_db TO ROLE customer_db_admin;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE customer_db TO ROLE customer_db_admin;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE customer_db TO ROLE customer_db_admin;

-- ============================================================================
-- SECTION 2: Grant Privileges on Finance Database
-- ============================================================================
-- Assumes: finance_db database exists
-- ============================================================================

-- Grant USAGE privilege on database
GRANT USAGE ON DATABASE finance_db TO ROLE finance_db_usage;
GRANT USAGE ON DATABASE finance_db TO ROLE finance_db_read;
GRANT USAGE ON DATABASE finance_db TO ROLE finance_db_read_write;
GRANT USAGE ON DATABASE finance_db TO ROLE finance_db_admin;

-- Grant USAGE on all schemas
GRANT USAGE ON ALL SCHEMAS IN DATABASE finance_db TO ROLE finance_db_read;
GRANT USAGE ON ALL SCHEMAS IN DATABASE finance_db TO ROLE finance_db_read_write;
GRANT USAGE ON ALL SCHEMAS IN DATABASE finance_db TO ROLE finance_db_admin;

-- Grant USAGE on future schemas
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE finance_db TO ROLE finance_db_read;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE finance_db TO ROLE finance_db_read_write;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE finance_db TO ROLE finance_db_admin;

-- Grant SELECT (READ role)
GRANT SELECT ON ALL TABLES IN DATABASE finance_db TO ROLE finance_db_read;
GRANT SELECT ON ALL VIEWS IN DATABASE finance_db TO ROLE finance_db_read;
GRANT SELECT ON FUTURE TABLES IN DATABASE finance_db TO ROLE finance_db_read;
GRANT SELECT ON FUTURE VIEWS IN DATABASE finance_db TO ROLE finance_db_read;

-- Grant DML (READ_WRITE role)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE finance_db TO ROLE finance_db_read_write;
GRANT SELECT ON ALL VIEWS IN DATABASE finance_db TO ROLE finance_db_read_write;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE finance_db TO ROLE finance_db_read_write;
GRANT SELECT ON FUTURE VIEWS IN DATABASE finance_db TO ROLE finance_db_read_write;

-- Grant admin privileges (ADMIN role)
GRANT ALL PRIVILEGES ON DATABASE finance_db TO ROLE finance_db_admin;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE finance_db TO ROLE finance_db_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE finance_db TO ROLE finance_db_admin;
GRANT ALL PRIVILEGES ON ALL VIEWS IN DATABASE finance_db TO ROLE finance_db_admin;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE finance_db TO ROLE finance_db_admin;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE finance_db TO ROLE finance_db_admin;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE finance_db TO ROLE finance_db_admin;

-- ============================================================================
-- SECTION 3: Grant Privileges on HR Database
-- ============================================================================
-- Assumes: hr_db database exists
-- ============================================================================

-- Grant USAGE privilege on database
GRANT USAGE ON DATABASE hr_db TO ROLE hr_db_usage;
GRANT USAGE ON DATABASE hr_db TO ROLE hr_db_read;
GRANT USAGE ON DATABASE hr_db TO ROLE hr_db_read_write;
GRANT USAGE ON DATABASE hr_db TO ROLE hr_db_admin;

-- Grant USAGE on all schemas
GRANT USAGE ON ALL SCHEMAS IN DATABASE hr_db TO ROLE hr_db_read;
GRANT USAGE ON ALL SCHEMAS IN DATABASE hr_db TO ROLE hr_db_read_write;
GRANT USAGE ON ALL SCHEMAS IN DATABASE hr_db TO ROLE hr_db_admin;

-- Grant USAGE on future schemas
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE hr_db TO ROLE hr_db_read;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE hr_db TO ROLE hr_db_read_write;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE hr_db TO ROLE hr_db_admin;

-- Grant SELECT (READ role)
GRANT SELECT ON ALL TABLES IN DATABASE hr_db TO ROLE hr_db_read;
GRANT SELECT ON ALL VIEWS IN DATABASE hr_db TO ROLE hr_db_read;
GRANT SELECT ON FUTURE TABLES IN DATABASE hr_db TO ROLE hr_db_read;
GRANT SELECT ON FUTURE VIEWS IN DATABASE hr_db TO ROLE hr_db_read;

-- Grant DML (READ_WRITE role)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE hr_db TO ROLE hr_db_read_write;
GRANT SELECT ON ALL VIEWS IN DATABASE hr_db TO ROLE hr_db_read_write;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE hr_db TO ROLE hr_db_read_write;
GRANT SELECT ON FUTURE VIEWS IN DATABASE hr_db TO ROLE hr_db_read_write;

-- Grant admin privileges (ADMIN role)
GRANT ALL PRIVILEGES ON DATABASE hr_db TO ROLE hr_db_admin;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE hr_db TO ROLE hr_db_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE hr_db TO ROLE hr_db_admin;
GRANT ALL PRIVILEGES ON ALL VIEWS IN DATABASE hr_db TO ROLE hr_db_admin;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE hr_db TO ROLE hr_db_admin;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE hr_db TO ROLE hr_db_admin;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE hr_db TO ROLE hr_db_admin;

-- ============================================================================
-- SECTION 4: Grant Privileges on Sales Database
-- ============================================================================
-- Assumes: sales_db database exists
-- ============================================================================

-- Grant USAGE privilege on database
GRANT USAGE ON DATABASE sales_db TO ROLE sales_db_usage;
GRANT USAGE ON DATABASE sales_db TO ROLE sales_db_read;
GRANT USAGE ON DATABASE sales_db TO ROLE sales_db_read_write;
GRANT USAGE ON DATABASE sales_db TO ROLE sales_db_admin;

-- Grant USAGE on all schemas
GRANT USAGE ON ALL SCHEMAS IN DATABASE sales_db TO ROLE sales_db_read;
GRANT USAGE ON ALL SCHEMAS IN DATABASE sales_db TO ROLE sales_db_read_write;
GRANT USAGE ON ALL SCHEMAS IN DATABASE sales_db TO ROLE sales_db_admin;

-- Grant USAGE on future schemas
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE sales_db TO ROLE sales_db_read;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE sales_db TO ROLE sales_db_read_write;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE sales_db TO ROLE sales_db_admin;

-- Grant SELECT (READ role)
GRANT SELECT ON ALL TABLES IN DATABASE sales_db TO ROLE sales_db_read;
GRANT SELECT ON ALL VIEWS IN DATABASE sales_db TO ROLE sales_db_read;
GRANT SELECT ON FUTURE TABLES IN DATABASE sales_db TO ROLE sales_db_read;
GRANT SELECT ON FUTURE VIEWS IN DATABASE sales_db TO ROLE sales_db_read;

-- Grant DML (READ_WRITE role)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE sales_db TO ROLE sales_db_read_write;
GRANT SELECT ON ALL VIEWS IN DATABASE sales_db TO ROLE sales_db_read_write;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE sales_db TO ROLE sales_db_read_write;
GRANT SELECT ON FUTURE VIEWS IN DATABASE sales_db TO ROLE sales_db_read_write;

-- Grant admin privileges (ADMIN role)
GRANT ALL PRIVILEGES ON DATABASE sales_db TO ROLE sales_db_admin;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE sales_db TO ROLE sales_db_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE sales_db TO ROLE sales_db_admin;
GRANT ALL PRIVILEGES ON ALL VIEWS IN DATABASE sales_db TO ROLE sales_db_admin;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE sales_db TO ROLE sales_db_admin;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE sales_db TO ROLE sales_db_admin;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE sales_db TO ROLE sales_db_admin;

-- ============================================================================
-- SECTION 5: Grant Privileges on Marketing Database
-- ============================================================================
-- Assumes: marketing_db database exists
-- ============================================================================

-- Grant USAGE privilege on database
GRANT USAGE ON DATABASE marketing_db TO ROLE marketing_db_usage;
GRANT USAGE ON DATABASE marketing_db TO ROLE marketing_db_read;
GRANT USAGE ON DATABASE marketing_db TO ROLE marketing_db_read_write;
GRANT USAGE ON DATABASE marketing_db TO ROLE marketing_db_admin;

-- Grant USAGE on all schemas
GRANT USAGE ON ALL SCHEMAS IN DATABASE marketing_db TO ROLE marketing_db_read;
GRANT USAGE ON ALL SCHEMAS IN DATABASE marketing_db TO ROLE marketing_db_read_write;
GRANT USAGE ON ALL SCHEMAS IN DATABASE marketing_db TO ROLE marketing_db_admin;

-- Grant USAGE on future schemas
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE marketing_db TO ROLE marketing_db_read;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE marketing_db TO ROLE marketing_db_read_write;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE marketing_db TO ROLE marketing_db_admin;

-- Grant SELECT (READ role)
GRANT SELECT ON ALL TABLES IN DATABASE marketing_db TO ROLE marketing_db_read;
GRANT SELECT ON ALL VIEWS IN DATABASE marketing_db TO ROLE marketing_db_read;
GRANT SELECT ON FUTURE TABLES IN DATABASE marketing_db TO ROLE marketing_db_read;
GRANT SELECT ON FUTURE VIEWS IN DATABASE marketing_db TO ROLE marketing_db_read;

-- Grant DML (READ_WRITE role)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE marketing_db TO ROLE marketing_db_read_write;
GRANT SELECT ON ALL VIEWS IN DATABASE marketing_db TO ROLE marketing_db_read_write;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE marketing_db TO ROLE marketing_db_read_write;
GRANT SELECT ON FUTURE VIEWS IN DATABASE marketing_db TO ROLE marketing_db_read_write;

-- Grant admin privileges (ADMIN role)
GRANT ALL PRIVILEGES ON DATABASE marketing_db TO ROLE marketing_db_admin;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE marketing_db TO ROLE marketing_db_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE marketing_db TO ROLE marketing_db_admin;
GRANT ALL PRIVILEGES ON ALL VIEWS IN DATABASE marketing_db TO ROLE marketing_db_admin;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE marketing_db TO ROLE marketing_db_admin;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE marketing_db TO ROLE marketing_db_admin;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE marketing_db TO ROLE marketing_db_admin;

-- ============================================================================
-- SECTION 6: Grant Warehouse Privileges
-- ============================================================================
-- Assumes: warehouses exist (compute_wh_small, compute_wh_medium, etc.)
-- ============================================================================

-- Small warehouse
GRANT USAGE ON WAREHOUSE compute_wh_small TO ROLE warehouse_small_usage;
GRANT OPERATE ON WAREHOUSE compute_wh_small TO ROLE warehouse_small_usage;

-- Medium warehouse
GRANT USAGE ON WAREHOUSE compute_wh_medium TO ROLE warehouse_medium_usage;
GRANT OPERATE ON WAREHOUSE compute_wh_medium TO ROLE warehouse_medium_usage;

-- Large warehouse
GRANT USAGE ON WAREHOUSE compute_wh_large TO ROLE warehouse_large_usage;
GRANT OPERATE ON WAREHOUSE compute_wh_large TO ROLE warehouse_large_usage;

-- X-Large warehouse
GRANT USAGE ON WAREHOUSE compute_wh_xlarge TO ROLE warehouse_xlarge_usage;
GRANT OPERATE ON WAREHOUSE compute_wh_xlarge TO ROLE warehouse_xlarge_usage;

-- Warehouse admin gets full control
GRANT ALL PRIVILEGES ON WAREHOUSE compute_wh_small TO ROLE warehouse_admin;
GRANT ALL PRIVILEGES ON WAREHOUSE compute_wh_medium TO ROLE warehouse_admin;
GRANT ALL PRIVILEGES ON WAREHOUSE compute_wh_large TO ROLE warehouse_admin;
GRANT ALL PRIVILEGES ON WAREHOUSE compute_wh_xlarge TO ROLE warehouse_admin;

-- ============================================================================
-- SECTION 7: Grant Privileges for Cross-Database Roles
-- ============================================================================

-- Analytics cross-database read role
GRANT USAGE ON DATABASE customer_db TO ROLE analytics_cross_db_read;
GRANT USAGE ON DATABASE sales_db TO ROLE analytics_cross_db_read;
GRANT USAGE ON DATABASE marketing_db TO ROLE analytics_cross_db_read;

GRANT USAGE ON ALL SCHEMAS IN DATABASE customer_db TO ROLE analytics_cross_db_read;
GRANT USAGE ON ALL SCHEMAS IN DATABASE sales_db TO ROLE analytics_cross_db_read;
GRANT USAGE ON ALL SCHEMAS IN DATABASE marketing_db TO ROLE analytics_cross_db_read;

GRANT SELECT ON ALL TABLES IN DATABASE customer_db TO ROLE analytics_cross_db_read;
GRANT SELECT ON ALL TABLES IN DATABASE sales_db TO ROLE analytics_cross_db_read;
GRANT SELECT ON ALL TABLES IN DATABASE marketing_db TO ROLE analytics_cross_db_read;

GRANT SELECT ON ALL VIEWS IN DATABASE customer_db TO ROLE analytics_cross_db_read;
GRANT SELECT ON ALL VIEWS IN DATABASE sales_db TO ROLE analytics_cross_db_read;
GRANT SELECT ON ALL VIEWS IN DATABASE marketing_db TO ROLE analytics_cross_db_read;

-- Reporting cross-database read role
GRANT USAGE ON DATABASE customer_db TO ROLE reporting_cross_db_read;
GRANT USAGE ON DATABASE sales_db TO ROLE reporting_cross_db_read;
GRANT USAGE ON DATABASE finance_db TO ROLE reporting_cross_db_read;

GRANT USAGE ON ALL SCHEMAS IN DATABASE customer_db TO ROLE reporting_cross_db_read;
GRANT USAGE ON ALL SCHEMAS IN DATABASE sales_db TO ROLE reporting_cross_db_read;
GRANT USAGE ON ALL SCHEMAS IN DATABASE finance_db TO ROLE reporting_cross_db_read;

GRANT SELECT ON ALL TABLES IN DATABASE customer_db TO ROLE reporting_cross_db_read;
GRANT SELECT ON ALL TABLES IN DATABASE sales_db TO ROLE reporting_cross_db_read;
GRANT SELECT ON ALL TABLES IN DATABASE finance_db TO ROLE reporting_cross_db_read;

-- ETL cross-database read-write role
GRANT USAGE ON DATABASE customer_db TO ROLE etl_cross_db_read_write;
GRANT USAGE ON DATABASE sales_db TO ROLE etl_cross_db_read_write;
GRANT USAGE ON DATABASE marketing_db TO ROLE etl_cross_db_read_write;

GRANT USAGE ON ALL SCHEMAS IN DATABASE customer_db TO ROLE etl_cross_db_read_write;
GRANT USAGE ON ALL SCHEMAS IN DATABASE sales_db TO ROLE etl_cross_db_read_write;
GRANT USAGE ON ALL SCHEMAS IN DATABASE marketing_db TO ROLE etl_cross_db_read_write;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE customer_db TO ROLE etl_cross_db_read_write;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE sales_db TO ROLE etl_cross_db_read_write;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE marketing_db TO ROLE etl_cross_db_read_write;

-- ============================================================================
-- SECTION 8: Grant Privileges for Special Purpose Roles
-- ============================================================================

-- Monitoring role - read access to account usage
GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE monitoring_role;

-- Backup restore role
GRANT ALL PRIVILEGES ON DATABASE customer_db TO ROLE backup_restore_role;
GRANT ALL PRIVILEGES ON DATABASE sales_db TO ROLE backup_restore_role;
GRANT ALL PRIVILEGES ON DATABASE marketing_db TO ROLE backup_restore_role;
GRANT ALL PRIVILEGES ON DATABASE finance_db TO ROLE backup_restore_role;
GRANT ALL PRIVILEGES ON DATABASE hr_db TO ROLE backup_restore_role;

-- Data quality role - read access for validation
GRANT USAGE ON DATABASE customer_db TO ROLE data_quality_role;
GRANT USAGE ON DATABASE sales_db TO ROLE data_quality_role;
GRANT USAGE ON DATABASE marketing_db TO ROLE data_quality_role;

GRANT USAGE ON ALL SCHEMAS IN DATABASE customer_db TO ROLE data_quality_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE sales_db TO ROLE data_quality_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE marketing_db TO ROLE data_quality_role;

GRANT SELECT ON ALL TABLES IN DATABASE customer_db TO ROLE data_quality_role;
GRANT SELECT ON ALL TABLES IN DATABASE sales_db TO ROLE data_quality_role;
GRANT SELECT ON ALL TABLES IN DATABASE marketing_db TO ROLE data_quality_role;

-- ============================================================================
-- SECTION 9: Create Stored Procedure for Automated Grant Management
-- ============================================================================

USE ROLE governance_admin;
USE SCHEMA governance.metadata;

-- Procedure to grant standard privileges to a role on a database
CREATE OR REPLACE PROCEDURE sp_grant_database_privileges(
    p_database_name VARCHAR,
    p_role_name VARCHAR,
    p_access_level VARCHAR -- 'USAGE', 'READ', 'READ_WRITE', 'ADMIN'
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    grant_sql VARCHAR;
    result_msg VARCHAR;
BEGIN
    -- Validate access level
    IF (p_access_level NOT IN ('USAGE', 'READ', 'READ_WRITE', 'ADMIN')) THEN
        RETURN 'Error: Invalid access level. Must be USAGE, READ, READ_WRITE, or ADMIN';
    END IF;
    
    -- Grant USAGE on database
    grant_sql := 'GRANT USAGE ON DATABASE ' || p_database_name || ' TO ROLE ' || p_role_name;
    EXECUTE IMMEDIATE :grant_sql;
    
    IF (p_access_level IN ('READ', 'READ_WRITE', 'ADMIN')) THEN
        -- Grant USAGE on all schemas
        grant_sql := 'GRANT USAGE ON ALL SCHEMAS IN DATABASE ' || p_database_name || ' TO ROLE ' || p_role_name;
        EXECUTE IMMEDIATE :grant_sql;
        
        -- Grant USAGE on future schemas
        grant_sql := 'GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ' || p_database_name || ' TO ROLE ' || p_role_name;
        EXECUTE IMMEDIATE :grant_sql;
        
        -- Grant SELECT on all tables and views
        grant_sql := 'GRANT SELECT ON ALL TABLES IN DATABASE ' || p_database_name || ' TO ROLE ' || p_role_name;
        EXECUTE IMMEDIATE :grant_sql;
        
        grant_sql := 'GRANT SELECT ON ALL VIEWS IN DATABASE ' || p_database_name || ' TO ROLE ' || p_role_name;
        EXECUTE IMMEDIATE :grant_sql;
        
        -- Grant SELECT on future tables and views
        grant_sql := 'GRANT SELECT ON FUTURE TABLES IN DATABASE ' || p_database_name || ' TO ROLE ' || p_role_name;
        EXECUTE IMMEDIATE :grant_sql;
        
        grant_sql := 'GRANT SELECT ON FUTURE VIEWS IN DATABASE ' || p_database_name || ' TO ROLE ' || p_role_name;
        EXECUTE IMMEDIATE :grant_sql;
    END IF;
    
    IF (p_access_level IN ('READ_WRITE', 'ADMIN')) THEN
        -- Grant INSERT, UPDATE, DELETE
        grant_sql := 'GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE ' || p_database_name || ' TO ROLE ' || p_role_name;
        EXECUTE IMMEDIATE :grant_sql;
        
        grant_sql := 'GRANT INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE ' || p_database_name || ' TO ROLE ' || p_role_name;
        EXECUTE IMMEDIATE :grant_sql;
    END IF;
    
    IF (p_access_level = 'ADMIN') THEN
        -- Grant ALL PRIVILEGES
        grant_sql := 'GRANT ALL PRIVILEGES ON DATABASE ' || p_database_name || ' TO ROLE ' || p_role_name;
        EXECUTE IMMEDIATE :grant_sql;
        
        grant_sql := 'GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE ' || p_database_name || ' TO ROLE ' || p_role_name;
        EXECUTE IMMEDIATE :grant_sql;
        
        grant_sql := 'GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE ' || p_database_name || ' TO ROLE ' || p_role_name;
        EXECUTE IMMEDIATE :grant_sql;
    END IF;
    
    result_msg := 'Successfully granted ' || p_access_level || ' privileges on database ' || 
                  p_database_name || ' to role ' || p_role_name;
    
    RETURN result_msg;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error granting privileges: ' || SQLERRM;
END;
$$;

-- Grant execute to securityadmin
GRANT USAGE ON PROCEDURE governance.metadata.sp_grant_database_privileges(VARCHAR, VARCHAR, VARCHAR) 
    TO ROLE securityadmin;

-- ============================================================================
-- SECTION 10: Validation Queries
-- ============================================================================

USE ROLE securityadmin;

-- Verify grants by role
SELECT 'Database privileges granted successfully!' AS status;

-- Show grants for a specific role (example)
SELECT 
    grantee_name AS role_name,
    privilege,
    granted_on,
    name AS object_name,
    COUNT(*) AS grant_count
FROM snowflake.account_usage.grants_to_roles
WHERE deleted_on IS NULL
  AND grantee_name LIKE '%_db_%'
  AND granted_on IN ('DATABASE', 'SCHEMA', 'TABLE', 'VIEW')
GROUP BY grantee_name, privilege, granted_on, name
ORDER BY grantee_name, granted_on, privilege;

-- Summary by database
SELECT 
    name AS database_name,
    grantee_name AS role_name,
    COUNT(DISTINCT privilege) AS privilege_count,
    LISTAGG(DISTINCT privilege, ', ') AS privileges
FROM snowflake.account_usage.grants_to_roles
WHERE deleted_on IS NULL
  AND granted_on = 'DATABASE'
  AND name NOT IN ('SNOWFLAKE', 'GOVERNANCE')
GROUP BY name, grantee_name
ORDER BY name, grantee_name;

-- Summary output
SELECT '
=============================================================================
DATABASE PRIVILEGES GRANTED SUCCESSFULLY
=============================================================================

Privileges Granted:
✓ Customer DB: USAGE, READ, READ_WRITE, ADMIN roles
✓ Finance DB: USAGE, READ, READ_WRITE, ADMIN roles
✓ HR DB: USAGE, READ, READ_WRITE, ADMIN roles
✓ Sales DB: USAGE, READ, READ_WRITE, ADMIN roles
✓ Marketing DB: USAGE, READ, READ_WRITE, ADMIN roles
✓ Warehouse Access: Small, Medium, Large, XLarge
✓ Cross-Database Access: Analytics, Reporting, ETL
✓ Special Purpose: Monitoring, Backup/Restore, Data Quality

Future Grants Applied:
- All future schemas will inherit appropriate privileges
- All future tables will inherit appropriate privileges
- All future views will inherit appropriate privileges

Helper Procedure:
To grant privileges on a new database:
  CALL governance.metadata.sp_grant_database_privileges(
      ''new_database'',
      ''data_analyst_role'',
      ''READ''
  );

Next Steps:
1. Create functional roles
2. Build role hierarchy
3. Assign roles to users
4. Test access with different roles

For questions, contact: data-platform-team@company.com
=============================================================================
' AS summary;

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
