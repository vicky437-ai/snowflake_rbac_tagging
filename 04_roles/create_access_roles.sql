-- ============================================================================
-- Script: 04_roles/create_access_roles.sql
-- Purpose: Create granular access roles for database objects
-- Author: Venkannababu Thatavarthi
-- Date: October 13, 2025
-- Version: 1.0
-- ============================================================================
-- Prerequisites: USERADMIN role or CREATE ROLE privilege
-- Execution Time: ~5 minutes
-- ============================================================================
-- IMPORTANT: Customize this script based on your specific databases and schemas
-- ============================================================================

USE ROLE useradmin;

-- ============================================================================
-- SECTION 1: Define Naming Convention
-- ============================================================================
/*
Access Role Naming Convention: <DATABASE>_<SCHEMA>_<ACCESS_LEVEL>

Access Levels:
- READ: SELECT only
- READ_WRITE: SELECT, INSERT, UPDATE, DELETE
- ADMIN: Full privileges including DDL
- USAGE: USAGE privilege only (for databases/schemas)

Examples:
- CUSTOMER_DB_PUBLIC_READ
- SALES_DB_TRANSACTIONS_READ_WRITE
- FINANCE_DB_ADMIN
*/

-- ============================================================================
-- SECTION 2: Create Database-Level Access Roles
-- ============================================================================
-- Purpose: Control access at the database level
-- ============================================================================

-- Example: Customer Database Access Roles
CREATE ROLE IF NOT EXISTS customer_db_usage
    COMMENT = 'Usage privilege on customer_db';

CREATE ROLE IF NOT EXISTS customer_db_read
    COMMENT = 'Read-only access to all schemas in customer_db';

CREATE ROLE IF NOT EXISTS customer_db_read_write
    COMMENT = 'Read-write access to all schemas in customer_db';

CREATE ROLE IF NOT EXISTS customer_db_admin
    COMMENT = 'Administrative access to customer_db including DDL operations';

-- Example: Finance Database Access Roles
CREATE ROLE IF NOT EXISTS finance_db_usage
    COMMENT = 'Usage privilege on finance_db';

CREATE ROLE IF NOT EXISTS finance_db_read
    COMMENT = 'Read-only access to all schemas in finance_db';

CREATE ROLE IF NOT EXISTS finance_db_read_write
    COMMENT = 'Read-write access to all schemas in finance_db';

CREATE ROLE IF NOT EXISTS finance_db_admin
    COMMENT = 'Administrative access to finance_db including DDL operations';

-- Example: HR Database Access Roles
CREATE ROLE IF NOT EXISTS hr_db_usage
    COMMENT = 'Usage privilege on hr_db';

CREATE ROLE IF NOT EXISTS hr_db_read
    COMMENT = 'Read-only access to all schemas in hr_db';

CREATE ROLE IF NOT EXISTS hr_db_read_write
    COMMENT = 'Read-write access to all schemas in hr_db';

CREATE ROLE IF NOT EXISTS hr_db_admin
    COMMENT = 'Administrative access to hr_db including DDL operations';

-- Example: Sales Database Access Roles
CREATE ROLE IF NOT EXISTS sales_db_usage
    COMMENT = 'Usage privilege on sales_db';

CREATE ROLE IF NOT EXISTS sales_db_read
    COMMENT = 'Read-only access to all schemas in sales_db';

CREATE ROLE IF NOT EXISTS sales_db_read_write
    COMMENT = 'Read-write access to all schemas in sales_db';

CREATE ROLE IF NOT EXISTS sales_db_admin
    COMMENT = 'Administrative access to sales_db including DDL operations';

-- Example: Marketing Database Access Roles
CREATE ROLE IF NOT EXISTS marketing_db_usage
    COMMENT = 'Usage privilege on marketing_db';

CREATE ROLE IF NOT EXISTS marketing_db_read
    COMMENT = 'Read-only access to all schemas in marketing_db';

CREATE ROLE IF NOT EXISTS marketing_db_read_write
    COMMENT = 'Read-write access to all schemas in marketing_db';

CREATE ROLE IF NOT EXISTS marketing_db_admin
    COMMENT = 'Administrative access to marketing_db including DDL operations';

-- ============================================================================
-- SECTION 3: Create Schema-Specific Access Roles
-- ============================================================================
-- Purpose: Granular access control at schema level
-- ============================================================================

-- Example: Customer Database - Public Schema
CREATE ROLE IF NOT EXISTS customer_db_public_read
    COMMENT = 'Read-only access to customer_db.public schema';

CREATE ROLE IF NOT EXISTS customer_db_public_read_write
    COMMENT = 'Read-write access to customer_db.public schema';

-- Example: Finance Database - Accounting Schema
CREATE ROLE IF NOT EXISTS finance_db_accounting_read
    COMMENT = 'Read-only access to finance_db.accounting schema';

CREATE ROLE IF NOT EXISTS finance_db_accounting_read_write
    COMMENT = 'Read-write access to finance_db.accounting schema';

-- Example: Finance Database - Treasury Schema
CREATE ROLE IF NOT EXISTS finance_db_treasury_read
    COMMENT = 'Read-only access to finance_db.treasury schema';

CREATE ROLE IF NOT EXISTS finance_db_treasury_read_write
    COMMENT = 'Read-write access to finance_db.treasury schema';

-- Example: HR Database - Employees Schema
CREATE ROLE IF NOT EXISTS hr_db_employees_read
    COMMENT = 'Read-only access to hr_db.employees schema';

CREATE ROLE IF NOT EXISTS hr_db_employees_read_write
    COMMENT = 'Read-write access to hr_db.employees schema';

-- Example: HR Database - Payroll Schema (restricted)
CREATE ROLE IF NOT EXISTS hr_db_payroll_read
    COMMENT = 'Read-only access to hr_db.payroll schema';

CREATE ROLE IF NOT EXISTS hr_db_payroll_read_write
    COMMENT = 'Read-write access to hr_db.payroll schema';

-- ============================================================================
-- SECTION 4: Create Warehouse Access Roles
-- ============================================================================
-- Purpose: Control compute resource access
-- ============================================================================

-- Small warehouse for ad-hoc queries
CREATE ROLE IF NOT EXISTS warehouse_small_usage
    COMMENT = 'Usage privilege on small warehouse for ad-hoc queries';

-- Medium warehouse for regular analytics
CREATE ROLE IF NOT EXISTS warehouse_medium_usage
    COMMENT = 'Usage privilege on medium warehouse for analytics';

-- Large warehouse for data engineering
CREATE ROLE IF NOT EXISTS warehouse_large_usage
    COMMENT = 'Usage privilege on large warehouse for ETL and data engineering';

-- X-Large warehouse for intensive operations
CREATE ROLE IF NOT EXISTS warehouse_xlarge_usage
    COMMENT = 'Usage privilege on xlarge warehouse for intensive processing';

-- Create warehouse admin role
CREATE ROLE IF NOT EXISTS warehouse_admin
    COMMENT = 'Administrative role for warehouse management';

-- ============================================================================
-- SECTION 5: Create Cross-Database Access Roles
-- ============================================================================
-- Purpose: Roles that span multiple databases for specific use cases
-- ============================================================================

-- Analytics access across multiple databases
CREATE ROLE IF NOT EXISTS analytics_cross_db_read
    COMMENT = 'Read access to analytics-relevant schemas across databases';

-- Reporting access
CREATE ROLE IF NOT EXISTS reporting_cross_db_read
    COMMENT = 'Read access to reporting schemas across databases';

-- Data integration access
CREATE ROLE IF NOT EXISTS etl_cross_db_read_write
    COMMENT = 'Read-write access for ETL processes across databases';

-- ============================================================================
-- SECTION 6: Create Special Purpose Access Roles
-- ============================================================================

-- Backup and restore role
CREATE ROLE IF NOT EXISTS backup_restore_role
    COMMENT = 'Role for backup and restore operations';

-- Monitoring role
CREATE ROLE IF NOT EXISTS monitoring_role
    COMMENT = 'Role for monitoring and observability';

-- Data quality role
CREATE ROLE IF NOT EXISTS data_quality_role
    COMMENT = 'Role for data quality checks and validation';

-- ============================================================================
-- SECTION 7: Document Roles in Metadata
-- ============================================================================

USE ROLE governance_admin;
USE SCHEMA governance.metadata;

-- Document all access roles
INSERT INTO role_documentation 
    (role_name, role_type, role_description, technical_owner, review_frequency_days)
VALUES
    -- Customer DB roles
    ('CUSTOMER_DB_USAGE', 'ACCESS', 'Usage on customer_db', 'Data Platform Team', 90),
    ('CUSTOMER_DB_READ', 'ACCESS', 'Read-only on customer_db', 'Data Platform Team', 90),
    ('CUSTOMER_DB_READ_WRITE', 'ACCESS', 'Read-write on customer_db', 'Data Platform Team', 90),
    ('CUSTOMER_DB_ADMIN', 'ACCESS', 'Admin on customer_db', 'Data Platform Team', 90),
    
    -- Finance DB roles
    ('FINANCE_DB_USAGE', 'ACCESS', 'Usage on finance_db', 'Finance Team', 90),
    ('FINANCE_DB_READ', 'ACCESS', 'Read-only on finance_db', 'Finance Team', 90),
    ('FINANCE_DB_READ_WRITE', 'ACCESS', 'Read-write on finance_db', 'Finance Team', 90),
    ('FINANCE_DB_ADMIN', 'ACCESS', 'Admin on finance_db', 'Finance Team', 90),
    
    -- HR DB roles
    ('HR_DB_USAGE', 'ACCESS', 'Usage on hr_db', 'HR Team', 90),
    ('HR_DB_READ', 'ACCESS', 'Read-only on hr_db', 'HR Team', 90),
    ('HR_DB_READ_WRITE', 'ACCESS', 'Read-write on hr_db', 'HR Team', 90),
    ('HR_DB_ADMIN', 'ACCESS', 'Admin on hr_db', 'HR Team', 90),
    
    -- Sales DB roles
    ('SALES_DB_USAGE', 'ACCESS', 'Usage on sales_db', 'Sales Ops Team', 90),
    ('SALES_DB_READ', 'ACCESS', 'Read-only on sales_db', 'Sales Ops Team', 90),
    ('SALES_DB_READ_WRITE', 'ACCESS', 'Read-write on sales_db', 'Sales Ops Team', 90),
    ('SALES_DB_ADMIN', 'ACCESS', 'Admin on sales_db', 'Sales Ops Team', 90),
    
    -- Marketing DB roles
    ('MARKETING_DB_USAGE', 'ACCESS', 'Usage on marketing_db', 'Marketing Ops Team', 90),
    ('MARKETING_DB_READ', 'ACCESS', 'Read-only on marketing_db', 'Marketing Ops Team', 90),
    ('MARKETING_DB_READ_WRITE', 'ACCESS', 'Read-write on marketing_db', 'Marketing Ops Team', 90),
    ('MARKETING_DB_ADMIN', 'ACCESS', 'Admin on marketing_db', 'Marketing Ops Team', 90),
    
    -- Warehouse roles
    ('WAREHOUSE_SMALL_USAGE', 'ACCESS', 'Small warehouse usage', 'Data Platform Team', 90),
    ('WAREHOUSE_MEDIUM_USAGE', 'ACCESS', 'Medium warehouse usage', 'Data Platform Team', 90),
    ('WAREHOUSE_LARGE_USAGE', 'ACCESS', 'Large warehouse usage', 'Data Platform Team', 90),
    ('WAREHOUSE_XLARGE_USAGE', 'ACCESS', 'XLarge warehouse usage', 'Data Platform Team', 90),
    ('WAREHOUSE_ADMIN', 'ACCESS', 'Warehouse administration', 'Data Platform Team', 90),
    
    -- Cross-database roles
    ('ANALYTICS_CROSS_DB_READ', 'ACCESS', 'Analytics read across DBs', 'Analytics Team', 90),
    ('REPORTING_CROSS_DB_READ', 'ACCESS', 'Reporting read across DBs', 'BI Team', 90),
    ('ETL_CROSS_DB_READ_WRITE', 'ACCESS', 'ETL read-write across DBs', 'Data Engineering Team', 90),
    
    -- Special purpose roles
    ('BACKUP_RESTORE_ROLE', 'ACCESS', 'Backup and restore operations', 'Data Platform Team', 90),
    ('MONITORING_ROLE', 'ACCESS', 'Monitoring and observability', 'Data Platform Team', 90),
    ('DATA_QUALITY_ROLE', 'ACCESS', 'Data quality checks', 'Data Quality Team', 90);

-- ============================================================================
-- SECTION 8: Create Helper Procedure for Role Generation
-- ============================================================================

-- Procedure to generate access roles for a new database
CREATE OR REPLACE PROCEDURE sp_create_database_access_roles(
    p_database_name VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    db_lower VARCHAR;
    result_msg VARCHAR;
BEGIN
    -- Convert database name to lowercase for role names
    db_lower := LOWER(p_database_name);
    
    -- Create usage role
    EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS ' || db_lower || '_db_usage ' ||
                      'COMMENT = ''Usage privilege on ' || p_database_name || '''';
    
    -- Create read role
    EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS ' || db_lower || '_db_read ' ||
                      'COMMENT = ''Read-only access to ' || p_database_name || '''';
    
    -- Create read-write role
    EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS ' || db_lower || '_db_read_write ' ||
                      'COMMENT = ''Read-write access to ' || p_database_name || '''';
    
    -- Create admin role
    EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS ' || db_lower || '_db_admin ' ||
                      'COMMENT = ''Administrative access to ' || p_database_name || '''';
    
    result_msg := 'Successfully created access roles for database: ' || p_database_name;
    RETURN result_msg;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error creating roles: ' || SQLERRM;
END;
$$;

-- Grant execute privilege to useradmin
GRANT USAGE ON PROCEDURE governance.metadata.sp_create_database_access_roles(VARCHAR) 
    TO ROLE useradmin;

-- ============================================================================
-- SECTION 9: Create View for Role Catalog
-- ============================================================================

CREATE OR REPLACE VIEW v_access_role_catalog AS
SELECT 
    role_name,
    role_type,
    role_description,
    business_owner,
    technical_owner,
    created_date,
    last_reviewed_date,
    is_active
FROM governance.metadata.role_documentation
WHERE role_type = 'ACCESS'
  AND is_active = TRUE
ORDER BY role_name;

-- Grant select to relevant roles
GRANT SELECT ON governance.metadata.v_access_role_catalog TO ROLE PUBLIC;

-- ============================================================================
-- SECTION 10: Validation
-- ============================================================================

-- Show all created access roles
SELECT 'Access roles created successfully!' AS status;

SHOW ROLES LIKE '%_db_%';
SHOW ROLES LIKE '%warehouse%';

-- Count roles by category
SELECT 
    CASE
        WHEN name LIKE '%_db_usage' THEN 'Database Usage'
        WHEN name LIKE '%_db_read' AND name NOT LIKE '%write%' THEN 'Database Read'
        WHEN name LIKE '%_db_read_write%' THEN 'Database Read-Write'
        WHEN name LIKE '%_db_admin%' THEN 'Database Admin'
        WHEN name LIKE '%warehouse%' THEN 'Warehouse Access'
        WHEN name LIKE '%cross_db%' THEN 'Cross-Database'
        ELSE 'Other'
    END AS role_category,
    COUNT(*) AS role_count
FROM snowflake.account_usage.roles
WHERE deleted_on IS NULL
  AND name NOT LIKE 'SNOWFLAKE%'
  AND (name LIKE '%_db_%' OR name LIKE '%warehouse%' OR name LIKE '%cross_db%')
GROUP BY role_category
ORDER BY role_count DESC;

-- Summary output
SELECT '
=============================================================================
ACCESS ROLES CREATED SUCCESSFULLY
=============================================================================

Role Categories Created:
1. Database Usage Roles (5)
   - Grant USAGE privilege on databases
   
2. Database Read Roles (5)
   - Grant SELECT privilege on all tables
   
3. Database Read-Write Roles (5)
   - Grant SELECT, INSERT, UPDATE, DELETE privileges
   
4. Database Admin Roles (5)
   - Grant full DDL and DML privileges
   
5. Warehouse Access Roles (5)
   - Grant USAGE on compute warehouses
   
6. Schema-Specific Roles (8)
   - Granular access at schema level
   
7. Cross-Database Roles (3)
   - Access spanning multiple databases
   
8. Special Purpose Roles (3)
   - Backup, monitoring, data quality

Next Steps:
1. Grant object privileges to access roles (see next script)
2. Create functional roles
3. Build role hierarchy
4. Assign roles to users

Helper Procedure:
To create roles for a new database:
  CALL governance.metadata.sp_create_database_access_roles(''new_db'');

View Role Catalog:
  SELECT * FROM governance.metadata.v_access_role_catalog;

For questions, contact: data-platform-team@company.com
=============================================================================
' AS summary;

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
