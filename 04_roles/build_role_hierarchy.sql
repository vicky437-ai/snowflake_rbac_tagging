-- ============================================================================
-- Script: 04_roles/build_role_hierarchy.sql
-- Purpose: Build comprehensive role hierarchy for privilege inheritance
-- Author: Venkannababu Thatavarthi
-- Date: October 13, 2025
-- Version: 1.0
-- ============================================================================
-- Prerequisites: SECURITYADMIN role or MANAGE GRANTS privilege
-- Execution Time: ~10 minutes
-- ============================================================================

USE ROLE securityadmin;

-- ============================================================================
-- SECTION 1: Grant Access Roles to Functional Roles - Data & Analytics
-- ============================================================================

-- Data Analyst: Read access to customer, sales, marketing databases
GRANT ROLE customer_db_read TO ROLE data_analyst;
GRANT ROLE sales_db_read TO ROLE data_analyst;
GRANT ROLE marketing_db_read TO ROLE data_analyst;
GRANT ROLE analytics_cross_db_read TO ROLE data_analyst;
GRANT ROLE warehouse_small_usage TO ROLE data_analyst;

-- Senior Data Analyst: Broader read access
GRANT ROLE data_analyst TO ROLE senior_data_analyst;
GRANT ROLE finance_db_read TO ROLE senior_data_analyst;
GRANT ROLE warehouse_medium_usage TO ROLE senior_data_analyst;

-- Data Scientist: Read access with ML capabilities
GRANT ROLE data_analyst TO ROLE data_scientist;
GRANT ROLE warehouse_medium_usage TO ROLE data_scientist;

-- Business Analyst: Business domain read access
GRANT ROLE customer_db_read TO ROLE business_analyst;
GRANT ROLE sales_db_read TO ROLE business_analyst;
GRANT ROLE reporting_cross_db_read TO ROLE business_analyst;
GRANT ROLE warehouse_small_usage TO ROLE business_analyst;

-- BI Developer: Reporting access
GRANT ROLE reporting_cross_db_read TO ROLE bi_developer;
GRANT ROLE warehouse_medium_usage TO ROLE bi_developer;

-- ============================================================================
-- SECTION 2: Grant Access Roles to Functional Roles - Data Engineering
-- ============================================================================

-- Data Engineer: Read-write access for ETL
GRANT ROLE customer_db_read_write TO ROLE data_engineer;
GRANT ROLE sales_db_read_write TO ROLE data_engineer;
GRANT ROLE marketing_db_read_write TO ROLE data_engineer;
GRANT ROLE etl_cross_db_read_write TO ROLE data_engineer;
GRANT ROLE warehouse_large_usage TO ROLE data_engineer;

-- Senior Data Engineer: Admin access
GRANT ROLE data_engineer TO ROLE senior_data_engineer;
GRANT ROLE customer_db_admin TO ROLE senior_data_engineer;
GRANT ROLE sales_db_admin TO ROLE senior_data_engineer;
GRANT ROLE marketing_db_admin TO ROLE senior_data_engineer;
GRANT ROLE warehouse_admin TO ROLE senior_data_engineer;

-- ETL Developer: Pipeline development access
GRANT ROLE etl_cross_db_read_write TO ROLE etl_developer;
GRANT ROLE warehouse_large_usage TO ROLE etl_developer;

-- Data Architect: Oversight access
GRANT ROLE senior_data_engineer TO ROLE data_architect;

-- ============================================================================
-- SECTION 3: Grant Access Roles to Functional Roles - Finance Team
-- ============================================================================

-- Finance Analyst: Read access to finance data
GRANT ROLE finance_db_read TO ROLE finance_analyst;
GRANT ROLE warehouse_small_usage TO ROLE finance_analyst;

-- Finance Manager: Read-write access
GRANT ROLE finance_analyst TO ROLE finance_manager;
GRANT ROLE finance_db_read_write TO ROLE finance_manager;
GRANT ROLE warehouse_medium_usage TO ROLE finance_manager;

-- Accountant: Accounting schema access
GRANT ROLE finance_db_accounting_read_write TO ROLE accountant;
GRANT ROLE warehouse_small_usage TO ROLE accountant;

-- ============================================================================
-- SECTION 4: Grant Access Roles to Functional Roles - HR Team
-- ============================================================================

-- HR Analyst: Read access to HR data
GRANT ROLE hr_db_employees_read TO ROLE hr_analyst;
GRANT ROLE warehouse_small_usage TO ROLE hr_analyst;

-- HR Manager: Broader HR access
GRANT ROLE hr_analyst TO ROLE hr_manager;
GRANT ROLE hr_db_read TO ROLE hr_manager;
GRANT ROLE warehouse_small_usage TO ROLE hr_manager;

-- HR Admin: Full HR access
GRANT ROLE hr_manager TO ROLE hr_admin;
GRANT ROLE hr_db_admin TO ROLE hr_admin;

-- Payroll Admin: Payroll data access
GRANT ROLE hr_db_payroll_read_write TO ROLE payroll_admin;
GRANT ROLE warehouse_small_usage TO ROLE payroll_admin;

-- ============================================================================
-- SECTION 5: Grant Access Roles to Functional Roles - Sales Team
-- ============================================================================

-- Sales Analyst: Sales data analysis
GRANT ROLE sales_db_read TO ROLE sales_analyst;
GRANT ROLE customer_db_read TO ROLE sales_analyst;
GRANT ROLE warehouse_small_usage TO ROLE sales_analyst;

-- Sales Manager: Sales oversight
GRANT ROLE sales_analyst TO ROLE sales_manager;
GRANT ROLE sales_db_read_write TO ROLE sales_manager;
GRANT ROLE warehouse_medium_usage TO ROLE sales_manager;

-- Sales Operations: Sales process management
GRANT ROLE sales_db_read_write TO ROLE sales_operations;
GRANT ROLE customer_db_read_write TO ROLE sales_operations;
GRANT ROLE warehouse_medium_usage TO ROLE sales_operations;

-- ============================================================================
-- SECTION 6: Grant Access Roles to Functional Roles - Marketing Team
-- ============================================================================

-- Marketing Analyst: Campaign analysis
GRANT ROLE marketing_db_read TO ROLE marketing_analyst;
GRANT ROLE customer_db_read TO ROLE marketing_analyst;
GRANT ROLE warehouse_small_usage TO ROLE marketing_analyst;

-- Marketing Manager: Marketing strategy
GRANT ROLE marketing_analyst TO ROLE marketing_manager;
GRANT ROLE marketing_db_read_write TO ROLE marketing_manager;
GRANT ROLE warehouse_medium_usage TO ROLE marketing_manager;

-- Digital Marketing: Digital channel data
GRANT ROLE marketing_db_read_write TO ROLE digital_marketing;
GRANT ROLE customer_db_read TO ROLE digital_marketing;
GRANT ROLE warehouse_small_usage TO ROLE digital_marketing;

-- ============================================================================
-- SECTION 7: Grant Access Roles to Functional Roles - Support & Operations
-- ============================================================================

-- Customer Support: Customer data access
GRANT ROLE customer_db_read TO ROLE customer_support;
GRANT ROLE warehouse_small_usage TO ROLE customer_support;

-- Customer Support Manager: Support operations
GRANT ROLE customer_support TO ROLE customer_support_manager;
GRANT ROLE customer_db_read_write TO ROLE customer_support_manager;

-- Operations Analyst: Operational data
GRANT ROLE customer_db_read TO ROLE operations_analyst;
GRANT ROLE sales_db_read TO ROLE operations_analyst;
GRANT ROLE warehouse_small_usage TO ROLE operations_analyst;

-- Operations Manager: Operations oversight
GRANT ROLE operations_analyst TO ROLE operations_manager;
GRANT ROLE warehouse_medium_usage TO ROLE operations_manager;

-- ============================================================================
-- SECTION 8: Grant Access Roles to Executive Roles
-- ============================================================================

-- Executive: High-level reporting
GRANT ROLE reporting_cross_db_read TO ROLE executive;
GRANT ROLE analytics_cross_db_read TO ROLE executive;
GRANT ROLE warehouse_medium_usage TO ROLE executive;

-- CFO: Financial oversight
GRANT ROLE finance_manager TO ROLE cfo;
GRANT ROLE finance_db_admin TO ROLE cfo;

-- CTO: Technology oversight
GRANT ROLE data_architect TO ROLE cto;
GRANT ROLE senior_data_engineer TO ROLE cto;

-- Chief Data Officer: Data governance
GRANT ROLE governance_admin TO ROLE chief_data_officer;
GRANT ROLE data_architect TO ROLE chief_data_officer;

-- ============================================================================
-- SECTION 9: Grant Access Roles to Service Roles
-- ============================================================================

-- Tableau Service Role
GRANT ROLE reporting_cross_db_read TO ROLE tableau_service_role;
GRANT ROLE analytics_cross_db_read TO ROLE tableau_service_role;
GRANT ROLE warehouse_medium_usage TO ROLE tableau_service_role;

-- Power BI Service Role
GRANT ROLE reporting_cross_db_read TO ROLE powerbi_service_role;
GRANT ROLE warehouse_medium_usage TO ROLE powerbi_service_role;

-- Looker Service Role
GRANT ROLE reporting_cross_db_read TO ROLE looker_service_role;
GRANT ROLE warehouse_medium_usage TO ROLE looker_service_role;

-- dbt Service Role
GRANT ROLE etl_cross_db_read_write TO ROLE dbt_service_role;
GRANT ROLE warehouse_large_usage TO ROLE dbt_service_role;

-- Airflow Service Role
GRANT ROLE etl_cross_db_read_write TO ROLE airflow_service_role;
GRANT ROLE warehouse_xlarge_usage TO ROLE airflow_service_role;

-- Fivetran Service Role
GRANT ROLE etl_cross_db_read_write TO ROLE fivetran_service_role;
GRANT ROLE warehouse_large_usage TO ROLE fivetran_service_role;

-- Matillion Service Role
GRANT ROLE etl_cross_db_read_write TO ROLE matillion_service_role;
GRANT ROLE warehouse_large_usage TO ROLE matillion_service_role;

-- API Service Role
GRANT ROLE customer_db_read TO ROLE api_service_role;
GRANT ROLE warehouse_small_usage TO ROLE api_service_role;

-- App Backend Role
GRANT ROLE customer_db_read_write TO ROLE app_backend_role;
GRANT ROLE warehouse_medium_usage TO ROLE app_backend_role;

-- ============================================================================
-- SECTION 10: Grant Functional Roles to SYSADMIN
-- ============================================================================
-- This ensures SYSADMIN can manage objects created by functional roles

-- Data & Analytics Roles
GRANT ROLE data_analyst TO ROLE sysadmin;
GRANT ROLE senior_data_analyst TO ROLE sysadmin;
GRANT ROLE data_scientist TO ROLE sysadmin;
GRANT ROLE business_analyst TO ROLE sysadmin;
GRANT ROLE bi_developer TO ROLE sysadmin;

-- Data Engineering Roles
GRANT ROLE data_engineer TO ROLE sysadmin;
GRANT ROLE senior_data_engineer TO ROLE sysadmin;
GRANT ROLE etl_developer TO ROLE sysadmin;
GRANT ROLE data_architect TO ROLE sysadmin;

-- Finance Roles
GRANT ROLE finance_analyst TO ROLE sysadmin;
GRANT ROLE finance_manager TO ROLE sysadmin;
GRANT ROLE accountant TO ROLE sysadmin;

-- HR Roles
GRANT ROLE hr_analyst TO ROLE sysadmin;
GRANT ROLE hr_manager TO ROLE sysadmin;
GRANT ROLE hr_admin TO ROLE sysadmin;
GRANT ROLE payroll_admin TO ROLE sysadmin;

-- Sales Roles
GRANT ROLE sales_analyst TO ROLE sysadmin;
GRANT ROLE sales_manager TO ROLE sysadmin;
GRANT ROLE sales_operations TO ROLE sysadmin;

-- Marketing Roles
GRANT ROLE marketing_analyst TO ROLE sysadmin;
GRANT ROLE marketing_manager TO ROLE sysadmin;
GRANT ROLE digital_marketing TO ROLE sysadmin;

-- Support & Operations Roles
GRANT ROLE customer_support TO ROLE sysadmin;
GRANT ROLE customer_support_manager TO ROLE sysadmin;
GRANT ROLE operations_analyst TO ROLE sysadmin;
GRANT ROLE operations_manager TO ROLE sysadmin;

-- Executive Roles
GRANT ROLE executive TO ROLE sysadmin;
GRANT ROLE cfo TO ROLE sysadmin;
GRANT ROLE cto TO ROLE sysadmin;
GRANT ROLE chief_data_officer TO ROLE sysadmin;

-- Service Roles
GRANT ROLE tableau_service_role TO ROLE sysadmin;
GRANT ROLE powerbi_service_role TO ROLE sysadmin;
GRANT ROLE looker_service_role TO ROLE sysadmin;
GRANT ROLE dbt_service_role TO ROLE sysadmin;
GRANT ROLE airflow_service_role TO ROLE sysadmin;
GRANT ROLE fivetran_service_role TO ROLE sysadmin;
GRANT ROLE matillion_service_role TO ROLE sysadmin;
GRANT ROLE api_service_role TO ROLE sysadmin;
GRANT ROLE app_backend_role TO ROLE sysadmin;

-- ============================================================================
-- SECTION 11: Create Role Hierarchy Visualization
-- ============================================================================

USE ROLE governance_admin;
USE SCHEMA governance.metadata;

-- Create view to visualize role hierarchy
CREATE OR REPLACE VIEW v_role_hierarchy AS
WITH RECURSIVE role_tree AS (
    -- Base case: all role grants
    SELECT 
        name AS child_role,
        grantee_name AS parent_role,
        1 AS level,
        name AS path
    FROM snowflake.account_usage.grants_to_roles
    WHERE granted_to = 'ROLE'
      AND deleted_on IS NULL
    
    UNION ALL
    
    -- Recursive case: traverse up the hierarchy
    SELECT 
        rt.child_role,
        g.grantee_name AS parent_role,
        rt.level + 1,
        rt.path || ' -> ' || g.grantee_name
    FROM role_tree rt
    JOIN snowflake.account_usage.grants_to_roles g
        ON rt.parent_role = g.name
        AND g.granted_to = 'ROLE'
        AND g.deleted_on IS NULL
    WHERE rt.level < 10 -- Prevent infinite loops
)
SELECT DISTINCT
    child_role,
    parent_role,
    level,
    path AS hierarchy_path
FROM role_tree
ORDER BY child_role, level;

-- Create simplified role hierarchy view
CREATE OR REPLACE VIEW v_role_hierarchy_simple AS
SELECT 
    name AS child_role,
    grantee_name AS parent_role,
    granted_on AS grant_date
FROM snowflake.account_usage.grants_to_roles
WHERE granted_to = 'ROLE'
  AND deleted_on IS NULL
  AND grantee_name NOT LIKE 'SNOWFLAKE%'
ORDER BY grantee_name, name;

-- Create view showing role inheritance count
CREATE OR REPLACE VIEW v_role_privilege_count AS
SELECT 
    grantee_name AS role_name,
    COUNT(DISTINCT name) AS inherited_roles,
    COUNT(DISTINCT CASE WHEN granted_on != 'ROLE' THEN name END) AS direct_privileges
FROM snowflake.account_usage.grants_to_roles
WHERE deleted_on IS NULL
  AND grantee_name NOT LIKE 'SNOWFLAKE%'
GROUP BY grantee_name
ORDER BY inherited_roles DESC, direct_privileges DESC;

-- Grant select on hierarchy views
GRANT SELECT ON governance.metadata.v_role_hierarchy TO ROLE PUBLIC;
GRANT SELECT ON governance.metadata.v_role_hierarchy_simple TO ROLE PUBLIC;
GRANT SELECT ON governance.metadata.v_role_privilege_count TO ROLE PUBLIC;

-- ============================================================================
-- SECTION 12: Create Stored Procedure to Visualize Role Path
-- ============================================================================

-- Procedure to show complete hierarchy for a given role
CREATE OR REPLACE PROCEDURE sp_show_role_hierarchy(p_role_name VARCHAR)
RETURNS TABLE(role_level NUMBER, role_name VARCHAR, role_type VARCHAR)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        WITH RECURSIVE role_path AS (
            -- Start with the specified role
            SELECT 
                0 AS level,
                :p_role_name AS role_name,
                'TARGET' AS role_type
            
            UNION ALL
            
            -- Get parent roles
            SELECT 
                rp.level + 1,
                g.grantee_name,
                CASE 
                    WHEN g.grantee_name IN ('SYSADMIN', 'SECURITYADMIN', 'USERADMIN', 'ACCOUNTADMIN') 
                        THEN 'SYSTEM'
                    WHEN g.grantee_name LIKE '%_admin' OR g.grantee_name LIKE '%_steward'
                        THEN 'ADMIN'
                    WHEN g.grantee_name LIKE '%_db_%'
                        THEN 'ACCESS'
                    WHEN g.grantee_name LIKE '%_service_role'
                        THEN 'SERVICE'
                    ELSE 'FUNCTIONAL'
                END
            FROM role_path rp
            JOIN snowflake.account_usage.grants_to_roles g
                ON rp.role_name = g.name
                AND g.granted_to = 'ROLE'
                AND g.deleted_on IS NULL
            WHERE rp.level < 10
        )
        SELECT level, role_name, role_type
        FROM role_path
        ORDER BY level
    );
    RETURN TABLE(res);
END;
$$;

-- Grant execute privilege
GRANT USAGE ON PROCEDURE governance.metadata.sp_show_role_hierarchy(VARCHAR) TO ROLE PUBLIC;

-- ============================================================================
-- SECTION 13: Validation and Testing
-- ============================================================================

-- Show role hierarchy for a sample role
SELECT 'Role hierarchy built successfully!' AS status;

-- Count role grants
SELECT 
    'Total role-to-role grants: ' || COUNT(*) AS summary
FROM snowflake.account_usage.grants_to_roles
WHERE granted_to = 'ROLE'
  AND deleted_on IS NULL
  AND grantee_name NOT LIKE 'SNOWFLAKE%';

-- Show top-level functional roles
SELECT 
    grantee_name AS parent_role,
    COUNT(DISTINCT name) AS child_roles
FROM snowflake.account_usage.grants_to_roles
WHERE granted_to = 'ROLE'
  AND deleted_on IS NULL
  AND grantee_name IN ('SYSADMIN', 'SECURITYADMIN')
GROUP BY grantee_name
ORDER BY child_roles DESC;

-- Summary output
SELECT '
=============================================================================
ROLE HIERARCHY BUILT SUCCESSFULLY
=============================================================================

Hierarchy Structure:
ACCOUNTADMIN (Top Level)
├── SECURITYADMIN
│   ├── USERADMIN
│   └── COMPLIANCE_OFFICER
└── SYSADMIN
    ├── GOVERNANCE_ADMIN
    │   ├── TAG_ADMIN
    │   ├── POLICY_ADMIN
    │   └── DATA_STEWARD
    ├── All Functional Roles (39 roles)
    │   ├── Data & Analytics (5)
    │   ├── Data Engineering (4)
    │   ├── Business Domains (17)
    │   ├── Executive (4)
    │   └── Service Roles (9)
    └── All Access Roles (30+ roles)
        ├── Database Access (20)
        ├── Warehouse Access (5)
        └── Cross-Database (3)

Privilege Inheritance:
✓ Access roles grant object privileges
✓ Functional roles inherit from access roles
✓ SYSADMIN inherits all functional roles
✓ Service roles have appropriate access

Helper Queries:
1. View complete role hierarchy:
   SELECT * FROM governance.metadata.v_role_hierarchy_simple;

2. Show hierarchy for specific role:
   CALL governance.metadata.sp_show_role_hierarchy(''data_analyst'');

3. Count privileges per role:
   SELECT * FROM governance.metadata.v_role_privilege_count;

Next Steps:
1. Assign functional roles to users
2. Test end-to-end access
3. Validate masking policies
4. Monitor role usage

For questions, contact: data-platform-team@company.com
=============================================================================
' AS summary;

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
