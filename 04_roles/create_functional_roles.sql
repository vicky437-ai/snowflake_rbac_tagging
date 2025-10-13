-- ============================================================================
-- Script: 04_roles/create_functional_roles.sql
-- Purpose: Create functional roles aligned with business functions
-- Author: Venkannababu Thatavarthi
-- Date: October 13, 2025
-- Version: 1.0
-- ============================================================================
-- Prerequisites: USERADMIN role or CREATE ROLE privilege
-- Execution Time: ~5 minutes
-- ============================================================================
-- IMPORTANT: Customize roles based on your organization's structure
-- ============================================================================

USE ROLE useradmin;

-- ============================================================================
-- SECTION 1: Data and Analytics Functional Roles
-- ============================================================================

-- Data Analyst Role
CREATE ROLE IF NOT EXISTS data_analyst
    COMMENT = 'Functional role for data analysts - read access to analytical datasets';

-- Senior Data Analyst Role
CREATE ROLE IF NOT EXISTS senior_data_analyst
    COMMENT = 'Functional role for senior data analysts - broader read access';

-- Data Scientist Role
CREATE ROLE IF NOT EXISTS data_scientist
    COMMENT = 'Functional role for data scientists - read access with ML capabilities';

-- Business Analyst Role
CREATE ROLE IF NOT EXISTS business_analyst
    COMMENT = 'Functional role for business analysts - read access to business data';

-- BI Developer Role
CREATE ROLE IF NOT EXISTS bi_developer
    COMMENT = 'Functional role for BI developers - access to reporting and visualization tools';

-- ============================================================================
-- SECTION 2: Data Engineering Functional Roles
-- ============================================================================

-- Data Engineer Role
CREATE ROLE IF NOT EXISTS data_engineer
    COMMENT = 'Functional role for data engineers - read-write access for ETL operations';

-- Senior Data Engineer Role
CREATE ROLE IF NOT EXISTS senior_data_engineer
    COMMENT = 'Functional role for senior data engineers - admin access to data pipelines';

-- ETL Developer Role
CREATE ROLE IF NOT EXISTS etl_developer
    COMMENT = 'Functional role for ETL developers - pipeline development and maintenance';

-- Data Architect Role
CREATE ROLE IF NOT EXISTS data_architect
    COMMENT = 'Functional role for data architects - design and architecture oversight';

-- ============================================================================
-- SECTION 3: Business Domain Functional Roles
-- ============================================================================

-- Finance Team Role
CREATE ROLE IF NOT EXISTS finance_analyst
    COMMENT = 'Functional role for finance analysts - access to financial data';

CREATE ROLE IF NOT EXISTS finance_manager
    COMMENT = 'Functional role for finance managers - broader financial data access';

CREATE ROLE IF NOT EXISTS accountant
    COMMENT = 'Functional role for accountants - accounting data access';

-- HR Team Roles
CREATE ROLE IF NOT EXISTS hr_analyst
    COMMENT = 'Functional role for HR analysts - access to employee data';

CREATE ROLE IF NOT EXISTS hr_manager
    COMMENT = 'Functional role for HR managers - broader HR data access';

CREATE ROLE IF NOT EXISTS hr_admin
    COMMENT = 'Functional role for HR administrators - full HR data access';

CREATE ROLE IF NOT EXISTS payroll_admin
    COMMENT = 'Functional role for payroll administrators - payroll data access';

-- Sales Team Roles
CREATE ROLE IF NOT EXISTS sales_analyst
    COMMENT = 'Functional role for sales analysts - sales data analysis';

CREATE ROLE IF NOT EXISTS sales_manager
    COMMENT = 'Functional role for sales managers - sales team oversight';

CREATE ROLE IF NOT EXISTS sales_operations
    COMMENT = 'Functional role for sales operations - sales process management';

-- Marketing Team Roles
CREATE ROLE IF NOT EXISTS marketing_analyst
    COMMENT = 'Functional role for marketing analysts - campaign analysis';

CREATE ROLE IF NOT EXISTS marketing_manager
    COMMENT = 'Functional role for marketing managers - marketing strategy oversight';

CREATE ROLE IF NOT EXISTS digital_marketing
    COMMENT = 'Functional role for digital marketing - digital channel data';

-- Customer Support Roles
CREATE ROLE IF NOT EXISTS customer_support
    COMMENT = 'Functional role for customer support - customer data access';

CREATE ROLE IF NOT EXISTS customer_support_manager
    COMMENT = 'Functional role for support managers - support operations oversight';

-- Operations Roles
CREATE ROLE IF NOT EXISTS operations_analyst
    COMMENT = 'Functional role for operations analysts - operational data access';

CREATE ROLE IF NOT EXISTS operations_manager
    COMMENT = 'Functional role for operations managers - operations oversight';

-- ============================================================================
-- SECTION 4: Executive and Leadership Roles
-- ============================================================================

-- Executive Leadership
CREATE ROLE IF NOT EXISTS executive
    COMMENT = 'Functional role for executives - high-level reporting access';

CREATE ROLE IF NOT EXISTS cfo
    COMMENT = 'Functional role for CFO - financial oversight';

CREATE ROLE IF NOT EXISTS cto
    COMMENT = 'Functional role for CTO - technology oversight';

CREATE ROLE IF NOT EXISTS chief_data_officer
    COMMENT = 'Functional role for Chief Data Officer - data governance oversight';

-- ============================================================================
-- SECTION 5: Service and Application Roles
-- ============================================================================

-- BI Tool Integration Roles
CREATE ROLE IF NOT EXISTS tableau_service_role
    COMMENT = 'Service role for Tableau integration';

CREATE ROLE IF NOT EXISTS powerbi_service_role
    COMMENT = 'Service role for Power BI integration';

CREATE ROLE IF NOT EXISTS looker_service_role
    COMMENT = 'Service role for Looker integration';

-- ETL Tool Integration Roles
CREATE ROLE IF NOT EXISTS dbt_service_role
    COMMENT = 'Service role for dbt transformations';

CREATE ROLE IF NOT EXISTS airflow_service_role
    COMMENT = 'Service role for Apache Airflow orchestration';

CREATE ROLE IF NOT EXISTS fivetran_service_role
    COMMENT = 'Service role for Fivetran data ingestion';

CREATE ROLE IF NOT EXISTS matillion_service_role
    COMMENT = 'Service role for Matillion ETL';

-- Application Integration Roles
CREATE ROLE IF NOT EXISTS api_service_role
    COMMENT = 'Service role for API integrations';

CREATE ROLE IF NOT EXISTS app_backend_role
    COMMENT = 'Service role for application backend services';

-- ============================================================================
-- SECTION 6: Temporary and Project-Based Roles
-- ============================================================================

-- Temporary Access Template
CREATE ROLE IF NOT EXISTS temp_contractor_role
    COMMENT = 'Template role for temporary contractors - expires after project';

-- Project-specific roles
CREATE ROLE IF NOT EXISTS project_migration_team
    COMMENT = 'Project role for data migration initiatives';

CREATE ROLE IF NOT EXISTS project_ml_initiative
    COMMENT = 'Project role for machine learning initiatives';

-- ============================================================================
-- SECTION 7: Document Functional Roles
-- ============================================================================

USE ROLE governance_admin;
USE SCHEMA governance.metadata;

-- Document all functional roles
INSERT INTO role_documentation 
    (role_name, role_type, role_description, technical_owner, review_frequency_days)
VALUES
    -- Data & Analytics Roles
    ('DATA_ANALYST', 'FUNCTIONAL', 'Data analysts - analytical data access', 'Analytics Team', 90),
    ('SENIOR_DATA_ANALYST', 'FUNCTIONAL', 'Senior data analysts - broader access', 'Analytics Team', 90),
    ('DATA_SCIENTIST', 'FUNCTIONAL', 'Data scientists - ML and advanced analytics', 'Data Science Team', 90),
    ('BUSINESS_ANALYST', 'FUNCTIONAL', 'Business analysts - business data access', 'Business Analytics', 90),
    ('BI_DEVELOPER', 'FUNCTIONAL', 'BI developers - reporting tools', 'BI Team', 90),
    
    -- Data Engineering Roles
    ('DATA_ENGINEER', 'FUNCTIONAL', 'Data engineers - ETL operations', 'Data Engineering Team', 90),
    ('SENIOR_DATA_ENGINEER', 'FUNCTIONAL', 'Senior data engineers - pipeline admin', 'Data Engineering Team', 90),
    ('ETL_DEVELOPER', 'FUNCTIONAL', 'ETL developers - pipeline development', 'Data Engineering Team', 90),
    ('DATA_ARCHITECT', 'FUNCTIONAL', 'Data architects - architecture oversight', 'Architecture Team', 90),
    
    -- Finance Roles
    ('FINANCE_ANALYST', 'FUNCTIONAL', 'Finance analysts', 'Finance Team', 90),
    ('FINANCE_MANAGER', 'FUNCTIONAL', 'Finance managers', 'Finance Team', 90),
    ('ACCOUNTANT', 'FUNCTIONAL', 'Accountants', 'Finance Team', 90),
    
    -- HR Roles
    ('HR_ANALYST', 'FUNCTIONAL', 'HR analysts', 'HR Team', 90),
    ('HR_MANAGER', 'FUNCTIONAL', 'HR managers', 'HR Team', 90),
    ('HR_ADMIN', 'FUNCTIONAL', 'HR administrators', 'HR Team', 90),
    ('PAYROLL_ADMIN', 'FUNCTIONAL', 'Payroll administrators', 'HR Team', 90),
    
    -- Sales Roles
    ('SALES_ANALYST', 'FUNCTIONAL', 'Sales analysts', 'Sales Ops Team', 90),
    ('SALES_MANAGER', 'FUNCTIONAL', 'Sales managers', 'Sales Leadership', 90),
    ('SALES_OPERATIONS', 'FUNCTIONAL', 'Sales operations', 'Sales Ops Team', 90),
    
    -- Marketing Roles
    ('MARKETING_ANALYST', 'FUNCTIONAL', 'Marketing analysts', 'Marketing Ops', 90),
    ('MARKETING_MANAGER', 'FUNCTIONAL', 'Marketing managers', 'Marketing Leadership', 90),
    ('DIGITAL_MARKETING', 'FUNCTIONAL', 'Digital marketing team', 'Marketing Ops', 90),
    
    -- Customer Support Roles
    ('CUSTOMER_SUPPORT', 'FUNCTIONAL', 'Customer support agents', 'Support Team', 90),
    ('CUSTOMER_SUPPORT_MANAGER', 'FUNCTIONAL', 'Support managers', 'Support Leadership', 90),
    
    -- Operations Roles
    ('OPERATIONS_ANALYST', 'FUNCTIONAL', 'Operations analysts', 'Operations Team', 90),
    ('OPERATIONS_MANAGER', 'FUNCTIONAL', 'Operations managers', 'Operations Leadership', 90),
    
    -- Executive Roles
    ('EXECUTIVE', 'FUNCTIONAL', 'Executive leadership', 'C-Suite', 90),
    ('CFO', 'FUNCTIONAL', 'Chief Financial Officer', 'Finance Leadership', 90),
    ('CTO', 'FUNCTIONAL', 'Chief Technology Officer', 'Technology Leadership', 90),
    ('CHIEF_DATA_OFFICER', 'FUNCTIONAL', 'Chief Data Officer', 'Data Leadership', 90),
    
    -- Service Roles
    ('TABLEAU_SERVICE_ROLE', 'SERVICE', 'Tableau BI service', 'BI Platform Team', 90),
    ('POWERBI_SERVICE_ROLE', 'SERVICE', 'Power BI service', 'BI Platform Team', 90),
    ('LOOKER_SERVICE_ROLE', 'SERVICE', 'Looker service', 'BI Platform Team', 90),
    ('DBT_SERVICE_ROLE', 'SERVICE', 'dbt transformation service', 'Data Engineering Team', 90),
    ('AIRFLOW_SERVICE_ROLE', 'SERVICE', 'Airflow orchestration', 'Data Engineering Team', 90),
    ('FIVETRAN_SERVICE_ROLE', 'SERVICE', 'Fivetran ingestion', 'Data Engineering Team', 90),
    ('MATILLION_SERVICE_ROLE', 'SERVICE', 'Matillion ETL service', 'Data Engineering Team', 90),
    ('API_SERVICE_ROLE', 'SERVICE', 'API integration service', 'Platform Team', 90),
    ('APP_BACKEND_ROLE', 'SERVICE', 'Application backend', 'Application Team', 90);

-- ============================================================================
-- SECTION 8: Create Role Assignment Procedure
-- ============================================================================

-- Procedure to assign functional role to user with logging
CREATE OR REPLACE PROCEDURE sp_assign_functional_role_to_user(
    p_user_name VARCHAR,
    p_role_name VARCHAR,
    p_justification VARCHAR,
    p_expiration_date DATE DEFAULT NULL
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
    -- Grant role to user
    grant_sql := 'GRANT ROLE ' || p_role_name || ' TO USER ' || p_user_name;
    EXECUTE IMMEDIATE :grant_sql;
    
    -- Log the assignment
    INSERT INTO governance.metadata.role_assignment_log
        (role_name, granted_to_type, grantee_name, business_justification, expiration_date)
    VALUES
        (:p_role_name, 'USER', :p_user_name, :p_justification, :p_expiration_date);
    
    result_msg := 'Successfully assigned role ' || p_role_name || ' to user ' || p_user_name;
    
    IF (p_expiration_date IS NOT NULL) THEN
        result_msg := result_msg || ' (expires: ' || TO_VARCHAR(p_expiration_date) || ')';
    END IF;
    
    RETURN result_msg;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error assigning role: ' || SQLERRM;
END;
$$;

-- Grant execute to useradmin and securityadmin
GRANT USAGE ON PROCEDURE governance.metadata.sp_assign_functional_role_to_user(VARCHAR, VARCHAR, VARCHAR, DATE) 
    TO ROLE useradmin;
GRANT USAGE ON PROCEDURE governance.metadata.sp_assign_functional_role_to_user(VARCHAR, VARCHAR, VARCHAR, DATE) 
    TO ROLE securityadmin;

-- ============================================================================
-- SECTION 9: Create Views for Role Management
-- ============================================================================

-- View functional roles catalog
CREATE OR REPLACE VIEW v_functional_role_catalog AS
SELECT 
    role_name,
    role_type,
    role_description,
    business_owner,
    technical_owner,
    created_date,
    last_reviewed_date,
    review_frequency_days,
    is_active
FROM governance.metadata.role_documentation
WHERE role_type IN ('FUNCTIONAL', 'SERVICE')
  AND is_active = TRUE
ORDER BY role_type, role_name;

-- View role assignments by user
CREATE OR REPLACE VIEW v_user_role_assignments AS
SELECT 
    grantee_name AS user_name,
    name AS role_name,
    granted_on AS grant_date,
    granted_by,
    deleted_on
FROM snowflake.account_usage.grants_to_users
WHERE granted_to = 'USER'
  AND deleted_on IS NULL
ORDER BY grantee_name, name;

-- Grant select privileges
GRANT SELECT ON governance.metadata.v_functional_role_catalog TO ROLE PUBLIC;
GRANT SELECT ON governance.metadata.v_user_role_assignments TO ROLE useradmin;
GRANT SELECT ON governance.metadata.v_user_role_assignments TO ROLE securityadmin;

-- ============================================================================
-- SECTION 10: Validation
-- ============================================================================

USE ROLE useradmin;

SELECT 'Functional roles created successfully!' AS status;

-- Show all functional and service roles
SHOW ROLES LIKE '%analyst%';
SHOW ROLES LIKE '%manager%';
SHOW ROLES LIKE '%engineer%';
SHOW ROLES LIKE '%service_role%';

-- Count roles by type
SELECT 
    role_type,
    COUNT(*) AS role_count
FROM governance.metadata.role_documentation
WHERE is_active = TRUE
GROUP BY role_type
ORDER BY role_count DESC;

-- Summary output
SELECT '
=============================================================================
FUNCTIONAL ROLES CREATED SUCCESSFULLY
=============================================================================

Role Categories:
1. Data & Analytics (5 roles)
   - Data Analyst, Senior Data Analyst, Data Scientist
   - Business Analyst, BI Developer

2. Data Engineering (4 roles)
   - Data Engineer, Senior Data Engineer
   - ETL Developer, Data Architect

3. Business Domains (17 roles)
   - Finance: Analyst, Manager, Accountant
   - HR: Analyst, Manager, Admin, Payroll Admin
   - Sales: Analyst, Manager, Operations
   - Marketing: Analyst, Manager, Digital Marketing
   - Customer Support: Agent, Manager
   - Operations: Analyst, Manager

4. Executive Leadership (4 roles)
   - Executive, CFO, CTO, Chief Data Officer

5. Service/Application Roles (9 roles)
   - BI Tools: Tableau, Power BI, Looker
   - ETL Tools: dbt, Airflow, Fivetran, Matillion
   - Applications: API Service, App Backend

Total Functional/Service Roles: 39

Next Steps:
1. Build role hierarchy by granting access roles to functional roles
2. Grant functional roles to SYSADMIN
3. Assign functional roles to users
4. Test end-to-end access

Helper Procedure:
To assign a role to a user with logging:
  CALL governance.metadata.sp_assign_functional_role_to_user(
      ''user@company.com'',
      ''data_analyst'',
      ''New hire - analytics team'',
      NULL
  );

View All Functional Roles:
  SELECT * FROM governance.metadata.v_functional_role_catalog;

For questions, contact: data-platform-team@company.com
=============================================================================
' AS summary;

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
