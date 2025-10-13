-- ============================================================================
-- Script: 02_tags/grant_tag_privileges.sql
-- Purpose: Grant tag application privileges to appropriate roles
-- Author: Venkannababu Thatavarthi
-- Date: October 13, 2025
-- Version: 1.0
-- ============================================================================
-- Prerequisites: SECURITYADMIN or TAG_ADMIN role
-- Execution Time: ~3 minutes
-- ============================================================================

USE ROLE tag_admin;

-- ============================================================================
-- SECTION 1: Grant APPLY Privileges to DATA_STEWARD
-- ============================================================================
-- Purpose: Allow data stewards to apply tags to data objects
-- ============================================================================

-- Grant APPLY on all current tags
GRANT APPLY ON ALL TAGS IN SCHEMA governance.tags TO ROLE data_steward;

-- Grant APPLY on all future tags
GRANT APPLY ON FUTURE TAGS IN SCHEMA governance.tags TO ROLE data_steward;

-- Verify grants
SHOW GRANTS TO ROLE data_steward;

-- ============================================================================
-- SECTION 2: Grant Tag Privileges to Specific Functional Roles
-- ============================================================================
-- Purpose: Allow certain functional roles to apply specific tags
-- ============================================================================

-- Create helper procedure to grant APPLY on specific tags
CREATE OR REPLACE PROCEDURE governance.metadata.sp_grant_tag_apply(
    p_tag_name VARCHAR,
    p_role_name VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    grant_sql VARCHAR;
BEGIN
    grant_sql := 'GRANT APPLY ON TAG governance.tags.' || p_tag_name || ' TO ROLE ' || p_role_name;
    EXECUTE IMMEDIATE :grant_sql;
    RETURN 'APPLY privilege granted on tag ' || p_tag_name || ' to role ' || p_role_name;
END;
$$;

-- ============================================================================
-- SECTION 3: Grant Read-Only Access to Tags
-- ============================================================================
-- Purpose: Allow roles to view tag definitions and assignments
-- ============================================================================

-- Grant usage on governance database to PUBLIC (read-only)
USE ROLE sysadmin;
GRANT USAGE ON DATABASE governance TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA governance.tags TO ROLE PUBLIC;

-- Note: Users can view tag assignments through INFORMATION_SCHEMA and ACCOUNT_USAGE
-- No additional grants needed for read-only tag visibility

-- ============================================================================
-- SECTION 4: Grant Tag Privileges to Database Owners
-- ============================================================================
-- Purpose: Allow database owners to apply tags within their databases
-- ============================================================================

/*
Example: Grant APPLY privileges to specific database owner roles
Uncomment and customize for your environment

-- Finance database owner can apply finance-related tags
CALL governance.metadata.sp_grant_tag_apply('data_domain', 'finance_db_owner');
CALL governance.metadata.sp_grant_tag_apply('data_sensitivity', 'finance_db_owner');
CALL governance.metadata.sp_grant_tag_apply('pii_type', 'finance_db_owner');

-- HR database owner can apply HR-related tags
CALL governance.metadata.sp_grant_tag_apply('data_domain', 'hr_db_owner');
CALL governance.metadata.sp_grant_tag_apply('data_sensitivity', 'hr_db_owner');
CALL governance.metadata.sp_grant_tag_apply('pii_type', 'hr_db_owner');
CALL governance.metadata.sp_grant_tag_apply('compliance_scope', 'hr_db_owner');
*/

-- ============================================================================
-- SECTION 5: Create Tag Application Templates
-- ============================================================================
-- Purpose: Provide easy-to-use procedures for common tagging scenarios
-- ============================================================================

USE ROLE governance_admin;
USE SCHEMA governance.metadata;

-- Procedure to tag a PII column
CREATE OR REPLACE PROCEDURE sp_tag_pii_column(
    p_database VARCHAR,
    p_schema VARCHAR,
    p_table VARCHAR,
    p_column VARCHAR,
    p_pii_type VARCHAR,
    p_sensitivity VARCHAR DEFAULT 'CONFIDENTIAL',
    p_justification VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    tag_sql VARCHAR;
    log_message VARCHAR;
BEGIN
    -- Construct ALTER TABLE statement
    tag_sql := 'ALTER TABLE ' || p_database || '.' || p_schema || '.' || p_table || 
               ' MODIFY COLUMN ' || p_column || 
               ' SET TAG governance.tags.pii_type = ' || CHR(39) || p_pii_type || CHR(39) || 
               ', governance.tags.data_sensitivity = ' || CHR(39) || p_sensitivity || CHR(39);
    
    -- Execute the tagging
    EXECUTE IMMEDIATE :tag_sql;
    
    -- Log the action
    INSERT INTO governance.metadata.tag_assignment_log
        (tag_name, object_database, object_schema, object_name, 
         object_type, tag_value, business_justification)
    VALUES
        ('pii_type', :p_database, :p_schema, :p_table || '.' || :p_column, 
         'COLUMN', :p_pii_type, :p_justification),
        ('data_sensitivity', :p_database, :p_schema, :p_table || '.' || :p_column, 
         'COLUMN', :p_sensitivity, :p_justification);
    
    log_message := 'Successfully tagged column ' || p_database || '.' || p_schema || '.' || 
                   p_table || '.' || p_column || ' with PII type: ' || p_pii_type;
    
    RETURN log_message;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error tagging column: ' || SQLERRM;
END;
$$;

-- Procedure to tag a table with domain and sensitivity
CREATE OR REPLACE PROCEDURE sp_tag_table(
    p_database VARCHAR,
    p_schema VARCHAR,
    p_table VARCHAR,
    p_data_domain VARCHAR,
    p_sensitivity VARCHAR,
    p_compliance VARCHAR DEFAULT NULL,
    p_justification VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    tag_sql VARCHAR;
    log_message VARCHAR;
BEGIN
    -- Construct base ALTER TABLE statement
    tag_sql := 'ALTER TABLE ' || p_database || '.' || p_schema || '.' || p_table || 
               ' SET TAG governance.tags.data_domain = ' || CHR(39) || p_data_domain || CHR(39) || 
               ', governance.tags.data_sensitivity = ' || CHR(39) || p_sensitivity || CHR(39);
    
    -- Add compliance tag if provided
    IF (p_compliance IS NOT NULL) THEN
        tag_sql := tag_sql || ', governance.tags.compliance_scope = ' || CHR(39) || p_compliance || CHR(39);
    END IF;
    
    -- Execute the tagging
    EXECUTE IMMEDIATE :tag_sql;
    
    -- Log the action
    INSERT INTO governance.metadata.tag_assignment_log
        (tag_name, object_database, object_schema, object_name, 
         object_type, tag_value, business_justification)
    VALUES
        ('data_domain', :p_database, :p_schema, :p_table, 
         'TABLE', :p_data_domain, :p_justification),
        ('data_sensitivity', :p_database, :p_schema, :p_table, 
         'TABLE', :p_sensitivity, :p_justification);
    
    log_message := 'Successfully tagged table ' || p_database || '.' || p_schema || '.' || p_table;
    RETURN log_message;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error tagging table: ' || SQLERRM;
END;
$$;

-- Procedure to tag entire database
CREATE OR REPLACE PROCEDURE sp_tag_database(
    p_database VARCHAR,
    p_data_domain VARCHAR,
    p_environment VARCHAR,
    p_compliance VARCHAR DEFAULT NULL,
    p_justification VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    tag_sql VARCHAR;
    log_message VARCHAR;
BEGIN
    -- Construct ALTER DATABASE statement
    tag_sql := 'ALTER DATABASE ' || p_database || 
               ' SET TAG governance.tags.data_domain = ' || CHR(39) || p_data_domain || CHR(39) || 
               ', governance.tags.environment = ' || CHR(39) || p_environment || CHR(39);
    
    -- Add compliance tag if provided
    IF (p_compliance IS NOT NULL) THEN
        tag_sql := tag_sql || ', governance.tags.compliance_scope = ' || CHR(39) || p_compliance || CHR(39);
    END IF;
    
    -- Execute the tagging
    EXECUTE IMMEDIATE :tag_sql;
    
    -- Log the action
    INSERT INTO governance.metadata.tag_assignment_log
        (tag_name, object_database, object_schema, object_name, 
         object_type, tag_value, business_justification)
    VALUES
        ('data_domain', :p_database, NULL, NULL, 
         'DATABASE', :p_data_domain, :p_justification),
        ('environment', :p_database, NULL, NULL, 
         'DATABASE', :p_environment, :p_justification);
    
    log_message := 'Successfully tagged database ' || p_database;
    RETURN log_message;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error tagging database: ' || SQLERRM;
END;
$$;

-- ============================================================================
-- SECTION 6: Grant Execute Privileges on Helper Procedures
-- ============================================================================

-- Grant execute on tag application procedures to data_steward
GRANT USAGE ON PROCEDURE governance.metadata.sp_tag_pii_column(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) 
    TO ROLE data_steward;

GRANT USAGE ON PROCEDURE governance.metadata.sp_tag_table(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) 
    TO ROLE data_steward;

GRANT USAGE ON PROCEDURE governance.metadata.sp_tag_database(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) 
    TO ROLE data_steward;

-- Grant to governance_admin as well
GRANT USAGE ON PROCEDURE governance.metadata.sp_tag_pii_column(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) 
    TO ROLE governance_admin;

GRANT USAGE ON PROCEDURE governance.metadata.sp_tag_table(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) 
    TO ROLE governance_admin;

GRANT USAGE ON PROCEDURE governance.metadata.sp_tag_database(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) 
    TO ROLE governance_admin;

-- ============================================================================
-- SECTION 7: Create Views for Tag Monitoring
-- ============================================================================

USE ROLE governance_admin;
USE SCHEMA governance.metadata;

-- View to show current tag coverage by database
CREATE OR REPLACE VIEW v_tag_coverage_by_database AS
SELECT 
    tr.object_database,
    tr.object_schema,
    tr.tag_name,
    COUNT(DISTINCT tr.object_name) AS tagged_objects,
    tr.tag_value,
    tr.level AS tag_level
FROM snowflake.account_usage.tag_references tr
WHERE tr.tag_database = 'GOVERNANCE'
  AND tr.tag_schema = 'TAGS'
  AND tr.deleted IS NULL
GROUP BY tr.object_database, tr.object_schema, tr.tag_name, tr.tag_value, tr.level
ORDER BY tr.object_database, tr.object_schema, tr.tag_name;

-- View to identify untagged tables
CREATE OR REPLACE VIEW v_untagged_tables AS
SELECT 
    t.table_catalog AS database_name,
    t.table_schema AS schema_name,
    t.table_name,
    t.table_type,
    t.created AS table_created,
    t.row_count,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM snowflake.account_usage.tag_references tr
            WHERE tr.object_database = t.table_catalog
              AND tr.object_schema = t.table_schema
              AND tr.object_name = t.table_name
              AND tr.tag_database = 'GOVERNANCE'
              AND tr.deleted IS NULL
        ) THEN 'TAGGED'
        ELSE 'UNTAGGED'
    END AS tag_status
FROM snowflake.account_usage.tables t
WHERE t.deleted IS NULL
  AND t.table_schema NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
  AND t.table_catalog != 'SNOWFLAKE'
HAVING tag_status = 'UNTAGGED'
ORDER BY t.row_count DESC NULLS LAST;

-- View to show PII columns
CREATE OR REPLACE VIEW v_pii_columns AS
SELECT 
    tr.object_database,
    tr.object_schema,
    tr.object_name AS table_name,
    tr.column_name,
    tr.tag_value AS pii_type,
    tr.tag_created,
    c.data_type,
    c.is_nullable
FROM snowflake.account_usage.tag_references tr
JOIN snowflake.account_usage.columns c
    ON tr.object_database = c.table_catalog
    AND tr.object_schema = c.table_schema
    AND tr.object_name = c.table_name
    AND tr.column_name = c.column_name
WHERE tr.tag_name = 'PII_TYPE'
  AND tr.deleted IS NULL
  AND c.deleted IS NULL
ORDER BY tr.object_database, tr.object_schema, tr.object_name, tr.column_name;

-- Grant select on monitoring views to relevant roles
GRANT SELECT ON governance.metadata.v_tag_coverage_by_database TO ROLE data_steward;
GRANT SELECT ON governance.metadata.v_untagged_tables TO ROLE data_steward;
GRANT SELECT ON governance.metadata.v_pii_columns TO ROLE data_steward;
GRANT SELECT ON governance.metadata.v_tag_coverage_by_database TO ROLE compliance_officer;
GRANT SELECT ON governance.metadata.v_untagged_tables TO ROLE compliance_officer;
GRANT SELECT ON governance.metadata.v_pii_columns TO ROLE compliance_officer;

-- ============================================================================
-- SECTION 8: Usage Examples
-- ============================================================================

SELECT '
=============================================================================
TAG PRIVILEGES GRANTED SUCCESSFULLY
=============================================================================

Usage Examples:

1. Tag a PII column:
   CALL governance.metadata.sp_tag_pii_column(
       ''customer_db'', ''public'', ''customers'', ''email'',
       ''EMAIL'', ''CONFIDENTIAL'', ''Customer email addresses''
   );

2. Tag a table:
   CALL governance.metadata.sp_tag_table(
       ''sales_db'', ''public'', ''transactions'',
       ''SALES'', ''CONFIDENTIAL'', ''PCI_DSS'',
       ''Sales transaction data subject to PCI compliance''
   );

3. Tag a database:
   CALL governance.metadata.sp_tag_database(
       ''finance_db'', ''FINANCE'', ''PRODUCTION'', ''SOX'',
       ''Financial data subject to SOX compliance''
   );

4. Check tag coverage:
   SELECT * FROM governance.metadata.v_tag_coverage_by_database;

5. Find untagged tables:
   SELECT * FROM governance.metadata.v_untagged_tables LIMIT 100;

6. View all PII columns:
   SELECT * FROM governance.metadata.v_pii_columns;

Roles with Tag Privileges:
- DATA_STEWARD: Can apply all tags
- TAG_ADMIN: Can create and manage tag definitions
- GOVERNANCE_ADMIN: Full tag administration
- Database Owners: Can apply tags within their databases

Next Steps:
1. Begin tagging existing data assets
2. Create masking policies
3. Associate policies with tags
4. Monitor tag coverage regularly

For questions, contact: governance-team@company.com
=============================================================================
' AS usage_examples;

-- ============================================================================
-- SECTION 9: Validation
-- ============================================================================

-- Verify tag privileges
SELECT 'Tag Privileges Summary:' AS status;

SELECT 
    grantee_name AS role_name,
    privilege,
    name AS tag_name,
    granted_on
FROM snowflake.account_usage.grants_to_roles
WHERE deleted_on IS NULL
  AND privilege = 'APPLY'
  AND granted_on = 'TAG'
  AND name LIKE 'GOVERNANCE.TAGS.%'
ORDER BY grantee_name, name;

-- Show procedure grants
SELECT 'Procedure Execution Privileges:' AS status;

SELECT 
    grantee_name AS role_name,
    name AS procedure_name,
    privilege
FROM snowflake.account_usage.grants_to_roles
WHERE deleted_on IS NULL
  AND granted_on = 'PROCEDURE'
  AND name LIKE 'GOVERNANCE.METADATA.SP_TAG%'
ORDER BY grantee_name, name;

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
