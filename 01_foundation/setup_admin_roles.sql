-- ============================================================================
-- Script: 01_foundation/setup_admin_roles.sql
-- Purpose: Create administrative roles for governance operations
-- Author: Venkannababu Thatavarthi
-- Date: October 13, 2025
-- Version: 1.0
-- ============================================================================
-- Prerequisites: USERADMIN or ACCOUNTADMIN role
-- Execution Time: ~3 minutes
-- ============================================================================

-- Set context to USERADMIN role for role creation
USE ROLE USERADMIN;

-- ============================================================================
-- SECTION 1: Create Administrative Roles
-- ============================================================================

-- Tag Administrator Role
CREATE ROLE IF NOT EXISTS tag_admin
    COMMENT = 'Role for creating and managing tags across the account';

-- Policy Administrator Role
CREATE ROLE IF NOT EXISTS policy_admin
    COMMENT = 'Role for creating and managing masking and row access policies';

-- Data Steward Role
CREATE ROLE IF NOT EXISTS data_steward
    COMMENT = 'Role for applying tags and managing data classification';

-- Governance Administrator Role
CREATE ROLE IF NOT EXISTS governance_admin
    COMMENT = 'Role for overall governance operations and reporting';

-- Compliance Officer Role
CREATE ROLE IF NOT EXISTS compliance_officer
    COMMENT = 'Role for compliance monitoring and audit access';

-- Data Owner Role (template)
CREATE ROLE IF NOT EXISTS data_owner
    COMMENT = 'Role for data owners to approve access requests';

-- Verify role creation
SHOW ROLES LIKE '%admin%';
SHOW ROLES LIKE '%steward%';
SHOW ROLES LIKE '%compliance%';

-- ============================================================================
-- SECTION 2: Grant Privileges to Administrative Roles
-- ============================================================================

-- Switch to SECURITYADMIN for privilege grants
USE ROLE SECURITYADMIN;

-- ---------------------------------------------------------------------------
-- TAG_ADMIN Role Privileges
-- ---------------------------------------------------------------------------
-- Purpose: Create and manage tag definitions

-- Grant CREATE TAG privilege on governance schemas
GRANT USAGE ON DATABASE governance TO ROLE tag_admin;
GRANT USAGE ON SCHEMA governance.tags TO ROLE tag_admin;
GRANT CREATE TAG ON SCHEMA governance.tags TO ROLE tag_admin;
GRANT ALL PRIVILEGES ON ALL TAGS IN SCHEMA governance.tags TO ROLE tag_admin;
GRANT ALL PRIVILEGES ON FUTURE TAGS IN SCHEMA governance.tags TO ROLE tag_admin;

-- Grant access to metadata for logging
GRANT USAGE ON SCHEMA governance.metadata TO ROLE tag_admin;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA governance.metadata TO ROLE tag_admin;

-- ---------------------------------------------------------------------------
-- POLICY_ADMIN Role Privileges
-- ---------------------------------------------------------------------------
-- Purpose: Create and manage masking policies

-- Grant CREATE MASKING POLICY privilege
GRANT USAGE ON DATABASE governance TO ROLE policy_admin;
GRANT USAGE ON SCHEMA governance.policies TO ROLE policy_admin;
GRANT CREATE MASKING POLICY ON SCHEMA governance.policies TO ROLE policy_admin;
GRANT CREATE ROW ACCESS POLICY ON SCHEMA governance.policies TO ROLE policy_admin;
GRANT ALL PRIVILEGES ON ALL MASKING POLICIES IN SCHEMA governance.policies TO ROLE policy_admin;
GRANT ALL PRIVILEGES ON FUTURE MASKING POLICIES IN SCHEMA governance.policies TO ROLE policy_admin;

-- Grant access to metadata
GRANT USAGE ON SCHEMA governance.metadata TO ROLE policy_admin;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA governance.metadata TO ROLE policy_admin;

-- Grant access to tags schema to associate policies with tags
GRANT USAGE ON SCHEMA governance.tags TO ROLE policy_admin;
GRANT APPLY ON ALL TAGS IN SCHEMA governance.tags TO ROLE policy_admin;

-- ---------------------------------------------------------------------------
-- DATA_STEWARD Role Privileges
-- ---------------------------------------------------------------------------
-- Purpose: Apply tags to data objects

-- Grant APPLY privilege on tags
GRANT USAGE ON DATABASE governance TO ROLE data_steward;
GRANT USAGE ON SCHEMA governance.tags TO ROLE data_steward;
GRANT APPLY ON ALL TAGS IN SCHEMA governance.tags TO ROLE data_steward;
GRANT APPLY ON FUTURE TAGS IN SCHEMA governance.tags TO ROLE data_steward;

-- Grant access to metadata for logging
GRANT USAGE ON SCHEMA governance.metadata TO ROLE data_steward;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA governance.metadata TO ROLE data_steward;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA governance.metadata TO ROLE data_steward;

-- Grant MONITOR privilege to view object details
-- Note: Database-specific USAGE will be granted separately per database
GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE data_steward;

-- ---------------------------------------------------------------------------
-- GOVERNANCE_ADMIN Role Privileges
-- ---------------------------------------------------------------------------
-- Purpose: Overall governance oversight

-- Grant access to all governance schemas
GRANT USAGE ON DATABASE governance TO ROLE governance_admin;
GRANT USAGE ON ALL SCHEMAS IN DATABASE governance TO ROLE governance_admin;
GRANT SELECT ON ALL TABLES IN SCHEMA governance.metadata TO ROLE governance_admin;
GRANT SELECT ON ALL VIEWS IN SCHEMA governance.metadata TO ROLE governance_admin;
GRANT SELECT ON FUTURE TABLES IN SCHEMA governance.metadata TO ROLE governance_admin;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA governance.metadata TO ROLE governance_admin;

-- Grant access to ACCOUNT_USAGE for monitoring
GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE governance_admin;

-- Grant tag_admin, policy_admin, and data_steward to governance_admin
GRANT ROLE tag_admin TO ROLE governance_admin;
GRANT ROLE policy_admin TO ROLE governance_admin;
GRANT ROLE data_steward TO ROLE governance_admin;

-- ---------------------------------------------------------------------------
-- COMPLIANCE_OFFICER Role Privileges
-- ---------------------------------------------------------------------------
-- Purpose: Audit and compliance monitoring

-- Grant read access to governance metadata
GRANT USAGE ON DATABASE governance TO ROLE compliance_officer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE governance TO ROLE compliance_officer;
GRANT SELECT ON ALL TABLES IN SCHEMA governance.metadata TO ROLE compliance_officer;
GRANT SELECT ON ALL VIEWS IN SCHEMA governance.metadata TO ROLE compliance_officer;

-- Grant access to ACCOUNT_USAGE for compliance reporting
GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE compliance_officer;

-- Note: compliance_officer should see unmasked data for audit purposes
-- This will be configured in masking policies

-- ---------------------------------------------------------------------------
-- DATA_OWNER Role Privileges
-- ---------------------------------------------------------------------------
-- Purpose: Template role for data owners

-- Grant basic access to governance metadata
GRANT USAGE ON DATABASE governance TO ROLE data_owner;
GRANT USAGE ON SCHEMA governance.metadata TO ROLE data_owner;
GRANT SELECT ON ALL VIEWS IN SCHEMA governance.metadata TO ROLE data_owner;

-- ============================================================================
-- SECTION 3: Build Administrative Role Hierarchy
-- ============================================================================

-- Create hierarchy: governance_admin inherits other admin roles
-- Already granted above:
-- GRANT ROLE tag_admin TO ROLE governance_admin;
-- GRANT ROLE policy_admin TO ROLE governance_admin;
-- GRANT ROLE data_steward TO ROLE governance_admin;

-- Grant governance_admin to SYSADMIN
GRANT ROLE governance_admin TO ROLE sysadmin;

-- Grant compliance_officer to SECURITYADMIN for oversight
GRANT ROLE compliance_officer TO ROLE securityadmin;

-- Grant data_owner to SYSADMIN (will be inherited by specific data owners)
GRANT ROLE data_owner TO ROLE sysadmin;

-- ============================================================================
-- SECTION 4: Document Roles in Metadata
-- ============================================================================

USE ROLE governance_admin;
USE SCHEMA governance.metadata;

-- Document the administrative roles
INSERT INTO governance.metadata.role_documentation 
    (role_name, role_type, role_description, technical_owner, review_frequency_days)
VALUES
    ('TAG_ADMIN', 'ADMIN', 
     'Administrative role for creating and managing tag definitions', 
     'Data Governance Team', 90),
    ('POLICY_ADMIN', 'ADMIN', 
     'Administrative role for creating and managing masking and row access policies', 
     'Security Team', 90),
    ('DATA_STEWARD', 'ADMIN', 
     'Role for applying tags to data objects and managing classifications', 
     'Data Governance Team', 90),
    ('GOVERNANCE_ADMIN', 'ADMIN', 
     'Master governance role with full oversight of governance operations', 
     'Data Governance Team', 90),
    ('COMPLIANCE_OFFICER', 'ADMIN', 
     'Role for compliance monitoring and audit access', 
     'Compliance Team', 90),
    ('DATA_OWNER', 'ADMIN', 
     'Template role for data owners to approve access requests', 
     'Various', 90);

-- ============================================================================
-- SECTION 5: Create Initial Administrative Users (Example)
-- ============================================================================

-- Switch back to USERADMIN for user creation
USE ROLE USERADMIN;

-- Example: Create governance administrator user
-- Note: Replace with actual email and customize as needed
-- Uncomment and modify the following lines for your environment:

/*
CREATE USER IF NOT EXISTS governance_admin_user
    PASSWORD = 'TemporaryPassword123!'
    EMAIL = 'governance.admin@company.com'
    MUST_CHANGE_PASSWORD = TRUE
    DEFAULT_ROLE = governance_admin
    DEFAULT_WAREHOUSE = compute_wh
    COMMENT = 'Primary governance administrator account';

-- Grant governance_admin role to the user
USE ROLE SECURITYADMIN;
GRANT ROLE governance_admin TO USER governance_admin_user;

-- Example: Create compliance officer user
USE ROLE USERADMIN;
CREATE USER IF NOT EXISTS compliance_officer_user
    PASSWORD = 'TemporaryPassword123!'
    EMAIL = 'compliance.officer@company.com'
    MUST_CHANGE_PASSWORD = TRUE
    DEFAULT_ROLE = compliance_officer
    DEFAULT_WAREHOUSE = compute_wh
    COMMENT = 'Compliance and audit officer account';

USE ROLE SECURITYADMIN;
GRANT ROLE compliance_officer TO USER compliance_officer_user;
*/

-- ============================================================================
-- SECTION 6: Create Alert for Role Changes
-- ============================================================================

-- Create notification integration (requires ACCOUNTADMIN)
-- This is optional but recommended for production

/*
USE ROLE ACCOUNTADMIN;

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS governance_email_int
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('security-team@company.com', 'governance-team@company.com');

-- Create alert for new role creations
USE ROLE governance_admin;

CREATE OR REPLACE ALERT governance.metadata.alert_new_roles
    WAREHOUSE = compute_wh
    SCHEDULE = '60 MINUTE'
    IF (EXISTS (
        SELECT 1 
        FROM snowflake.account_usage.roles
        WHERE created_on > DATEADD(hour, -1, CURRENT_TIMESTAMP())
          AND name NOT LIKE 'SNOWFLAKE%'
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL(
            'governance_email_int',
            'governance-team@company.com',
            'New Role Created Alert',
            'New custom role(s) have been created in the Snowflake account. Please review.'
        );

ALTER ALERT governance.metadata.alert_new_roles RESUME;
*/

-- ============================================================================
-- SECTION 7: Validation Queries
-- ============================================================================

USE ROLE SECURITYADMIN;

-- Show all administrative roles
SELECT 'Administrative Roles Created:' AS status;
SHOW ROLES LIKE '%admin%';
SHOW ROLES LIKE '%steward%';
SHOW ROLES LIKE '%compliance%';
SHOW ROLES LIKE '%owner%';

-- Show role grants
SELECT 'Role Hierarchy:' AS status;
SELECT 
    grantee_name AS parent_role,
    name AS child_role,
    granted_on
FROM snowflake.account_usage.grants_to_roles
WHERE deleted_on IS NULL
  AND granted_to = 'ROLE'
  AND grantee_name IN ('GOVERNANCE_ADMIN', 'SYSADMIN', 'SECURITYADMIN')
ORDER BY grantee_name, name;

-- Show privileges granted to administrative roles
SELECT 'Privileges by Role:' AS status;
SELECT 
    grantee_name AS role_name,
    granted_on,
    privilege,
    name AS object_name,
    COUNT(*) AS privilege_count
FROM snowflake.account_usage.grants_to_roles
WHERE deleted_on IS NULL
  AND grantee_name IN (
      'TAG_ADMIN', 'POLICY_ADMIN', 'DATA_STEWARD', 
      'GOVERNANCE_ADMIN', 'COMPLIANCE_OFFICER', 'DATA_OWNER'
  )
GROUP BY grantee_name, granted_on, privilege, name
ORDER BY grantee_name, granted_on;

-- ============================================================================
-- SECTION 8: Post-Setup Instructions
-- ============================================================================

SELECT '
=============================================================================
ADMINISTRATIVE ROLES SETUP COMPLETED SUCCESSFULLY
=============================================================================

Next Steps:
1. Assign administrative roles to appropriate users
2. Enable MFA for all administrative users
3. Document administrative procedures
4. Schedule initial governance training
5. Proceed to Phase 2: Tag Framework Creation

Administrative Roles Created:
- TAG_ADMIN: Create and manage tags
- POLICY_ADMIN: Create and manage policies  
- DATA_STEWARD: Apply tags to data objects
- GOVERNANCE_ADMIN: Overall governance oversight
- COMPLIANCE_OFFICER: Compliance monitoring
- DATA_OWNER: Data ownership and access approval

Role Hierarchy:
SYSADMIN
  └── GOVERNANCE_ADMIN
      ├── TAG_ADMIN
      ├── POLICY_ADMIN
      └── DATA_STEWARD

SECURITYADMIN
  └── COMPLIANCE_OFFICER

Important Security Notes:
- Limit administrative role assignments
- Enable MFA for all admin users
- Review role assignments quarterly
- Audit administrative actions regularly

For questions or issues, contact: governance-team@company.com
=============================================================================
' AS post_setup_instructions;

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
