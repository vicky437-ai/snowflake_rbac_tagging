-- ============================================================================
-- Script: 01_foundation/create_governance_database.sql
-- Purpose: Create governance database and schemas for RBAC and Tagging
-- Author: Venkannababu Thatavarthi
-- Date: October 13, 2025
-- Version: 1.0
-- ============================================================================
-- Prerequisites: SYSADMIN or ACCOUNTADMIN role
-- Execution Time: ~2 minutes
-- ============================================================================

-- Set context to SYSADMIN role
USE ROLE SYSADMIN;

-- ============================================================================
-- SECTION 1: Create Governance Database
-- ============================================================================
-- Purpose: Central repository for tags, policies, and metadata
-- ============================================================================

-- Create governance database (idempotent)
CREATE DATABASE IF NOT EXISTS governance
    COMMENT = 'Centralized governance database for tags, policies, and security metadata';

-- Verify database creation
SHOW DATABASES LIKE 'governance';

-- ============================================================================
-- SECTION 2: Create Schemas
-- ============================================================================

-- Use the governance database
USE DATABASE governance;

-- Schema for tag definitions
CREATE SCHEMA IF NOT EXISTS governance.tags
    COMMENT = 'Schema containing all tag definitions for data classification';

-- Schema for masking policies
CREATE SCHEMA IF NOT EXISTS governance.policies
    COMMENT = 'Schema containing masking and row access policies';

-- Schema for metadata and audit information
CREATE SCHEMA IF NOT EXISTS governance.metadata
    COMMENT = 'Schema for governance metadata and audit tables';

-- Schema for role documentation
CREATE SCHEMA IF NOT EXISTS governance.documentation
    COMMENT = 'Schema for role and access documentation';

-- Verify schema creation
SHOW SCHEMAS IN DATABASE governance;

-- ============================================================================
-- SECTION 3: Create Metadata Tables
-- ============================================================================

USE SCHEMA governance.metadata;

-- Table to track tag assignments and decisions
CREATE OR REPLACE TABLE tag_assignment_log (
    log_id NUMBER AUTOINCREMENT,
    tag_name VARCHAR(255) NOT NULL,
    object_database VARCHAR(255),
    object_schema VARCHAR(255),
    object_name VARCHAR(255),
    object_type VARCHAR(50),
    tag_value VARCHAR(255),
    assigned_by VARCHAR(255) DEFAULT CURRENT_USER(),
    assigned_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    business_justification VARCHAR(1000),
    PRIMARY KEY (log_id)
)
COMMENT = 'Audit log for all tag assignments with business justification';

-- Table to track role assignments
CREATE OR REPLACE TABLE role_assignment_log (
    log_id NUMBER AUTOINCREMENT,
    role_name VARCHAR(255) NOT NULL,
    granted_to_type VARCHAR(50) NOT NULL, -- USER or ROLE
    grantee_name VARCHAR(255) NOT NULL,
    granted_by VARCHAR(255) DEFAULT CURRENT_USER(),
    granted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    business_justification VARCHAR(1000),
    expiration_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (log_id)
)
COMMENT = 'Audit log for role grants with expiration tracking';

-- Table to track policy exceptions
CREATE OR REPLACE TABLE policy_exception_log (
    exception_id NUMBER AUTOINCREMENT,
    policy_name VARCHAR(255) NOT NULL,
    exception_role VARCHAR(255) NOT NULL,
    granted_by VARCHAR(255) DEFAULT CURRENT_USER(),
    granted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    expiration_date DATE NOT NULL,
    business_justification VARCHAR(1000) NOT NULL,
    approval_ticket VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (exception_id)
)
COMMENT = 'Track temporary policy exceptions with expiration dates';

-- Table to document role purposes
CREATE OR REPLACE TABLE role_documentation (
    role_name VARCHAR(255) PRIMARY KEY,
    role_type VARCHAR(50) NOT NULL, -- FUNCTIONAL, ACCESS, SERVICE, SYSTEM
    role_description VARCHAR(1000),
    business_owner VARCHAR(255),
    technical_owner VARCHAR(255),
    created_date DATE DEFAULT CURRENT_DATE(),
    last_reviewed_date DATE,
    review_frequency_days NUMBER DEFAULT 90,
    is_active BOOLEAN DEFAULT TRUE
)
COMMENT = 'Central documentation for all roles in the account';

-- Verify table creation
SHOW TABLES IN SCHEMA governance.metadata;

-- ============================================================================
-- SECTION 4: Create Views for Common Queries
-- ============================================================================

-- View to show active role assignments
CREATE OR REPLACE VIEW governance.metadata.v_active_role_assignments AS
SELECT 
    role_name,
    granted_to_type,
    grantee_name,
    granted_by,
    granted_at,
    business_justification,
    expiration_date,
    DATEDIFF(day, CURRENT_DATE(), expiration_date) AS days_until_expiration
FROM governance.metadata.role_assignment_log
WHERE is_active = TRUE
ORDER BY granted_at DESC;

-- View to show expiring access
CREATE OR REPLACE VIEW governance.metadata.v_expiring_access AS
SELECT 
    role_name,
    grantee_name,
    granted_at,
    expiration_date,
    DATEDIFF(day, CURRENT_DATE(), expiration_date) AS days_remaining,
    CASE 
        WHEN DATEDIFF(day, CURRENT_DATE(), expiration_date) < 0 THEN 'EXPIRED'
        WHEN DATEDIFF(day, CURRENT_DATE(), expiration_date) <= 7 THEN 'EXPIRING_SOON'
        WHEN DATEDIFF(day, CURRENT_DATE(), expiration_date) <= 30 THEN 'EXPIRING_THIS_MONTH'
        ELSE 'ACTIVE'
    END AS status
FROM governance.metadata.role_assignment_log
WHERE is_active = TRUE
    AND expiration_date IS NOT NULL
ORDER BY expiration_date ASC;

-- View to show policy exceptions
CREATE OR REPLACE VIEW governance.metadata.v_active_policy_exceptions AS
SELECT 
    exception_id,
    policy_name,
    exception_role,
    granted_by,
    granted_at,
    expiration_date,
    business_justification,
    approval_ticket,
    DATEDIFF(day, CURRENT_DATE(), expiration_date) AS days_remaining
FROM governance.metadata.policy_exception_log
WHERE is_active = TRUE
    AND expiration_date >= CURRENT_DATE()
ORDER BY expiration_date ASC;

-- ============================================================================
-- SECTION 5: Create Stored Procedures for Common Operations
-- ============================================================================

-- Procedure to log role assignments
CREATE OR REPLACE PROCEDURE governance.metadata.sp_log_role_assignment(
    p_role_name VARCHAR,
    p_granted_to_type VARCHAR,
    p_grantee_name VARCHAR,
    p_business_justification VARCHAR,
    p_expiration_date DATE
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO governance.metadata.role_assignment_log (
        role_name,
        granted_to_type,
        grantee_name,
        business_justification,
        expiration_date
    )
    VALUES (
        :p_role_name,
        :p_granted_to_type,
        :p_grantee_name,
        :p_business_justification,
        :p_expiration_date
    );
    
    RETURN 'Role assignment logged successfully';
END;
$$;

-- Procedure to log tag assignments
CREATE OR REPLACE PROCEDURE governance.metadata.sp_log_tag_assignment(
    p_tag_name VARCHAR,
    p_object_database VARCHAR,
    p_object_schema VARCHAR,
    p_object_name VARCHAR,
    p_object_type VARCHAR,
    p_tag_value VARCHAR,
    p_business_justification VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO governance.metadata.tag_assignment_log (
        tag_name,
        object_database,
        object_schema,
        object_name,
        object_type,
        tag_value,
        business_justification
    )
    VALUES (
        :p_tag_name,
        :p_object_database,
        :p_object_schema,
        :p_object_name,
        :p_object_type,
        :p_tag_value,
        :p_business_justification
    );
    
    RETURN 'Tag assignment logged successfully';
END;
$$;

-- ============================================================================
-- SECTION 6: Grant Privileges on Governance Database
-- ============================================================================

-- Grant usage on database to all roles
GRANT USAGE ON DATABASE governance TO ROLE PUBLIC;

-- Grant usage on metadata schema for read access
GRANT USAGE ON SCHEMA governance.metadata TO ROLE PUBLIC;
GRANT SELECT ON ALL VIEWS IN SCHEMA governance.metadata TO ROLE PUBLIC;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA governance.metadata TO ROLE PUBLIC;

-- ============================================================================
-- Validation and Output
-- ============================================================================

SELECT 'Governance database setup completed successfully!' AS status;

-- Show summary of created objects
SELECT 'Database Created: governance' AS summary
UNION ALL
SELECT 'Schemas Created: 4 (tags, policies, metadata, documentation)'
UNION ALL
SELECT 'Metadata Tables: 4'
UNION ALL
SELECT 'Views Created: 3'
UNION ALL
SELECT 'Stored Procedures: 2';

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
