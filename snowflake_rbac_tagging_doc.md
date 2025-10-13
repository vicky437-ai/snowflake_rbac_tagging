# Snowflake RBAC & Tagging Strategy
## Implementation Architecture & Guidelines

**Prepared by:** Venkannababu Thatavarthi  
**Date:** October 13, 2025  
**Version:** 1.0

---

## Executive Summary

This document outlines the comprehensive Role-Based Access Control (RBAC) and Tagging Strategy for implementing secure, scalable, and maintainable data governance in the Snowflake environment. The strategy combines traditional RBAC with tag-based access control to provide granular data security while maintaining operational efficiency.

The architecture presented here has been designed to meet enterprise security requirements while ensuring ease of management and compliance with data protection regulations.

---

## 1. RBAC Architecture Overview

### 1.1 Core Principles

Our RBAC implementation is built on three fundamental principles:

**Principle of Least Privilege**: Users receive only the minimum permissions necessary to perform their job functions. This minimizes security risks and reduces the potential impact of compromised accounts.

**Separation of Duties**: Administrative functions are segregated across different system roles to prevent any single user from having excessive control over the environment.

**Role Hierarchy**: Privileges flow through a well-structured hierarchy, ensuring consistent access patterns and simplified management.

### 1.2 Role Classification

We have categorized roles into three distinct types to maintain clarity and separation of concerns:

#### Functional Roles
These roles align with business functions and are assigned to end users. They represent job titles or departments within your organization.

Examples:
- `DATA_ANALYST`
- `FINANCE_ANALYST`
- `HR_MANAGER`
- `MARKETING_TEAM`

#### Access Roles
These are granular, object-specific roles that control access to databases and schemas. They serve as building blocks in the hierarchy.

Naming Convention: `<DATABASE>_<SCHEMA>_<ACCESS_LEVEL>`

Examples:
- `CUSTOMER_DB_READ`
- `FINANCE_DB_READ_WRITE`
- `HR_DB_ADMIN`

#### Service Roles
These roles are designed for application integrations, ETL processes, and automated services rather than human users.

Examples:
- `DBT_SERVICE_ROLE`
- `TABLEAU_SERVICE_ROLE`
- `FIVETRAN_SERVICE_ROLE`

### 1.3 System-Defined Roles

Snowflake provides several system roles that form the foundation of the access control framework:

| Role | Purpose | Scope |
|------|---------|-------|
| ACCOUNTADMIN | Top-level administrative role | Full account control |
| SECURITYADMIN | Security and grant management | User/role management, grant oversight |
| USERADMIN | User and role creation | User/role lifecycle management |
| SYSADMIN | Database and warehouse management | Object creation and management |
| PUBLIC | Default role for all users | Minimal baseline permissions |

**Critical Guidelines:**

- Limit ACCOUNTADMIN assignment to 2-3 trusted administrators only
- Enable Multi-Factor Authentication (MFA) for all ACCOUNTADMIN users
- Never use ACCOUNTADMIN for routine operations or object creation
- Do not use ACCOUNTADMIN in automated scripts or service accounts

---

## 2. Role Hierarchy Architecture

### 2.1 Recommended Hierarchy Structure

```
ACCOUNTADMIN (Top Level - Restricted Access)
    │
    ├── SECURITYADMIN (Grant Management)
    │   └── USERADMIN (User/Role Creation)
    │
    └── SYSADMIN (Object Management)
        │
        ├── FUNCTIONAL_ROLE_1 (e.g., DATA_ENGINEER)
        │   ├── ACCESS_ROLE_1 (e.g., RAW_DB_READ_WRITE)
        │   ├── ACCESS_ROLE_2 (e.g., ANALYTICS_DB_READ)
        │   └── ACCESS_ROLE_3 (e.g., STAGING_DB_ADMIN)
        │
        ├── FUNCTIONAL_ROLE_2 (e.g., DATA_ANALYST)
        │   ├── ACCESS_ROLE_4 (e.g., ANALYTICS_DB_READ)
        │   └── ACCESS_ROLE_5 (e.g., REPORTING_DB_READ)
        │
        └── SERVICE_ROLE_1 (e.g., DBT_TRANSFORM_ROLE)
            ├── ACCESS_ROLE_6 (e.g., TRANSFORM_DB_READ_WRITE)
            └── ACCESS_ROLE_7 (e.g., RAW_DB_READ)
```

### 2.2 Privilege Inheritance Flow

In a role hierarchy, privileges flow upward from child roles to parent roles:

- When Role A is granted to Role B, Role B inherits all privileges of Role A
- Users assigned Role B automatically receive privileges from both roles
- Changes to child role privileges automatically propagate to parent roles
- This eliminates the need to duplicate grants across multiple roles

**Example Scenario:**

```
ACCESS_ROLE: SALES_DB_READ
  - USAGE on DATABASE sales_db
  - USAGE on SCHEMA sales_db.transactions
  - SELECT on ALL TABLES in sales_db.transactions

FUNCTIONAL_ROLE: SALES_ANALYST
  - Granted ACCESS_ROLE: SALES_DB_READ
  - Granted ACCESS_ROLE: WAREHOUSE_SMALL_USAGE

USER: john.smith@company.com
  - Granted FUNCTIONAL_ROLE: SALES_ANALYST
  - Inherits all privileges from both access roles
```

### 2.3 Database Roles Integration

For database-specific access control, we implement database roles within individual databases:

```
ACCOUNT LEVEL:
    FUNCTIONAL_ROLE: FINANCE_TEAM
        │
        └── Granted to: DB_ROLE: finance_db.finance_reader
                          │
                          ├── SELECT on all tables
                          ├── USAGE on all schemas
                          └── USAGE on database
```

**Key Benefits:**

- Database owners can manage access independently
- Simplified sharing workflows
- Clear ownership boundaries
- Reduced complexity in multi-database environments

---

## 3. Tagging Strategy

### 3.1 Tag-Based Access Control (TBAC) Overview

Tags are schema-level objects that provide metadata classification for Snowflake objects. When combined with masking policies and row access policies, tags enable dynamic, attribute-based access control.

**Why Use Tags?**

- Centralized data classification
- Automated policy enforcement
- Scalable governance for large datasets
- Audit-friendly compliance tracking
- Reduced manual configuration overhead

### 3.2 Tag Categories

We have defined the following tag categories for comprehensive data governance:

#### Sensitivity Classification Tags

```sql
-- Tag: DATA_SENSITIVITY
-- Allowed Values: PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED
CREATE TAG data_sensitivity ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED';
```

| Level | Description | Access Control |
|-------|-------------|----------------|
| PUBLIC | Non-sensitive, freely shareable | All authenticated users |
| INTERNAL | Internal use only | Company employees only |
| CONFIDENTIAL | Restricted to authorized personnel | Specific functional roles |
| RESTRICTED | Highly sensitive, minimal access | C-level and compliance only |

#### PII Classification Tags

```sql
-- Tag: PII_TYPE
-- Allowed Values: EMAIL, PHONE, SSN, ADDRESS, CREDIT_CARD, HEALTH_INFO
CREATE TAG pii_type ALLOWED_VALUES 'EMAIL', 'PHONE', 'SSN', 'ADDRESS', 'CREDIT_CARD', 'HEALTH_INFO';
```

#### Compliance Tags

```sql
-- Tag: COMPLIANCE_SCOPE
-- Allowed Values: GDPR, HIPAA, PCI_DSS, SOX, CCPA
CREATE TAG compliance_scope ALLOWED_VALUES 'GDPR', 'HIPAA', 'PCI_DSS', 'SOX', 'CCPA';
```

#### Data Domain Tags

```sql
-- Tag: DATA_DOMAIN
-- Allowed Values: FINANCE, HR, SALES, MARKETING, OPERATIONS, LEGAL
CREATE TAG data_domain ALLOWED_VALUES 'FINANCE', 'HR', 'SALES', 'MARKETING', 'OPERATIONS', 'LEGAL';
```

### 3.3 Tag Inheritance Model

Tags automatically inherit through the Snowflake object hierarchy:

```
DATABASE (Tagged: data_domain = FINANCE)
  └── SCHEMA (Inherits: data_domain = FINANCE)
      └── TABLE (Inherits: data_domain = FINANCE)
          └── COLUMN (Can override: pii_type = SSN)
```

**Inheritance Rules:**

- Child objects inherit tags from parent objects
- Tags set directly on an object override inherited tags
- Column-level tags take precedence over table-level tags
- Maximum 50 unique tags per object

### 3.4 Tag Assignment Strategy

**Database Level:**
- Assign `data_domain` tag to classify entire databases
- Assign `compliance_scope` for regulatory requirements

**Schema Level:**
- Refine `data_sensitivity` classification
- Add environment indicators (PROD, DEV, TEST)

**Table Level:**
- Specific `data_sensitivity` levels
- Retention policy indicators
- Cost center allocation tags

**Column Level:**
- Detailed `pii_type` classification
- Masking policy assignments
- Data quality indicators

---

## 4. Tag-Based Masking Policies

### 4.1 Masking Policy Architecture

Masking policies transform data before it reaches users based on their roles and the sensitivity of the data. By attaching masking policies to tags, we achieve scalable, centralized policy management.

### 4.2 Masking Policy Patterns

#### Email Masking Policy

```sql
CREATE OR REPLACE MASKING POLICY mask_email AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'DATA_STEWARD') THEN val
    WHEN CURRENT_ROLE() IN ('CUSTOMER_SUPPORT', 'MARKETING_TEAM') THEN 
      REGEXP_REPLACE(val, '(.{2}).*(@.*)', '\\1***\\2')
    ELSE '***@***'
  END;

-- Attach policy to tag
ALTER TAG pii_type SET MASKING POLICY mask_email;
```

#### Phone Number Masking Policy

```sql
CREATE OR REPLACE MASKING POLICY mask_phone AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'COMPLIANCE_OFFICER') THEN val
    WHEN CURRENT_ROLE() IN ('CUSTOMER_SUPPORT') THEN 
      CONCAT('***-***-', SUBSTRING(val, -4))
    ELSE '***-***-****'
  END;

ALTER TAG pii_type SET MASKING POLICY mask_phone;
```

#### SSN/Tax ID Masking Policy

```sql
CREATE OR REPLACE MASKING POLICY mask_ssn AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'HR_ADMIN', 'PAYROLL_ADMIN') THEN val
    WHEN CURRENT_ROLE() IN ('HR_ANALYST') THEN 
      CONCAT('***-**-', SUBSTRING(val, -4))
    ELSE '***-**-****'
  END;

ALTER TAG pii_type SET MASKING POLICY mask_ssn;
```

#### Financial Data Masking Policy

```sql
CREATE OR REPLACE MASKING POLICY mask_financial AS (val NUMBER) RETURNS NUMBER ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'FINANCE_ADMIN', 'CFO') THEN val
    WHEN CURRENT_ROLE() IN ('FINANCE_ANALYST') THEN ROUND(val, -3)
    ELSE NULL
  END;

ALTER TAG data_sensitivity SET MASKING POLICY mask_financial;
```

### 4.3 Tag-to-Policy Mapping

This approach allows us to apply one policy to hundreds or thousands of columns automatically:

```sql
-- Apply tag to columns
ALTER TABLE customers 
  MODIFY COLUMN email SET TAG pii_type = 'EMAIL';

ALTER TABLE customers 
  MODIFY COLUMN phone_number SET TAG pii_type = 'PHONE';

ALTER TABLE employees 
  MODIFY COLUMN ssn SET TAG pii_type = 'SSN';

-- All tagged columns automatically inherit the associated masking policy
```

---

## 5. Implementation Flow Diagrams

### 5.1 RBAC Implementation Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    RBAC Implementation Process                │
└─────────────────────────────────────────────────────────────┘

Step 1: Define Requirements
┌────────────────────┐
│ Business Analysis  │
│ - Job Functions    │
│ - Data Access Needs│
│ - Compliance Reqs  │
└─────────┬──────────┘
          │
          ▼
Step 2: Create Role Structure
┌────────────────────┐
│ Create Access      │◄───── Database Level
│ Roles              │       Object Privileges
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│ Create Functional  │◄───── Business Function
│ Roles              │       Alignment
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│ Create Service     │◄───── Application
│ Roles              │       Integrations
└─────────┬──────────┘
          │
          ▼
Step 3: Build Hierarchy
┌────────────────────┐
│ Grant Access Roles │
│ to Functional Roles│
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│ Grant Functional   │
│ Roles to SYSADMIN  │
└─────────┬──────────┘
          │
          ▼
Step 4: Assign to Users
┌────────────────────┐
│ Assign Functional  │
│ Roles to Users     │
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│ Verify Access and  │
│ Test Permissions   │
└────────────────────┘
```

### 5.2 Tag Synchronization Flow

```
┌─────────────────────────────────────────────────────────────┐
│              Tag Sync & Policy Enforcement Flow              │
└─────────────────────────────────────────────────────────────┘

Data Discovery Phase
┌────────────────────┐
│ New Table Created  │
│ or Schema Modified │
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│ Data Classification│───► Automated: Snowflake Classification
│ Process            │───► Manual: Data Steward Review
└─────────┬──────────┘
          │
          ▼
Tag Assignment Phase
┌────────────────────┐
│ Apply Tags to      │
│ Columns/Tables     │
│ - PII_TYPE         │
│ - DATA_SENSITIVITY │
│ - COMPLIANCE_SCOPE │
└─────────┬──────────┘
          │
          ▼
Policy Enforcement
┌────────────────────┐
│ Tag-Policy Mapping │◄───── Pre-configured
│ Activated          │       Policy Associations
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│ Masking Policy     │───► Applied automatically
│ Auto-Applied       │      on tagged columns
└─────────┬──────────┘
          │
          ▼
Access Enforcement
┌────────────────────┐
│ User Query         │
│ Executed           │
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│ Policy Evaluation  │───► Check: Current Role
│                    │───► Check: Column Tags
│                    │───► Check: Row Policies
└─────────┬──────────┘
          │
          ├─────────► Authorized Role
          │           └─► Return Unmasked Data
          │
          └─────────► Unauthorized Role
                      └─► Return Masked Data
```

### 5.3 Access Request and Approval Flow

```
┌─────────────────────────────────────────────────────────────┐
│                Access Request Workflow                       │
└─────────────────────────────────────────────────────────────┘

User Request
┌────────────────────┐
│ User Submits       │
│ Access Request     │
│ - Data Source      │
│ - Business Just.   │
└─────────┬──────────┘
          │
          ▼
Manager Approval
┌────────────────────┐
│ Manager Reviews    │────No──► Request Denied
│ Business Need      │
└─────────┬──────────┘
          │ Yes
          ▼
Data Owner Approval
┌────────────────────┐
│ Data Owner Reviews │────No──► Request Denied
│ - Data Sensitivity │
│ - Compliance       │
└─────────┬──────────┘
          │ Yes
          ▼
Security Review
┌────────────────────┐
│ Security Team      │────No──► Request Denied
│ Validates Request  │          or Modified
└─────────┬──────────┘
          │ Yes
          ▼
Implementation
┌────────────────────┐
│ SECURITYADMIN      │
│ Grants Role        │
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│ Access Granted     │
│ Notification Sent  │
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│ Audit Log Updated  │
└────────────────────┘
```

---

## 6. Implementation Guidelines

### 6.1 Phase 1: Foundation Setup (Weeks 1-2)

**Objective:** Establish system roles and governance framework

**Tasks:**

1. **Secure ACCOUNTADMIN Role**
   - Identify 2-3 administrators
   - Enable MFA for all administrators
   - Document emergency access procedures

2. **Create Administrative Roles**
   ```sql
   USE ROLE SECURITYADMIN;
   
   -- Create role for tag administration
   CREATE ROLE tag_admin;
   GRANT CREATE TAG ON SCHEMA governance.tags TO ROLE tag_admin;
   GRANT tag_admin TO ROLE sysadmin;
   
   -- Create role for policy administration
   CREATE ROLE policy_admin;
   GRANT CREATE MASKING POLICY ON SCHEMA governance.policies TO ROLE policy_admin;
   GRANT policy_admin TO ROLE sysadmin;
   ```

3. **Establish Governance Database**
   ```sql
   USE ROLE SYSADMIN;
   
   CREATE DATABASE governance;
   CREATE SCHEMA governance.tags;
   CREATE SCHEMA governance.policies;
   CREATE SCHEMA governance.metadata;
   ```

### 6.2 Phase 2: Tag Framework Creation (Weeks 2-3)

**Objective:** Build comprehensive tagging framework

**Tasks:**

1. **Create Core Tags**
   ```sql
   USE ROLE tag_admin;
   USE SCHEMA governance.tags;
   
   -- Sensitivity classification
   CREATE TAG data_sensitivity 
     ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED';
   
   -- PII classification
   CREATE TAG pii_type 
     ALLOWED_VALUES 'EMAIL', 'PHONE', 'SSN', 'ADDRESS', 'CREDIT_CARD';
   
   -- Compliance scope
   CREATE TAG compliance_scope 
     ALLOWED_VALUES 'GDPR', 'HIPAA', 'PCI_DSS', 'SOX', 'CCPA';
   
   -- Data domain
   CREATE TAG data_domain 
     ALLOWED_VALUES 'FINANCE', 'HR', 'SALES', 'MARKETING', 'OPERATIONS';
   ```

2. **Grant Tag Usage Privileges**
   ```sql
   -- Allow data stewards to apply tags
   GRANT APPLY ON TAG governance.tags.data_sensitivity TO ROLE data_steward;
   GRANT APPLY ON TAG governance.tags.pii_type TO ROLE data_steward;
   ```

### 6.3 Phase 3: Masking Policy Development (Weeks 3-4)

**Objective:** Create and test masking policies

**Tasks:**

1. **Develop Masking Policies**
   ```sql
   USE ROLE policy_admin;
   USE SCHEMA governance.policies;
   
   -- Create comprehensive email masking
   CREATE OR REPLACE MASKING POLICY mask_email AS (val STRING) 
   RETURNS STRING ->
     CASE
       WHEN CURRENT_ROLE() IN ('SYSADMIN', 'DATA_STEWARD', 'COMPLIANCE_OFFICER') 
         THEN val
       WHEN CURRENT_ROLE() IN ('CUSTOMER_SUPPORT', 'SALES_TEAM') 
         THEN REGEXP_REPLACE(val, '(.{2}).*(@.*)', '\\1***\\2')
       ELSE '***@***'
     END;
   ```

2. **Associate Policies with Tags**
   ```sql
   -- Link masking policy to PII tag
   ALTER TAG governance.tags.pii_type 
     SET MASKING POLICY governance.policies.mask_email;
   ```

3. **Test Masking Policies**
   - Create test tables with tagged columns
   - Execute queries as different roles
   - Verify appropriate masking behavior
   - Document test results

### 6.4 Phase 4: RBAC Hierarchy Construction (Weeks 4-6)

**Objective:** Build complete role hierarchy

**Tasks:**

1. **Create Access Roles**
   ```sql
   USE ROLE USERADMIN;
   
   -- Finance database access roles
   CREATE ROLE finance_db_read;
   CREATE ROLE finance_db_read_write;
   CREATE ROLE finance_db_admin;
   
   -- HR database access roles
   CREATE ROLE hr_db_read;
   CREATE ROLE hr_db_read_write;
   CREATE ROLE hr_db_admin;
   ```

2. **Grant Object Privileges to Access Roles**
   ```sql
   USE ROLE SECURITYADMIN;
   
   -- Grant read access
   GRANT USAGE ON DATABASE finance_db TO ROLE finance_db_read;
   GRANT USAGE ON ALL SCHEMAS IN DATABASE finance_db TO ROLE finance_db_read;
   GRANT SELECT ON ALL TABLES IN DATABASE finance_db TO ROLE finance_db_read;
   GRANT SELECT ON FUTURE TABLES IN DATABASE finance_db TO ROLE finance_db_read;
   ```

3. **Create Functional Roles**
   ```sql
   USE ROLE USERADMIN;
   
   CREATE ROLE data_analyst;
   CREATE ROLE data_engineer;
   CREATE ROLE business_analyst;
   ```

4. **Build Role Hierarchy**
   ```sql
   USE ROLE SECURITYADMIN;
   
   -- Grant access roles to functional roles
   GRANT ROLE finance_db_read TO ROLE data_analyst;
   GRANT ROLE hr_db_read TO ROLE data_analyst;
   
   -- Grant functional roles to SYSADMIN
   GRANT ROLE data_analyst TO ROLE sysadmin;
   GRANT ROLE data_engineer TO ROLE sysadmin;
   ```

### 6.5 Phase 5: Data Classification and Tagging (Weeks 6-8)

**Objective:** Tag existing data assets

**Tasks:**

1. **Automated Classification**
   - Enable Snowflake's data classification feature
   - Review classification results
   - Validate against compliance requirements

2. **Manual Tagging**
   ```sql
   -- Tag sensitive columns
   ALTER TABLE finance_db.transactions.customer_data 
     MODIFY COLUMN email SET TAG governance.tags.pii_type = 'EMAIL';
   
   ALTER TABLE finance_db.transactions.customer_data 
     MODIFY COLUMN phone_number SET TAG governance.tags.pii_type = 'PHONE';
   ```

3. **Bulk Tagging Operations**
   ```sql
   -- Tag all tables in HR database
   ALTER DATABASE hr_db 
     SET TAG governance.tags.data_domain = 'HR';
   
   ALTER DATABASE hr_db 
     SET TAG governance.tags.compliance_scope = 'GDPR';
   ```

### 6.6 Phase 6: User Assignment and Validation (Weeks 8-10)

**Objective:** Assign roles to users and validate access

**Tasks:**

1. **User-Role Assignment**
   ```sql
   USE ROLE SECURITYADMIN;
   
   -- Assign functional roles to users
   GRANT ROLE data_analyst TO USER john.smith@company.com;
   GRANT ROLE data_engineer TO USER jane.doe@company.com;
   ```

2. **Access Validation Testing**
   - Have users log in and test access
   - Verify masking policies work as expected
   - Confirm least privilege implementation
   - Document any access issues

3. **Create Monitoring Queries**
   ```sql
   -- Query to monitor role grants
   SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
   WHERE granted_to = 'ROLE'
   AND deleted_on IS NULL
   ORDER BY created_on DESC;
   
   -- Query to monitor tag assignments
   SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
   WHERE deleted IS NULL
   ORDER BY tag_created DESC;
   ```

---

## 7. Security Best Practices

### 7.1 Role Management

**Do's:**
- Create specific access roles for each database/schema combination
- Use future grants to automatically secure new objects
- Regularly review and audit role assignments
- Document the purpose of each custom role
- Implement time-limited access for temporary needs

**Don'ts:**
- Don't create objects using ACCOUNTADMIN
- Don't grant ACCOUNTADMIN to service accounts
- Don't use PUBLIC role for sensitive data access
- Don't create circular role grants
- Don't grant ownership of critical objects to user-specific roles

### 7.2 Tag Management

**Do's:**
- Use consistent naming conventions for tags
- Document tag meanings and allowed values
- Centralize tag creation and management
- Version control tag policies
- Regular tag audits to identify untagged sensitive data

**Don'ts:**
- Don't create duplicate tags with different names
- Don't over-tag objects (maximum 50 tags per object)
- Don't modify tag allowed values without impact analysis
- Don't delete tags without checking dependencies

### 7.3 Masking Policy Best Practices

**Do's:**
- Test masking policies in development before production
- Use role-based conditions in masking logic
- Document which roles can see unmasked data
- Version control all masking policies
- Regular review of policy effectiveness

**Don'ts:**
- Don't create overly complex masking logic
- Don't hard-code user names in policies
- Don't apply masking to join keys or foreign keys
- Don't forget to handle NULL values in policies

---

## 8. Compliance and Auditing

### 8.1 Audit Monitoring Queries

**Track Role Changes:**
```sql
SELECT 
  query_text,
  user_name,
  role_name,
  execution_status,
  start_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%GRANT ROLE%'
  OR query_text ILIKE '%CREATE ROLE%'
  OR query_text ILIKE '%DROP ROLE%'
ORDER BY start_time DESC
LIMIT 100;
```

**Monitor Tag Assignments:**
```sql
SELECT 
  object_database,
  object_schema,
  object_name,
  tag_name,
  tag_value,
  tag_created
FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
WHERE tag_name IN ('PII_TYPE', 'DATA_SENSITIVITY')
  AND deleted IS NULL
ORDER BY tag_created DESC;
```

**Track Policy Applications:**
```sql
SELECT 
  policy_name,
  policy_kind,
  policy_owner,
  created,
  last_altered
FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
WHERE policy_kind = 'MASKING_POLICY'
  AND deleted IS NULL;
```

### 8.2 Compliance Reporting

**GDPR Compliance Check:**
```sql
-- Find all columns tagged as PII
SELECT 
  object_database,
  object_schema,
  object_name,
  column_name,
  tag_value AS pii_type
FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
WHERE tag_name = 'PII_TYPE'
  AND deleted IS NULL;
```

**Access Review Report:**
```sql
-- Generate quarterly access review report
SELECT 
  grantee_name AS role_name,
  COUNT(DISTINCT privilege) AS privilege_count,
  LISTAGG(DISTINCT privilege, ', ') AS privileges,
  COUNT(DISTINCT name) AS object_count
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE deleted_on IS NULL
  AND granted_to = 'ROLE'
GROUP BY grantee_name
ORDER BY object_count DESC;
```

### 8.3 Scheduled Compliance Reviews

Establish quarterly reviews to ensure ongoing compliance:

1. **Access Certification** (Quarterly)
   - Review all user-to-role assignments
   - Validate business justification for access
   - Remove access for terminated employees
   - Adjust access for role changes

2. **Tag Validation** (Monthly)
   - Identify untagged sensitive data
   - Verify tag accuracy
   - Update tags for data classification changes

3. **Policy Effectiveness Review** (Quarterly)
   - Test masking policies with sample queries
   - Review role exemptions in policies
   - Update policies based on new requirements

---

## 9. Disaster Recovery and Business Continuity

### 9.1 Role and Grant Backup

Create automated scripts to export role configurations:

```sql
-- Export all role grants
SELECT 
  'GRANT ROLE ' || role_name || ' TO ROLE ' || granted_to_role_name || ';' AS grant_statement
FROM (
  SELECT name AS role_name, granted_to_name AS granted_to_role_name
  FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
  WHERE granted_to = 'ROLE' AND deleted_on IS NULL
);

-- Export all privilege grants
SELECT 
  'GRANT ' || privilege || ' ON ' || granted_on || ' ' || name || ' TO ROLE ' || grantee_name || ';' AS grant_statement
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE deleted_on IS NULL AND granted_to = 'ROLE';
```

### 9.2 Emergency Access Procedures

**Break-Glass Account:**
- Maintain emergency ACCOUNTADMIN account
- Store credentials in secure vault
- Log all emergency access usage
- Require post-incident review

---

## 10. Maintenance and Operations

### 10.1 Regular Maintenance Tasks

**Daily:**
- Monitor failed login attempts
- Review privilege escalation attempts
- Check for new untagged tables

**Weekly:**
- Review role assignment changes
- Validate masking policy effectiveness
- Check for orphaned roles

**Monthly:**
- Access certification review
- Tag coverage assessment
- Policy performance review

**Quarterly:**
- Complete access recertification
- Update role documentation
- Conduct security training
- Review and update policies

### 10.2 Change Management Process

All changes to RBAC or tagging structure must follow this process:

1. **Request Submission**
   - Document change requirement
   - Include business justification
   - Identify affected users/objects

2. **Impact Analysis**
   - Review affected roles and users
   - Assess security implications
   - Identify testing requirements

3. **Approval**
   - Security team review
   - Data owner approval
   - Change advisory board sign-off

4. **Implementation**
   - Execute in development environment
   - Validate functionality
   - Deploy to production
   - Document changes

5. **Validation**
   - Test access patterns
   - Verify masking behavior
   - User acceptance testing

---

## 11. Troubleshooting Guide

### 11.1 Common Issues

**Issue: User Cannot Access Expected Data**

Investigation Steps:
```sql
-- Check user's roles
SHOW GRANTS TO USER username@company.com;

-- Check role hierarchy
SHOW GRANTS TO ROLE role_name;

-- Verify object privileges
SHOW GRANTS ON TABLE database.schema.table_name;
```

**Issue: Masking Policy Not Applied**

Investigation Steps:
```sql
-- Check if column is tagged
SELECT * FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES('table_name', 'TABLE'));

-- Verify policy-tag association
SHOW MASKING POLICIES;

-- Check policy definition
DESC MASKING POLICY policy_name;
```

**Issue: Role Hierarchy Not Working**

Investigation Steps:
```sql
-- Visualize role hierarchy
SELECT 
  granted_to_name AS parent_role,
  name AS child_role,
  granted_on
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE granted_to = 'ROLE'
  AND deleted_on IS NULL;
```

### 11.2 Support Escalation

**Level 1: User Support**
- Password resets
- Basic access questions
- Application errors

**Level 2: Security Team**
- Role assignment changes
- Tag application requests
- Policy exceptions

**Level 3: Snowflake Support**
- Platform issues
- Performance problems
- Bug reports

---

## 12. Success Metrics

Track these KPIs to measure the success of the implementation:

| Metric | Target | Measurement Frequency |
|--------|--------|----------------------|
| Time to provision new user | < 2 hours | Weekly |
| % of sensitive data tagged | > 95% | Monthly |
| Access review completion rate | 100% | Quarterly |
| Average masking policy coverage | > 90% | Monthly |
| Number of privilege violations | 0 | Weekly |
| Policy exception requests | < 5/month | Monthly |

---

## 13. Conclusion

This RBAC and Tagging Strategy provides a comprehensive framework for securing your Snowflake environment while maintaining operational efficiency. The combination of role hierarchies, tag-based access control, and automated policy enforcement creates a scalable governance model that grows with your organization.

**Key Takeaways:**

1. Role hierarchies simplify management and ensure consistent access patterns
2. Tag-based access control provides dynamic, scalable data governance
3. Masking policies protect sensitive data while enabling analytics
4. Regular audits and reviews ensure ongoing compliance
5. Automation reduces manual overhead and human error

**Next Steps:**

1. Review and approve this architecture
2. Schedule implementation kickoff meeting
3. Assign roles and responsibilities
4. Begin Phase 1 implementation

---

## Appendix A: SQL Scripts Repository

Complete implementation scripts are maintained in the following repository structure:

```
/snowflake_rbac_tagging/
├── /01_foundation/
│   ├── create_governance_database.sql
│   └── setup_admin_roles.sql
├── /02_tags/
│   ├── create_core_tags.sql
│   └── grant_tag_privileges.sql
├── /03_policies/
│   ├── create_masking_policies.sql
│   └── associate_policies_with_tags.sql
├── /04_roles/
│   ├── create_access_roles.sql
│   ├── create_functional_roles.sql
│   └── build_role_hierarchy.sql
├── /05_grants/
│   ├── grant_database_privileges.sql
│   └── grant_future_privileges.sql
└── /06_monitoring/
    ├── audit_queries.sql
    └── compliance_reports.sql
```

---

## Appendix B: Contact Information

**Project Owner:** Venkannababu Thatavarthi  
**Email:** venkannababu.t@company.com  
**Security Team:** security@company.com  
**Snowflake Support:** support.snowflake.com

---

**Document Version History:**

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | October 13, 2025 | Venkannababu Thatavarthi | Initial document creation |

---

*This document is confidential and intended for internal use only.*