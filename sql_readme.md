# Snowflake RBAC & Tagging Implementation - SQL Scripts Repository

**Author:** Venkannababu Thatavarthi  
**Date:** October 13, 2025  
**Version:** 1.0

---

## Overview

This repository contains production-ready SQL scripts for implementing a comprehensive Role-Based Access Control (RBAC) and Tagging strategy in Snowflake. The scripts are organized by implementation phase and designed to be executed sequentially.

---

## Repository Structure

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
│   └── grant_database_privileges.sql
└── /06_monitoring/
    └── audit_queries.sql
```

---

## Implementation Timeline

| Phase | Duration | Scripts | Prerequisites |
|-------|----------|---------|---------------|
| Phase 1: Foundation | Week 1-2 | 01_foundation/* | ACCOUNTADMIN access |
| Phase 2: Tags | Week 2-3 | 02_tags/* | Phase 1 complete |
| Phase 3: Policies | Week 3-4 | 03_policies/* | Phase 2 complete |
| Phase 4: RBAC | Week 4-6 | 04_roles/*, 05_grants/* | Phase 3 complete |
| Phase 5: Monitoring | Ongoing | 06_monitoring/* | All phases |

**Total Implementation Time:** 8-10 weeks

---

## Script Descriptions

### Phase 1: Foundation Setup

#### `01_foundation/create_governance_database.sql`
- **Purpose:** Create centralized governance database and schemas
- **Creates:**
  - `governance` database
  - Schemas: `tags`, `policies`, `metadata`, `documentation`
  - Metadata tables for logging and tracking
  - Helper stored procedures
- **Role Required:** SYSADMIN
- **Execution Time:** ~2 minutes

#### `01_foundation/setup_admin_roles.sql`
- **Purpose:** Create administrative roles for governance operations
- **Creates:**
  - TAG_ADMIN, POLICY_ADMIN, DATA_STEWARD roles
  - GOVERNANCE_ADMIN, COMPLIANCE_OFFICER roles
  - Grants appropriate privileges
- **Role Required:** USERADMIN, SECURITYADMIN
- **Execution Time:** ~3 minutes

---

### Phase 2: Tag Framework

#### `02_tags/create_core_tags.sql`
- **Purpose:** Create comprehensive tag framework
- **Creates:** 26 tags across categories:
  - Data sensitivity classification
  - PII classification (11 specific types)
  - Compliance and regulatory tags
  - Data domain and ownership tags
  - Data quality and lifecycle tags
  - Special purpose tags
- **Role Required:** TAG_ADMIN
- **Execution Time:** ~5 minutes

#### `02_tags/grant_tag_privileges.sql`
- **Purpose:** Grant tag application privileges and create helper functions
- **Creates:**
  - Privilege grants to DATA_STEWARD
  - Helper procedures for tagging operations
  - Tag coverage monitoring views
  - Tag suggestion function
- **Role Required:** TAG_ADMIN, SECURITYADMIN
- **Execution Time:** ~3 minutes

---

### Phase 3: Masking Policies

#### `03_policies/create_masking_policies.sql`
- **Purpose:** Create comprehensive masking policies
- **Creates:** 21 masking policies:
  - Email, phone, SSN masking
  - Financial data masking
  - Healthcare data masking (HIPAA)
  - Generic PII masking
  - Conditional and NULL-safe policies
- **Role Required:** POLICY_ADMIN
- **Execution Time:** ~5 minutes

#### `03_policies/associate_policies_with_tags.sql`
- **Purpose:** Associate masking policies with specific tags
- **Creates:**
  - 11 specific PII tags (pii_email, pii_phone, etc.)
  - Tag-to-policy associations
  - Helper procedures for policy management
  - Tag suggestion function
- **Role Required:** POLICY_ADMIN, TAG_ADMIN
- **Execution Time:** ~3 minutes
- **Important Note:** This script implements specific tags per PII type (recommended approach)

---

### Phase 4: RBAC Implementation

#### `04_roles/create_access_roles.sql`
- **Purpose:** Create granular access roles for database objects
- **Creates:** 30+ access roles:
  - Database-level roles (READ, READ_WRITE, ADMIN)
  - Schema-specific roles
  - Warehouse access roles
  - Cross-database access roles
  - Special purpose roles
- **Role Required:** USERADMIN
- **Execution Time:** ~5 minutes

#### `04_roles/create_functional_roles.sql`
- **Purpose:** Create functional roles aligned with business functions
- **Creates:** 39 functional/service roles:
  - Data & Analytics roles (5)
  - Data Engineering roles (4)
  - Business domain roles (17)
  - Executive roles (4)
  - Service/application roles (9)
- **Role Required:** USERADMIN
- **Execution Time:** ~5 minutes

#### `04_roles/build_role_hierarchy.sql`
- **Purpose:** Build complete role hierarchy
- **Creates:**
  - Grants access roles to functional roles
  - Grants functional roles to SYSADMIN
  - Role hierarchy visualization views
  - Helper procedures
- **Role Required:** SECURITYADMIN
- **Execution Time:** ~10 minutes

---

### Phase 5: Privilege Grants

#### `05_grants/grant_database_privileges.sql`
- **Purpose:** Grant object privileges to access roles
- **Grants:**
  - Database, schema, table, view privileges
  - Warehouse usage privileges
  - Future grants for automatic privilege inheritance
  - Cross-database access
- **Role Required:** SECURITYADMIN
- **Execution Time:** ~10 minutes
- **Customization Required:** Update database names to match your environment

---

### Phase 6: Monitoring & Audit

#### `06_monitoring/audit_queries.sql`
- **Purpose:** Comprehensive monitoring and audit queries
- **Includes:** 29 queries across 10 categories:
  1. Role assignment monitoring
  2. Privilege monitoring
  3. Tag coverage monitoring
  4. Masking policy monitoring
  5. Access pattern analysis
  6. Compliance reporting
  7. Security alerts
  8. Performance metrics
  9. Summary dashboards
  10. Automated reporting
- **Role Required:** GOVERNANCE_ADMIN, COMPLIANCE_OFFICER
- **Execution Time:** Varies by query

---

## Prerequisites

### Required Roles
- **ACCOUNTADMIN:** Initial setup and oversight
- **SYSADMIN:** Database and object creation
- **SECURITYADMIN:** Grant management
- **USERADMIN:** Role and user creation

### System Requirements
- Snowflake Enterprise Edition (for tag-based masking)
- Access to `SNOWFLAKE.ACCOUNT_USAGE` schema
- At least 2 ACCOUNTADMIN users with MFA enabled

### Preparation Checklist
- [ ] Document existing databases and schemas
- [ ] Identify business functions and user groups
- [ ] Define data classification requirements
- [ ] Obtain compliance requirements (GDPR, HIPAA, etc.)
- [ ] Identify sensitive data and PII columns
- [ ] Review and approve role naming conventions
- [ ] Set up backup and rollback procedures

---

## Execution Instructions

### Sequential Execution

Execute scripts in the following order:

```sql
-- Phase 1: Foundation
USE ROLE SYSADMIN;
@01_foundation/create_governance_database.sql

USE ROLE USERADMIN;
@01_foundation/setup_admin_roles.sql

-- Phase 2: Tags
USE ROLE TAG_ADMIN;
@02_tags/create_core_tags.sql
@02_tags/grant_tag_privileges.sql

-- Phase 3: Policies
USE ROLE POLICY_ADMIN;
@03_policies/create_masking_policies.sql
@03_policies/associate_policies_with_tags.sql

-- Phase 4: RBAC
USE ROLE USERADMIN;
@04_roles/create_access_roles.sql
@04_roles/create_functional_roles.sql

USE ROLE SECURITYADMIN;
@04_roles/build_role_hierarchy.sql

-- Phase 5: Grants
USE ROLE SECURITYADMIN;
@05_grants/grant_database_privileges.sql

-- Phase 6: Monitoring (run queries as needed)
USE ROLE GOVERNANCE_ADMIN;
@06_monitoring/audit_queries.sql
```

### Validation After Each Phase

After each phase, run these validation queries:

```sql
-- Verify objects created
SHOW DATABASES LIKE 'governance';
SHOW SCHEMAS IN DATABASE governance;
SHOW TAGS IN SCHEMA governance.tags;
SHOW MASKING POLICIES IN SCHEMA governance.policies;
SHOW ROLES LIKE '%admin%';

-- Check grants
SHOW GRANTS TO ROLE <role_name>;
SELECT * FROM governance.metadata.role_documentation;
```

---

## Customization Guide

### Before Execution

1. **Update Database Names** in `05_grants/grant_database_privileges.sql`:
   - Replace example database names (customer_db, finance_db, etc.) with your actual databases
   - Update warehouse names to match your environment

2. **Update Functional Roles** in `04_roles/create_functional_roles.sql`:
   - Add/remove roles based on your organizational structure
   - Update role descriptions and ownership

3. **Customize Tags** in `02_tags/create_core_tags.sql`:
   - Add company-specific tags
   - Modify allowed values to match your requirements

4. **Adjust Masking Policies** in `03_policies/create_masking_policies.sql`:
   - Modify role-based conditions in masking logic
   - Add company-specific masking rules

### Configuration Variables

Key variables to customize across scripts:

```sql
-- Databases (update in 05_grants/grant_database_privileges.sql)
CUSTOMER_DB → your_customer_database
FINANCE_DB → your_finance_database
HR_DB → your_hr_database

-- Warehouses (update in 05_grants/grant_database_privileges.sql)
COMPUTE_WH_SMALL → your_small_warehouse
COMPUTE_WH_MEDIUM → your_medium_warehouse

-- Email domains (for notifications)
governance-team@company.com → your_team_email
security-team@company.com → your_security_email
```

---

## Common Issues and Troubleshooting

### Issue 1: Insufficient Privileges
**Error:** "Insufficient privileges to operate on..."
**Solution:** Ensure you're using the correct role. Check prerequisites for each script.

### Issue 2: Object Already Exists
**Error:** "Object already exists..."
**Solution:** Most scripts use `IF NOT EXISTS` or `OR REPLACE`. If error persists, drop the object first or use `CREATE OR REPLACE`.

### Issue 3: Tag Not Found
**Error:** "Tag not found..."
**Solution:** Verify tags were created successfully in Phase 2. Check tag names match exactly (case-sensitive).

### Issue 4: Masking Policy Not Applied
**Issue:** Policy created but not masking data
**Solution:** 
- Verify policy is associated with tag: `SHOW MASKING POLICIES`
- Check column is tagged: `SELECT * FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES('table_name', 'TABLE'))`
- Test with different role: `USE ROLE <role_name>; SELECT * FROM table;`

### Issue 5: Role Hierarchy Not Working
**Issue:** User can't access data despite having role
**Solution:**
- Verify role hierarchy: `SELECT * FROM governance.metadata.v_role_hierarchy_simple`
- Check user has role: `SHOW GRANTS TO USER <user_name>`
- Ensure user is using correct role: `SELECT CURRENT_ROLE()`

---

## Testing Procedures

### Phase 1 Testing
```sql
-- Verify governance database
USE DATABASE governance;
SHOW SCHEMAS;

-- Test metadata procedures
CALL governance.metadata.sp_log_role_assignment(
    'test_role', 'USER', 'test_user', 'Testing', NULL
);

SELECT * FROM governance.metadata.role_assignment_log;
```

### Phase 2 Testing
```sql
-- Verify tags created
SHOW TAGS IN SCHEMA governance.tags;

-- Test tag application
CREATE OR REPLACE TABLE test_db.public.test_table (
    email VARCHAR,
    name VARCHAR
);

ALTER TABLE test_db.public.test_table
    MODIFY COLUMN email SET TAG governance.tags.pii_email = 'true';

-- Verify tag assignment
SELECT * FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES('test_table', 'TABLE'));
```

### Phase 3 Testing
```sql
-- Create test data
CREATE OR REPLACE TABLE test_db.public.masked_test (
    email VARCHAR,
    phone VARCHAR,
    ssn VARCHAR
);

INSERT INTO test_db.public.masked_test VALUES
    ('john.doe@example.com', '555-123-4567', '123-45-6789'),
    ('jane.smith@example.com', '555-987-6543', '987-65-4321');

-- Apply tags
ALTER TABLE test_db.public.masked_test
    MODIFY COLUMN email SET TAG governance.tags.pii_email = 'true',
    MODIFY COLUMN phone SET TAG governance.tags.pii_phone = 'true',
    MODIFY COLUMN ssn SET TAG governance.tags.pii_ssn = 'true';

-- Test masking with different roles
USE ROLE data_analyst;
SELECT * FROM test_db.public.masked_test; -- Should show masked data

USE ROLE sysadmin;
SELECT * FROM test_db.public.masked_test; -- Should show unmasked data
```

### Phase 4 Testing
```sql
-- Test role hierarchy
CALL governance.metadata.sp_show_role_hierarchy('data_analyst');

-- Test user assignment
CALL governance.metadata.sp_assign_functional_role_to_user(
    'test.user@company.com',
    'data_analyst',
    'Testing role assignment',
    NULL
);

-- Verify privileges
USE ROLE data_analyst;
SHOW GRANTS TO ROLE data_analyst;
SELECT CURRENT_ROLE();
```

---

## Rollback Procedures

### Emergency Rollback

If issues occur, rollback in reverse order:

```sql
-- 1. Remove role grants
USE ROLE SECURITYADMIN;
REVOKE ROLE <functional_role> FROM ROLE sysadmin;
REVOKE ROLE <access_role> FROM ROLE <functional_role>;

-- 2. Remove policy associations
USE ROLE POLICY_ADMIN;
ALTER TAG governance.tags.pii_email UNSET MASKING POLICY;

-- 3. Drop policies
DROP MASKING POLICY IF EXISTS governance.policies.mask_email;

-- 4. Drop tags
USE ROLE TAG_ADMIN;
DROP TAG IF EXISTS governance.tags.pii_email;

-- 5. Drop roles
USE ROLE USERADMIN;
DROP ROLE IF EXISTS <role_name>;

-- 6. Drop governance database (CAUTION!)
USE ROLE SYSADMIN;
DROP DATABASE IF EXISTS governance;
```

### Partial Rollback

To rollback specific components:

```sql
-- Rollback a single role
DROP ROLE IF EXISTS role_name;

-- Rollback a tag
ALTER TABLE table_name MODIFY COLUMN column_name UNSET TAG tag_name;
DROP TAG IF EXISTS tag_name;

-- Rollback a policy
ALTER TAG tag_name UNSET MASKING POLICY;
DROP MASKING POLICY IF EXISTS policy_name;
```

---

## Maintenance and Updates

### Daily Tasks
- Review security alerts (Query 24-26)
- Monitor ACCOUNTADMIN usage (Query 25)
- Check failed access attempts (Query 19)

### Weekly Tasks
- Review role assignments (Query 1)
- Check tag coverage (Query 11)
- Monitor privilege changes (Query 10)

### Monthly Tasks
- Access certification review (Query 2)
- Update role documentation
- Review and update masking policies
- Compliance reporting (Queries 21-23)

### Quarterly Tasks
- Complete access recertification
- Review and update role hierarchy
- Audit tag accuracy
- Update security procedures
- Conduct governance training

---

## Support and Documentation

### Internal Contacts
- **Data Governance Team:** governance-team@company.com
- **Security Team:** security-team@company.com
- **Data Platform Team:** data-platform-team@company.com

### Snowflake Resources
- [Snowflake Documentation](https://docs.snowflake.com)
- [RBAC Overview](https://docs.snowflake.com/en/user-guide/security-access-control-overview)
- [Object Tagging](https://docs.snowflake.com/en/user-guide/object-tagging)
- [Masking Policies](https://docs.snowflake.com/en/user-guide/security-column-ddm-intro)

### Change Management
All changes to RBAC or tagging must follow the change management process:
1. Submit change request
2. Impact analysis
3. Security review
4. Implementation in DEV
5. Testing and validation
6. Approval
7. Production deployment
8. Post-implementation review

---

## Appendix: Quick Reference

### Common Commands

```sql
-- View current role
SELECT CURRENT_ROLE();

-- Switch role
USE ROLE role_name;

-- Show all roles
SHOW ROLES;

-- Show grants to role
SHOW GRANTS TO ROLE role_name;

-- Show grants to user
SHOW GRANTS TO USER user_name;

-- Show tags on table
SELECT * FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES('table_name', 'TABLE'));

-- Show masking policies
SHOW MASKING POLICIES IN SCHEMA governance.policies;

-- Test masking
USE ROLE role_name;
SELECT * FROM database.schema.table;
```

### Useful Views

```sql
-- Role documentation
SELECT * FROM governance.metadata.v_functional_role_catalog;

-- Active role assignments
SELECT * FROM governance.metadata.v_active_role_assignments;

-- Tag coverage
SELECT * FROM governance.metadata.v_tag_coverage_by_database;

-- Untagged tables
SELECT * FROM governance.metadata.v_untagged_tables LIMIT 100;

-- PII inventory
SELECT * FROM governance.metadata.v_pii_columns;

-- Role hierarchy
SELECT * FROM governance.metadata.v_role_hierarchy_simple;
```

---

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | October 13, 2025 | Venkannababu Thatavarthi | Initial release |

---

## License and Usage

This implementation is provided as-is for internal use. Customize as needed for your organization's requirements.

**Confidential:** This documentation is for internal use only.

---

**END OF README**