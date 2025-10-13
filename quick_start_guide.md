# Snowflake RBAC & Tagging Strategy
## Quick-Start Implementation Guide

**Prepared for Client Presentation**  
**Author:** Venkannababu Thatavarthi  
**Date:** October 13, 2025

---

## Welcome to Your Implementation Journey

This guide will walk you through implementing a production-grade RBAC and Tagging framework in Snowflake. I've designed this to be straightforward - you can complete the basic setup in about 4 hours and have a working governance framework in place.

The full implementation typically takes 8-10 weeks, but you'll see value from day one. Let's get started.

---

## Before You Begin - Critical Checklist

Take 15 minutes to ensure you have these prerequisites in place:

**Access Requirements:**
- [ ] You have ACCOUNTADMIN access (or someone on your team does)
- [ ] At least one other person has ACCOUNTADMIN for backup
- [ ] Multi-factor authentication (MFA) is enabled for all admin users
- [ ] You have SYSADMIN and SECURITYADMIN access

**Environment Setup:**
- [ ] You're working in a non-production environment first (highly recommended)
- [ ] You have a SQL client connected (SnowSQL, Snowsight, or DBeaver)
- [ ] You can create databases and schemas
- [ ] Someone on your team understands your current database structure

**Knowledge Check:**
- [ ] You know which databases contain sensitive data
- [ ] You have a rough idea of who needs access to what
- [ ] You understand basic compliance requirements (GDPR, HIPAA, etc.)

**Time Allocation:**
- Day 1 (4 hours): Foundation setup
- Week 1 (2 days): Tag framework and policies
- Week 2-3 (3 days): Role creation and hierarchy
- Week 4-8 (ongoing): Tagging existing data and user assignment

---

## Phase 1: Foundation Setup (2-3 Hours)

This is where we build the infrastructure. Think of this as laying the foundation of a house - everything else depends on it.

### Step 1: Create the Governance Database (30 minutes)

Open your SQL client and connect to Snowflake. Switch to SYSADMIN:

```sql
USE ROLE SYSADMIN;
```

Now run the `create_governance_database.sql` script. This creates:
- A central governance database
- Four schemas (tags, policies, metadata, documentation)
- Metadata tables to track everything
- Helper procedures for common operations

**What to watch for:**
- The script should complete in about 2 minutes
- You should see "Governance database setup completed successfully!"
- If you get permission errors, make sure you're using SYSADMIN

**Verify it worked:**
```sql
SHOW DATABASES LIKE 'governance';
SHOW SCHEMAS IN DATABASE governance;
```

You should see 4 schemas listed.

### Step 2: Set Up Administrative Roles (30 minutes)

Switch to USERADMIN (for creating roles) and then SECURITYADMIN (for granting privileges):

```sql
USE ROLE USERADMIN;
```

Run the `setup_admin_roles.sql` script. This creates:
- TAG_ADMIN: For creating and managing tags
- POLICY_ADMIN: For creating masking policies
- DATA_STEWARD: For applying tags to data
- GOVERNANCE_ADMIN: Overall governance oversight
- COMPLIANCE_OFFICER: For audit and compliance

**What to watch for:**
- The script creates 6 administrative roles
- Privileges are automatically granted
- Role hierarchy is set up

**Verify it worked:**
```sql
SHOW ROLES LIKE '%admin%';
SHOW GRANTS TO ROLE governance_admin;
```

### Step 3: Assign Administrative Access (15 minutes)

This is important - you need to assign these new roles to real people on your team.

**Who should get what:**

```sql
USE ROLE SECURITYADMIN;

-- Give yourself GOVERNANCE_ADMIN (full governance control)
GRANT ROLE governance_admin TO USER your.email@company.com;

-- Assign data stewards (people who will tag data)
GRANT ROLE data_steward TO USER data.steward@company.com;

-- Assign compliance officer (for auditing)
GRANT ROLE compliance_officer TO USER compliance@company.com;
```

**Pro tip:** Start with just 2-3 people in these roles. You can always add more later.

**Verify it worked:**
```sql
-- Switch to your new role and test it
USE ROLE governance_admin;
SELECT CURRENT_ROLE(); -- Should show GOVERNANCE_ADMIN

-- Check you can access governance database
USE DATABASE governance;
SHOW SCHEMAS;
```

---

## Phase 2: Tag Framework (1-2 Hours)

Now we're going to create the tags that will classify your data. I've included 26 pre-built tags covering most common scenarios.

### Step 4: Create Core Tags (20 minutes)

```sql
USE ROLE tag_admin;
```

Run the `create_core_tags.sql` script. This creates tags for:
- **PII classification** (11 specific types: email, phone, SSN, etc.)
- **Data sensitivity** (PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED)
- **Compliance** (GDPR, HIPAA, PCI_DSS, SOX, etc.)
- **Data domains** (FINANCE, HR, SALES, etc.)
- **Data quality tiers** (GOLD, SILVER, BRONZE, RAW)

**What to watch for:**
- 26 tags should be created
- All tags are in the `governance.tags` schema
- You'll see a success message at the end

**Verify it worked:**
```sql
SHOW TAGS IN SCHEMA governance.tags;
-- Should show 26 tags

-- Look at a specific tag definition
DESC TAG governance.tags.data_sensitivity;
```

### Step 5: Grant Tag Privileges (15 minutes)

Run the `grant_tag_privileges.sql` script. This:
- Allows DATA_STEWARD role to apply tags
- Creates helper procedures for common tagging operations
- Sets up monitoring views

**What to watch for:**
- Privileges granted to DATA_STEWARD
- Helper procedures created successfully

**Test it immediately:**
```sql
USE ROLE data_steward;

-- Try creating a test table and tagging it
CREATE OR REPLACE TABLE governance.metadata.tag_test (
    test_column VARCHAR
);

-- Apply a tag
ALTER TABLE governance.metadata.tag_test 
    SET TAG governance.tags.data_sensitivity = 'INTERNAL';

-- Verify the tag was applied
SELECT * FROM TABLE(
    governance.metadata.information_schema.tag_references_all_columns(
        'governance.metadata.tag_test', 
        'table'
    )
);

-- Clean up
DROP TABLE governance.metadata.tag_test;
```

If this works, you're good to go!

---

## Phase 3: Masking Policies (1 Hour)

This is where the magic happens - we're going to create policies that automatically protect sensitive data.

### Step 6: Create Masking Policies (30 minutes)

```sql
USE ROLE policy_admin;
```

Run the `create_masking_policies.sql` script. This creates 21 policies including:
- Email masking (shows partial email based on role)
- Phone number masking
- SSN masking
- Financial data masking
- Healthcare data masking

**What to watch for:**
- 21 policies should be created
- All policies are in `governance.policies` schema
- No syntax errors

**Verify it worked:**
```sql
SHOW MASKING POLICIES IN SCHEMA governance.policies;

-- Look at a specific policy
DESC MASKING POLICY governance.policies.mask_email;
```

### Step 7: Link Policies to Tags (30 minutes)

This is the clever part - we're linking policies to tags so they apply automatically.

Run the `associate_policies_with_tags.sql` script.

**Important note:** This script creates specific PII tags (pii_email, pii_phone, etc.) instead of using tag values. This is a Snowflake limitation workaround, and it actually makes things simpler.

**What to watch for:**
- 11 new specific PII tags created
- Each tag automatically linked to its masking policy
- Mapping table populated

**Test the masking immediately:**
```sql
-- Create a test table in your database
CREATE OR REPLACE DATABASE test_masking_db;
CREATE SCHEMA test_schema;

CREATE TABLE test_schema.customer_test (
    customer_id INT,
    email VARCHAR,
    phone VARCHAR
);

-- Insert test data
INSERT INTO test_schema.customer_test VALUES
    (1, 'john.doe@example.com', '555-123-4567'),
    (2, 'jane.smith@example.com', '555-987-6543');

-- Apply tags
ALTER TABLE test_schema.customer_test
    MODIFY COLUMN email SET TAG governance.tags.pii_email = 'true',
    MODIFY COLUMN phone SET TAG governance.tags.pii_phone = 'true';

-- Test masking with different roles
USE ROLE sysadmin;
SELECT * FROM test_schema.customer_test;
-- You should see full data

USE ROLE governance_admin;
SELECT * FROM test_schema.customer_test;
-- You should see full data

-- Create a test analyst role if you don't have one
USE ROLE useradmin;
CREATE ROLE IF NOT EXISTS test_analyst;

USE ROLE securityadmin;
GRANT USAGE ON DATABASE test_masking_db TO ROLE test_analyst;
GRANT USAGE ON SCHEMA test_schema TO ROLE test_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA test_schema TO ROLE test_analyst;
GRANT ROLE test_analyst TO USER your.email@company.com;

-- Now test as analyst
USE ROLE test_analyst;
SELECT * FROM test_schema.customer_test;
-- Email and phone should be masked!
```

**If you see masked data for test_analyst but full data for sysadmin, congratulations! Your masking is working perfectly.**

---

## Phase 4: Role Creation (2-3 Hours)

Now we're setting up the access control layer. We'll create roles that users will actually use.

### Step 8: Create Access Roles (45 minutes)

Access roles control what databases and objects users can access.

```sql
USE ROLE useradmin;
```

Run the `create_access_roles.sql` script.

**Before you run this, customize it:**
- Replace example database names (customer_db, finance_db) with YOUR actual databases
- Replace warehouse names with YOUR actual warehouses
- Add or remove roles based on your needs

**What gets created:**
- Database-level roles (DB_READ, DB_READ_WRITE, DB_ADMIN) for each database
- Warehouse access roles
- Cross-database access roles

**Verify it worked:**
```sql
SHOW ROLES LIKE '%_db_%';
-- Should show your database access roles
```

### Step 9: Grant Database Privileges (45 minutes)

Now we connect those roles to actual database privileges.

```sql
USE ROLE securityadmin;
```

Run the `grant_database_privileges.sql` script.

**Critical: You MUST customize this script with your actual database and warehouse names!**

**What this does:**
- Grants SELECT privileges to READ roles
- Grants INSERT, UPDATE, DELETE to READ_WRITE roles
- Grants full control to ADMIN roles
- Sets up FUTURE GRANTS (so new tables automatically inherit permissions)

**Verify it worked:**
```sql
-- Check what privileges a role has
SHOW GRANTS TO ROLE customer_db_read; -- Replace with your role name

-- Should see USAGE on database, schemas, and SELECT on tables
```

### Step 10: Create Functional Roles (30 minutes)

Functional roles represent job functions in your organization.

```sql
USE ROLE useradmin;
```

Run the `create_functional_roles.sql` script.

**Customize this based on your org:**
- Keep roles that match your teams (data_analyst, data_engineer, etc.)
- Remove roles you don't need
- Add company-specific roles

**What gets created:**
- Data & Analytics roles
- Data Engineering roles
- Business team roles (Finance, HR, Sales, Marketing)
- Executive roles
- Service roles (for tools like Tableau, dbt, etc.)

### Step 11: Build Role Hierarchy (45 minutes)

This is where everything connects together.

```sql
USE ROLE securityadmin;
```

Run the `build_role_hierarchy.sql` script.

**What this does:**
- Links access roles to functional roles
- Links functional roles to SYSADMIN
- Creates the complete privilege inheritance tree

**Visualize your hierarchy:**
```sql
USE ROLE governance_admin;

-- See the complete hierarchy
SELECT * FROM governance.metadata.v_role_hierarchy_simple
ORDER BY parent_role, child_role;

-- See hierarchy for a specific role
CALL governance.metadata.sp_show_role_hierarchy('data_analyst');
```

---

## Phase 5: Start Using the System (Ongoing)

You now have a complete governance framework! Here's how to start using it.

### Assign Roles to Users

```sql
USE ROLE securityadmin;

-- Assign a functional role to a user
GRANT ROLE data_analyst TO USER john.doe@company.com;

-- Or use the logging procedure
CALL governance.metadata.sp_assign_functional_role_to_user(
    'john.doe@company.com',
    'data_analyst',
    'New hire - Analytics team',
    NULL  -- No expiration date
);
```

### Tag Your Sensitive Data

Start tagging your most sensitive tables first:

```sql
USE ROLE data_steward;

-- Tag a table with sensitivity
ALTER TABLE your_db.your_schema.customers
    SET TAG governance.tags.data_sensitivity = 'CONFIDENTIAL',
        governance.tags.data_domain = 'CUSTOMER',
        governance.tags.compliance_scope = 'GDPR';

-- Tag specific PII columns
ALTER TABLE your_db.your_schema.customers
    MODIFY COLUMN email SET TAG governance.tags.pii_email = 'true',
    MODIFY COLUMN phone SET TAG governance.tags.pii_phone = 'true',
    MODIFY COLUMN address SET TAG governance.tags.pii_address = 'true';

-- Or use the helper procedure
CALL governance.metadata.sp_tag_pii_column(
    'your_db',
    'your_schema',
    'customers',
    'email',
    'EMAIL',
    'CONFIDENTIAL',
    'Customer contact information'
);
```

### Find Untagged Data

```sql
USE ROLE governance_admin;

-- Find tables that need tagging
SELECT * FROM governance.metadata.v_untagged_tables
ORDER BY row_count DESC
LIMIT 50;

-- Find potential PII columns without masking
SELECT 
    table_catalog,
    table_schema,
    table_name,
    column_name,
    data_type
FROM snowflake.account_usage.columns
WHERE deleted IS NULL
  AND (
      column_name ILIKE '%email%'
      OR column_name ILIKE '%phone%'
      OR column_name ILIKE '%ssn%'
  )
  AND table_catalog != 'SNOWFLAKE'
ORDER BY table_catalog, table_schema, table_name;
```

---

## Daily Operations - What You'll Actually Do

### For Data Stewards (15 minutes daily)

```sql
USE ROLE data_steward;

-- Check for new tables
SELECT * FROM governance.metadata.v_untagged_tables
WHERE table_created > DATEADD(day, -1, CURRENT_DATE());

-- Tag new tables as you find them
ALTER TABLE new_table 
    SET TAG governance.tags.data_sensitivity = 'INTERNAL';
```

### For Security Team (30 minutes daily)

```sql
USE ROLE compliance_officer;

-- Check for new role assignments
SELECT * 
FROM snowflake.account_usage.grants_to_users
WHERE granted_on > DATEADD(day, -1, CURRENT_TIMESTAMP())
ORDER BY granted_on DESC;

-- Check for failed access attempts
SELECT 
    user_name,
    query_text,
    error_message,
    start_time
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
  AND error_code IN ('1063', '3001', '3003')
ORDER BY start_time DESC;
```

### For Governance Admin (Weekly review)

```sql
USE ROLE governance_admin;

-- Check tag coverage
SELECT 
    object_database,
    COUNT(DISTINCT object_name) as tagged_tables
FROM snowflake.account_usage.tag_references
WHERE tag_database = 'GOVERNANCE'
  AND deleted IS NULL
GROUP BY object_database;

-- Check masking policy coverage
SELECT 
    policy_name,
    COUNT(DISTINCT ref_entity_name) as protected_objects
FROM snowflake.account_usage.policy_references
WHERE deleted IS NULL
GROUP BY policy_name;
```

---

## Common Issues and Quick Fixes

### Issue: "Insufficient privileges" error

**Fix:**
```sql
-- Make sure you're using the right role
SELECT CURRENT_ROLE();

-- Switch to appropriate role
USE ROLE sysadmin;  -- or securityadmin, depending on operation
```

### Issue: Masking not working

**Fix:**
```sql
-- Check if tag is applied
SELECT * FROM TABLE(
    information_schema.tag_references('your_table', 'table')
);

-- Check if policy is associated with tag
SHOW MASKING POLICIES IN SCHEMA governance.policies;

-- Verify you're using a role that should see masked data
SELECT CURRENT_ROLE();
```

### Issue: User can't access database

**Fix:**
```sql
USE ROLE securityadmin;

-- Check what roles user has
SHOW GRANTS TO USER username@company.com;

-- Check what privileges role has
SHOW GRANTS TO ROLE role_name;

-- Grant missing privileges
GRANT USAGE ON DATABASE db_name TO ROLE role_name;
GRANT USAGE ON SCHEMA db_name.schema_name TO ROLE role_name;
GRANT SELECT ON ALL TABLES IN SCHEMA db_name.schema_name TO ROLE role_name;
```

---

## Success Checklist - You're Done When...

After 2 weeks, you should have:

- ✅ Governance database created and accessible
- ✅ At least 3 people assigned to administrative roles
- ✅ 26 tags created and documented
- ✅ 21 masking policies created and tested
- ✅ 10+ functional roles created for your teams
- ✅ 20+ access roles matching your databases
- ✅ At least 50% of sensitive tables tagged
- ✅ At least 80% of PII columns identified and tagged
- ✅ Masking verified working for 3+ different roles
- ✅ 10+ users assigned to functional roles
- ✅ Weekly monitoring queries scheduled

---

## Next Steps - Weeks 3-8

### Week 3-4: Expand Tagging
- Tag remaining 50% of sensitive tables
- Focus on high-row-count tables first
- Document any special cases

### Week 5-6: User Onboarding
- Assign all users to appropriate functional roles
- Conduct training sessions
- Create user documentation

### Week 7-8: Monitoring & Refinement
- Set up automated alerts
- Review access patterns
- Adjust masking policies based on feedback
- Conduct first compliance audit

---

## Getting Help

### Built-in Documentation

All helper views and procedures:
```sql
-- View all governance views
SHOW VIEWS IN SCHEMA governance.metadata;

-- View all helper procedures
SHOW PROCEDURES IN SCHEMA governance.metadata;

-- Role documentation
SELECT * FROM governance.metadata.v_functional_role_catalog;

-- Tag catalog
SELECT * FROM governance.documentation.v_tag_catalog;
```

### Useful Monitoring Queries

See `audit_queries.sql` for 29 pre-built monitoring queries covering:
- Role assignments
- Privilege changes
- Tag coverage
- Masking effectiveness
- Access patterns
- Security alerts

### Key Resources

- **Snowflake Documentation:** https://docs.snowflake.com
- **RBAC Overview:** https://docs.snowflake.com/en/user-guide/security-access-control-overview
- **Tag-Based Masking:** https://docs.snowflake.com/en/user-guide/object-tagging

---

## Final Thoughts

You've just implemented enterprise-grade data governance in Snowflake. This framework will:
- Protect your sensitive data automatically
- Scale as your organization grows
- Simplify compliance audits
- Give you visibility into data access

The key to success is **starting small and iterating**. Don't try to tag everything on day one. Focus on your most sensitive data first, get feedback from users, and expand from there.

Remember: This is a living system. Review it quarterly, adjust as needed, and keep your documentation up to date.

Good luck with your implementation! 

---

**Questions or issues? Document them in your governance metadata tables - that's what they're there for.**

---

**Prepared by:** Venkannababu Thatavarthi  
**For:** Client Implementation  
**Version:** 1.0  
**Date:** October 13, 2025