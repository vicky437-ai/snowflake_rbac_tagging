# Missing Sections Content for Snowflake Admin Guide
## Ready to Incorporate into Your Document

---

## Section 5.3: Aggregation Policies

### Overview

Aggregation policies are schema-level objects that control what type of query can access data from a table or view. When applied to a table, queries must aggregate data into groups of a minimum size to return results, preventing queries from returning information from individual records. A table or view with an aggregation policy is said to be *aggregation-constrained*.

### Why Use Aggregation Policies?

| Benefit | Description |
|---------|-------------|
| Privacy Protection | Prevents disclosure of individual record values through aggregation requirements |
| Data Sharing Security | Providers maintain control over shared data usage |
| Re-identification Prevention | Minimum group sizes reduce risk of identifying individuals |
| Compliance Support | Supports privacy regulations requiring aggregate-only access |

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|----------|------------|-------------------|
| Aggregation Policies | — | ✓ | ✓ |
| Entity-Level Privacy | — | ✓ | ✓ |
| Row-Level Privacy | — | ✓ | ✓ |

### How It Works

```
AGGREGATION POLICY FLOW

┌─────────────────────────┐
│     QUERY SUBMITTED     │
│   SELECT region,        │
│   COUNT(*) FROM sales   │
│   GROUP BY region       │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   POLICY EVALUATION     │
│   Min Group Size = 5    │
└───────────┬─────────────┘
            │
    ┌───────┴───────┐
    │               │
    ▼               ▼
┌─────────┐    ┌─────────┐
│ Group   │    │ Group   │
│ Size≥5  │    │ Size<5  │
│ RETURN  │    │ →NULL   │
│ RESULTS │    │REMAINDER│
└─────────┘    └─────────┘
```

### Query Results with Aggregation Policy

When a query returns groups smaller than the minimum group size, those groups are combined into a *remainder group* with NULL as the grouping key:

| Query Result | Group Size | Policy Action |
|--------------|------------|---------------|
| Region: "East" (25 rows) | 25 | ✓ Returns normally |
| Region: "West" (18 rows) | 18 | ✓ Returns normally |
| Region: "North" (3 rows) | 3 | Combined into remainder |
| Region: "South" (2 rows) | 2 | Combined into remainder |
| **Region: NULL** | 5 | ✓ Remainder group returned |

### Allowed Aggregation Functions

Only these aggregation functions are permitted on aggregation-constrained tables:

- `AVG()`
- `COUNT()` / `COUNT(DISTINCT)`
- `HLL()`
- `SUM()`

### SQL Examples

```sql
-- Create aggregation policy with minimum group size of 5
CREATE AGGREGATION POLICY my_agg_policy
  AS () RETURNS AGGREGATION_CONSTRAINT -> 
  AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 5);

-- Create conditional aggregation policy (admin bypasses)
CREATE AGGREGATION POLICY conditional_agg_policy
  AS () RETURNS AGGREGATION_CONSTRAINT ->
  CASE
    WHEN CURRENT_ROLE() IN ('DATA_ADMIN', 'COMPLIANCE_OFFICER')
      THEN NO_AGGREGATION_CONSTRAINT()
    ELSE AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 10)
  END;

-- Assign aggregation policy to table
ALTER TABLE SALES.CUSTOMER_DATA 
  SET AGGREGATION POLICY my_agg_policy;

-- Replace aggregation policy (atomic operation)
ALTER TABLE SALES.CUSTOMER_DATA 
  SET AGGREGATION POLICY new_agg_policy FORCE;

-- Remove aggregation policy
ALTER TABLE SALES.CUSTOMER_DATA 
  UNSET AGGREGATION POLICY;

-- View aggregation policies in account
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.AGGREGATION_POLICIES
ORDER BY POLICY_NAME;

-- Find tables with aggregation policies
SELECT policy_name, policy_kind, ref_entity_name, ref_entity_domain
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
  policy_name => 'MY_DB.MY_SCHEMA.MY_AGG_POLICY'));
```

### Privileges Required

| Operation | Required Privilege |
|-----------|-------------------|
| Create aggregation policy | CREATE AGGREGATION POLICY on schema |
| Assign to table | APPLY AGGREGATION POLICY on account OR APPLY on policy + OWNERSHIP on table |
| Alter policy | OWNERSHIP on policy |
| Drop policy | OWNERSHIP on policy |

### Best Practices

| Practice | Rationale |
|----------|-----------|
| Set appropriate minimum group size | Balance privacy protection with data utility |
| Use conditional policies for internal users | Allow analysts unrestricted access while protecting shared data |
| Combine with projection policies | Defense in depth for sensitive columns |
| Monitor query patterns | Review ACCESS_HISTORY for potential policy circumvention |
| Document policy rationale | Use COMMENT field for compliance documentation |

### Limitations

- Cannot be applied to external tables
- Cannot use GROUP BY ROLLUP, CUBE, or GROUPING SETS
- Window functions not allowed on aggregation-constrained tables
- Most set operators (except UNION ALL) not supported

⚠️ **Important**: Aggregation policies are best suited for partners and customers with an existing level of trust. While they limit access to individual records, determined attackers could potentially work around aggregation requirements with enough query attempts.

📖 Documentation: https://docs.snowflake.com/en/user-guide/aggregation-policies

---

## Section 6.3: Projection Policies

### Overview

Projection policies are schema-level objects that define whether a column can be projected (displayed) in the output of a SQL query. A column with a projection policy is said to be *projection-constrained*. Unlike masking policies that transform values, projection policies completely prevent column values from appearing in query results for unauthorized users.

### Why Use Projection Policies?

| Benefit | Description |
|---------|-------------|
| Column-Level Privacy | Prevent sensitive columns from appearing in query results |
| Data Sharing Control | Constrain what consumers can see in shared data |
| Analysis Without Exposure | Allow filtering/joining on sensitive data without revealing values |
| Complement to Masking | Stronger protection than masking for highly sensitive columns |

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|----------|------------|-------------------|
| Projection Policies | — | ✓ | ✓ |
| Conditional Projection | — | ✓ | ✓ |
| Tag-Based Logic | — | ✓ | ✓ |

### How It Works

```
PROJECTION POLICY BEHAVIOR

┌─────────────────────────────────────────┐
│  SELECT name, ssn, salary FROM employees│
│  WHERE department = 'FINANCE'           │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│         POLICY EVALUATION               │
│  SSN column: Projection Policy Applied  │
└─────────────────┬───────────────────────┘
                  │
    ┌─────────────┴─────────────┐
    │                           │
    ▼                           ▼
┌─────────────────┐   ┌─────────────────┐
│  ADMIN ROLE     │   │  ANALYST ROLE   │
│  ALLOW = true   │   │  ALLOW = false  │
└────────┬────────┘   └────────┬────────┘
         │                     │
         ▼                     ▼
┌─────────────────┐   ┌─────────────────┐
│ name │ssn │sal  │   │ ERROR: Cannot   │
│ John │123 │100k │   │ project column  │
│ Jane │456 │95k  │   │ 'SSN'           │
└─────────────────┘   └─────────────────┘
```

### Projection Policy Behavior Options

| Behavior | ALLOW | ENFORCEMENT | Result |
|----------|-------|-------------|--------|
| Allow projection | true | - | Column values displayed |
| Block query | false | (default) | Query fails with error |
| Return NULL | false | 'NULLIFY' | Query succeeds, column shows NULL |

### Key Characteristics

| Aspect | Projection Policy Behavior |
|--------|---------------------------|
| Inner queries | Policy NOT enforced (only final output) |
| WHERE clauses | Column CAN be used for filtering |
| JOIN conditions | Column CAN be used as join key |
| Aggregations | Column CAN be aggregated (result blocked) |
| Final SELECT | Column CANNOT be projected if blocked |

### SQL Examples

```sql
-- Create projection policy to block column projection
CREATE PROJECTION POLICY block_projection
  AS () RETURNS PROJECTION_CONSTRAINT ->
  PROJECTION_CONSTRAINT(ALLOW => false);

-- Create projection policy that returns NULL instead of error
CREATE PROJECTION POLICY nullify_projection
  AS () RETURNS PROJECTION_CONSTRAINT ->
  PROJECTION_CONSTRAINT(ALLOW => false, ENFORCEMENT => 'NULLIFY');

-- Create conditional projection policy (role-based)
CREATE PROJECTION POLICY role_based_projection
  AS () RETURNS PROJECTION_CONSTRAINT ->
  CASE
    WHEN CURRENT_ROLE() IN ('HR_ADMIN', 'COMPLIANCE')
      THEN PROJECTION_CONSTRAINT(ALLOW => true)
    ELSE PROJECTION_CONSTRAINT(ALLOW => false)
  END;

-- Create projection policy for data sharing
CREATE PROJECTION POLICY share_projection
  AS () RETURNS PROJECTION_CONSTRAINT ->
  CASE
    WHEN INVOKER_SHARE() IN ('PARTNER_SHARE_1', 'PARTNER_SHARE_2')
      THEN PROJECTION_CONSTRAINT(ALLOW => true)
    ELSE PROJECTION_CONSTRAINT(ALLOW => false)
  END;

-- Apply projection policy to column
ALTER TABLE HR.EMPLOYEES 
  MODIFY COLUMN SSN 
  SET PROJECTION POLICY block_projection;

-- Apply at table creation
CREATE TABLE CUSTOMERS (
  ID INT,
  NAME VARCHAR,
  SSN VARCHAR WITH PROJECTION POLICY role_based_projection,
  EMAIL VARCHAR WITH PROJECTION POLICY nullify_projection
);

-- Replace projection policy (atomic)
ALTER TABLE HR.EMPLOYEES 
  MODIFY COLUMN SSN 
  SET PROJECTION POLICY new_projection_policy FORCE;

-- Remove projection policy
ALTER TABLE HR.EMPLOYEES 
  MODIFY COLUMN SSN 
  UNSET PROJECTION POLICY;

-- Monitor projection policies
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.PROJECTION_POLICIES
ORDER BY POLICY_NAME;

-- Find columns with projection policies
SELECT policy_name, ref_entity_name, ref_column_name
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
  policy_name => 'MY_DB.MY_SCHEMA.MY_PROJ_POLICY'));
```

### Projection Policy vs Masking Policy

| Aspect | Projection Policy | Masking Policy |
|--------|------------------|----------------|
| Purpose | Hide column completely | Transform column values |
| Query behavior | Block/NULL entire column | Return masked value |
| Use in WHERE | ✓ Allowed | ✓ Allowed |
| Use in JOIN | ✓ Allowed | ✓ Allowed |
| Analytics on column | Limited (can't see results) | ✓ Can analyze masked values |
| Reversibility | N/A (no value returned) | Depends on mask function |
| Performance | Slightly better | Slight overhead |

### Privileges Required

| Operation | Required Privilege |
|-----------|-------------------|
| Create projection policy | CREATE PROJECTION POLICY on schema |
| Assign to column | APPLY PROJECTION POLICY on account OR APPLY on policy + OWNERSHIP on table |
| Alter policy | OWNERSHIP on policy |
| Drop policy | OWNERSHIP on policy |

### Best Practices

| Practice | Rationale |
|----------|-----------|
| Use NULLIFY for reporting queries | Allows queries to complete with partial data |
| Use block (default) for strict security | Prevents any data leakage |
| Combine with masking policies | Apply masking for authorized users who can project |
| Consider join exposure | Column values may leak through joins with unprotected tables |
| Document policy intent | Use COMMENT for compliance tracking |

### Considerations

⚠️ **Important Limitations**:

1. **Join Exposure**: If a projection-constrained column is joined with an unprotected table, values may leak:
   ```sql
   -- Values can leak through joins!
   SELECT unprotected.ssn 
   FROM unprotected 
   JOIN protected ON unprotected.ssn = protected.ssn;
   ```

2. **Filter Targeting**: Users can still filter on constrained columns to target individuals even without seeing values

3. **Not a Complete Privacy Solution**: For guaranteed privacy, consider differential privacy or omitting the column entirely

📖 Documentation: https://docs.snowflake.com/en/user-guide/projection-policies

---

## Section 9.6: Trust Center

### Overview

Trust Center is a Snowflake security monitoring and compliance dashboard that automatically evaluates your account against security best practices and identifies potential vulnerabilities. It uses scheduled scanners to check configurations, detect risky users, and ensure compliance with industry benchmarks like CIS (Center for Internet Security).

### Why Use Trust Center?

| Benefit | Description |
|---------|-------------|
| Automated Security Assessment | Continuous evaluation of account security posture |
| Compliance Monitoring | CIS Benchmark alignment for audit readiness |
| Risk Identification | Detect misconfigured users, roles, and policies |
| Actionable Remediation | Step-by-step guidance to fix security issues |
| Centralized Visibility | Organization-level security dashboard |

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|----------|------------|-------------------|
| Trust Center | ✓ | ✓ | ✓ |
| Security Essentials Scanner | ✓ (Free) | ✓ (Free) | ✓ (Free) |
| CIS Benchmarks Scanner | ✓ | ✓ | ✓ |
| Threat Intelligence Scanner | ✓ | ✓ | ✓ |

### Trust Center Architecture

```
TRUST CENTER OVERVIEW

┌─────────────────────────────────────────────────────────┐
│                    TRUST CENTER                         │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────────┐  ┌─────────────────┐              │
│  │   SCANNER       │  │   FINDINGS      │              │
│  │   PACKAGES      │  │   (Violations)  │              │
│  └────────┬────────┘  └────────┬────────┘              │
│           │                    │                        │
│           ▼                    ▼                        │
│  ┌─────────────────────────────────────────┐           │
│  │           SCANNER TYPES                  │           │
│  ├─────────────────────────────────────────┤           │
│  │ • Security Essentials (Free, Monthly)   │           │
│  │ • CIS Benchmarks (Daily, Configurable)  │           │
│  │ • Threat Intelligence (Daily)           │           │
│  └─────────────────────────────────────────┘           │
│                       │                                 │
│                       ▼                                 │
│  ┌─────────────────────────────────────────┐           │
│  │         REMEDIATION GUIDANCE            │           │
│  │   Step-by-step fix instructions         │           │
│  └─────────────────────────────────────────┘           │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Scanner Packages

#### Security Essentials (Free)

| What It Checks | Severity |
|----------------|----------|
| MFA enforcement for human users with password authentication | Critical |
| MFA enrollment status for all password-based users | High |
| Account-level network policy configuration | High |
| Event table setup for Native App logging | Medium |

- **Cost**: Free (no compute charges)
- **Schedule**: Monthly (fixed, cannot be changed)
- **Status**: Enabled by default

#### CIS Benchmarks Scanner

| CIS Section | What It Checks |
|-------------|----------------|
| 1.x - Identity & Access | User authentication, MFA, password policies |
| 2.x - Monitoring | Activity monitoring (complex queries, review manually) |
| 3.x - Network | Network policy configuration |
| 4.x - Data Protection | Time Travel, masking policies, row access policies |

- **Cost**: Serverless compute charges apply
- **Schedule**: Daily (configurable)
- **Status**: Must be enabled manually

#### Threat Intelligence Scanner

| What It Detects | Risk Level |
|-----------------|------------|
| Human users without MFA who login with passwords | High |
| Users with passwords who haven't logged in for 90 days | Medium |
| Legacy service accounts using password authentication | High |
| Users with high authentication failure rates | Critical |
| Users with high job error rates | Medium |

- **Cost**: Serverless compute charges apply
- **Schedule**: Daily (configurable)
- **Status**: Must be enabled manually

### Required Privileges

| Trust Center Tab | Required Application Role |
|------------------|--------------------------|
| Findings (View Only) | SNOWFLAKE.TRUST_CENTER_VIEWER |
| Scanner Packages (Admin) | SNOWFLAKE.TRUST_CENTER_ADMIN |
| Organization View | ORGANIZATION_SECURITY_VIEWER + Trust Center role |

### SQL Examples

```sql
-- Grant Trust Center viewer access
USE ROLE ACCOUNTADMIN;

CREATE ROLE trust_center_viewer_role;
GRANT APPLICATION ROLE SNOWFLAKE.TRUST_CENTER_VIEWER 
  TO ROLE trust_center_viewer_role;
GRANT ROLE trust_center_viewer_role TO USER security_analyst;

-- Grant Trust Center admin access
CREATE ROLE trust_center_admin_role;
GRANT APPLICATION ROLE SNOWFLAKE.TRUST_CENTER_ADMIN 
  TO ROLE trust_center_admin_role;
GRANT ROLE trust_center_admin_role TO USER security_admin;

-- View Trust Center cost (after Dec 1, 2024)
SELECT 
  usage_date AS date,
  credits_used AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE service_type = 'TRUST_CENTER'
  AND usage_date > '2024-12-01'
ORDER BY usage_date DESC;

-- View total Trust Center cost for a period
SELECT SUM(credits_used) AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE service_type = 'TRUST_CENTER'
  AND start_time >= '2025-01-01'
  AND end_time <= '2025-01-31';

-- Query Trust Center findings directly
SELECT 
  start_timestamp,
  end_timestamp,
  scanner_id,
  scanner_short_description,
  impact,
  severity,
  total_at_risk_count,
  at_risk_entities
FROM SNOWFLAKE.TRUST_CENTER.FINDINGS
WHERE completion_status = 'SUCCEEDED'
ORDER BY start_timestamp DESC;

-- Find CIS monitoring violations (Section 2 scanners)
SELECT 
  scanner_id,
  severity,
  total_at_risk_count,
  at_risk_entities
FROM SNOWFLAKE.TRUST_CENTER.FINDINGS
WHERE scanner_type = 'Threat'
  AND scanner_id LIKE 'CIS_BENCHMARKS_CIS2%'
  AND completion_status = 'SUCCEEDED'
ORDER BY start_timestamp DESC;
```

### Findings Lifecycle Management

| Status | Description | Email Notifications |
|--------|-------------|---------------------|
| Open | New violation detected | ✓ Sent |
| Resolved | Marked as addressed/not applicable | ✗ Suppressed |
| Reopened | Previously resolved, now open again | ✓ Resume |

### Trust Center Workflow

```
TRUST CENTER FINDINGS WORKFLOW

1. SCANNER RUNS
   │
   ▼
2. VIOLATIONS DETECTED
   │
   ▼
3. FINDINGS RAISED (Status: OPEN)
   │
   ├──► 4a. REMEDIATE
   │         │
   │         ▼
   │    CONFIGURATION FIXED
   │         │
   │         ▼
   │    NEXT SCAN: No violation
   │
   └──► 4b. TRIAGE
             │
             ▼
        MARK AS RESOLVED
        (with justification)
             │
             ▼
        NOTIFICATIONS SUPPRESSED
```

### Best Practices

| Practice | Rationale |
|----------|-----------|
| Enable all scanner packages | Comprehensive security coverage |
| Review findings weekly | Stay on top of security posture |
| Document resolution reasons | Audit trail for compliance |
| Configure email notifications | Immediate awareness of new issues |
| Monitor scanner costs | Balance coverage with compute expenses |
| Review CIS Section 2 manually | Complex queries require expert review |

### Common Findings and Remediation

| Finding | Severity | Remediation |
|---------|----------|-------------|
| Users without MFA | Critical | Create/apply authentication policy requiring MFA |
| No account-level network policy | High | Create and apply network policy with allowed IPs |
| High privilege grants detected | High | Review and revoke unnecessary ACCOUNTADMIN grants |
| Inactive users with credentials | Medium | Disable or remove inactive user accounts |
| Service accounts with passwords | Medium | Convert to key-pair authentication |

### Accessing Trust Center

1. Sign in to Snowsight
2. Navigate to **Admin** → **Security** → **Trust Center**
3. View **Findings** tab for violations
4. View **Scanner Packages** tab to enable/configure scanners

📖 Documentation: https://docs.snowflake.com/en/user-guide/trust-center/overview

---

## How to Incorporate These Sections

### Suggested Placement

```
Your Current Document Structure:
├── Section 5: Column-Level Security
│   ├── 5.1 Dynamic Data Masking (existing)
│   ├── 5.2 External Tokenization (existing)
│   └── 5.3 Aggregation Policies (NEW) ◄──────────
│
├── Section 6: Row-Level Security
│   ├── 6.1 Row Access Policies (existing)
│   ├── 6.2 Secure Views (existing)
│   └── 6.3 Projection Policies (NEW) ◄──────────
│
└── Section 9: Governance & Compliance
    ├── 9.1 Object Tagging (existing)
    ├── 9.2 Data Classification (existing)
    ├── 9.3 Access History (existing)
    ├── 9.4 Audit Logging (existing)
    ├── 9.5 Compliance Certifications (existing)
    └── 9.6 Trust Center (NEW) ◄──────────
```

### Update Edition Comparison Table (Section 10)

Add these rows to your Edition Comparison Summary table:

| Feature | Standard | Enterprise | Business Critical |
|---------|----------|------------|-------------------|
| **Privacy Policies** | | | |
| Aggregation Policies | — | ✓ | ✓ |
| Projection Policies | — | ✓ | ✓ |
| **Governance** | | | |
| Trust Center | ✓ | ✓ | ✓ |
| Security Essentials Scanner | ✓ (Free) | ✓ (Free) | ✓ (Free) |
| CIS Benchmarks Scanner | ✓ | ✓ | ✓ |
| Threat Intelligence Scanner | ✓ | ✓ | ✓ |

### Update Best Practices Summary (Section 10)

Add to Priority 2 Recommended:

| # | Practice | Why It Matters |
|---|----------|----------------|
| 11 | Enable Trust Center scanners | Continuous security monitoring and CIS compliance |
| 12 | Use aggregation policies for shared data | Prevent individual record disclosure |
| 13 | Apply projection policies to sensitive columns | Prevent column values from appearing in results |

---

*Content prepared: January 17, 2026*
*Based on official Snowflake documentation*
*Ready for incorporation into Snowflake Admin Guide*
