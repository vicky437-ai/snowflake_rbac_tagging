# Snowflake RBAC & Tagging Strategy
## Visual Flow Diagrams and Architecture

**Prepared by:** Venkannababu Thatavarthi  
**Date:** October 13, 2025  
**Version:** 1.0

---

## 1. Implementation Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE RBAC & TAGGING IMPLEMENTATION              │
│                         10-Week Implementation Plan                      │
└─────────────────────────────────────────────────────────────────────────┘

WEEK 1-2: FOUNDATION SETUP
┌──────────────────────────────────────────────────────────────────┐
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │  Create          │────────>│  Setup Admin     │              │
│  │  Governance DB   │         │  Roles           │              │
│  └──────────────────┘         └──────────────────┘              │
│         │                              │                         │
│         v                              v                         │
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │  - governance    │         │  - TAG_ADMIN     │              │
│  │  - tags schema   │         │  - POLICY_ADMIN  │              │
│  │  - policies      │         │  - DATA_STEWARD  │              │
│  │  - metadata      │         │  - GOVERNANCE    │              │
│  └──────────────────┘         └──────────────────┘              │
└──────────────────────────────────────────────────────────────────┘
                            │
                            v
WEEK 2-3: TAG FRAMEWORK CREATION
┌──────────────────────────────────────────────────────────────────┐
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │  Create Core     │────────>│  Grant Tag       │              │
│  │  Tags (26)       │         │  Privileges      │              │
│  └──────────────────┘         └──────────────────┘              │
│         │                              │                         │
│         v                              v                         │
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │  - PII Tags      │         │  - DATA_STEWARD  │              │
│  │  - Sensitivity   │         │  - DB Owners     │              │
│  │  - Compliance    │         │  - Helper Procs  │              │
│  │  - Domain        │         │  - Views         │              │
│  └──────────────────┘         └──────────────────┘              │
└──────────────────────────────────────────────────────────────────┘
                            │
                            v
WEEK 3-4: MASKING POLICY DEVELOPMENT
┌──────────────────────────────────────────────────────────────────┐
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │  Create Masking  │────────>│  Associate       │              │
│  │  Policies (21)   │         │  with Tags       │              │
│  └──────────────────┘         └──────────────────┘              │
│         │                              │                         │
│         v                              v                         │
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │  - Email Mask    │         │  pii_email ───>  │              │
│  │  - Phone Mask    │         │    mask_email    │              │
│  │  - SSN Mask      │         │  pii_phone ───>  │              │
│  │  - Financial     │         │    mask_phone    │              │
│  └──────────────────┘         └──────────────────┘              │
└──────────────────────────────────────────────────────────────────┘
                            │
                            v
WEEK 4-6: RBAC HIERARCHY CONSTRUCTION
┌──────────────────────────────────────────────────────────────────┐
│  ┌──────────────────┐    ┌──────────────────┐    ┌───────────┐  │
│  │  Create Access   │───>│  Create          │───>│  Build    │  │
│  │  Roles (30+)     │    │  Functional (39) │    │  Hierarchy│  │
│  └──────────────────┘    └──────────────────┘    └───────────┘  │
│         │                        │                      │        │
│         v                        v                      v        │
│  ┌──────────────────┐    ┌──────────────────┐    ┌───────────┐  │
│  │  - DB_READ       │    │  - DATA_ANALYST  │    │  Grant to │  │
│  │  - DB_READ_WRITE │    │  - DATA_ENGINEER │    │  SYSADMIN │  │
│  │  - DB_ADMIN      │    │  - FINANCE_TEAM  │    │           │  │
│  │  - WAREHOUSE     │    │  - SERVICE_ROLES │    │           │  │
│  └──────────────────┘    └──────────────────┘    └───────────┘  │
└──────────────────────────────────────────────────────────────────┘
                            │
                            v
WEEK 6-8: DATA CLASSIFICATION & TAGGING
┌──────────────────────────────────────────────────────────────────┐
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │  Automated       │────────>│  Manual Tag      │              │
│  │  Classification  │         │  Application     │              │
│  └──────────────────┘         └──────────────────┘              │
│         │                              │                         │
│         v                              v                         │
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │  Snowflake       │         │  Data Stewards   │              │
│  │  Data            │         │  Apply Tags      │              │
│  │  Classification  │         │  Column by       │              │
│  │  Feature         │         │  Column          │              │
│  └──────────────────┘         └──────────────────┘              │
└──────────────────────────────────────────────────────────────────┘
                            │
                            v
WEEK 8-10: USER ASSIGNMENT & VALIDATION
┌──────────────────────────────────────────────────────────────────┐
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │  Assign Roles    │────────>│  Test & Validate │              │
│  │  to Users        │         │  Access          │              │
│  └──────────────────┘         └──────────────────┘              │
│         │                              │                         │
│         v                              v                         │
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │  - Map users to  │         │  - Test masking  │              │
│  │    functional    │         │  - Verify access │              │
│  │    roles         │         │  - Monitor logs  │              │
│  │  - Log all       │         │  - User feedback │              │
│  │    assignments   │         │                  │              │
│  └──────────────────┘         └──────────────────┘              │
└──────────────────────────────────────────────────────────────────┘
                            │
                            v
                  ┌────────────────────┐
                  │  ONGOING MONITORING │
                  │  & MAINTENANCE      │
                  └────────────────────┘
```

---

## 2. Role Hierarchy Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                         SNOWFLAKE ROLE HIERARCHY                           │
│                      (Top-Down Privilege Inheritance)                      │
└────────────────────────────────────────────────────────────────────────────┘

                        ┌─────────────────────┐
                        │   ACCOUNTADMIN      │  ◄── Restricted Access
                        │   (Top-Level)       │      2-3 users only
                        └──────────┬──────────┘      MFA Required
                                   │
                ┌──────────────────┴──────────────────┐
                │                                     │
                v                                     v
      ┌──────────────────┐                  ┌──────────────────┐
      │  SECURITYADMIN   │                  │    SYSADMIN      │
      │  (Grant Mgmt)    │                  │  (Object Mgmt)   │
      └────────┬─────────┘                  └─────────┬────────┘
               │                                      │
               v                                      │
      ┌──────────────────┐                           │
      │   USERADMIN      │                           │
      │  (User/Role)     │                           │
      └──────────────────┘                           │
                                                      │
                        ┌─────────────────────────────┤
                        │                             │
                        v                             v
              ┌──────────────────┐        ┌──────────────────────┐
              │ COMPLIANCE_OFFICER│        │  GOVERNANCE_ADMIN    │
              │  (Audit Access)   │        │  (Governance Ops)    │
              └──────────────────┘        └──────────┬───────────┘
                                                      │
                        ┌─────────────────────────────┼─────────────────┐
                        │                             │                 │
                        v                             v                 v
              ┌──────────────────┐        ┌──────────────────┐  ┌──────────────┐
              │    TAG_ADMIN     │        │  POLICY_ADMIN    │  │ DATA_STEWARD │
              │  (Tag Creation)  │        │ (Policy Creation)│  │ (Tag Apply)  │
              └──────────────────┘        └──────────────────┘  └──────────────┘


                        FUNCTIONAL ROLES LAYER
                        ─────────────────────
      ┌─────────────────────────────┬───────────────────────────────┐
      │                             │                               │
      v                             v                               v
┌──────────────┐          ┌──────────────────┐          ┌──────────────────┐
│ DATA_ANALYST │          │  DATA_ENGINEER   │          │  FINANCE_ANALYST │
│              │          │                  │          │                  │
└──────┬───────┘          └────────┬─────────┘          └────────┬─────────┘
       │                           │                              │
       │                           │                              │
       v                           v                              v
  Inherits:                   Inherits:                      Inherits:
  - customer_db_read          - customer_db_read_write       - finance_db_read
  - sales_db_read             - etl_cross_db_read_write      - warehouse_small
  - warehouse_small           - warehouse_large              


                        ACCESS ROLES LAYER
                        ──────────────────
      ┌─────────────────────────────┬───────────────────────────────┐
      │                             │                               │
      v                             v                               v
┌──────────────────┐      ┌──────────────────────┐      ┌────────────────────┐
│ customer_db_read │      │ customer_db_read_write│      │  customer_db_admin │
│                  │      │                      │      │                    │
│ Privileges:      │      │ Privileges:          │      │ Privileges:        │
│ - USAGE on DB    │      │ - USAGE on DB        │      │ - ALL on DB        │
│ - USAGE on       │      │ - USAGE on schemas   │      │ - ALL on schemas   │
│   schemas        │      │ - SELECT, INSERT,    │      │ - ALL on tables    │
│ - SELECT on      │      │   UPDATE, DELETE     │      │ - CREATE objects   │
│   tables/views   │      │   on tables          │      │                    │
└──────────────────┘      └──────────────────────┘      └────────────────────┘


                        WAREHOUSE ACCESS LAYER
                        ──────────────────────
      ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
      │ warehouse_small  │      │ warehouse_medium │      │ warehouse_large  │
      │ - USAGE          │      │ - USAGE          │      │ - USAGE          │
      │ - OPERATE        │      │ - OPERATE        │      │ - OPERATE        │
      └──────────────────┘      └──────────────────┘      └──────────────────┘
```

---

## 3. Tag Synchronization and Policy Enforcement Flow

```
┌────────────────────────────────────────────────────────────────────────────┐
│                   TAG SYNC & POLICY ENFORCEMENT FLOW                       │
└────────────────────────────────────────────────────────────────────────────┘

STEP 1: DATA DISCOVERY
──────────────────────
┌──────────────────────────────────────────────────────────────┐
│  New Data Source                                             │
│  ┌────────────┐                                              │
│  │ New Table  │                                              │
│  │  Created   │                                              │
│  └─────┬──────┘                                              │
│        │                                                     │
│        v                                                     │
│  ┌──────────────────────────────────────────┐               │
│  │     Data Classification Process          │               │
│  ├──────────────────────────────────────────┤               │
│  │  Automated:                              │               │
│  │  ├─> Snowflake Classification Feature    │               │
│  │  ├─> Column name pattern matching        │               │
│  │  └─> Data type analysis                  │               │
│  │                                           │               │
│  │  Manual:                                  │               │
│  │  ├─> Data Steward review                 │               │
│  │  ├─> Business context analysis           │               │
│  │  └─> Compliance requirements check       │               │
│  └───────────────────┬───────────────────────┘               │
└────────────────────────────────────────────────────────────────┘
                       │
                       v
STEP 2: TAG ASSIGNMENT
──────────────────────
┌──────────────────────────────────────────────────────────────┐
│  Apply Tags to Objects                                       │
│  ┌───────────────────────────────────────┐                   │
│  │  ALTER TABLE customers                │                   │
│  │    MODIFY COLUMN email                │                   │
│  │      SET TAG pii_email = 'true';      │                   │
│  │                                       │                   │
│  │    MODIFY COLUMN phone                │                   │
│  │      SET TAG pii_phone = 'true';      │                   │
│  │                                       │                   │
│  │    MODIFY COLUMN ssn                  │                   │
│  │      SET TAG pii_ssn = 'true';        │                   │
│  └───────────────┬───────────────────────┘                   │
│                  │                                           │
│                  v                                           │
│  ┌───────────────────────────────────────┐                   │
│  │  Log Assignment                       │                   │
│  │  ├─> governance.metadata.            │                   │
│  │  │   tag_assignment_log              │                   │
│  │  ├─> Business justification          │                   │
│  │  └─> Timestamp & assigned_by         │                   │
│  └───────────────────────────────────────┘                   │
└──────────────────────────────────────────────────────────────┘
                       │
                       v
STEP 3: POLICY ACTIVATION
──────────────────────────
┌──────────────────────────────────────────────────────────────┐
│  Tag-Policy Mapping Activated                                │
│                                                              │
│  ┌─────────────┐      ┌──────────────┐      ┌────────────┐  │
│  │ pii_email   │─────>│ mask_email   │─────>│ Enforced   │  │
│  │ tag         │      │ policy       │      │ on column  │  │
│  └─────────────┘      └──────────────┘      └────────────┘  │
│                                                              │
│  ┌─────────────┐      ┌──────────────┐      ┌────────────┐  │
│  │ pii_phone   │─────>│ mask_phone   │─────>│ Enforced   │  │
│  │ tag         │      │ policy       │      │ on column  │  │
│  └─────────────┘      └──────────────┘      └────────────┘  │
│                                                              │
│  Policy automatically applied to all columns with this tag  │
└──────────────────────────────────────────────────────────────┘
                       │
                       v
STEP 4: ACCESS ENFORCEMENT
───────────────────────────
┌──────────────────────────────────────────────────────────────┐
│  User Query Execution                                        │
│                                                              │
│  ┌───────────────────────────────────────┐                   │
│  │  User executes:                       │                   │
│  │  SELECT email, phone FROM customers;  │                   │
│  └─────────────────┬─────────────────────┘                   │
│                    │                                         │
│                    v                                         │
│  ┌────────────────────────────────────────────────┐          │
│  │  Snowflake Policy Evaluation Engine            │          │
│  ├────────────────────────────────────────────────┤          │
│  │  1. Identify current role                      │          │
│  │     └─> CURRENT_ROLE() = 'DATA_ANALYST'       │          │
│  │                                                │          │
│  │  2. Check column tags                          │          │
│  │     └─> email: pii_email                      │          │
│  │     └─> phone: pii_phone                      │          │
│  │                                                │          │
│  │  3. Evaluate masking policies                  │          │
│  │     └─> mask_email policy                     │          │
│  │         CASE                                   │          │
│  │           WHEN ROLE IN ('SYSADMIN')           │          │
│  │             THEN show_full                    │          │
│  │           WHEN ROLE IN ('DATA_ANALYST')       │          │
│  │             THEN show_masked                  │          │
│  │                                                │          │
│  │  4. Apply transformation                       │          │
│  │     └─> email: j***@example.com               │          │
│  │     └─> phone: XXX-XXX-4567                   │          │
│  └────────────────────────────────────────────────┘          │
│                    │                                         │
│                    v                                         │
│  ┌───────────────────────────────────────┐                   │
│  │  Return Transformed Results            │                   │
│  │  ┌───────────────────────────────────┐ │                   │
│  │  │ email          │ phone            │ │                   │
│  │  ├────────────────┼──────────────────┤ │                   │
│  │  │ j***@example.  │ XXX-XXX-4567     │ │                   │
│  │  │ j***@example.  │ XXX-XXX-9876     │ │                   │
│  │  └───────────────────────────────────┘ │                   │
│  └───────────────────────────────────────┘                   │
└──────────────────────────────────────────────────────────────┘

ROLE-BASED MASKING BEHAVIOR
────────────────────────────
┌────────────────────┬──────────────────────┬──────────────────────┐
│ Role               │ Email Result         │ Phone Result         │
├────────────────────┼──────────────────────┼──────────────────────┤
│ SYSADMIN           │ john@example.com     │ 555-123-4567         │
│ GOVERNANCE_ADMIN   │ john@example.com     │ 555-123-4567         │
│ COMPLIANCE_OFFICER │ john@example.com     │ 555-123-4567         │
├────────────────────┼──────────────────────┼──────────────────────┤
│ CUSTOMER_SUPPORT   │ jo***@example.com    │ XXX-XXX-4567         │
│ SALES_TEAM         │ jo***@example.com    │ XXX-XXX-4567         │
├────────────────────┼──────────────────────┼──────────────────────┤
│ DATA_ANALYST       │ ***@example.com      │ XXX-XXX-XXXX         │
│ MARKETING_TEAM     │ ***@example.com      │ XXX-XXX-XXXX         │
├────────────────────┼──────────────────────┼──────────────────────┤
│ PUBLIC             │ ***@***              │ ***-***-****         │
└────────────────────┴──────────────────────┴──────────────────────┘
```

---

## 4. Access Request and Approval Workflow

```
┌────────────────────────────────────────────────────────────────────────────┐
│                      ACCESS REQUEST WORKFLOW                               │
│               (User Requests Access to Sensitive Data)                     │
└────────────────────────────────────────────────────────────────────────────┘

STAGE 1: USER REQUEST INITIATION
─────────────────────────────────
┌──────────────────────────────────────────────────────────────┐
│  User Submits Access Request                                 │
│  ┌────────────────────────────────────────────┐              │
│  │  Request Details:                          │              │
│  │  ├─> Requester: john.doe@company.com       │              │
│  │  ├─> Role Requested: FINANCE_ANALYST       │              │
│  │  ├─> Data Source: finance_db               │              │
│  │  ├─> Business Justification:               │              │
│  │  │   "Need access for Q4 budget analysis"  │              │
│  │  ├─> Duration: Permanent / Temporary       │              │
│  │  └─> Urgency: Normal / Urgent              │              │
│  └────────────────────────────────────────────┘              │
│                       │                                      │
│                       v                                      │
│  ┌────────────────────────────────────────────┐              │
│  │  Ticket Created: REQ-2025-001              │              │
│  │  Status: PENDING_MANAGER_APPROVAL          │              │
│  └────────────────────────────────────────────┘              │
└──────────────────────────────────────────────────────────────┘
                       │
                       v
STAGE 2: MANAGER REVIEW
────────────────────────
┌──────────────────────────────────────────────────────────────┐
│  Manager Reviews Business Need                               │
│  ┌────────────────────────────────────────────┐              │
│  │  Manager: Jane Smith                       │              │
│  │  Review Criteria:                          │              │
│  │  ├─> Is access necessary for role?         │              │
│  │  ├─> Is justification valid?               │              │
│  │  ├─> Principle of least privilege met?     │              │
│  │  └─> Temporary or permanent access?        │              │
│  └────────────────────────────────────────────┘              │
│                       │                                      │
│        ┌──────────────┴───────────────┐                      │
│        │                              │                      │
│        v                              v                      │
│  ┌──────────┐                   ┌──────────┐                │
│  │ APPROVED │                   │ REJECTED │                │
│  └────┬─────┘                   └────┬─────┘                │
│       │                              │                      │
│       │                              v                      │
│       │                    ┌──────────────────┐             │
│       │                    │ Notify Requester │             │
│       │                    │ Request Denied   │             │
│       │                    └──────────────────┘             │
│       │                                                     │
└───────┼─────────────────────────────────────────────────────┘
        │
        v
STAGE 3: DATA OWNER REVIEW
───────────────────────────
┌──────────────────────────────────────────────────────────────┐
│  Data Owner Validates Compliance                             │
│  ┌────────────────────────────────────────────┐              │
│  │  Data Owner: Finance Team Lead             │              │
│  │  Review Criteria:                          │              │
│  │  ├─> Data sensitivity level appropriate?   │              │
│  │  ├─> Compliance requirements met?          │              │
│  │  │   (GDPR, SOX, PCI-DSS, etc.)           │              │
│  │  ├─> Training requirements completed?      │              │
│  │  └─> Data usage agreement signed?          │              │
│  └────────────────────────────────────────────┘              │
│                       │                                      │
│        ┌──────────────┴───────────────┐                      │
│        │                              │                      │
│        v                              v                      │
│  ┌──────────┐                   ┌──────────┐                │
│  │ APPROVED │                   │ REJECTED │                │
│  └────┬─────┘                   └────┬─────┘                │
│       │                              │                      │
│       │                              v                      │
│       │                    ┌──────────────────┐             │
│       │                    │ Notify Requester │             │
│       │                    │ & Manager        │             │
│       │                    └──────────────────┘             │
└───────┼─────────────────────────────────────────────────────┘
        │
        v
STAGE 4: SECURITY TEAM REVIEW
──────────────────────────────
┌──────────────────────────────────────────────────────────────┐
│  Security Team Final Validation                              │
│  ┌────────────────────────────────────────────┐              │
│  │  Security Officer: Security Team           │              │
│  │  Review Criteria:                          │              │
│  │  ├─> No security policy violations?        │              │
│  │  ├─> User has completed security training? │              │
│  │  ├─> MFA enabled if required?              │              │
│  │  ├─> Appropriate role for access level?    │              │
│  │  └─> Separation of duties maintained?      │              │
│  └────────────────────────────────────────────┘              │
│                       │                                      │
│        ┌──────────────┴───────────────┐                      │
│        │                              │                      │
│        v                              v                      │
│  ┌──────────┐                   ┌──────────────┐            │
│  │ APPROVED │                   │ CONDITIONALLY│            │
│  │          │                   │ APPROVED     │            │
│  └────┬─────┘                   └──────┬───────┘            │
│       │                                │                    │
│       │                                v                    │
│       │                    ┌──────────────────────┐         │
│       │                    │ Modify request with  │         │
│       │                    │ reduced privileges   │         │
│       │                    └──────────┬───────────┘         │
│       │                                │                    │
│       └────────────────────────────────┘                    │
└───────┼─────────────────────────────────────────────────────┘
        │
        v
STAGE 5: IMPLEMENTATION
────────────────────────
┌──────────────────────────────────────────────────────────────┐
│  SECURITYADMIN Grants Role                                   │
│  ┌────────────────────────────────────────────┐              │
│  │  USE ROLE SECURITYADMIN;                   │              │
│  │                                            │              │
│  │  -- Grant role to user                     │              │
│  │  GRANT ROLE finance_analyst                │              │
│  │    TO USER john.doe@company.com;           │              │
│  │                                            │              │
│  │  -- Log the assignment                     │              │
│  │  CALL governance.metadata.                 │              │
│  │    sp_assign_functional_role_to_user(      │              │
│  │      'john.doe@company.com',               │              │
│  │      'finance_analyst',                    │              │
│  │      'Approved via REQ-2025-001',          │              │
│  │      NULL -- permanent access              │              │
│  │    );                                      │              │
│  └────────────────────────────────────────────┘              │
│                       │                                      │
│                       v                                      │
│  ┌────────────────────────────────────────────┐              │
│  │  Audit Log Entry Created                   │              │
│  │  ├─> Timestamp: 2025-10-13 10:30:00       │              │
│  │  ├─> Granted by: security.admin            │              │
│  │  ├─> Request ID: REQ-2025-001              │              │
│  │  └─> Status: ACTIVE                        │              │
│  └────────────────────────────────────────────┘              │
└──────────────────────────────────────────────────────────────┘
                       │
                       v
STAGE 6: NOTIFICATION & VERIFICATION
─────────────────────────────────────
┌──────────────────────────────────────────────────────────────┐
│  User Notified and Access Verified                          │
│  ┌────────────────────────────────────────────┐              │
│  │  Email Notification Sent:                  │              │
│  │  ┌──────────────────────────────────────┐  │              │
│  │  │ To: john.doe@company.com             │  │              │
│  │  │ Subject: Access Request Approved     │  │              │
│  │  │                                      │  │              │
│  │  │ Your request for FINANCE_ANALYST     │  │              │
│  │  │ role has been approved.              │  │              │
│  │  │                                      │  │              │
│  │  │ Access includes:                     │  │              │
│  │  │ - Read access to finance_db          │  │              │
│  │  │ - Small warehouse usage              │  │              │
│  │  │                                      │  │              │
│  │  │ Please verify access and report      │  │              │
│  │  │ any issues within 24 hours.          │  │              │
│  │  └──────────────────────────────────────┘  │              │
│  └────────────────────────────────────────────┘              │
│                       │                                      │
│                       v                                      │
│  ┌────────────────────────────────────────────┐              │
│  │  User Tests Access:                        │              │
│  │  1. Log into Snowflake                     │              │
│  │  2. USE ROLE finance_analyst;              │              │
│  │  3. SELECT * FROM finance_db.public.test;  │              │
│  │  4. Verify masked data displays correctly  │              │
│  │  5. Confirm expected privileges work       │              │
│  └────────────────────────────────────────────┘              │
│                       │                                      │
│        ┌──────────────┴───────────────┐                      │
│        │                              │                      │
│        v                              v                      │
│  ┌──────────┐                   ┌──────────┐                │
│  │ SUCCESS  │                   │  ISSUE   │                │
│  └────┬─────┘                   └────┬─────┘                │
│       │                              │                      │
│       │                              v                      │
│       │                    ┌──────────────────┐             │
│       │                    │ Escalate to      │             │
│       │                    │ Security Team    │             │
│       │                    └──────────────────┘             │
│       │                                                     │
│       v                                                     │
│  ┌─────────────────────────────────────────┐                │
│  │  Request Completed                      │                │
│  │  Status: ACTIVE                         │                │
│  │  Next Review: 90 days                   │                │
│  └─────────────────────────────────────────┘                │
└──────────────────────────────────────────────────────────────┘

ONGOING MONITORING
──────────────────
┌──────────────────────────────────────────────────────────────┐
│  Continuous Monitoring & Review                              │
│  ┌────────────────────────────────────────────┐              │
│  │  Automated Checks:                         │              │
│  │  ├─> Access usage monitoring               │              │
│  │  ├─> Unusual pattern detection             │              │
│  │  ├─> Failed access attempts                │              │
│  │  └─> Privilege escalation attempts         │              │
│  │                                            │              │
│  │  Scheduled Reviews:                        │              │
│  │  ├─> Quarterly access recertification      │              │
│  │  ├─> Manager approval renewal              │              │
│  │  ├─> Compliance audit                      │              │
│  │  └─> Security assessment                   │              │
│  └────────────────────────────────────────────┘              │
└──────────────────────────────────────────────────────────────┘
```

---

## 5. Data Classification Decision Tree

```
┌────────────────────────────────────────────────────────────────────────────┐
│                     DATA CLASSIFICATION DECISION TREE                      │
└────────────────────────────────────────────────────────────────────────────┘

                    START: New Data/Column Identified
                                    │
                                    v
              ┌─────────────────────────────────────┐
              │ Does it contain information about   │
              │ an identified or identifiable       │
              │ individual?                         │
              └──────────────┬──────────────────────┘
                             │
                 ┌───────────┴───────────┐
                 │                       │
                YES                     NO
                 │                       │
                 v                       v
    ┌────────────────────────┐  ┌──────────────────────┐
    │ PERSONALLY IDENTIFIABLE│  │ Does it contain      │
    │ INFORMATION (PII)      │  │ business-sensitive   │
    │                        │  │ information?         │
    └────────┬───────────────┘  └──────────┬───────────┘
             │                             │
             v                   ┌─────────┴─────────┐
    ┌─────────────────┐          │                   │
    │ What type of PII?│         YES                 NO
    └────────┬─────────┘          │                   │
             │                    v                   v
    ┌────────┴────────┐  ┌──────────────┐   ┌──────────────┐
    │ Direct          │  │ CONFIDENTIAL │   │ INTERNAL or  │
    │ Identifier?     │  │ DATA         │   │ PUBLIC       │
    └────┬────────────┘  └──────────────┘   └──────────────┘
         │
    ┌────┴────┐
    │         │
   YES       NO (Quasi-identifier)
    │         │
    v         v
┌────────┐ ┌────────────┐
│        │ │ Can it be  │
│ Tag as:│ │ combined   │
│        │ │ to identify│
│ - Name │ │ individual?│
│ - Email│ └─────┬──────┘
│ - SSN  │       │
│ - Phone│   ┌───┴───┐
│ - Tax  │   │       │
│   ID   │  YES     NO
└────────┘   │       │
             v       v
         ┌───────┐ ┌────────┐
         │ HIGH  │ │ MEDIUM │
         │ RISK  │ │ RISK   │
         └───────┘ └────────┘

TAGGING DECISION MATRIX
────────────────────────

┌─────────────────┬──────────────────┬──────────────────┬──────────────────┐
│ Data Type       │ Tag              │ Masking Policy   │ Access Level     │
├─────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Email Address   │ pii_email        │ mask_email       │ CONFIDENTIAL     │
│ Phone Number    │ pii_phone        │ mask_phone       │ CONFIDENTIAL     │
│ SSN             │ pii_ssn          │ mask_ssn         │ RESTRICTED       │
│ Credit Card     │ pii_credit_card  │ mask_credit_card │ RESTRICTED       │
│ Bank Account    │ pii_bank_account │ mask_bank_account│ RESTRICTED       │
│ Name            │ pii_name         │ mask_name        │ CONFIDENTIAL     │
│ Address         │ pii_address      │ mask_address     │ CONFIDENTIAL     │
│ Date of Birth   │ pii_date_of_birth│ mask_dob         │ CONFIDENTIAL     │
│ IP Address      │ pii_ip_address   │ mask_ip_address  │ INTERNAL         │
├─────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Financial Data  │ data_sensitivity │ mask_financial   │ CONFIDENTIAL/    │
│ (Revenue, etc.) │ = CONFIDENTIAL   │                  │ RESTRICTED       │
│ Customer Data   │ data_domain =    │ Various          │ CONFIDENTIAL     │
│                 │ CUSTOMER         │                  │                  │
│ HR Data         │ data_domain =    │ Various          │ RESTRICTED       │
│                 │ HR               │                  │                  │
└─────────────────┴──────────────────┴──────────────────┴──────────────────┘

COMPLIANCE TAGGING
──────────────────

┌─────────────────┬──────────────────┬──────────────────────────────────┐
│ Data Type       │ Compliance Tag   │ Requirements                     │
├─────────────────┼──────────────────┼──────────────────────────────────┤
│ EU Customer     │ GDPR, CCPA       │ - Right to erasure               │
│ Data            │                  │ - Data portability               │
│                 │                  │ - Consent management             │
├─────────────────┼──────────────────┼──────────────────────────────────┤
│ Healthcare      │ HIPAA            │ - PHI protection                 │
│ Records         │                  │ - Audit logging                  │
│                 │                  │ - Access controls                │
├─────────────────┼──────────────────┼──────────────────────────────────┤
│ Payment Card    │ PCI_DSS          │ - Encryption required            │
│ Data            │                  │ - Limited storage                │
│                 │                  │ - Quarterly audits               │
├─────────────────┼──────────────────┼──────────────────────────────────┤
│ Financial       │ SOX              │ - Financial accuracy             │
│ Reports         │                  │ - Audit trail                    │
│                 │                  │ - Segregation of duties          │
└─────────────────┴──────────────────┴──────────────────────────────────┘
```

---

## 6. Query Execution Flow with Masking

```
┌────────────────────────────────────────────────────────────────────────────┐
│              QUERY EXECUTION WITH DYNAMIC MASKING FLOW                     │
└────────────────────────────────────────────────────────────────────────────┘

USER INITIATES QUERY
────────────────────
┌──────────────────────────────────────────────────────────────┐
│  User: john.doe@company.com                                  │
│  Role: DATA_ANALYST                                          │
│  Query: SELECT name, email, phone, salary FROM employees;   │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     v
SNOWFLAKE QUERY PROCESSING
───────────────────────────
┌──────────────────────────────────────────────────────────────┐
│  Step 1: Parse Query                                         │
│  ┌────────────────────────────────────────┐                  │
│  │ Identify tables: employees             │                  │
│  │ Identify columns: name, email, phone,  │                  │
│  │                   salary                │                  │
│  └────────────────────────────────────────┘                  │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     v
┌──────────────────────────────────────────────────────────────┐
│  Step 2: Authorization Check                                 │
│  ┌────────────────────────────────────────┐                  │
│  │ Check privileges for DATA_ANALYST:     │                  │
│  │ ✓ USAGE on database                    │                  │
│  │ ✓ USAGE on schema                      │                  │
│  │ ✓ SELECT on employees table            │                  │
│  │ Result: AUTHORIZED                     │                  │
│  └────────────────────────────────────────┘                  │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     v
┌──────────────────────────────────────────────────────────────┐
│  Step 3: Tag Detection                                       │
│  ┌────────────────────────────────────────┐                  │
│  │ Query tag metadata:                    │                  │
│  │ - name column:   pii_name              │                  │
│  │ - email column:  pii_email             │                  │
│  │ - phone column:  pii_phone             │                  │
│  │ - salary column: data_sensitivity      │                  │
│  │                  = CONFIDENTIAL        │                  │
│  └────────────────────────────────────────┘                  │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     v
┌──────────────────────────────────────────────────────────────┐
│  Step 4: Policy Lookup                                       │
│  ┌────────────────────────────────────────┐                  │
│  │ Find associated masking policies:      │                  │
│  │ - pii_name      → mask_name            │                  │
│  │ - pii_email     → mask_email           │                  │
│  │ - pii_phone     → mask_phone           │                  │
│  │ - CONFIDENTIAL  → mask_financial_amount│                  │
│  └────────────────────────────────────────┘                  │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     v
┌──────────────────────────────────────────────────────────────┐
│  Step 5: Policy Evaluation                                   │
│  ┌─────────────────────────────────────────────────┐         │
│  │ For each column, evaluate masking policy:       │         │
│  │                                                  │         │
│  │ NAME column (mask_name):                        │         │
│  │   CASE                                           │         │
│  │     WHEN CURRENT_ROLE() = 'SYSADMIN' THEN val   │         │
│  │     WHEN CURRENT_ROLE() = 'DATA_ANALYST' THEN   │         │
│  │       '*** ***'                                  │         │
│  │   END                                            │         │
│  │   Result: Apply mask → '*** ***'                │         │
│  │                                                  │         │
│  │ EMAIL column (mask_email):                      │         │
│  │   CASE                                           │         │
│  │     WHEN CURRENT_ROLE() = 'SYSADMIN' THEN val   │         │
│  │     WHEN CURRENT_ROLE() = 'DATA_ANALYST' THEN   │         │
│  │       '***@' || SPLIT_PART(val, '@', 2)         │         │
│  │   END                                            │         │
│  │   Result: Apply mask → '***@company.com'        │         │
│  │                                                  │         │
│  │ PHONE column (mask_phone):                      │         │
│  │   CASE                                           │         │
│  │     WHEN CURRENT_ROLE() = 'SYSADMIN' THEN val   │         │
│  │     WHEN CURRENT_ROLE() = 'DATA_ANALYST' THEN   │         │
│  │       'XXX-XXX-XXXX'                             │         │
│  │   END                                            │         │
│  │   Result: Apply mask → 'XXX-XXX-XXXX'           │         │
│  │                                                  │         │
│  │ SALARY column (mask_financial_amount):          │         │
│  │   CASE                                           │         │
│  │     WHEN CURRENT_ROLE() = 'SYSADMIN' THEN val   │         │
│  │     WHEN CURRENT_ROLE() = 'DATA_ANALYST' THEN   │         │
│  │       ROUND(val, -4)                             │         │
│  │   END                                            │         │
│  │   Result: Apply mask → 80000 (from 85000)       │         │
│  └─────────────────────────────────────────────────┘         │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     v
┌──────────────────────────────────────────────────────────────┐
│  Step 6: Query Execution                                     │
│  ┌────────────────────────────────────────┐                  │
│  │ Execute query with transformations:    │                  │
│  │                                        │                  │
│  │ SELECT                                 │                  │
│  │   '*** ***' AS name,                   │                  │
│  │   '***@company.com' AS email,          │                  │
│  │   'XXX-XXX-XXXX' AS phone,             │                  │
│  │   ROUND(salary, -4) AS salary          │                  │
│  │ FROM employees;                        │                  │
│  └────────────────────────────────────────┘                  │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     v
┌──────────────────────────────────────────────────────────────┐
│  Step 7: Return Results                                      │
│  ┌────────────────────────────────────────┐                  │
│  │ Result Set (Masked):                   │                  │
│  │ ┌────────┬──────────────┬────────────┬────────┐          │
│  │ │ NAME   │ EMAIL        │ PHONE      │ SALARY │          │
│  │ ├────────┼──────────────┼────────────┼────────┤          │
│  │ │ *** ***│***@company.  │XXX-XXX-XXXX│ 80000  │          │
│  │ │ *** ***│***@company.  │XXX-XXX-XXXX│ 90000  │          │
│  │ │ *** ***│***@company.  │XXX-XXX-XXXX│ 70000  │          │
│  │ └────────┴──────────────┴────────────┴────────┘          │
│  │                                        │                  │
│  │ User sees masked data only             │                  │
│  └────────────────────────────────────────┘                  │
└──────────────────────────────────────────────────────────────┘

COMPARISON: SAME QUERY, DIFFERENT ROLE
───────────────────────────────────────

If same query executed by SYSADMIN:
┌──────────────────────────────────────────────────────────────┐
│  Role: SYSADMIN                                              │
│  ┌────────────────────────────────────────┐                  │
│  │ Result Set (Unmasked):                 │                  │
│  │ ┌──────────┬────────────────┬──────────────┬────────┐    │
│  │ │ NAME     │ EMAIL          │ PHONE        │ SALARY │    │
│  │ ├──────────┼────────────────┼──────────────┼────────┤    │
│  │ │ John Doe │john@company.com│555-123-4567  │ 85000  │    │
│  │ │ Jane S.  │jane@company.com│555-987-6543  │ 92000  │    │
│  │ │ Bob Lee  │bob@company.com │555-456-7890  │ 73000  │    │
│  │ └──────────┴────────────────┴──────────────┴────────┘    │
│  │                                        │                  │
│  │ SYSADMIN sees all data unmasked        │                  │
│  └────────────────────────────────────────┘                  │
└──────────────────────────────────────────────────────────────┘
```

---

## 7. Key Success Metrics Dashboard

```
┌────────────────────────────────────────────────────────────────────────────┐
│                    GOVERNANCE HEALTH DASHBOARD                             │
│                        Key Performance Indicators                          │
└────────────────────────────────────────────────────────────────────────────┘

SECURITY METRICS
────────────────
┌──────────────────────────────────────────────────────────────┐
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Users with ACCOUNTADMIN Role              │ Target: ≤3 │  │
│  │ ████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░    │ Actual: 2  │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ MFA Enabled for Admin Users               │ Target:100%│  │
│  │ ██████████████████████████████████████████ │ Actual:100%│  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Failed Access Attempts (This Week)        │ Target: <10│  │
│  │ ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ │ Actual: 3  │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘

TAG COVERAGE METRICS
────────────────────
┌──────────────────────────────────────────────────────────────┐
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Tables Tagged with Sensitivity        │ Target: >95%   │  │
│  │ ████████████████████████████████████░░ │ Actual: 92%   │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ PII Columns Identified and Tagged     │ Target: >90%   │  │
│  │ ██████████████████████████████████████ │ Actual: 96%   │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Columns with Masking Policies         │ Target: >90%   │  │
│  │ ████████████████████████████████████░░ │ Actual: 88%   │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘

COMPLIANCE METRICS
──────────────────
┌──────────────────────────────────────────────────────────────┐
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Access Review Completion Rate         │ Target: 100%   │  │
│  │ ██████████████████████████████████████ │ Actual: 100%  │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ GDPR-Required Tags Applied            │ Target: 100%   │  │
│  │ ██████████████████████████████████████ │ Actual: 100%  │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Policy Exception Requests (Monthly)   │ Target: <5     │  │
│  │ ██░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ │ Actual: 2     │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘

OPERATIONAL METRICS
───────────────────
┌──────────────────────────────────────────────────────────────┐
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Time to Provision New User            │ Target: <2hrs  │  │
│  │ ████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ │ Actual: 1.5hrs│  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Average Access Request Time           │ Target: <3days │  │
│  │ ██████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░ │ Actual: 2.3day│  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Role Documentation Up-to-Date         │ Target: 100%   │  │
│  │ ███████████████████████████████████░░░ │ Actual: 94%   │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘

SUMMARY STATISTICS
──────────────────
┌─────────────────────┬─────────────────────────────────────────┐
│ Metric              │ Count                                   │
├─────────────────────┼─────────────────────────────────────────┤
│ Total Users         │ 847                                     │
│ Custom Roles        │ 76 (39 functional + 37 access)          │
│ Tagged Objects      │ 12,456                                  │
│ Active Policies     │ 21                                      │
│ Databases Covered   │ 15                                      │
│ PII Columns         │ 1,234                                   │
└─────────────────────┴─────────────────────────────────────────┘

ALERTS & ACTIONS REQUIRED
──────────────────────────
⚠️  HIGH PRIORITY
    └─> 8% of sensitive tables still untagged
        Action: Schedule tagging sprint for next week

⚠️  MEDIUM PRIORITY
    └─> 6% of role documentation outdated
        Action: Quarterly documentation review scheduled

✓   NO CRITICAL ISSUES
```

---

## Conclusion

These visual flow diagrams provide a comprehensive view of the RBAC and Tagging implementation architecture. Use these diagrams for:

- **Stakeholder presentations**
- **Training sessions**
- **Documentation reference**
- **Troubleshooting guides**
- **Audit preparation**

For detailed implementation instructions, refer to the main documentation and SQL scripts repository.

---

**Document End**