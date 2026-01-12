# Snowflake Security & Data Protection Guide

## Part 1: Access Control, Authentication, Network & Data Security

**Version:** 2025.1  
**Scope:** Multi-Customer Educational Documentation  
**Editions Covered:** Standard, Enterprise, Business Critical

---

# Section 1: Access Control Framework

## 1.1 Access Control Overview

### Overview
Snowflake implements a comprehensive access control framework combining Discretionary Access Control (DAC) and Role-Based Access Control (RBAC). This dual approach provides granular control over who can access data and what operations they can perform, while maintaining administrative flexibility.

### How It Works

Snowflake's access control combines three models:

| Model | Description | Key Characteristic |
|-------|-------------|-------------------|
| **Discretionary Access Control (DAC)** | Object owners control access to their objects | Owners grant privileges to other roles |
| **Role-Based Access Control (RBAC)** | Privileges assigned to roles, roles assigned to users | Users inherit privileges from active role |
| **User-Based Access Control (UBAC)** | Privileges can be assigned directly to users | Used with secondary roles (USE SECONDARY ROLES ALL) |

**Key Principles:**
- Every securable object has exactly one owner (a role)
- Privileges are granted to roles, not directly to users (recommended)
- Users can have multiple roles assigned
- One primary role is active per session (plus optional secondary roles)
- Roles can inherit from other roles through hierarchy

### Access Control Relationship Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    ACCESS CONTROL FRAMEWORK                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌──────────┐         OWNERSHIP (DAC)        ┌──────────┐      │
│   │  ROLE 1  │ ──────────────────────────────>│ OBJECT 1 │      │
│   │ (Owner)  │                                │          │      │
│   └────┬─────┘                                └──────────┘      │
│        │                                                         │
│        │ GRANT PRIVILEGES                                        │
│        v                                                         │
│   ┌──────────┐                                                   │
│   │  ROLE 2  │ <─── RBAC: Privileges on Object 1                │
│   └────┬─────┘                                                   │
│        │                                                         │
│        │ GRANT ROLE                                              │
│        v                                                         │
│   ┌──────────┐    ┌──────────┐                                  │
│   │  USER 1  │    │  USER 2  │  <─── Users inherit privileges   │
│   └──────────┘    └──────────┘                                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Securable Objects Hierarchy

```
┌─────────────────────────────────────────────────────────────────┐
│                         ORGANIZATION                             │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                        ACCOUNT                             │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │  │
│  │  │   USERS     │  │   ROLES     │  │ WAREHOUSES  │        │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘        │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │                    DATABASE                          │  │  │
│  │  │  ┌───────────────────────────────────────────────┐  │  │  │
│  │  │  │                   SCHEMA                       │  │  │  │
│  │  │  │  ┌─────────┐ ┌─────────┐ ┌─────────┐          │  │  │  │
│  │  │  │  │ TABLES  │ │ VIEWS   │ │ STAGES  │ ...      │  │  │  │
│  │  │  │  └─────────┘ └─────────┘ └─────────┘          │  │  │  │
│  │  │  └───────────────────────────────────────────────┘  │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| RBAC & DAC | ✓ | ✓ | ✓ |
| System-Defined Roles | ✓ | ✓ | ✓ |
| Custom Roles | ✓ | ✓ | ✓ |
| Role Hierarchy | ✓ | ✓ | ✓ |
| Future Grants | ✓ | ✓ | ✓ |
| Database Roles | ✓ | ✓ | ✓ |

### Documentation Reference
https://docs.snowflake.com/en/user-guide/security-access-control-overview

---

## 1.2 Users

### Overview
Users represent individual identities that can authenticate to Snowflake. Each user has configurable properties controlling authentication, default session settings, and account status.

### Key User Properties

| Property | Description | Example Value |
|----------|-------------|---------------|
| LOGIN_NAME | Username for authentication | `my_user` |
| DISPLAY_NAME | Friendly name for UI | `John Smith` |
| EMAIL | User email address | `john@company.com` |
| DEFAULT_ROLE | Role activated on login | `DATA_ANALYST` |
| DEFAULT_WAREHOUSE | Warehouse for session | `ANALYTICS_WH` |
| DEFAULT_NAMESPACE | Default database.schema | `PROD_DB.PUBLIC` |
| MUST_CHANGE_PASSWORD | Force password reset | TRUE/FALSE |
| DISABLED | Account enabled/disabled | TRUE/FALSE |
| DAYS_TO_EXPIRY | Password expiration | 90 |
| MINS_TO_UNLOCK | Lockout duration | 30 |

### SQL Examples

```sql
-- Create a new user with essential properties
CREATE USER MY_USER
  PASSWORD = 'ChangeMe123!'
  LOGIN_NAME = 'my_user'
  DISPLAY_NAME = 'Application User'
  EMAIL = 'user@company.com'
  DEFAULT_ROLE = DATA_ANALYST
  DEFAULT_WAREHOUSE = ANALYTICS_WH
  DEFAULT_NAMESPACE = PROD_DB.PUBLIC
  MUST_CHANGE_PASSWORD = TRUE
  COMMENT = 'Created for analytics team';

-- Create service account (no password, key-pair auth)
CREATE USER MY_SERVICE_ACCOUNT
  LOGIN_NAME = 'svc_etl_pipeline'
  DISPLAY_NAME = 'ETL Service Account'
  DEFAULT_ROLE = ETL_ROLE
  DEFAULT_WAREHOUSE = ETL_WH
  TYPE = SERVICE
  COMMENT = 'Service account for ETL processes';

-- Modify user properties
ALTER USER MY_USER SET
  DEFAULT_WAREHOUSE = NEW_WAREHOUSE
  DAYS_TO_EXPIRY = 90;

-- Reset user password
ALTER USER MY_USER SET PASSWORD = 'NewSecurePassword456!';

-- Force password change on next login
ALTER USER MY_USER SET MUST_CHANGE_PASSWORD = TRUE;

-- Disable user account (soft delete)
ALTER USER MY_USER SET DISABLED = TRUE;

-- Re-enable user account
ALTER USER MY_USER SET DISABLED = FALSE;

-- Drop user (permanent)
DROP USER MY_USER;

-- View all users
SHOW USERS;

-- View specific user details
DESCRIBE USER MY_USER;

-- Query user information from ACCOUNT_USAGE
SELECT 
  NAME,
  LOGIN_NAME,
  DISPLAY_NAME,
  EMAIL,
  DEFAULT_ROLE,
  DISABLED,
  LAST_SUCCESS_LOGIN,
  CREATED_ON
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE DELETED_ON IS NULL
ORDER BY CREATED_ON DESC;
```

### Best Practices

1. **Use descriptive LOGIN_NAME values** - Makes audit logs readable and user management easier
2. **Always set DEFAULT_ROLE** - Prevents users defaulting to PUBLIC role
3. **Disable rather than drop** - Preserves audit history and allows recovery
4. **Set password expiration** - Enforce periodic credential rotation
5. **Use service accounts for automation** - Separate human and machine identities

### Documentation Reference
https://docs.snowflake.com/en/user-guide/admin-user-management

---

## 1.3 Roles

### Overview
Roles are the primary mechanism for managing access in Snowflake. Privileges are granted to roles, and roles are granted to users. Snowflake provides system-defined roles with specific purposes and supports custom role creation for organizational needs.

### System-Defined Roles

| Role | Purpose | Key Privileges |
|------|---------|---------------|
| **ORGADMIN** | Organization-level administration | Manage accounts, view org usage |
| **ACCOUNTADMIN** | Top-level account role | SYSADMIN + SECURITYADMIN combined |
| **SECURITYADMIN** | Security administration | MANAGE GRANTS, user/role management |
| **USERADMIN** | User and role management | CREATE USER, CREATE ROLE |
| **SYSADMIN** | Object administration | Create warehouses, databases, schemas |
| **PUBLIC** | Default role for all users | Minimal privileges, granted to everyone |

### Role Hierarchy Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    RECOMMENDED ROLE HIERARCHY                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│                      ┌──────────────┐                           │
│                      │ ACCOUNTADMIN │ (Use sparingly, MFA req)  │
│                      └──────┬───────┘                           │
│               ┌─────────────┴─────────────┐                     │
│               v                           v                      │
│      ┌──────────────┐            ┌──────────────┐               │
│      │ SECURITYADMIN│            │   SYSADMIN   │               │
│      └──────┬───────┘            └──────┬───────┘               │
│             │                           │                        │
│             v                           │                        │
│      ┌──────────────┐                   │                        │
│      │  USERADMIN   │                   │                        │
│      └──────────────┘                   │                        │
│                                         │                        │
│                    ┌────────────────────┼────────────────────┐   │
│                    v                    v                    v   │
│           ┌──────────────┐     ┌──────────────┐    ┌──────────┐ │
│           │ CUSTOM_ROLE_1│     │ CUSTOM_ROLE_2│    │CUSTOM_..│  │
│           └──────────────┘     └──────────────┘    └──────────┘ │
│                    │                    │                        │
│                    v                    v                        │
│           ┌──────────────┐     ┌──────────────┐                 │
│           │ DB_ROLE_READ │     │ DB_ROLE_WRITE│ (Database Roles)│
│           └──────────────┘     └──────────────┘                 │
│                                                                  │
│                         ┌────────┐                               │
│                         │ PUBLIC │ (All users, minimal access)  │
│                         └────────┘                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Role Design Patterns

**Pattern 1: Functional Roles**
```
DATA_ENGINEER     - Build and maintain data pipelines
DATA_ANALYST      - Query and analyze data
DATA_SCIENTIST    - Advanced analytics and ML
BI_DEVELOPER      - Build reports and dashboards
```

**Pattern 2: Access Roles**
```
PROD_DB_READ      - Read access to production
PROD_DB_WRITE     - Write access to production
PROD_DB_ADMIN     - Admin access to production
```

**Pattern 3: Combined (Recommended)**
```
Access Roles: Grant object privileges
Functional Roles: Inherit from access roles
Users: Assigned functional roles only
```

### SQL Examples

```sql
-- Create custom role
CREATE ROLE DATA_ANALYST
  COMMENT = 'Role for data analysts with read access';

-- Create access role
CREATE ROLE PROD_DB_READ
  COMMENT = 'Read access to production database';

-- Build role hierarchy (custom roles -> SYSADMIN)
GRANT ROLE PROD_DB_READ TO ROLE DATA_ANALYST;
GRANT ROLE DATA_ANALYST TO ROLE SYSADMIN;

-- Grant role to user
GRANT ROLE DATA_ANALYST TO USER MY_USER;

-- Grant role to another role (hierarchy)
GRANT ROLE JUNIOR_ANALYST TO ROLE SENIOR_ANALYST;

-- View all roles
SHOW ROLES;

-- View roles granted to a user
SHOW GRANTS TO USER MY_USER;

-- View privileges granted to a role
SHOW GRANTS TO ROLE DATA_ANALYST;

-- View roles that a role is granted to
SHOW GRANTS OF ROLE DATA_ANALYST;

-- View role hierarchy
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE GRANTED_ON = 'ROLE'
  AND DELETED_ON IS NULL
ORDER BY GRANTEE_NAME;

-- Create database role (scoped to single database)
USE DATABASE MY_DATABASE;
CREATE DATABASE ROLE DB_READER
  COMMENT = 'Read access within this database only';

-- Grant database role to account role
GRANT DATABASE ROLE MY_DATABASE.DB_READER TO ROLE DATA_ANALYST;
```

### Best Practices

1. **Never use ACCOUNTADMIN for daily work** - Reserve for administrative tasks only
2. **Require MFA for ACCOUNTADMIN** - Protect highest-privilege account
3. **Grant custom roles to SYSADMIN** - Ensures administrative oversight
4. **Use database roles for database-scoped access** - Improves security isolation
5. **Document role purposes** - Use COMMENT field for clarity
6. **Review role assignments regularly** - Remove unnecessary grants

### Why This Matters
Proper role design prevents privilege escalation, simplifies access management, and enables audit compliance. The hierarchy ensures that SYSADMIN can manage all objects while SECURITYADMIN controls security-related changes.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/security-access-control-overview

---

## 1.4 Privileges

### Overview
Privileges define what actions can be performed on securable objects. Snowflake uses a grant-based model where privileges must be explicitly granted—no access exists by default (deny by default).

### Privilege Categories

**Account-Level Privileges:**
| Privilege | Description |
|-----------|-------------|
| CREATE DATABASE | Create databases in account |
| CREATE WAREHOUSE | Create virtual warehouses |
| CREATE ROLE | Create new roles |
| CREATE USER | Create new users |
| MANAGE GRANTS | Grant/revoke any privilege |
| MONITOR USAGE | View account usage data |
| EXECUTE TASK | Execute tasks in account |
| APPLY MASKING POLICY | Apply masking policies |
| APPLY ROW ACCESS POLICY | Apply row access policies |

**Database-Level Privileges:**
| Privilege | Description |
|-----------|-------------|
| USAGE | Access database and schemas |
| CREATE SCHEMA | Create schemas in database |
| IMPORTED PRIVILEGES | Access shared database |
| MODIFY | Modify database properties |
| MONITOR | Monitor database |
| OWNERSHIP | Full control (one owner) |

**Schema-Level Privileges:**
| Privilege | Description |
|-----------|-------------|
| USAGE | Access objects in schema |
| CREATE TABLE | Create tables |
| CREATE VIEW | Create views |
| CREATE STAGE | Create stages |
| CREATE FUNCTION | Create UDFs |
| CREATE PROCEDURE | Create stored procedures |
| OWNERSHIP | Full control |

**Object-Level Privileges (Table/View):**
| Privilege | Description |
|-----------|-------------|
| SELECT | Query data |
| INSERT | Add rows |
| UPDATE | Modify rows |
| DELETE | Remove rows |
| TRUNCATE | Remove all rows |
| REFERENCES | Create foreign keys |
| OWNERSHIP | Full control |

### SQL Examples

```sql
-- Grant database usage
GRANT USAGE ON DATABASE MY_DATABASE TO ROLE DATA_ANALYST;

-- Grant schema usage
GRANT USAGE ON SCHEMA MY_DATABASE.MY_SCHEMA TO ROLE DATA_ANALYST;

-- Grant SELECT on specific table
GRANT SELECT ON TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE TO ROLE DATA_ANALYST;

-- Grant SELECT on all tables in schema
GRANT SELECT ON ALL TABLES IN SCHEMA MY_DATABASE.MY_SCHEMA TO ROLE DATA_ANALYST;

-- Grant with GRANT OPTION (delegate granting)
GRANT SELECT ON TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE 
  TO ROLE DATA_ANALYST
  WITH GRANT OPTION;

-- Revoke privilege
REVOKE SELECT ON TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE FROM ROLE DATA_ANALYST;

-- FUTURE GRANTS: Auto-grant on new objects
GRANT SELECT ON FUTURE TABLES IN SCHEMA MY_DATABASE.MY_SCHEMA TO ROLE DATA_ANALYST;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE MY_DATABASE TO ROLE DATA_ANALYST;

-- View future grants
SHOW FUTURE GRANTS IN SCHEMA MY_DATABASE.MY_SCHEMA;

-- Transfer ownership
GRANT OWNERSHIP ON TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE 
  TO ROLE NEW_OWNER_ROLE
  REVOKE CURRENT GRANTS;

-- View grants on an object
SHOW GRANTS ON TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE;

-- View all grants to a role
SHOW GRANTS TO ROLE DATA_ANALYST;

-- Comprehensive privilege audit query
SELECT 
  PRIVILEGE,
  GRANTED_ON,
  NAME AS OBJECT_NAME,
  GRANTEE_NAME,
  GRANT_OPTION,
  CREATED_ON
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE GRANTEE_NAME = 'DATA_ANALYST'
  AND DELETED_ON IS NULL
ORDER BY GRANTED_ON, NAME;
```

### Best Practices

1. **Use Future Grants** - Automates privilege management for new objects
2. **Grant to roles, not users** - Simplifies management and audit
3. **Apply least privilege principle** - Grant only what's needed
4. **Use WITH GRANT OPTION sparingly** - Limits privilege delegation
5. **Regular privilege reviews** - Audit using ACCOUNT_USAGE views

### Why This Matters
Proper privilege management ensures data security, simplifies compliance audits, and prevents unauthorized access. Future grants eliminate manual work when new objects are created.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/security-access-control-privileges

---

# Section 2: User Authentication

## 2.1 Multi-Factor Authentication (MFA)

### Overview
MFA adds a second authentication factor beyond passwords, significantly reducing the risk of unauthorized access from compromised credentials. Snowflake uses Duo Security for MFA and supports enforcement at account or user level.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| MFA (Optional) | ✓ | ✓ | ✓ |
| MFA Enforcement | ✓ | ✓ | ✓ |
| MFA for ACCOUNTADMIN | ✓ | ✓ | ✓ |

### How It Works

1. User enters username and password
2. Snowflake validates credentials
3. Duo Security sends push notification or requests TOTP code
4. User approves or enters code
5. Session established

### SQL Examples

```sql
-- Check MFA status for users
SELECT 
  NAME,
  LOGIN_NAME,
  EXT_AUTHN_DUO,
  HAS_MFA
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE DELETED_ON IS NULL;

-- Enforce MFA at account level (all users)
ALTER ACCOUNT SET REQUIRE_MFA = TRUE;

-- Set MFA bypass timeout (minutes a user can bypass after successful MFA)
ALTER USER MY_USER SET MINS_TO_BYPASS_MFA = 60;

-- View authentication policies
SHOW AUTHENTICATION POLICIES;

-- Create authentication policy requiring MFA
CREATE AUTHENTICATION POLICY MFA_REQUIRED_POLICY
  AUTHENTICATION_METHODS = ('PASSWORD')
  MFA_AUTHENTICATION_METHODS = ('TOTP')
  CLIENT_TYPES = ('SNOWFLAKE_UI', 'SNOWSQL')
  SECURITY_INTEGRATIONS = ()
  COMMENT = 'Requires MFA for all logins';

-- Apply authentication policy to account
ALTER ACCOUNT SET AUTHENTICATION POLICY = MFA_REQUIRED_POLICY;

-- Apply authentication policy to specific user
ALTER USER MY_USER SET AUTHENTICATION POLICY = MFA_REQUIRED_POLICY;
```

### Best Practices

1. **Enforce MFA for ACCOUNTADMIN** - Highest-priority security control
2. **Enforce MFA for all privileged roles** - SECURITYADMIN, SYSADMIN
3. **Consider MFA for all users** - Especially for cloud-only deployments
4. **Set reasonable bypass timeout** - Balance security with usability
5. **Monitor MFA enrollment** - Track users without MFA enabled

### Why This Matters
Passwords alone are insufficient protection. MFA blocks 99.9% of account compromise attacks according to Microsoft research. For privileged accounts, MFA is essential.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/security-mfa

---

## 2.2 Federated Authentication (SSO/SAML)

### Overview
Federated authentication enables Single Sign-On (SSO) using SAML 2.0, allowing users to authenticate via corporate Identity Providers (IdP) like Okta, Azure AD, or OneLogin. Users authenticate once and access Snowflake without separate credentials.

### Supported Identity Providers
- Okta
- Microsoft Azure AD / Entra ID
- OneLogin
- PingFederate
- ADFS (Active Directory Federation Services)
- Any SAML 2.0 compliant IdP

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| SAML 2.0 SSO | ✓ | ✓ | ✓ |
| Multiple IdPs | ✓ | ✓ | ✓ |
| IdP-initiated SSO | ✓ | ✓ | ✓ |
| SP-initiated SSO | ✓ | ✓ | ✓ |

### SQL Examples

```sql
-- Create SAML2 security integration
CREATE SECURITY INTEGRATION MY_SSO_INTEGRATION
  TYPE = SAML2
  ENABLED = TRUE
  SAML2_ISSUER = 'https://idp.company.com/saml'
  SAML2_SSO_URL = 'https://idp.company.com/saml/sso'
  SAML2_PROVIDER = 'OKTA'
  SAML2_X509_CERT = '-----BEGIN CERTIFICATE-----
MIIDpDCCAoygAwIBAgIGAX...
-----END CERTIFICATE-----'
  SAML2_SP_INITIATED_LOGIN_PAGE_LABEL = 'Company SSO'
  SAML2_ENABLE_SP_INITIATED = TRUE
  SAML2_SNOWFLAKE_ACS_URL = 'https://myaccount.snowflakecomputing.com/fed/login'
  SAML2_SNOWFLAKE_ISSUER_URL = 'https://myaccount.snowflakecomputing.com';

-- Describe integration to get SP metadata
DESCRIBE SECURITY INTEGRATION MY_SSO_INTEGRATION;

-- View all security integrations
SHOW SECURITY INTEGRATIONS;

-- Modify integration
ALTER SECURITY INTEGRATION MY_SSO_INTEGRATION SET ENABLED = FALSE;

-- Drop integration
DROP SECURITY INTEGRATION MY_SSO_INTEGRATION;

-- Monitor SSO logins
SELECT 
  USER_NAME,
  FIRST_AUTHENTICATION_FACTOR,
  SECOND_AUTHENTICATION_FACTOR,
  CLIENT_IP,
  EVENT_TIMESTAMP
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE FIRST_AUTHENTICATION_FACTOR = 'SAML2'
ORDER BY EVENT_TIMESTAMP DESC
LIMIT 100;
```

### Best Practices

1. **Use SSO for all human users** - Centralizes identity management
2. **Configure IdP-initiated and SP-initiated** - Supports both login flows
3. **Map IdP groups to Snowflake roles** - Use SCIM for automation
4. **Test thoroughly before enforcement** - Validate with pilot users
5. **Maintain emergency access** - Keep local admin account for recovery

### Why This Matters
SSO improves security by centralizing authentication, enables immediate access revocation when employees leave, and improves user experience with single login.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-overview

---

## 2.3 Key Pair Authentication

### Overview
Key pair authentication uses RSA public/private key pairs instead of passwords. The private key remains on the client, and only the public key is stored in Snowflake. This eliminates password transmission and is ideal for service accounts and automation.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Key Pair Auth | ✓ | ✓ | ✓ |
| Dual Key Support | ✓ | ✓ | ✓ |
| Key Rotation | ✓ | ✓ | ✓ |

### Key Generation (OpenSSL Commands)

```bash
# Generate 4096-bit RSA private key (recommended)
openssl genrsa 4096 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Generate public key from private key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# View public key (copy for Snowflake)
cat rsa_key.pub
```

### SQL Examples

```sql
-- Assign public key to user
ALTER USER MY_SERVICE_ACCOUNT SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqh...';

-- Assign second public key (for rotation)
ALTER USER MY_SERVICE_ACCOUNT SET RSA_PUBLIC_KEY_2 = 'MIIBIjANBgkqh...';

-- Verify key assignment
DESCRIBE USER MY_SERVICE_ACCOUNT;

-- Remove old key after rotation
ALTER USER MY_SERVICE_ACCOUNT UNSET RSA_PUBLIC_KEY;

-- Rename second key to primary after rotation
-- (Requires unset primary first, then set new primary)
ALTER USER MY_SERVICE_ACCOUNT UNSET RSA_PUBLIC_KEY;
ALTER USER MY_SERVICE_ACCOUNT SET RSA_PUBLIC_KEY = 'new_key_value';

-- Query key fingerprints
SELECT 
  NAME,
  RSA_PUBLIC_KEY_FP,
  RSA_PUBLIC_KEY_2_FP
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE NAME = 'MY_SERVICE_ACCOUNT';
```

### Key Rotation Process

```
┌─────────────────────────────────────────────────────────────────┐
│                    KEY ROTATION WORKFLOW                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Step 1: Generate new key pair                                  │
│          openssl genrsa 4096 | openssl pkcs8 ...                │
│                                                                  │
│  Step 2: Add new public key as RSA_PUBLIC_KEY_2                 │
│          ALTER USER ... SET RSA_PUBLIC_KEY_2 = '...'            │
│                                                                  │
│  Step 3: Update clients to use new private key                  │
│          (Both keys work during transition)                     │
│                                                                  │
│  Step 4: Verify all clients using new key                       │
│          Check LOGIN_HISTORY for authentication method          │
│                                                                  │
│  Step 5: Remove old key                                         │
│          ALTER USER ... UNSET RSA_PUBLIC_KEY                    │
│                                                                  │
│  Step 6: Move RSA_PUBLIC_KEY_2 to RSA_PUBLIC_KEY                │
│          (For next rotation cycle)                              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Best Practices

1. **Use 4096-bit keys** - Stronger than minimum 2048-bit
2. **Rotate keys annually** - Or per security policy
3. **Store private keys securely** - Use secrets manager (Vault, AWS Secrets Manager)
4. **Never commit private keys** - Add to .gitignore
5. **Use dual keys for rotation** - Zero-downtime key changes

### Why This Matters
Key pair authentication eliminates password exposure in code, enables secure automation, and provides stronger authentication than passwords alone. Essential for CI/CD pipelines and service accounts.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/key-pair-auth

---

## 2.4 OAuth

### Overview
OAuth enables token-based authentication for applications and BI tools. Snowflake supports both native Snowflake OAuth and External OAuth with providers like Azure AD, Okta, and others.

### OAuth Types

| Type | Description | Use Case |
|------|-------------|----------|
| **Snowflake OAuth** | Built-in OAuth implementation | BI tools, custom apps |
| **External OAuth** | Integration with external IdPs | Enterprise SSO apps |

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Snowflake OAuth | ✓ | ✓ | ✓ |
| External OAuth | ✓ | ✓ | ✓ |

### SQL Examples

```sql
-- Create Snowflake OAuth integration
CREATE SECURITY INTEGRATION MY_OAUTH_INTEGRATION
  TYPE = OAUTH
  ENABLED = TRUE
  OAUTH_CLIENT = CUSTOM
  OAUTH_CLIENT_TYPE = 'CONFIDENTIAL'
  OAUTH_REDIRECT_URI = 'https://myapp.company.com/oauth/callback'
  OAUTH_ISSUE_REFRESH_TOKENS = TRUE
  OAUTH_REFRESH_TOKEN_VALIDITY = 86400
  COMMENT = 'OAuth for internal application';

-- Create External OAuth integration (Azure AD example)
CREATE SECURITY INTEGRATION MY_AZURE_OAUTH
  TYPE = EXTERNAL_OAUTH
  ENABLED = TRUE
  EXTERNAL_OAUTH_TYPE = AZURE
  EXTERNAL_OAUTH_ISSUER = 'https://sts.windows.net/tenant-id/'
  EXTERNAL_OAUTH_JWS_KEYS_URL = 'https://login.microsoftonline.com/tenant-id/discovery/v2.0/keys'
  EXTERNAL_OAUTH_AUDIENCE_LIST = ('https://myaccount.snowflakecomputing.com')
  EXTERNAL_OAUTH_TOKEN_USER_MAPPING_CLAIM = 'upn'
  EXTERNAL_OAUTH_SNOWFLAKE_USER_MAPPING_ATTRIBUTE = 'LOGIN_NAME'
  EXTERNAL_OAUTH_ANY_ROLE_MODE = 'ENABLE';

-- View OAuth integrations
SHOW SECURITY INTEGRATIONS;

-- Describe integration details
DESCRIBE SECURITY INTEGRATION MY_OAUTH_INTEGRATION;

-- Monitor OAuth authentications
SELECT 
  USER_NAME,
  FIRST_AUTHENTICATION_FACTOR,
  CLIENT_IP,
  EVENT_TIMESTAMP
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE FIRST_AUTHENTICATION_FACTOR LIKE '%OAUTH%'
ORDER BY EVENT_TIMESTAMP DESC;
```

### Best Practices

1. **Use short token lifetimes** - Reduce exposure from stolen tokens
2. **Enable refresh tokens carefully** - Balance security with usability
3. **Validate audience claims** - Prevent token reuse attacks
4. **Monitor OAuth usage** - Track authentication patterns
5. **Use External OAuth for enterprise apps** - Leverages existing IdP investment

### Documentation Reference
https://docs.snowflake.com/en/user-guide/oauth-intro

---

## 2.5 SCIM (User Provisioning)

### Overview
SCIM (System for Cross-domain Identity Management) automates user and group provisioning from Identity Providers to Snowflake. When users are added/removed in your IdP, changes automatically sync to Snowflake.

### Supported Providers
- Okta
- Microsoft Azure AD / Entra ID
- OneLogin
- Custom SCIM implementations

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| SCIM Provisioning | ✓ | ✓ | ✓ |
| Group Sync | ✓ | ✓ | ✓ |
| Role Mapping | ✓ | ✓ | ✓ |

### SQL Examples

```sql
-- Create SCIM integration
CREATE SECURITY INTEGRATION MY_SCIM_INTEGRATION
  TYPE = SCIM
  SCIM_CLIENT = 'OKTA'
  RUN_AS_ROLE = 'OKTA_PROVISIONER'
  COMMENT = 'SCIM provisioning from Okta';

-- Create role for SCIM operations
CREATE ROLE OKTA_PROVISIONER;
GRANT CREATE USER ON ACCOUNT TO ROLE OKTA_PROVISIONER;
GRANT CREATE ROLE ON ACCOUNT TO ROLE OKTA_PROVISIONER;
GRANT ROLE OKTA_PROVISIONER TO ROLE ACCOUNTADMIN;

-- Generate SCIM token
-- (Execute in Snowsight or via system function)
SELECT SYSTEM$GENERATE_SCIM_ACCESS_TOKEN('MY_SCIM_INTEGRATION');

-- View SCIM integrations
SHOW SECURITY INTEGRATIONS LIKE '%SCIM%';

-- Monitor SCIM-provisioned users
SELECT 
  NAME,
  LOGIN_NAME,
  CREATED_ON,
  COMMENT
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE COMMENT LIKE '%SCIM%'
   OR COMMENT LIKE '%provisioned%';
```

### SCIM Workflow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                      SCIM PROVISIONING FLOW                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────┐          ┌─────────────┐          ┌──────────┐ │
│  │             │  Create  │             │  SCIM    │          │ │
│  │   HR System │ ──────> │    IdP      │ ──────> │Snowflake │ │
│  │             │  User    │ (Okta/AAD) │  API     │          │ │
│  └─────────────┘          └─────────────┘          └──────────┘ │
│                                                                  │
│  Actions Synced:                                                │
│  • Create users                                                 │
│  • Update user attributes                                       │
│  • Disable/enable users                                         │
│  • Group membership → Role assignment                           │
│  • Delete users (deactivate)                                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Best Practices

1. **Map IdP groups to Snowflake roles** - Automates role assignment
2. **Use dedicated provisioner role** - Limits SCIM permissions
3. **Test with pilot group first** - Validate mapping before full rollout
4. **Monitor sync status** - Check for provisioning errors
5. **Combine with SSO** - Complete identity lifecycle management

### Why This Matters
SCIM eliminates manual user management, ensures immediate access revocation when employees leave, and maintains consistency between IdP and Snowflake.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/scim

---

# Section 3: Network Security

## 3.1 Network Policies

### Overview
Network policies restrict access to Snowflake based on IP addresses. Define allowed and blocked IP ranges to limit connections to trusted networks only. This is a critical control for reducing attack surface.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Network Policies | - | ✓ | ✓ |
| Account-Level Policies | - | ✓ | ✓ |
| User-Level Policies | - | ✓ | ✓ |
| Integration-Level Policies | - | ✓ | ✓ |

### How It Works

- **ALLOWED_IP_LIST**: IP addresses/ranges that CAN connect
- **BLOCKED_IP_LIST**: IP addresses/ranges that CANNOT connect
- **Precedence**: Blocked list takes priority over allowed list
- **CIDR Notation**: Supports IP ranges (e.g., 192.168.1.0/24)

### SQL Examples

```sql
-- Create network policy with allowed IPs
CREATE NETWORK POLICY MY_NETWORK_POLICY
  ALLOWED_IP_LIST = ('192.168.1.0/24', '10.0.0.0/8', '203.0.113.50')
  BLOCKED_IP_LIST = ('192.168.1.100')  -- Block specific IP within allowed range
  COMMENT = 'Corporate network access only';

-- Apply network policy to account (all users)
ALTER ACCOUNT SET NETWORK_POLICY = MY_NETWORK_POLICY;

-- Apply network policy to specific user
ALTER USER MY_USER SET NETWORK_POLICY = MY_NETWORK_POLICY;

-- View network policies
SHOW NETWORK POLICIES;

-- Describe specific policy
DESCRIBE NETWORK POLICY MY_NETWORK_POLICY;

-- Modify network policy
ALTER NETWORK POLICY MY_NETWORK_POLICY SET
  ALLOWED_IP_LIST = ('192.168.1.0/24', '10.0.0.0/8', '172.16.0.0/12');

-- Add IP to existing policy
ALTER NETWORK POLICY MY_NETWORK_POLICY SET
  ALLOWED_IP_LIST = ('192.168.1.0/24', '10.0.0.0/8', '172.16.0.0/12', '203.0.113.0/24');

-- Remove network policy from account
ALTER ACCOUNT UNSET NETWORK_POLICY;

-- Drop network policy
DROP NETWORK POLICY MY_NETWORK_POLICY;

-- Monitor blocked connection attempts
SELECT 
  USER_NAME,
  CLIENT_IP,
  ERROR_CODE,
  ERROR_MESSAGE,
  EVENT_TIMESTAMP
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE IS_SUCCESS = 'NO'
  AND ERROR_MESSAGE LIKE '%network policy%'
ORDER BY EVENT_TIMESTAMP DESC;

-- Create network rule for more granular control
CREATE NETWORK RULE CORP_NETWORK_RULE
  TYPE = IPV4
  VALUE_LIST = ('192.168.0.0/16', '10.0.0.0/8')
  MODE = INGRESS
  COMMENT = 'Corporate network ranges';

-- Use network rule in network policy
CREATE NETWORK POLICY RULE_BASED_POLICY
  ALLOWED_NETWORK_RULE_LIST = ('CORP_NETWORK_RULE')
  COMMENT = 'Policy using network rules';
```

### Best Practices

1. **Start with monitoring** - Review LOGIN_HISTORY before enforcing
2. **Include VPN ranges** - Account for remote workers
3. **Maintain admin bypass** - Emergency access IP for recovery
4. **Document all IP ranges** - Track ownership and purpose
5. **Test before account-level enforcement** - Apply to test users first
6. **Plan for cloud services** - Include CI/CD, BI tool IPs

### Why This Matters
Network policies are a fundamental security control that limits the attack surface. Even with stolen credentials, attackers cannot connect from unauthorized networks.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/network-policies

---

## 3.2 Private Connectivity (Business Critical)

### Overview
Private connectivity establishes secure, private network connections between your cloud environment and Snowflake, eliminating exposure to the public internet. Available for AWS PrivateLink, Azure Private Link, and Google Cloud Private Service Connect.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| AWS PrivateLink | - | - | ✓ |
| Azure Private Link | - | - | ✓ |
| GCP Private Service Connect | - | - | ✓ |

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                   PRIVATE CONNECTIVITY ARCHITECTURE              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────────┐          ┌────────────────────┐         │
│  │   Customer VPC     │          │   Snowflake VPC    │         │
│  │   ┌────────────┐   │          │   ┌────────────┐   │         │
│  │   │ Application│   │          │   │  Snowflake │   │         │
│  │   │  Server    │   │          │   │  Service   │   │         │
│  │   └─────┬──────┘   │          │   └─────▲──────┘   │         │
│  │         │          │          │         │          │         │
│  │         │          │          │         │          │         │
│  │   ┌─────▼──────┐   │          │   ┌─────┴──────┐   │         │
│  │   │ VPC        │   │  Private │   │ Endpoint   │   │         │
│  │   │ Endpoint   │──────Link────│   │ Service    │   │         │
│  │   └────────────┘   │ (No Public│   └────────────┘   │         │
│  │                    │ Internet) │                    │         │
│  └────────────────────┘          └────────────────────┘         │
│                                                                  │
│  Traffic stays entirely on cloud provider backbone              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Configuration Steps (High-Level)

1. **Snowflake**: Retrieve PrivateLink configuration details
2. **Customer**: Create VPC endpoint in your cloud environment
3. **Customer**: Configure DNS to resolve Snowflake URLs to private endpoint
4. **Customer**: Create network policy to block public access
5. **Test**: Verify private connectivity works

### SQL Examples

```sql
-- Get PrivateLink configuration (AWS)
SELECT SYSTEM$GET_PRIVATELINK_CONFIG();

-- After PrivateLink is configured, block public access
CREATE NETWORK POLICY PRIVATE_ONLY_POLICY
  ALLOWED_IP_LIST = ('0.0.0.0/0')  -- All IPs via PrivateLink
  BLOCKED_IP_LIST = ()
  COMMENT = 'Allow only PrivateLink connections';

-- Verify connection method
SELECT 
  USER_NAME,
  CLIENT_IP,
  IS_SUCCESS,
  EVENT_TIMESTAMP
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
ORDER BY EVENT_TIMESTAMP DESC
LIMIT 10;
```

### Best Practices

1. **Plan DNS configuration carefully** - Critical for routing
2. **Test before blocking public access** - Ensure private path works
3. **Document endpoint configuration** - For disaster recovery
4. **Monitor connectivity** - Alert on connection failures
5. **Use with network policies** - Defense in depth

### Why This Matters
Private connectivity eliminates data exposure on the public internet, meeting regulatory requirements for sensitive data industries (finance, healthcare, government).

### Documentation Reference
https://docs.snowflake.com/en/user-guide/admin-security-privatelink

---

## 3.3 Session Policies

### Overview
Session policies control session behavior including idle timeout settings. This ensures inactive sessions are terminated, reducing the risk of unauthorized access from unattended workstations.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Session Policies | ✓ | ✓ | ✓ |
| Account-Level Policies | ✓ | ✓ | ✓ |
| User-Level Policies | ✓ | ✓ | ✓ |

### SQL Examples

```sql
-- Create session policy
CREATE SESSION POLICY MY_SESSION_POLICY
  SESSION_IDLE_TIMEOUT_MINS = 30
  SESSION_UI_IDLE_TIMEOUT_MINS = 15
  COMMENT = 'Standard session timeout policy';

-- Apply to account
ALTER ACCOUNT SET SESSION POLICY = MY_SESSION_POLICY;

-- Apply to specific user
ALTER USER MY_USER SET SESSION POLICY = MY_SESSION_POLICY;

-- View session policies
SHOW SESSION POLICIES;

-- Describe policy
DESCRIBE SESSION POLICY MY_SESSION_POLICY;

-- Remove policy from account
ALTER ACCOUNT UNSET SESSION POLICY;

-- Drop policy
DROP SESSION POLICY MY_SESSION_POLICY;
```

### Best Practices

1. **Set appropriate timeouts** - Balance security with usability
2. **Shorter UI timeouts** - Web sessions more vulnerable
3. **Consider user workflows** - Long-running queries may need longer timeouts
4. **Apply account-wide policy** - Consistent baseline
5. **Override for specific needs** - User-level exceptions when justified

### Documentation Reference
https://docs.snowflake.com/en/user-guide/session-policies

---

# Section 4: Data Encryption

## 4.1 Encryption at Rest

### Overview
ALL data in Snowflake is encrypted at rest automatically using AES-256 encryption. This is always-on, requires no configuration, and has zero performance impact. Snowflake uses a hierarchical key model with keys stored in Hardware Security Modules (HSMs).

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| AES-256 Encryption | ✓ | ✓ | ✓ |
| Hierarchical Key Model | ✓ | ✓ | ✓ |
| Automatic Key Rotation | ✓ | ✓ | ✓ |
| Periodic Rekeying | - | ✓ | ✓ |
| HSM-Protected Keys | ✓ | ✓ | ✓ |

### Hierarchical Key Model

```
┌─────────────────────────────────────────────────────────────────┐
│                  SNOWFLAKE KEY HIERARCHY                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Level 1: ROOT KEY                                              │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Hardware Security Module (HSM)               │   │
│  │                   FIPS 140-2 Level 3                      │   │
│  │                     ┌───────────┐                         │   │
│  │                     │ Root Key  │                         │   │
│  │                     └─────┬─────┘                         │   │
│  └───────────────────────────┼──────────────────────────────┘   │
│                              │ encrypts                          │
│                              ▼                                   │
│  Level 2: ACCOUNT MASTER KEYS (One per customer account)        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐                   │   │
│  │  │ AMK-1   │  │ AMK-2   │  │ AMK-3   │  ...              │   │
│  │  │(Acct A) │  │(Acct B) │  │(Acct C) │                   │   │
│  │  └────┬────┘  └────┬────┘  └────┬────┘                   │   │
│  └───────┼────────────┼────────────┼────────────────────────┘   │
│          │ encrypts   │            │                             │
│          ▼                                                       │
│  Level 3: TABLE MASTER KEYS (One per table)                     │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐                   │   │
│  │  │ TMK-1   │  │ TMK-2   │  │ TMK-3   │  ...              │   │
│  │  │(Table 1)│  │(Table 2)│  │(Table 3)│                   │   │
│  │  └────┬────┘  └────┬────┘  └────┬────┘                   │   │
│  └───────┼────────────┼────────────┼────────────────────────┘   │
│          │ encrypts   │            │                             │
│          ▼                                                       │
│  Level 4: FILE KEYS (One per micro-partition)                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                 │   │
│  │  │FK-1 │ │FK-2 │ │FK-3 │ │FK-4 │ │FK-5 │  ...            │   │
│  │  └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘                 │   │
│  └─────┼───────┼───────┼───────┼───────┼────────────────────┘   │
│        │       │       │       │       │                         │
│        ▼       ▼       ▼       ▼       ▼                         │
│     ┌─────────────────────────────────────────┐                 │
│     │        ENCRYPTED DATA FILES             │                 │
│     │     (50-500MB micro-partitions)         │                 │
│     └─────────────────────────────────────────┘                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Key Rotation Process

```
┌─────────────────────────────────────────────────────────────────┐
│                    AUTOMATIC KEY ROTATION                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Month 1          Month 2          Month 3          Month 4     │
│  ┌──────┐         ┌──────┐         ┌──────┐         ┌──────┐    │
│  │TMK v1│ ──────> │TMK v1│ ──────> │TMK v1│ ──────> │      │    │
│  │ACTIVE│         │RETIRE│         │RETIRE│         │DESTROY    │
│  └──────┘         └──────┘         └──────┘         └──────┘    │
│                   ┌──────┐         ┌──────┐         ┌──────┐    │
│                   │TMK v2│ ──────> │TMK v2│ ──────> │TMK v2│    │
│                   │ACTIVE│         │RETIRE│         │RETIRE│    │
│                   └──────┘         └──────┘         └──────┘    │
│                                    ┌──────┐         ┌──────┐    │
│                                    │TMK v3│ ──────> │TMK v3│    │
│                                    │ACTIVE│         │RETIRE│    │
│                                    └──────┘         └──────┘    │
│                                                     ┌──────┐    │
│                                                     │TMK v4│    │
│                                                     │ACTIVE│    │
│                                                     └──────┘    │
│                                                                  │
│  Keys rotate every 30 days automatically                        │
│  Active keys encrypt new data                                   │
│  Retired keys decrypt existing data                             │
│  Destroyed keys are permanently removed                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### SQL Examples

```sql
-- Enable periodic rekeying (Enterprise+)
ALTER ACCOUNT SET PERIODIC_DATA_REKEYING = TRUE;

-- Check rekeying status (no direct SQL - contact Snowflake support)

-- Verify encryption is active (always true)
-- All data is encrypted by default, no verification needed
```

### Compliance

| Standard | Supported |
|----------|-----------|
| FIPS 140-2 Level 3 (HSM) | ✓ |
| SOC 1/2 Type II | ✓ |
| ISO 27001 | ✓ |
| HIPAA (Business Critical) | ✓ |
| PCI-DSS (Business Critical) | ✓ |

### Why This Matters
Encryption at rest protects data from physical theft and unauthorized access to storage systems. The hierarchical model limits exposure if any single key is compromised.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/security-encryption

---

## 4.2 Encryption in Transit

### Overview
All data transmitted to and from Snowflake is encrypted using TLS (Transport Layer Security). This is automatic and requires no configuration. Snowflake requires TLS 1.2 minimum and supports TLS 1.3.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| TLS Encryption | ✓ | ✓ | ✓ |
| TLS 1.2 Minimum | ✓ | ✓ | ✓ |
| TLS 1.3 Support | ✓ | ✓ | ✓ |

### What's Encrypted

| Data Flow | Encrypted |
|-----------|-----------|
| Client to Snowflake | ✓ TLS 1.2+ |
| Snowflake internal services | ✓ TLS |
| Data loading (PUT/COPY) | ✓ TLS |
| Data unloading (GET/COPY INTO) | ✓ TLS |
| Cross-region replication | ✓ TLS |
| Cross-cloud replication | ✓ TLS |

### Why This Matters
TLS prevents eavesdropping and man-in-the-middle attacks during data transmission. All sensitive data remains protected throughout its journey.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/security-encryption

---

## 4.3 Tri-Secret Secure (Business Critical)

### Overview
Tri-Secret Secure enables customer-managed encryption keys using your cloud provider's Key Management Service (KMS). A composite master key is created from BOTH a Snowflake-managed key AND a customer-managed key. You can revoke access to your data by disabling your key.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Tri-Secret Secure | - | - | ✓ |
| AWS KMS Integration | - | - | ✓ |
| Azure Key Vault Integration | - | - | ✓ |
| GCP Cloud KMS Integration | - | - | ✓ |

### Composite Key Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    TRI-SECRET SECURE MODEL                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────┐      ┌─────────────────────┐           │
│  │  CUSTOMER-MANAGED   │      │  SNOWFLAKE-MANAGED  │           │
│  │        KEY          │      │        KEY          │           │
│  │  ┌───────────────┐  │      │  ┌───────────────┐  │           │
│  │  │ AWS KMS       │  │      │  │ Snowflake HSM │  │           │
│  │  │ Azure Key Vault│ │      │  │ Root Key      │  │           │
│  │  │ GCP Cloud KMS │  │      │  │               │  │           │
│  │  └───────┬───────┘  │      │  └───────┬───────┘  │           │
│  └──────────┼──────────┘      └──────────┼──────────┘           │
│             │                            │                       │
│             └──────────┬─────────────────┘                       │
│                        │                                         │
│                        ▼                                         │
│             ┌─────────────────────┐                             │
│             │   COMPOSITE KEY     │                             │
│             │  (Both keys needed) │                             │
│             └──────────┬──────────┘                             │
│                        │                                         │
│                        ▼                                         │
│             ┌─────────────────────┐                             │
│             │  Account Master Key │                             │
│             └──────────┬──────────┘                             │
│                        │                                         │
│                        ▼                                         │
│             ┌─────────────────────┐                             │
│             │  ENCRYPTED DATA     │                             │
│             └─────────────────────┘                             │
│                                                                  │
│  CUSTOMER CONTROL:                                              │
│  • Disable CMK → Data inaccessible                              │
│  • Rotate CMK → New encryption key                              │
│  • Delete CMK → Data permanently inaccessible                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### SQL Examples

```sql
-- Get CMK configuration info (AWS)
SELECT SYSTEM$GET_CMK_KMS_KEY_POLICY();

-- Get CMK consent URL (Azure)
SELECT SYSTEM$GET_CMK_AKV_CONSENT_URL();

-- Get CMK grant command (GCP)
SELECT SYSTEM$GET_GCP_KMS_CMK_GRANT_ACCESS_CMD();
```

### Key Requirements

| Requirement | Description |
|-------------|-------------|
| **Confidentiality** | Keep key secure and confidential |
| **Integrity** | Protect against modification/deletion |
| **Availability** | Must be continuously available to Snowflake |

### Why This Matters
Tri-Secret Secure provides ultimate control over data access. Required for industries with strict key custody requirements (finance, government, healthcare). Enables immediate data access revocation if needed.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/security-encryption-manage

---

# Section 5: Column-Level Security

## 5.1 Dynamic Data Masking

### Overview
Dynamic Data Masking applies masking functions to column data at query time based on the querying user's role. Data remains unchanged in storage, but unauthorized users see masked values. No application changes required.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Dynamic Data Masking | - | ✓ | ✓ |
| Tag-Based Masking | - | ✓ | ✓ |
| Conditional Masking | - | ✓ | ✓ |

### Masking Patterns

| Pattern | Input | Output | Use Case |
|---------|-------|--------|----------|
| Full Mask | 123-45-6789 | XXX-XX-XXXX | SSN, sensitive IDs |
| Partial Mask | john.doe@co.com | j***@co.com | Email addresses |
| NULL Mask | Any value | NULL | Complete hide |
| Hash Mask | John Smith | a1b2c3d4e5... | Analytics preservation |
| Redaction | Credit Card | ****-****-****-1234 | Payment data |

### SQL Examples

```sql
-- Create simple full masking policy
CREATE MASKING POLICY MASK_SSN AS (val STRING) 
  RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('HR_ADMIN', 'COMPLIANCE') THEN val
    ELSE 'XXX-XX-XXXX'
  END
  COMMENT = 'Masks SSN for unauthorized roles';

-- Create partial email masking policy
CREATE MASKING POLICY MASK_EMAIL AS (val STRING) 
  RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('DATA_ADMIN', 'SUPPORT') THEN val
    ELSE CONCAT(LEFT(val, 1), '***@', SPLIT_PART(val, '@', 2))
  END;

-- Create NULL masking policy
CREATE MASKING POLICY MASK_SALARY AS (val NUMBER) 
  RETURNS NUMBER ->
  CASE
    WHEN CURRENT_ROLE() IN ('HR_ADMIN', 'FINANCE') THEN val
    ELSE NULL
  END;

-- Create hash masking policy (preserves analytics)
CREATE MASKING POLICY MASK_CUSTOMER_ID AS (val STRING) 
  RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('DATA_ADMIN') THEN val
    ELSE SHA2(val)
  END;

-- Apply masking policy to column
ALTER TABLE MY_DATABASE.MY_SCHEMA.EMPLOYEES
  MODIFY COLUMN SSN SET MASKING POLICY MASK_SSN;

ALTER TABLE MY_DATABASE.MY_SCHEMA.EMPLOYEES
  MODIFY COLUMN EMAIL SET MASKING POLICY MASK_EMAIL;

ALTER TABLE MY_DATABASE.MY_SCHEMA.EMPLOYEES
  MODIFY COLUMN SALARY SET MASKING POLICY MASK_SALARY;

-- Remove masking policy from column
ALTER TABLE MY_DATABASE.MY_SCHEMA.EMPLOYEES
  MODIFY COLUMN SSN UNSET MASKING POLICY;

-- View masking policies
SHOW MASKING POLICIES;

-- View columns with masking policies
SELECT *
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
  POLICY_NAME => 'MASK_SSN'
));

-- View all masking policy assignments in schema
SELECT 
  POLICY_NAME,
  REF_DATABASE_NAME,
  REF_SCHEMA_NAME,
  REF_ENTITY_NAME,
  REF_COLUMN_NAME
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
  REF_ENTITY_DOMAIN => 'TABLE',
  REF_ENTITY_NAME => 'MY_DATABASE.MY_SCHEMA.EMPLOYEES'
));

-- Test masking with different roles
USE ROLE DATA_ANALYST;
SELECT SSN, EMAIL, SALARY FROM MY_DATABASE.MY_SCHEMA.EMPLOYEES LIMIT 5;
-- Results: Masked values

USE ROLE HR_ADMIN;
SELECT SSN, EMAIL, SALARY FROM MY_DATABASE.MY_SCHEMA.EMPLOYEES LIMIT 5;
-- Results: Actual values
```

### Tag-Based Masking (Centralized Management)

```sql
-- Create tag for PII classification
CREATE TAG PII_TYPE ALLOWED_VALUES = ('SSN', 'EMAIL', 'PHONE', 'ADDRESS');

-- Create masking policy for tag
CREATE MASKING POLICY TAG_BASED_PII_MASK AS (val STRING) 
  RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('DATA_ADMIN', 'COMPLIANCE') THEN val
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('PII_TYPE') = 'SSN' THEN 'XXX-XX-XXXX'
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('PII_TYPE') = 'EMAIL' THEN '***@***.com'
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('PII_TYPE') = 'PHONE' THEN '***-***-****'
    ELSE '*****'
  END;

-- Apply tag to column
ALTER TABLE EMPLOYEES MODIFY COLUMN SSN SET TAG PII_TYPE = 'SSN';
ALTER TABLE EMPLOYEES MODIFY COLUMN EMAIL SET TAG PII_TYPE = 'EMAIL';

-- Associate masking policy with tag
ALTER TAG PII_TYPE SET MASKING POLICY TAG_BASED_PII_MASK;
```

### Best Practices

1. **Use role-based conditions** - CURRENT_ROLE() for authorization
2. **Test with all affected roles** - Verify masking behavior
3. **Document policy purposes** - Use COMMENT field
4. **Consider analytics impact** - Hash masks preserve join capability
5. **Use tag-based masking at scale** - Centralizes policy management
6. **Apply to views for shared data** - Masking travels with shares

### Why This Matters
Dynamic masking enables least-privilege data access without duplicating data or modifying applications. Essential for PII protection, compliance (GDPR, CCPA), and data democratization.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/security-column-ddm-intro

---

## 5.2 External Tokenization

### Overview
External tokenization integrates Snowflake with third-party tokenization services (Protegrity, Voltage, TokenEx). Sensitive data is replaced with tokens, and detokenization occurs at query time for authorized users via External Functions.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| External Tokenization | - | ✓ | ✓ |
| External Functions | ✓ | ✓ | ✓ |

### Use Cases

- PCI-DSS compliance (payment card data)
- Existing tokenization infrastructure
- Cross-platform tokenization requirements

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                  EXTERNAL TOKENIZATION FLOW                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────┐    Query      ┌─────────────┐                  │
│  │   User      │ ────────────> │  Snowflake  │                  │
│  │ (Authorized)│               │             │                  │
│  └─────────────┘               └──────┬──────┘                  │
│                                       │                          │
│                          Masking Policy triggers                 │
│                          External Function call                  │
│                                       │                          │
│                                       ▼                          │
│                          ┌─────────────────────┐                │
│                          │  External Function  │                │
│                          │  (API Gateway)      │                │
│                          └──────────┬──────────┘                │
│                                     │                            │
│                                     ▼                            │
│                          ┌─────────────────────┐                │
│                          │ Tokenization Service│                │
│                          │ (Protegrity/Voltage)│                │
│                          └──────────┬──────────┘                │
│                                     │                            │
│                          Detokenized data returned               │
│                                     │                            │
│                                     ▼                            │
│                          ┌─────────────────────┐                │
│                          │  Query Results      │                │
│                          │  (Clear text for    │                │
│                          │   authorized users) │                │
│                          └─────────────────────┘                │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Documentation Reference
https://docs.snowflake.com/en/user-guide/security-column-ext-token-intro

---

# Section 6: Row-Level Security

## 6.1 Row Access Policies

### Overview
Row Access Policies (RAP) filter rows returned by queries based on the querying user's context (role, user attributes, mapping tables). Rows that don't satisfy the policy conditions are silently excluded from results.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Row Access Policies | - | ✓ | ✓ |
| Mapping Table Approach | - | ✓ | ✓ |
| Context Functions | - | ✓ | ✓ |

### Common Use Cases

| Use Case | Description |
|----------|-------------|
| Multi-tenancy | Isolate customer data |
| Regional restrictions | Users see only their region |
| Department isolation | Employees see only their department |
| Data sovereignty | Geographic data restrictions |

### SQL Examples

```sql
-- Create mapping table for access control
CREATE TABLE ACCESS_CONTROL.REGION_ACCESS (
  ROLE_NAME VARCHAR,
  ALLOWED_REGION VARCHAR
);

-- Populate mapping table
INSERT INTO ACCESS_CONTROL.REGION_ACCESS VALUES
  ('NA_SALES', 'North America'),
  ('EU_SALES', 'Europe'),
  ('APAC_SALES', 'Asia Pacific'),
  ('GLOBAL_ADMIN', 'ALL');

-- Create row access policy using mapping table
CREATE ROW ACCESS POLICY REGION_FILTER AS (region_col VARCHAR) 
  RETURNS BOOLEAN ->
  EXISTS (
    SELECT 1 
    FROM ACCESS_CONTROL.REGION_ACCESS ac
    WHERE ac.ROLE_NAME = CURRENT_ROLE()
      AND (ac.ALLOWED_REGION = region_col OR ac.ALLOWED_REGION = 'ALL')
  )
  COMMENT = 'Filters data by user region access';

-- Apply row access policy to table
ALTER TABLE SALES.ORDERS 
  ADD ROW ACCESS POLICY REGION_FILTER ON (REGION);

-- Simple role-based row access policy
CREATE ROW ACCESS POLICY DEPARTMENT_FILTER AS (dept_col VARCHAR) 
  RETURNS BOOLEAN ->
  CASE
    WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN TRUE
    WHEN CURRENT_ROLE() = 'HR_ADMIN' THEN TRUE
    WHEN CURRENT_ROLE() LIKE '%_' || dept_col || '_%' THEN TRUE
    ELSE FALSE
  END;

-- Apply to table
ALTER TABLE HR.EMPLOYEES 
  ADD ROW ACCESS POLICY DEPARTMENT_FILTER ON (DEPARTMENT);

-- Remove row access policy
ALTER TABLE SALES.ORDERS 
  DROP ROW ACCESS POLICY REGION_FILTER;

-- View row access policies
SHOW ROW ACCESS POLICIES;

-- View tables with row access policies
SELECT *
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
  POLICY_NAME => 'REGION_FILTER'
));

-- Test policy with different roles
USE ROLE NA_SALES;
SELECT COUNT(*) FROM SALES.ORDERS;
-- Returns: Only North America records

USE ROLE GLOBAL_ADMIN;
SELECT COUNT(*) FROM SALES.ORDERS;
-- Returns: All records
```

### Mapping Table Design Pattern

```
┌─────────────────────────────────────────────────────────────────┐
│                 MAPPING TABLE ARCHITECTURE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────┐                                │
│  │     ROLE_ACCESS_MAPPING     │                                │
│  ├─────────────────────────────┤                                │
│  │ ROLE_NAME    │ ACCESS_KEY   │                                │
│  ├──────────────┼──────────────┤                                │
│  │ NA_ANALYST   │ US           │                                │
│  │ NA_ANALYST   │ CA           │                                │
│  │ EU_ANALYST   │ UK           │                                │
│  │ EU_ANALYST   │ DE           │                                │
│  │ EU_ANALYST   │ FR           │                                │
│  │ GLOBAL_ADMIN │ ALL          │                                │
│  └──────────────┴──────────────┘                                │
│                                                                  │
│  Policy Logic:                                                  │
│  SELECT * FROM data_table                                       │
│  WHERE EXISTS (                                                 │
│    SELECT 1 FROM ROLE_ACCESS_MAPPING                            │
│    WHERE ROLE_NAME = CURRENT_ROLE()                             │
│      AND (ACCESS_KEY = data_table.country                       │
│           OR ACCESS_KEY = 'ALL')                                │
│  )                                                               │
│                                                                  │
│  Benefits:                                                      │
│  • Centralized access control                                   │
│  • Easy to audit and modify                                     │
│  • No policy changes when access changes                        │
│  • Self-service for access administrators                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Best Practices

1. **Use mapping tables** - Easier to manage than inline conditions
2. **Test thoroughly** - Verify filtering with all affected roles
3. **Consider performance** - Index mapping tables appropriately
4. **Combine with masking** - Defense in depth
5. **Document policy logic** - Use COMMENT field
6. **Regular access reviews** - Audit mapping table entries

### Why This Matters
Row access policies enable multi-tenant data architectures, simplify application development (no per-tenant databases), and ensure data isolation without application changes.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/security-row-intro

---

## 6.2 Secure Views

### Overview
Secure Views hide view definition from users and prevent data leakage through query optimizer. They provide row-level security in Standard Edition where Row Access Policies aren't available.

### Edition Requirements

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Secure Views | ✓ | ✓ | ✓ |
| View Definition Hidden | ✓ | ✓ | ✓ |

### SQL Examples

```sql
-- Create secure view with row filtering
CREATE SECURE VIEW SALES.V_MY_ORDERS AS
SELECT 
  ORDER_ID,
  CUSTOMER_ID,
  ORDER_DATE,
  AMOUNT,
  REGION
FROM SALES.ORDERS
WHERE REGION IN (
  SELECT ALLOWED_REGION 
  FROM ACCESS_CONTROL.USER_REGIONS 
  WHERE USER_NAME = CURRENT_USER()
);

-- Create secure view with column restrictions
CREATE SECURE VIEW HR.V_EMPLOYEE_PUBLIC AS
SELECT 
  EMPLOYEE_ID,
  FIRST_NAME,
  LAST_NAME,
  DEPARTMENT,
  HIRE_DATE
  -- Salary, SSN excluded
FROM HR.EMPLOYEES;

-- Grant access to secure view
GRANT SELECT ON SALES.V_MY_ORDERS TO ROLE SALES_USER;

-- View definition is hidden from users
SHOW VIEWS LIKE 'V_MY_ORDERS' IN SCHEMA SALES;
-- Text column shows: [Secure View - definition hidden]

-- Compare secure vs non-secure view
CREATE VIEW SALES.V_ORDERS_REGULAR AS
SELECT * FROM SALES.ORDERS WHERE REGION = 'NA';

SHOW VIEWS;
-- Regular view: Definition visible
-- Secure view: Definition hidden
```

### Secure View vs Row Access Policy

| Feature | Secure View | Row Access Policy |
|---------|-------------|-------------------|
| Edition | Standard+ | Enterprise+ |
| Definition Hidden | Yes | N/A |
| Applies to Base Table | No (view only) | Yes |
| Performance | Good | Good |
| Management | Per-view | Centralized |
| Data Sharing | Supported | Supported |

### Best Practices

1. **Use for Standard Edition RLS** - When RAP unavailable
2. **Always use SECURE keyword** - For sensitive data views
3. **Combine with grants** - Control who can query
4. **Test query performance** - Optimizer limitations
5. **Document filtering logic** - In view comments

### Why This Matters
Secure views provide row-level security in all editions and prevent information leakage through query plans or error messages.

### Documentation Reference
https://docs.snowflake.com/en/user-guide/views-secure

---

# Document Information

**Part 1 Coverage:** Sections 1-6
- Section 1: Access Control Framework
- Section 2: User Authentication
- Section 3: Network Security
- Section 4: Data Encryption
- Section 5: Column-Level Security
- Section 6: Row-Level Security

**Next:** Part 2 (Sections 7-10)
- Section 7: Data Storage & Protection
- Section 8: Data Sharing & Replication
- Section 9: Governance & Compliance
- Section 10: Best Practices Summary

---

**Document Version:** 2025.1  
**Based On:** Official Snowflake Documentation (docs.snowflake.com)  
**Validation Date:** January 2025
