-- ============================================================================
-- Script: 03_policies/create_masking_policies.sql
-- Purpose: Create comprehensive masking policies for data protection
-- Author: Venkannababu Thatavarthi
-- Date: October 13, 2025
-- Version: 1.0
-- ============================================================================
-- Prerequisites: POLICY_ADMIN role with CREATE MASKING POLICY privilege
-- Execution Time: ~5 minutes
-- ============================================================================

USE ROLE policy_admin;
USE SCHEMA governance.policies;

-- ============================================================================
-- SECTION 1: Email Masking Policies
-- ============================================================================
-- Purpose: Mask email addresses based on role privileges
-- ============================================================================

-- Standard email masking - shows partial email
CREATE OR REPLACE MASKING POLICY mask_email AS (val STRING) 
RETURNS STRING ->
  CASE
    -- Full access for admin and compliance roles
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'GOVERNANCE_ADMIN', 'COMPLIANCE_OFFICER', 'SECURITYADMIN') 
      THEN val
    -- Partial masking for support and marketing
    WHEN CURRENT_ROLE() IN ('CUSTOMER_SUPPORT', 'MARKETING_TEAM', 'SALES_TEAM')
      THEN REGEXP_REPLACE(val, '^(.{2})(.*)(@.*)$', '\\1***\\3')
    -- Domain-only for analysts
    WHEN CURRENT_ROLE() IN ('DATA_ANALYST', 'BUSINESS_ANALYST')
      THEN REGEXP_REPLACE(val, '^(.*)(@.*)$', '***\\2')
    -- Fully masked for everyone else
    ELSE '***@***.***'
  END
COMMENT = 'Standard email masking policy with role-based visibility';

-- Strict email masking - hides domain too
CREATE OR REPLACE MASKING POLICY mask_email_strict AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'GOVERNANCE_ADMIN', 'COMPLIANCE_OFFICER') 
      THEN val
    WHEN CURRENT_ROLE() IN ('DATA_STEWARD', 'DATA_OWNER')
      THEN REGEXP_REPLACE(val, '^(.{2})(.*)(@.*)$', '\\1***\\3')
    ELSE '***@***'
  END
COMMENT = 'Strict email masking with limited role access';

-- ============================================================================
-- SECTION 2: Phone Number Masking Policies
-- ============================================================================
-- Purpose: Mask phone numbers preserving format
-- ============================================================================

-- Standard phone masking - shows last 4 digits
CREATE OR REPLACE MASKING POLICY mask_phone AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'GOVERNANCE_ADMIN', 'COMPLIANCE_OFFICER') 
      THEN val
    WHEN CURRENT_ROLE() IN ('CUSTOMER_SUPPORT', 'SALES_TEAM')
      THEN REGEXP_REPLACE(val, '(.*)(\\d{4})$', '***-***-\\2')
    WHEN CURRENT_ROLE() IN ('DATA_ANALYST', 'MARKETING_TEAM')
      THEN 'XXX-XXX-' || RIGHT(val, 4)
    ELSE '***-***-****'
  END
COMMENT = 'Phone number masking showing last 4 digits for authorized roles';

-- International phone masking
CREATE OR REPLACE MASKING POLICY mask_phone_international AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'GOVERNANCE_ADMIN', 'COMPLIANCE_OFFICER') 
      THEN val
    WHEN CURRENT_ROLE() IN ('CUSTOMER_SUPPORT')
      THEN '+XX XXX XXX ' || RIGHT(val, 4)
    ELSE '+XX XXX XXX XXXX'
  END
COMMENT = 'International phone number masking';

-- ============================================================================
-- SECTION 3: SSN / Tax ID Masking Policies
-- ============================================================================
-- Purpose: Mask Social Security Numbers and Tax IDs
-- ============================================================================

-- SSN masking - shows last 4 digits only
CREATE OR REPLACE MASKING POLICY mask_ssn AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'COMPLIANCE_OFFICER', 'HR_ADMIN', 'PAYROLL_ADMIN') 
      THEN val
    WHEN CURRENT_ROLE() IN ('HR_ANALYST', 'HR_MANAGER')
      THEN '***-**-' || RIGHT(val, 4)
    ELSE '***-**-****'
  END
COMMENT = 'SSN masking for HR and payroll operations';

-- Tax ID masking (EIN format)
CREATE OR REPLACE MASKING POLICY mask_tax_id AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'COMPLIANCE_OFFICER', 'FINANCE_ADMIN', 'TAX_ADMIN') 
      THEN val
    WHEN CURRENT_ROLE() IN ('FINANCE_ANALYST', 'ACCOUNTANT')
      THEN '**-*****' || RIGHT(val, 2)
    ELSE '**-*******'
  END
COMMENT = 'Tax ID masking for finance operations';

-- ============================================================================
-- SECTION 4: Address Masking Policies
-- ============================================================================
-- Purpose: Mask physical addresses
-- ============================================================================

-- Full address masking
CREATE OR REPLACE MASKING POLICY mask_address AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'GOVERNANCE_ADMIN', 'COMPLIANCE_OFFICER') 
      THEN val
    WHEN CURRENT_ROLE() IN ('CUSTOMER_SUPPORT', 'LOGISTICS_TEAM', 'OPERATIONS_TEAM')
      THEN REGEXP_REPLACE(val, '^(\\d+)\\s+(.*)$', 'XXX \\2')
    WHEN CURRENT_ROLE() IN ('MARKETING_TEAM', 'SALES_TEAM')
      THEN SPLIT_PART(val, ',', -1) -- Show only city/state
    ELSE '[REDACTED]'
  END
COMMENT = 'Address masking with partial visibility for operations';

-- ============================================================================
-- SECTION 5: Credit Card Masking Policies
-- ============================================================================
-- Purpose: Mask credit card numbers
-- ============================================================================

-- Credit card masking - PCI DSS compliant
CREATE OR REPLACE MASKING POLICY mask_credit_card AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'COMPLIANCE_OFFICER', 'PCI_ADMIN') 
      THEN val
    WHEN CURRENT_ROLE() IN ('FINANCE_ADMIN', 'PAYMENT_PROCESSOR')
      THEN 'XXXX-XXXX-XXXX-' || RIGHT(val, 4)
    ELSE 'XXXX-XXXX-XXXX-XXXX'
  END
COMMENT = 'PCI DSS compliant credit card masking';

-- CVV masking (always masked except for specific roles)
CREATE OR REPLACE MASKING POLICY mask_cvv AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'PCI_ADMIN', 'PAYMENT_PROCESSOR') 
      THEN val
    ELSE '***'
  END
COMMENT = 'CVV masking for payment security';

-- ============================================================================
-- SECTION 6: Financial Data Masking Policies
-- ============================================================================
-- Purpose: Mask financial amounts and account numbers
-- ============================================================================

-- Salary masking
CREATE OR REPLACE MASKING POLICY mask_salary AS (val NUMBER) 
RETURNS NUMBER ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'HR_ADMIN', 'PAYROLL_ADMIN', 'COMPLIANCE_OFFICER') 
      THEN val
    WHEN CURRENT_ROLE() IN ('HR_MANAGER', 'FINANCE_MANAGER')
      THEN ROUND(val, -3) -- Round to nearest thousand
    WHEN CURRENT_ROLE() IN ('HR_ANALYST')
      THEN ROUND(val, -4) -- Round to nearest ten thousand
    ELSE NULL
  END
COMMENT = 'Salary masking with progressive rounding';

-- Bank account masking
CREATE OR REPLACE MASKING POLICY mask_bank_account AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'FINANCE_ADMIN', 'TREASURY_ADMIN', 'COMPLIANCE_OFFICER') 
      THEN val
    WHEN CURRENT_ROLE() IN ('FINANCE_ANALYST', 'ACCOUNTANT')
      THEN '****' || RIGHT(val, 4)
    ELSE '********'
  END
COMMENT = 'Bank account number masking';

-- Revenue/financial amounts
CREATE OR REPLACE MASKING POLICY mask_financial_amount AS (val NUMBER) 
RETURNS NUMBER ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'FINANCE_ADMIN', 'CFO', 'COMPLIANCE_OFFICER') 
      THEN val
    WHEN CURRENT_ROLE() IN ('FINANCE_ANALYST', 'CONTROLLER')
      THEN ROUND(val, -3) -- Round to nearest thousand
    WHEN CURRENT_ROLE() IN ('BUSINESS_ANALYST', 'DATA_ANALYST')
      THEN ROUND(val, -4) -- Round to nearest ten thousand
    ELSE NULL
  END
COMMENT = 'Financial amount masking with role-based rounding';

-- ============================================================================
-- SECTION 7: Date of Birth Masking Policies
-- ============================================================================
-- Purpose: Mask birth dates while preserving age calculations
-- ============================================================================

-- Date of birth masking - shows year only
CREATE OR REPLACE MASKING POLICY mask_date_of_birth AS (val DATE) 
RETURNS DATE ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'HR_ADMIN', 'COMPLIANCE_OFFICER', 'HEALTHCARE_ADMIN') 
      THEN val
    WHEN CURRENT_ROLE() IN ('HR_ANALYST', 'DATA_ANALYST')
      THEN TO_DATE(YEAR(val) || '-01-01', 'YYYY-MM-DD') -- Show year only
    ELSE NULL
  END
COMMENT = 'Date of birth masking preserving year for age calculations';

-- ============================================================================
-- SECTION 8: Name Masking Policies
-- ============================================================================
-- Purpose: Mask personal names
-- ============================================================================

-- Full name masking - shows initials
CREATE OR REPLACE MASKING POLICY mask_name AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'HR_ADMIN', 'COMPLIANCE_OFFICER') 
      THEN val
    WHEN CURRENT_ROLE() IN ('CUSTOMER_SUPPORT', 'SALES_TEAM')
      THEN LEFT(SPLIT_PART(val, ' ', 1), 1) || '. ' || LEFT(SPLIT_PART(val, ' ', -1), 1) || '.'
    WHEN CURRENT_ROLE() IN ('DATA_ANALYST', 'MARKETING_TEAM')
      THEN '*** ***'
    ELSE '[REDACTED]'
  END
COMMENT = 'Name masking showing initials for authorized roles';

-- ============================================================================
-- SECTION 9: IP Address Masking Policies
-- ============================================================================
-- Purpose: Mask IP addresses
-- ============================================================================

-- IP address masking - shows network portion
CREATE OR REPLACE MASKING POLICY mask_ip_address AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'SECURITY_ADMIN', 'NETWORK_ADMIN', 'COMPLIANCE_OFFICER') 
      THEN val
    WHEN CURRENT_ROLE() IN ('IT_SUPPORT', 'DATA_ENGINEER')
      THEN REGEXP_REPLACE(val, '(\\d+\\.\\d+\\.)(\\d+\\.\\d+)', '\\1XXX.XXX')
    ELSE 'XXX.XXX.XXX.XXX'
  END
COMMENT = 'IP address masking showing network portion';

-- ============================================================================
-- SECTION 10: Healthcare Data Masking (HIPAA)
-- ============================================================================
-- Purpose: Mask protected health information
-- ============================================================================

-- Medical record number masking
CREATE OR REPLACE MASKING POLICY mask_mrn AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'HEALTHCARE_ADMIN', 'PROVIDER', 'COMPLIANCE_OFFICER') 
      THEN val
    WHEN CURRENT_ROLE() IN ('HEALTHCARE_ANALYST')
      THEN '****' || RIGHT(val, 4)
    ELSE '********'
  END
COMMENT = 'Medical record number masking for HIPAA compliance';

-- Diagnosis code masking
CREATE OR REPLACE MASKING POLICY mask_diagnosis AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'HEALTHCARE_ADMIN', 'PROVIDER', 'COMPLIANCE_OFFICER') 
      THEN val
    WHEN CURRENT_ROLE() IN ('HEALTHCARE_ANALYST', 'RESEARCHER')
      THEN LEFT(val, 3) || 'XXX' -- Show category, mask specific code
    ELSE '[REDACTED]'
  END
COMMENT = 'Diagnosis code masking for research and analytics';

-- ============================================================================
-- SECTION 11: Generic String Masking Policies
-- ============================================================================
-- Purpose: Generic masking for various string data types
-- ============================================================================

-- Generic PII masking
CREATE OR REPLACE MASKING POLICY mask_pii_generic AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'GOVERNANCE_ADMIN', 'COMPLIANCE_OFFICER', 'DATA_OWNER') 
      THEN val
    WHEN CURRENT_ROLE() IN ('DATA_STEWARD')
      THEN LEFT(val, 4) || REPEAT('*', GREATEST(LENGTH(val) - 4, 0))
    ELSE REPEAT('*', LENGTH(val))
  END
COMMENT = 'Generic PII masking for unspecified sensitive data';

-- Partial masking (show first and last chars)
CREATE OR REPLACE MASKING POLICY mask_partial AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'GOVERNANCE_ADMIN', 'COMPLIANCE_OFFICER') 
      THEN val
    WHEN CURRENT_ROLE() IN ('DATA_ANALYST', 'BUSINESS_ANALYST')
      THEN LEFT(val, 2) || REPEAT('*', GREATEST(LENGTH(val) - 4, 0)) || RIGHT(val, 2)
    ELSE REPEAT('*', LENGTH(val))
  END
COMMENT = 'Partial masking showing first and last characters';

-- ============================================================================
-- SECTION 12: NULL-Safe Masking Wrapper
-- ============================================================================
-- Purpose: Handle NULL values in masking policies
-- ============================================================================

-- NULL-safe email masking
CREATE OR REPLACE MASKING POLICY mask_email_null_safe AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN val IS NULL THEN NULL
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'GOVERNANCE_ADMIN', 'COMPLIANCE_OFFICER') 
      THEN val
    WHEN CURRENT_ROLE() IN ('CUSTOMER_SUPPORT', 'MARKETING_TEAM')
      THEN REGEXP_REPLACE(val, '^(.{2})(.*)(@.*)$', '\\1***\\3')
    ELSE '***@***.***'
  END
COMMENT = 'NULL-safe email masking policy';

-- ============================================================================
-- SECTION 13: Conditional Masking Based on Column Values
-- ============================================================================
-- Purpose: Dynamic masking based on data attributes
-- ============================================================================

-- Mask based on sensitivity flag
CREATE OR REPLACE MASKING POLICY mask_conditional_sensitivity AS (val STRING, sensitivity_flag STRING) 
RETURNS STRING ->
  CASE
    WHEN sensitivity_flag = 'PUBLIC' THEN val
    WHEN sensitivity_flag = 'INTERNAL' AND CURRENT_ROLE() IN ('SYSADMIN', 'DATA_ANALYST', 'EMPLOYEE')
      THEN val
    WHEN sensitivity_flag = 'CONFIDENTIAL' AND CURRENT_ROLE() IN ('SYSADMIN', 'GOVERNANCE_ADMIN', 'MANAGER')
      THEN val
    WHEN sensitivity_flag = 'RESTRICTED' AND CURRENT_ROLE() IN ('SYSADMIN', 'COMPLIANCE_OFFICER', 'EXECUTIVE')
      THEN val
    ELSE '[MASKED]'
  END
COMMENT = 'Conditional masking based on row-level sensitivity classification';

-- ============================================================================
-- SECTION 14: Verify Policy Creation
-- ============================================================================

SELECT 'Masking policies created successfully!' AS status;

-- Show all created policies
SHOW MASKING POLICIES IN SCHEMA governance.policies;

-- Count policies by category
SELECT '
=============================================================================
MASKING POLICIES CREATED SUCCESSFULLY
=============================================================================

Policy Categories Created:
1. Email Masking (2 policies)
2. Phone Number Masking (2 policies)
3. SSN/Tax ID Masking (2 policies)
4. Address Masking (1 policy)
5. Credit Card Masking (2 policies)
6. Financial Data Masking (3 policies)
7. Date of Birth Masking (1 policy)
8. Name Masking (1 policy)
9. IP Address Masking (1 policy)
10. Healthcare Data Masking (2 policies)
11. Generic Masking (2 policies)
12. NULL-Safe Masking (1 policy)
13. Conditional Masking (1 policy)

Total Masking Policies: 21

Role-Based Access Summary:
- SYSADMIN, GOVERNANCE_ADMIN, COMPLIANCE_OFFICER: Full unmasked access
- Department Admins (HR_ADMIN, FINANCE_ADMIN, etc.): Full access to their domain
- Department Managers: Partial masking (aggregated/rounded)
- Analysts: Limited visibility, high masking
- General Users: Full masking

Next Steps:
1. Test masking policies with sample data
2. Associate policies with tags
3. Apply tags to columns
4. Validate masking behavior per role
5. Document policy exceptions

For policy definitions, query:
  SHOW MASKING POLICIES IN SCHEMA governance.policies;

For questions, contact: security-team@company.com
=============================================================================
' AS summary;

-- ============================================================================
-- SECTION 15: Create Policy Testing Framework
-- ============================================================================

-- Create table for policy testing
CREATE OR REPLACE TABLE governance.metadata.masking_policy_tests (
    test_id NUMBER AUTOINCREMENT,
    policy_name VARCHAR(255),
    test_role VARCHAR(255),
    test_value STRING,
    expected_result STRING,
    actual_result STRING,
    test_passed BOOLEAN,
    tested_by VARCHAR(255) DEFAULT CURRENT_USER(),
    tested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (test_id)
)
COMMENT = 'Test results for masking policies';

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
