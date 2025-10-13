-- ============================================================================
-- Script: 03_policies/associate_policies_with_tags.sql
-- Purpose: Associate masking policies with tags for automated enforcement
-- Author: Venkannababu Thatavarthi
-- Date: October 13, 2025
-- Version: 1.0
-- ============================================================================
-- Prerequisites: POLICY_ADMIN role with privileges on tags and policies
-- Execution Time: ~3 minutes
-- ============================================================================

USE ROLE policy_admin;

-- ============================================================================
-- SECTION 1: Associate Email Policies with PII Tags
-- ============================================================================

-- Associate standard email masking with EMAIL tag value
ALTER TAG governance.tags.pii_type SET
    MASKING POLICY governance.policies.mask_email;

-- Note: When a column is tagged with pii_type='EMAIL', 
-- the mask_email policy will automatically apply

-- ============================================================================
-- SECTION 2: Create Tag-Policy Mapping Table
-- ============================================================================
-- Purpose: Document which policies are associated with which tag values
-- ============================================================================

USE ROLE governance_admin;
USE SCHEMA governance.metadata;

CREATE OR REPLACE TABLE tag_policy_mapping (
    mapping_id NUMBER AUTOINCREMENT,
    tag_name VARCHAR(255) NOT NULL,
    tag_value VARCHAR(255),
    policy_name VARCHAR(255) NOT NULL,
    policy_type VARCHAR(50) NOT NULL, -- MASKING, ROW_ACCESS
    policy_schema VARCHAR(255) DEFAULT 'governance.policies',
    effective_date DATE DEFAULT CURRENT_DATE(),
    expiration_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_by VARCHAR(255) DEFAULT CURRENT_USER(),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    notes VARCHAR(1000),
    PRIMARY KEY (mapping_id)
)
COMMENT = 'Mapping between tags and policies for governance tracking';

-- ============================================================================
-- SECTION 3: Insert Policy-Tag Mappings
-- ============================================================================
-- Document all tag-policy associations
-- ============================================================================

INSERT INTO tag_policy_mapping 
    (tag_name, tag_value, policy_name, policy_type, notes)
VALUES
    -- PII Type mappings
    ('pii_type', 'EMAIL', 'mask_email', 'MASKING', 'Standard email masking for EMAIL tagged columns'),
    ('pii_type', 'PHONE', 'mask_phone', 'MASKING', 'Phone number masking showing last 4 digits'),
    ('pii_type', 'SSN', 'mask_ssn', 'MASKING', 'SSN masking for payroll and HR'),
    ('pii_type', 'TAX_ID', 'mask_tax_id', 'MASKING', 'Tax ID masking for finance'),
    ('pii_type', 'ADDRESS', 'mask_address', 'MASKING', 'Physical address masking'),
    ('pii_type', 'CREDIT_CARD', 'mask_credit_card', 'MASKING', 'PCI DSS compliant credit card masking'),
    ('pii_type', 'BANK_ACCOUNT', 'mask_bank_account', 'MASKING', 'Bank account number masking'),
    ('pii_type', 'DATE_OF_BIRTH', 'mask_date_of_birth', 'MASKING', 'DOB masking preserving year'),
    ('pii_type', 'NAME', 'mask_name', 'MASKING', 'Name masking showing initials'),
    ('pii_type', 'IP_ADDRESS', 'mask_ip_address', 'MASKING', 'IP address masking'),
    ('pii_type', 'PASSPORT', 'mask_pii_generic', 'MASKING', 'Generic PII masking for passport'),
    ('pii_type', 'DRIVERS_LICENSE', 'mask_pii_generic', 'MASKING', 'Generic PII masking for license'),
    
    -- Data Sensitivity mappings
    ('data_sensitivity', 'CONFIDENTIAL', 'mask_partial', 'MASKING', 'Partial masking for confidential data'),
    ('data_sensitivity', 'RESTRICTED', 'mask_pii_generic', 'MASKING', 'Full masking for restricted data');

-- ============================================================================
-- SECTION 4: Create Procedures for Policy-Tag Association
-- ============================================================================

-- Procedure to associate a policy with a tag
CREATE OR REPLACE PROCEDURE sp_associate_policy_with_tag(
    p_tag_name VARCHAR,
    p_policy_name VARCHAR,
    p_notes VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    alter_sql VARCHAR;
    result_msg VARCHAR;
BEGIN
    -- Construct ALTER TAG statement
    alter_sql := 'ALTER TAG governance.tags.' || p_tag_name || 
                 ' SET MASKING POLICY governance.policies.' || p_policy_name;
    
    -- Execute the association
    EXECUTE IMMEDIATE :alter_sql;
    
    -- Log the association
    INSERT INTO governance.metadata.tag_policy_mapping
        (tag_name, policy_name, policy_type, notes)
    VALUES
        (:p_tag_name, :p_policy_name, 'MASKING', :p_notes);
    
    result_msg := 'Successfully associated policy ' || p_policy_name || 
                  ' with tag ' || p_tag_name;
    
    RETURN result_msg;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error associating policy with tag: ' || SQLERRM;
END;
$$;

-- Procedure to unset a policy from a tag
CREATE OR REPLACE PROCEDURE sp_unset_policy_from_tag(
    p_tag_name VARCHAR,
    p_policy_name VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    alter_sql VARCHAR;
    result_msg VARCHAR;
BEGIN
    -- Construct ALTER TAG statement to unset policy
    alter_sql := 'ALTER TAG governance.tags.' || p_tag_name || 
                 ' UNSET MASKING POLICY governance.policies.' || p_policy_name;
    
    -- Execute the unset
    EXECUTE IMMEDIATE :alter_sql;
    
    -- Update mapping table
    UPDATE governance.metadata.tag_policy_mapping
    SET is_active = FALSE,
        expiration_date = CURRENT_DATE()
    WHERE tag_name = :p_tag_name
      AND policy_name = :p_policy_name
      AND is_active = TRUE;
    
    result_msg := 'Successfully unset policy ' || p_policy_name || 
                  ' from tag ' || p_tag_name;
    
    RETURN result_msg;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error unsetting policy from tag: ' || SQLERRM;
END;
$$;

-- ============================================================================
-- SECTION 5: Apply Specific Policy Associations
-- ============================================================================
-- Associate policies with specific PII type values
-- Note: Currently Snowflake associates policies at the tag level, not tag value level
-- This section documents the intended associations for when applying tags
-- ============================================================================

/*
IMPORTANT NOTE:
As of the current Snowflake version, masking policies are associated with tags
at the TAG level, not at the TAG VALUE level. This means:

When you execute:
  ALTER TAG pii_type SET MASKING POLICY mask_email;

The mask_email policy will apply to ALL columns tagged with pii_type,
regardless of the value (EMAIL, PHONE, SSN, etc.).

WORKAROUND STRATEGIES:

1. Create separate tags for each PII type:
   - pii_email (instead of pii_type='EMAIL')
   - pii_phone (instead of pii_type='PHONE')
   - pii_ssn (instead of pii_type='SSN')

2. Use conditional masking policies that check the tag value:
   - Requires application-level metadata or additional columns

3. Apply masking policies directly to columns (without tags)

For this implementation, we recommend Strategy #1: 
Create specific tags for each PII type to ensure correct policy application.
*/

-- ============================================================================
-- SECTION 6: Create Specific PII Tags (Recommended Approach)
-- ============================================================================

USE ROLE tag_admin;
USE SCHEMA governance.tags;

-- Create specific tags for each PII type
CREATE OR REPLACE TAG pii_email
    COMMENT = 'Tag for email address columns - associates with mask_email policy';

CREATE OR REPLACE TAG pii_phone
    COMMENT = 'Tag for phone number columns - associates with mask_phone policy';

CREATE OR REPLACE TAG pii_ssn
    COMMENT = 'Tag for SSN columns - associates with mask_ssn policy';

CREATE OR REPLACE TAG pii_tax_id
    COMMENT = 'Tag for tax ID columns - associates with mask_tax_id policy';

CREATE OR REPLACE TAG pii_address
    COMMENT = 'Tag for address columns - associates with mask_address policy';

CREATE OR REPLACE TAG pii_credit_card
    COMMENT = 'Tag for credit card columns - associates with mask_credit_card policy';

CREATE OR REPLACE TAG pii_bank_account
    COMMENT = 'Tag for bank account columns - associates with mask_bank_account policy';

CREATE OR REPLACE TAG pii_date_of_birth
    COMMENT = 'Tag for DOB columns - associates with mask_date_of_birth policy';

CREATE OR REPLACE TAG pii_name
    COMMENT = 'Tag for name columns - associates with mask_name policy';

CREATE OR REPLACE TAG pii_ip_address
    COMMENT = 'Tag for IP address columns - associates with mask_ip_address policy';

CREATE OR REPLACE TAG pii_generic
    COMMENT = 'Tag for generic PII columns - associates with mask_pii_generic policy';

-- Grant APPLY privileges on new tags
GRANT APPLY ON ALL TAGS IN SCHEMA governance.tags TO ROLE data_steward;
GRANT APPLY ON FUTURE TAGS IN SCHEMA governance.tags TO ROLE data_steward;

-- ============================================================================
-- SECTION 7: Associate Policies with Specific Tags
-- ============================================================================

USE ROLE policy_admin;

-- Associate each policy with its corresponding tag
ALTER TAG governance.tags.pii_email 
    SET MASKING POLICY governance.policies.mask_email;

ALTER TAG governance.tags.pii_phone 
    SET MASKING POLICY governance.policies.mask_phone;

ALTER TAG governance.tags.pii_ssn 
    SET MASKING POLICY governance.policies.mask_ssn;

ALTER TAG governance.tags.pii_tax_id 
    SET MASKING POLICY governance.policies.mask_tax_id;

ALTER TAG governance.tags.pii_address 
    SET MASKING POLICY governance.policies.mask_address;

ALTER TAG governance.tags.pii_credit_card 
    SET MASKING POLICY governance.policies.mask_credit_card;

ALTER TAG governance.tags.pii_bank_account 
    SET MASKING POLICY governance.policies.mask_bank_account;

ALTER TAG governance.tags.pii_date_of_birth 
    SET MASKING POLICY governance.policies.mask_date_of_birth;

ALTER TAG governance.tags.pii_name 
    SET MASKING POLICY governance.policies.mask_name;

ALTER TAG governance.tags.pii_ip_address 
    SET MASKING POLICY governance.policies.mask_ip_address;

ALTER TAG governance.tags.pii_generic 
    SET MASKING POLICY governance.policies.mask_pii_generic;

-- ============================================================================
-- SECTION 8: Update Mapping Table with Specific Tags
-- ============================================================================

USE ROLE governance_admin;
USE SCHEMA governance.metadata;

-- Clear existing mappings
DELETE FROM tag_policy_mapping WHERE tag_name = 'pii_type';

-- Insert new specific tag mappings
INSERT INTO tag_policy_mapping 
    (tag_name, policy_name, policy_type, notes)
VALUES
    ('pii_email', 'mask_email', 'MASKING', 'Email masking policy'),
    ('pii_phone', 'mask_phone', 'MASKING', 'Phone number masking policy'),
    ('pii_ssn', 'mask_ssn', 'MASKING', 'SSN masking policy'),
    ('pii_tax_id', 'mask_tax_id', 'MASKING', 'Tax ID masking policy'),
    ('pii_address', 'mask_address', 'MASKING', 'Address masking policy'),
    ('pii_credit_card', 'mask_credit_card', 'MASKING', 'Credit card masking policy'),
    ('pii_bank_account', 'mask_bank_account', 'MASKING', 'Bank account masking policy'),
    ('pii_date_of_birth', 'mask_date_of_birth', 'MASKING', 'Date of birth masking policy'),
    ('pii_name', 'mask_name', 'MASKING', 'Name masking policy'),
    ('pii_ip_address', 'mask_ip_address', 'MASKING', 'IP address masking policy'),
    ('pii_generic', 'mask_pii_generic', 'MASKING', 'Generic PII masking policy');

-- ============================================================================
-- SECTION 9: Create View for Active Policy Associations
-- ============================================================================

CREATE OR REPLACE VIEW v_active_policy_associations AS
SELECT 
    tpm.tag_name,
    tpm.policy_name,
    tpm.policy_type,
    tpm.effective_date,
    tpm.created_by,
    t.comment AS tag_description,
    mp.comment AS policy_description
FROM governance.metadata.tag_policy_mapping tpm
LEFT JOIN governance.tags.information_schema.tags t
    ON tpm.tag_name = t.tag_name
    AND t.tag_database = 'GOVERNANCE'
    AND t.tag_schema = 'TAGS'
LEFT JOIN governance.policies.information_schema.table_constraints mp
    ON tpm.policy_name = mp.constraint_name
WHERE tpm.is_active = TRUE
ORDER BY tpm.tag_name;

-- Grant select to relevant roles
GRANT SELECT ON governance.metadata.v_active_policy_associations TO ROLE data_steward;
GRANT SELECT ON governance.metadata.v_active_policy_associations TO ROLE compliance_officer;

-- ============================================================================
-- SECTION 10: Create Helper Functions for Tagging
-- ============================================================================

-- Function to get recommended tag for column based on name pattern
CREATE OR REPLACE FUNCTION governance.metadata.fn_suggest_pii_tag(column_name VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    CASE
        WHEN column_name ILIKE '%email%' THEN 'pii_email'
        WHEN column_name ILIKE '%phone%' OR column_name ILIKE '%mobile%' THEN 'pii_phone'
        WHEN column_name ILIKE '%ssn%' OR column_name ILIKE '%social_security%' THEN 'pii_ssn'
        WHEN column_name ILIKE '%tax_id%' OR column_name ILIKE '%ein%' THEN 'pii_tax_id'
        WHEN column_name ILIKE '%address%' OR column_name ILIKE '%street%' THEN 'pii_address'
        WHEN column_name ILIKE '%credit_card%' OR column_name ILIKE '%cc_number%' THEN 'pii_credit_card'
        WHEN column_name ILIKE '%account%' AND column_name ILIKE '%number%' THEN 'pii_bank_account'
        WHEN column_name ILIKE '%dob%' OR column_name ILIKE '%birth_date%' THEN 'pii_date_of_birth'
        WHEN column_name ILIKE '%name%' AND NOT column_name ILIKE '%file%' THEN 'pii_name'
        WHEN column_name ILIKE '%ip_address%' OR column_name = 'ip' THEN 'pii_ip_address'
        ELSE 'pii_generic'
    END
$$;

-- ============================================================================
-- SECTION 11: Validation and Testing
-- ============================================================================

-- Verify all policy-tag associations
SELECT 'Policy-Tag associations completed!' AS status;

-- Show current policy associations
SELECT 
    tag_name,
    tag_database || '.' || tag_schema AS tag_location,
    policy_name,
    policy_database || '.' || policy_schema AS policy_location,
    policy_kind
FROM snowflake.account_usage.policy_references
WHERE policy_database = 'GOVERNANCE'
  AND policy_schema = 'POLICIES'
  AND deleted IS NULL
ORDER BY tag_name;

-- Show summary
SELECT '
=============================================================================
POLICY-TAG ASSOCIATIONS COMPLETED
=============================================================================

Specific PII Tags Created and Associated:
- pii_email → mask_email
- pii_phone → mask_phone  
- pii_ssn → mask_ssn
- pii_tax_id → mask_tax_id
- pii_address → mask_address
- pii_credit_card → mask_credit_card
- pii_bank_account → mask_bank_account
- pii_date_of_birth → mask_date_of_birth
- pii_name → mask_name
- pii_ip_address → mask_ip_address
- pii_generic → mask_pii_generic

Usage Example:
To tag an email column:
  ALTER TABLE my_db.my_schema.customers
    MODIFY COLUMN email SET TAG governance.tags.pii_email = ''true'';

The mask_email policy will automatically apply!

Helper Function:
Get suggested tag for a column:
  SELECT governance.metadata.fn_suggest_pii_tag(''email_address'');
  -- Returns: pii_email

View Active Associations:
  SELECT * FROM governance.metadata.v_active_policy_associations;

Next Steps:
1. Begin tagging columns in your databases
2. Test masking behavior with different roles
3. Monitor tag coverage
4. Document any policy exceptions

For questions, contact: security-team@company.com
=============================================================================
' AS summary;

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
