-- ============================================================================
-- Script: 02_tags/create_core_tags.sql
-- Purpose: Create comprehensive tag framework for data governance
-- Author: Venkannababu Thatavarthi
-- Date: October 13, 2025
-- Version: 1.0
-- ============================================================================
-- Prerequisites: TAG_ADMIN role with CREATE TAG privilege
-- Execution Time: ~5 minutes
-- ============================================================================

-- Set context to TAG_ADMIN role
USE ROLE tag_admin;
USE SCHEMA governance.tags;

-- ============================================================================
-- SECTION 1: Data Sensitivity Classification Tags
-- ============================================================================
-- Purpose: Classify data based on sensitivity levels
-- ============================================================================

CREATE OR REPLACE TAG data_sensitivity
    ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    COMMENT = 'Data sensitivity classification: PUBLIC (non-sensitive), INTERNAL (company only), CONFIDENTIAL (restricted access), RESTRICTED (highly sensitive)';

-- ============================================================================
-- SECTION 2: PII (Personally Identifiable Information) Tags
-- ============================================================================
-- Purpose: Identify columns containing PII data types
-- ============================================================================

CREATE OR REPLACE TAG pii_type
    ALLOWED_VALUES 'EMAIL', 'PHONE', 'SSN', 'TAX_ID', 'ADDRESS', 
                   'CREDIT_CARD', 'BANK_ACCOUNT', 'PASSPORT', 
                   'DRIVERS_LICENSE', 'DATE_OF_BIRTH', 'NAME',
                   'IP_ADDRESS', 'BIOMETRIC'
    COMMENT = 'Personally Identifiable Information type classification';

-- Create more specific PII tags
CREATE OR REPLACE TAG pii_category
    ALLOWED_VALUES 'DIRECT_IDENTIFIER', 'QUASI_IDENTIFIER', 'SENSITIVE_PII'
    COMMENT = 'PII category: DIRECT_IDENTIFIER (uniquely identifies individual), QUASI_IDENTIFIER (combination may identify), SENSITIVE_PII (race, health, financial)';

-- ============================================================================
-- SECTION 3: Compliance and Regulatory Tags
-- ============================================================================
-- Purpose: Track data subject to specific regulations
-- ============================================================================

CREATE OR REPLACE TAG compliance_scope
    ALLOWED_VALUES 'GDPR', 'CCPA', 'HIPAA', 'PCI_DSS', 'SOX', 
                   'GLBA', 'FERPA', 'COPPA', 'LGPD', 'PIPEDA'
    COMMENT = 'Regulatory compliance scope for data governance';

-- Create tag for data residency requirements
CREATE OR REPLACE TAG data_residency
    ALLOWED_VALUES 'US', 'EU', 'UK', 'CANADA', 'APAC', 'GLOBAL', 'RESTRICTED'
    COMMENT = 'Data residency and geographic restriction requirements';

-- Create tag for retention requirements
CREATE OR REPLACE TAG retention_period
    ALLOWED_VALUES '30_DAYS', '90_DAYS', '1_YEAR', '3_YEARS', 
                   '7_YEARS', '10_YEARS', 'PERMANENT', 'CUSTOM'
    COMMENT = 'Data retention period requirements';

-- ============================================================================
-- SECTION 4: Data Domain and Ownership Tags
-- ============================================================================
-- Purpose: Organize data by business domain and ownership
-- ============================================================================

CREATE OR REPLACE TAG data_domain
    ALLOWED_VALUES 'FINANCE', 'HR', 'SALES', 'MARKETING', 'OPERATIONS',
                   'LEGAL', 'CUSTOMER_SERVICE', 'PRODUCT', 'ENGINEERING',
                   'SUPPLY_CHAIN', 'CORPORATE'
    COMMENT = 'Business domain ownership classification';

-- Create tag for data owner
CREATE OR REPLACE TAG data_owner
    COMMENT = 'Email or identifier of the data owner responsible for the data asset';

-- Create tag for business criticality
CREATE OR REPLACE TAG business_criticality
    ALLOWED_VALUES 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'
    COMMENT = 'Business criticality of the data asset';

-- ============================================================================
-- SECTION 5: Data Quality Tags
-- ============================================================================
-- Purpose: Track data quality metrics and certification
-- ============================================================================

CREATE OR REPLACE TAG data_quality_tier
    ALLOWED_VALUES 'GOLD', 'SILVER', 'BRONZE', 'RAW'
    COMMENT = 'Data quality tier: GOLD (certified, production-ready), SILVER (validated), BRONZE (cleansed), RAW (unprocessed)';

-- Create tag for data freshness
CREATE OR REPLACE TAG data_freshness
    ALLOWED_VALUES 'REAL_TIME', 'HOURLY', 'DAILY', 'WEEKLY', 'MONTHLY', 'BATCH'
    COMMENT = 'Data refresh frequency and freshness indicator';

-- Create tag for data validation status
CREATE OR REPLACE TAG validation_status
    ALLOWED_VALUES 'VALIDATED', 'IN_REVIEW', 'NEEDS_VALIDATION', 'FAILED'
    COMMENT = 'Data validation status';

-- ============================================================================
-- SECTION 6: Environment and Lifecycle Tags
-- ============================================================================
-- Purpose: Track environment and lifecycle stage
-- ============================================================================

CREATE OR REPLACE TAG environment
    ALLOWED_VALUES 'PRODUCTION', 'STAGING', 'DEVELOPMENT', 'TESTING', 'SANDBOX'
    COMMENT = 'Environment classification for the data object';

-- Create tag for deprecation status
CREATE OR REPLACE TAG deprecation_status
    ALLOWED_VALUES 'ACTIVE', 'DEPRECATED', 'SUNSET_PENDING', 'ARCHIVED'
    COMMENT = 'Lifecycle status of the data object';

-- ============================================================================
-- SECTION 7: Cost and Resource Management Tags
-- ============================================================================
-- Purpose: Track cost allocation and resource usage
-- ============================================================================

CREATE OR REPLACE TAG cost_center
    COMMENT = 'Cost center code for chargeback and budgeting';

-- Create tag for project allocation
CREATE OR REPLACE TAG project_code
    COMMENT = 'Project code for resource allocation and tracking';

-- ============================================================================
-- SECTION 8: Data Lineage and Source Tags
-- ============================================================================
-- Purpose: Track data origin and lineage
-- ============================================================================

CREATE OR REPLACE TAG data_source
    COMMENT = 'Original source system or application for the data';

-- Create tag for data classification
CREATE OR REPLACE TAG data_classification
    ALLOWED_VALUES 'MASTER_DATA', 'TRANSACTIONAL', 'REFERENCE', 
                   'ANALYTICAL', 'METADATA', 'OPERATIONAL'
    COMMENT = 'Data classification by purpose and usage pattern';

-- ============================================================================
-- SECTION 9: Access Control Tags
-- ============================================================================
-- Purpose: Support attribute-based access control
-- ============================================================================

CREATE OR REPLACE TAG access_level
    ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL', 'TOP_SECRET'
    COMMENT = 'Access level classification for role-based access';

-- Create tag for minimum clearance level
CREATE OR REPLACE TAG minimum_clearance
    ALLOWED_VALUES 'LEVEL_1', 'LEVEL_2', 'LEVEL_3', 'LEVEL_4', 'EXECUTIVE'
    COMMENT = 'Minimum clearance level required to access data';

-- ============================================================================
-- SECTION 10: Special Purpose Tags
-- ============================================================================

-- Tag for encryption requirements
CREATE OR REPLACE TAG encryption_required
    ALLOWED_VALUES 'YES', 'NO'
    COMMENT = 'Indicates if data requires encryption at rest and in transit';

-- Tag for anonymization requirement
CREATE OR REPLACE TAG anonymization_required
    ALLOWED_VALUES 'FULL', 'PARTIAL', 'NONE'
    COMMENT = 'Level of anonymization required for data';

-- Tag for customer visibility
CREATE OR REPLACE TAG customer_facing
    ALLOWED_VALUES 'YES', 'NO'
    COMMENT = 'Indicates if data is exposed to customers';

-- Tag for third-party sharing
CREATE OR REPLACE TAG third_party_sharing
    ALLOWED_VALUES 'ALLOWED', 'RESTRICTED', 'PROHIBITED'
    COMMENT = 'Indicates if data can be shared with third parties';

-- ============================================================================
-- SECTION 11: Custom Application Tags
-- ============================================================================

-- Tag for Tableau reports
CREATE OR REPLACE TAG tableau_certified
    ALLOWED_VALUES 'YES', 'NO'
    COMMENT = 'Indicates if data source is certified for Tableau reporting';

-- Tag for machine learning
CREATE OR REPLACE TAG ml_training_data
    ALLOWED_VALUES 'YES', 'NO'
    COMMENT = 'Indicates if data can be used for ML model training';

-- ============================================================================
-- SECTION 12: Verify Tag Creation
-- ============================================================================

-- Show all created tags
SELECT 'All tags created successfully!' AS status;

SHOW TAGS IN SCHEMA governance.tags;

-- Count tags by category
SELECT '
=============================================================================
TAG FRAMEWORK CREATED SUCCESSFULLY
=============================================================================

Tag Categories Created:
1. Data Sensitivity Classification (1 tag)
2. PII Classification (2 tags)
3. Compliance and Regulatory (3 tags)
4. Data Domain and Ownership (3 tags)
5. Data Quality (3 tags)
6. Environment and Lifecycle (2 tags)
7. Cost and Resource Management (2 tags)
8. Data Lineage and Source (2 tags)
9. Access Control (2 tags)
10. Special Purpose (4 tags)
11. Custom Application (2 tags)

Total Tags Created: 26

Next Steps:
1. Grant APPLY privileges to DATA_STEWARD role
2. Document tag usage guidelines
3. Create masking policies
4. Begin tagging existing data assets

For tag definitions and usage guidelines, query:
  SELECT * FROM governance.tags;

For questions, contact: governance-team@company.com
=============================================================================
' AS summary;

-- ============================================================================
-- SECTION 13: Create Tag Catalog View
-- ============================================================================

-- Switch to governance_admin to create catalog
USE ROLE governance_admin;
USE SCHEMA governance.documentation;

-- Create comprehensive tag catalog
CREATE OR REPLACE VIEW v_tag_catalog AS
SELECT 
    tag_name,
    tag_schema,
    tag_database,
    allowed_values,
    comment AS description,
    created AS created_date,
    owner AS owner_role
FROM governance.tags.information_schema.tags
WHERE tag_database = 'GOVERNANCE'
  AND tag_schema = 'TAGS'
ORDER BY tag_name;

-- Grant select on catalog to all users
GRANT SELECT ON governance.documentation.v_tag_catalog TO ROLE PUBLIC;

-- ============================================================================
-- SECTION 14: Log Tag Creation
-- ============================================================================

USE ROLE tag_admin;

-- Log the tag creation event
INSERT INTO governance.metadata.tag_assignment_log 
    (tag_name, object_type, business_justification, assigned_by)
SELECT 
    tag_name,
    'TAG_DEFINITION',
    'Initial tag framework creation',
    CURRENT_USER()
FROM governance.tags.information_schema.tags
WHERE tag_database = 'GOVERNANCE' 
  AND tag_schema = 'TAGS'
  AND tag_name NOT IN (
      SELECT DISTINCT tag_name 
      FROM governance.metadata.tag_assignment_log 
      WHERE object_type = 'TAG_DEFINITION'
  );

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
