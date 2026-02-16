-- ============================================================================
-- CDC FRAMEWORK - TEST SCENARIOS
-- Version: 1.0
-- Last Updated: 2026-02-16
-- Description: Comprehensive test scenarios for CDC Framework validation
-- ============================================================================

-- ============================================================================
-- PREREQUISITES: Execute CDC_Framework_Production.sql first
-- ============================================================================

-- ============================================================================
-- SECTION 1: TEST ENVIRONMENT SETUP
-- ============================================================================

-- 1.1 Create Test Databases
CREATE DATABASE IF NOT EXISTS D_BRONZE;
CREATE SCHEMA IF NOT EXISTS D_BRONZE.SALES;
CREATE DATABASE IF NOT EXISTS D_SILVER;
CREATE SCHEMA IF NOT EXISTS D_SILVER.SALES;

-- 1.2 Create Source Tables
CREATE OR REPLACE TABLE D_BRONZE.SALES.CUSTOMERS (
    CUSTOMER_ID NUMBER PRIMARY KEY,
    CUSTOMER_NAME VARCHAR(255),
    EMAIL VARCHAR(255),
    PHONE VARCHAR(50),
    CITY VARCHAR(100),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE D_BRONZE.SALES.ORDERS (
    ORDER_ID NUMBER PRIMARY KEY,
    CUSTOMER_ID NUMBER,
    ORDER_DATE DATE,
    TOTAL_AMOUNT DECIMAL(12,2),
    STATUS VARCHAR(50),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE D_BRONZE.SALES.PRODUCTS (
    PRODUCT_ID NUMBER PRIMARY KEY,
    PRODUCT_NAME VARCHAR(255),
    CATEGORY VARCHAR(100),
    PRICE DECIMAL(10,2),
    STOCK_QTY NUMBER,
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 1.3 Insert Test Data
INSERT INTO D_BRONZE.SALES.CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE, CITY)
VALUES 
    (1, 'John Smith', 'john@email.com', '555-0101', 'New York'),
    (2, 'Jane Doe', 'jane@email.com', '555-0102', 'Los Angeles'),
    (3, 'Bob Wilson', 'bob@email.com', '555-0103', 'Chicago'),
    (4, 'Alice Brown', 'alice@email.com', '555-0104', 'Houston'),
    (5, 'Charlie Davis', 'charlie@email.com', '555-0105', 'Phoenix');

INSERT INTO D_BRONZE.SALES.ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_DATE, TOTAL_AMOUNT, STATUS)
VALUES 
    (101, 1, '2025-01-15', 250.00, 'COMPLETED'),
    (102, 2, '2025-01-16', 175.50, 'COMPLETED'),
    (103, 3, '2025-01-17', 320.75, 'PENDING'),
    (104, 1, '2025-01-18', 89.99, 'SHIPPED'),
    (105, 4, '2025-01-19', 450.00, 'COMPLETED');

INSERT INTO D_BRONZE.SALES.PRODUCTS (PRODUCT_ID, PRODUCT_NAME, CATEGORY, PRICE, STOCK_QTY)
VALUES
    (1001, 'Laptop Pro', 'Electronics', 1299.99, 50),
    (1002, 'Wireless Mouse', 'Electronics', 29.99, 200),
    (1003, 'Office Chair', 'Furniture', 349.99, 75),
    (1004, 'Desk Lamp', 'Furniture', 49.99, 150),
    (1005, 'USB Cable', 'Electronics', 9.99, 500);

-- 1.4 Create Target Tables
CREATE OR REPLACE TABLE D_SILVER.SALES.CUSTOMERS AS 
SELECT *, FALSE AS IS_DELETED FROM D_BRONZE.SALES.CUSTOMERS WHERE 1=0;

CREATE OR REPLACE TABLE D_SILVER.SALES.ORDERS CLONE D_BRONZE.SALES.ORDERS;
TRUNCATE TABLE D_SILVER.SALES.ORDERS;

CREATE OR REPLACE TABLE D_SILVER.SALES.PRODUCTS CLONE D_BRONZE.SALES.PRODUCTS;
TRUNCATE TABLE D_SILVER.SALES.PRODUCTS;

-- ============================================================================
-- SECTION 2: REGISTER CDC CONFIGURATIONS
-- ============================================================================

-- 2.1 Clear existing configurations (for clean test)
DELETE FROM CDC_FRAMEWORK.CONFIG.CDC_WATERMARKS;
DELETE FROM CDC_FRAMEWORK.MONITORING.CDC_PROCESSING_LOG;
DELETE FROM CDC_FRAMEWORK.MONITORING.CDC_ERROR_LOG;
DELETE FROM CDC_FRAMEWORK.CONFIG.CDC_TABLE_CONFIG;

-- 2.2 Register test tables
-- CUSTOMERS: Soft Delete CDC
CALL CDC_FRAMEWORK.CONFIG.SP_REGISTER_CDC_TABLE(
    'D_BRONZE', 'SALES', 'CUSTOMERS',
    'D_SILVER', 'SALES', 'CUSTOMERS',
    'CUSTOMER_ID',
    'SOFT_DELETE',
    'UPDATED_AT',
    '',
    'Customer master with soft delete tracking'
);

-- ORDERS: Incremental CDC
CALL CDC_FRAMEWORK.CONFIG.SP_REGISTER_CDC_TABLE(
    'D_BRONZE', 'SALES', 'ORDERS',
    'D_SILVER', 'SALES', 'ORDERS',
    'ORDER_ID',
    'INCREMENTAL',
    'UPDATED_AT',
    '',
    'Orders with incremental load based on UPDATED_AT'
);

-- PRODUCTS: Full CDC
CALL CDC_FRAMEWORK.CONFIG.SP_REGISTER_CDC_TABLE(
    'D_BRONZE', 'SALES', 'PRODUCTS',
    'D_SILVER', 'SALES', 'PRODUCTS',
    'PRODUCT_ID',
    'FULL',
    NULL,
    '',
    'Products with full merge sync'
);

-- 2.3 Verify registrations
SELECT CONFIG_ID, SOURCE_TABLE, TARGET_TABLE, CDC_TYPE, IS_ACTIVE 
FROM CDC_FRAMEWORK.CONFIG.CDC_TABLE_CONFIG
ORDER BY CONFIG_ID;

-- ============================================================================
-- SECTION 3: TEST CASE 1 - INITIAL LOAD (DRY RUN)
-- ============================================================================
-- Objective: Validate generated SQL without executing

SELECT '=== TEST CASE 1: DRY RUN ===' AS TEST;

CALL CDC_FRAMEWORK.PROCESSING.SP_PROCESS_CDC_GENERIC(NULL, NULL, TRUE);

-- Expected: All 3 tables show DRY_RUN status with valid MERGE SQL

-- ============================================================================
-- SECTION 4: TEST CASE 2 - INITIAL LOAD (ACTUAL)
-- ============================================================================
-- Objective: Perform initial data sync

SELECT '=== TEST CASE 2: INITIAL LOAD ===' AS TEST;

CALL CDC_FRAMEWORK.PROCESSING.SP_PROCESS_CDC_GENERIC();

-- Validate counts
SELECT 'SOURCE_CUSTOMERS' AS TBL, COUNT(*) AS CNT FROM D_BRONZE.SALES.CUSTOMERS
UNION ALL SELECT 'TARGET_CUSTOMERS', COUNT(*) FROM D_SILVER.SALES.CUSTOMERS
UNION ALL SELECT 'SOURCE_ORDERS', COUNT(*) FROM D_BRONZE.SALES.ORDERS
UNION ALL SELECT 'TARGET_ORDERS', COUNT(*) FROM D_SILVER.SALES.ORDERS
UNION ALL SELECT 'SOURCE_PRODUCTS', COUNT(*) FROM D_BRONZE.SALES.PRODUCTS
UNION ALL SELECT 'TARGET_PRODUCTS', COUNT(*) FROM D_SILVER.SALES.PRODUCTS;

-- Expected: All source/target pairs should have matching counts

-- ============================================================================
-- SECTION 5: TEST CASE 3 - INSERT NEW RECORDS
-- ============================================================================
-- Objective: Test INSERT detection for all CDC types

SELECT '=== TEST CASE 3: INSERT NEW RECORDS ===' AS TEST;

-- Insert new data in source
INSERT INTO D_BRONZE.SALES.CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE, CITY)
VALUES (6, 'Diana Prince', 'diana@email.com', '555-0106', 'Seattle');

INSERT INTO D_BRONZE.SALES.ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_DATE, TOTAL_AMOUNT, STATUS)
VALUES (106, 5, '2025-01-20', 199.99, 'PENDING');

INSERT INTO D_BRONZE.SALES.PRODUCTS (PRODUCT_ID, PRODUCT_NAME, CATEGORY, PRICE, STOCK_QTY)
VALUES (1006, 'Keyboard', 'Electronics', 79.99, 100);

-- Run CDC
CALL CDC_FRAMEWORK.PROCESSING.SP_PROCESS_CDC_GENERIC();

-- Validate new records synced
SELECT * FROM D_SILVER.SALES.CUSTOMERS WHERE CUSTOMER_ID = 6;
SELECT * FROM D_SILVER.SALES.ORDERS WHERE ORDER_ID = 106;
SELECT * FROM D_SILVER.SALES.PRODUCTS WHERE PRODUCT_ID = 1006;

-- Expected: All new records should exist in target

-- ============================================================================
-- SECTION 6: TEST CASE 4 - UPDATE RECORDS
-- ============================================================================
-- Objective: Test UPDATE detection for all CDC types

SELECT '=== TEST CASE 4: UPDATE RECORDS ===' AS TEST;

-- Update existing records
UPDATE D_BRONZE.SALES.CUSTOMERS 
SET CITY = 'San Francisco', UPDATED_AT = CURRENT_TIMESTAMP()
WHERE CUSTOMER_ID = 1;

UPDATE D_BRONZE.SALES.ORDERS 
SET STATUS = 'DELIVERED', TOTAL_AMOUNT = 275.00, UPDATED_AT = CURRENT_TIMESTAMP()
WHERE ORDER_ID = 101;

UPDATE D_BRONZE.SALES.PRODUCTS 
SET PRICE = 1199.99, STOCK_QTY = 45, UPDATED_AT = CURRENT_TIMESTAMP()
WHERE PRODUCT_ID = 1001;

-- Run CDC
CALL CDC_FRAMEWORK.PROCESSING.SP_PROCESS_CDC_GENERIC();

-- Validate updates propagated
SELECT CUSTOMER_ID, CITY FROM D_SILVER.SALES.CUSTOMERS WHERE CUSTOMER_ID = 1;
-- Expected: San Francisco

SELECT ORDER_ID, STATUS, TOTAL_AMOUNT FROM D_SILVER.SALES.ORDERS WHERE ORDER_ID = 101;
-- Expected: DELIVERED, 275.00

SELECT PRODUCT_ID, PRICE, STOCK_QTY FROM D_SILVER.SALES.PRODUCTS WHERE PRODUCT_ID = 1001;
-- Expected: 1199.99, 45

-- ============================================================================
-- SECTION 7: TEST CASE 5 - SOFT DELETE
-- ============================================================================
-- Objective: Test soft delete detection for SOFT_DELETE CDC type

SELECT '=== TEST CASE 5: SOFT DELETE ===' AS TEST;

-- Delete a customer from source (simulate delete)
DELETE FROM D_BRONZE.SALES.CUSTOMERS WHERE CUSTOMER_ID = 3;

-- Run soft delete detection
CALL CDC_FRAMEWORK.PROCESSING.SP_DETECT_SOFT_DELETES(1); -- CONFIG_ID for CUSTOMERS

-- Validate soft delete flagged
SELECT CUSTOMER_ID, CUSTOMER_NAME, IS_DELETED 
FROM D_SILVER.SALES.CUSTOMERS 
WHERE CUSTOMER_ID = 3;

-- Expected: IS_DELETED = TRUE

-- ============================================================================
-- SECTION 8: TEST CASE 6 - INCREMENTAL WATERMARK
-- ============================================================================
-- Objective: Validate watermark tracking for INCREMENTAL CDC

SELECT '=== TEST CASE 6: WATERMARK TRACKING ===' AS TEST;

-- Check current watermark
SELECT * FROM CDC_FRAMEWORK.CONFIG.CDC_WATERMARKS WHERE CONFIG_ID = 2;

-- Add records with timestamps after watermark
INSERT INTO D_BRONZE.SALES.ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_DATE, TOTAL_AMOUNT, STATUS, UPDATED_AT)
VALUES (107, 2, '2025-01-21', 500.00, 'COMPLETED', CURRENT_TIMESTAMP());

-- Run CDC (should only pick up new record)
CALL CDC_FRAMEWORK.PROCESSING.SP_PROCESS_CDC_GENERIC(2, NULL, FALSE);

-- Validate watermark updated
SELECT * FROM CDC_FRAMEWORK.CONFIG.CDC_WATERMARKS WHERE CONFIG_ID = 2;

-- ============================================================================
-- SECTION 9: TEST CASE 7 - FILTER BY CONFIG_ID
-- ============================================================================
-- Objective: Test single table processing

SELECT '=== TEST CASE 7: SINGLE TABLE PROCESSING ===' AS TEST;

-- Process only PRODUCTS (assuming CONFIG_ID = 3)
CALL CDC_FRAMEWORK.PROCESSING.SP_PROCESS_CDC_GENERIC(3, NULL, FALSE);

-- Expected: Only PRODUCTS table processed

-- ============================================================================
-- SECTION 10: TEST CASE 8 - FILTER BY TABLE NAME
-- ============================================================================
-- Objective: Test table name filter

SELECT '=== TEST CASE 8: TABLE NAME FILTER ===' AS TEST;

-- Process only tables matching 'CUST%'
CALL CDC_FRAMEWORK.PROCESSING.SP_PROCESS_CDC_GENERIC(NULL, 'CUST%', FALSE);

-- Expected: Only CUSTOMERS table processed

-- ============================================================================
-- SECTION 11: TEST CASE 9 - MONITORING VIEWS
-- ============================================================================
-- Objective: Validate monitoring views

SELECT '=== TEST CASE 9: MONITORING ===' AS TEST;

-- Dashboard view
SELECT * FROM CDC_FRAMEWORK.MONITORING.V_CDC_DASHBOARD;

-- Statistics view
SELECT * FROM CDC_FRAMEWORK.MONITORING.V_CDC_STATISTICS;

-- Recent errors (should be empty if all tests passed)
SELECT * FROM CDC_FRAMEWORK.MONITORING.V_CDC_RECENT_ERRORS;

-- ============================================================================
-- SECTION 12: TEST CASE 10 - UTILITY PROCEDURES
-- ============================================================================
-- Objective: Test utility procedures

SELECT '=== TEST CASE 10: UTILITIES ===' AS TEST;

-- Deactivate a configuration
CALL CDC_FRAMEWORK.CONFIG.SP_DEACTIVATE_CDC(3);
SELECT CONFIG_ID, IS_ACTIVE FROM CDC_FRAMEWORK.CONFIG.CDC_TABLE_CONFIG WHERE CONFIG_ID = 3;
-- Expected: IS_ACTIVE = FALSE

-- Reactivate
CALL CDC_FRAMEWORK.CONFIG.SP_ACTIVATE_CDC(3);
SELECT CONFIG_ID, IS_ACTIVE FROM CDC_FRAMEWORK.CONFIG.CDC_TABLE_CONFIG WHERE CONFIG_ID = 3;
-- Expected: IS_ACTIVE = TRUE

-- Reset watermark
CALL CDC_FRAMEWORK.CONFIG.SP_RESET_WATERMARK(2, '2025-01-01 00:00:00');
SELECT * FROM CDC_FRAMEWORK.CONFIG.CDC_WATERMARKS WHERE CONFIG_ID = 2;
-- Expected: LAST_WATERMARK_VALUE = '2025-01-01 00:00:00'

-- ============================================================================
-- SECTION 13: COMPOSITE KEY TEST
-- ============================================================================
-- Objective: Test tables with composite primary keys

SELECT '=== TEST CASE 11: COMPOSITE PRIMARY KEY ===' AS TEST;

-- Create table with composite key
CREATE OR REPLACE TABLE D_BRONZE.SALES.ORDER_ITEMS (
    ORDER_ID NUMBER,
    ITEM_ID NUMBER,
    PRODUCT_ID NUMBER,
    QUANTITY NUMBER,
    UNIT_PRICE DECIMAL(10,2),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ORDER_ID, ITEM_ID)
);

CREATE OR REPLACE TABLE D_SILVER.SALES.ORDER_ITEMS CLONE D_BRONZE.SALES.ORDER_ITEMS;
TRUNCATE TABLE D_SILVER.SALES.ORDER_ITEMS;

INSERT INTO D_BRONZE.SALES.ORDER_ITEMS (ORDER_ID, ITEM_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE)
VALUES (101, 1, 1001, 2, 1299.99), (101, 2, 1002, 3, 29.99), (102, 1, 1003, 1, 349.99);

-- Register with composite key
CALL CDC_FRAMEWORK.CONFIG.SP_REGISTER_CDC_TABLE(
    'D_BRONZE', 'SALES', 'ORDER_ITEMS',
    'D_SILVER', 'SALES', 'ORDER_ITEMS',
    'ORDER_ID,ITEM_ID',  -- Composite key
    'FULL',
    NULL,
    '',
    'Order items with composite primary key'
);

-- Run CDC
CALL CDC_FRAMEWORK.PROCESSING.SP_PROCESS_CDC_GENERIC(NULL, 'ORDER_ITEMS', FALSE);

-- Validate
SELECT COUNT(*) AS CNT FROM D_SILVER.SALES.ORDER_ITEMS;
-- Expected: 3 rows

-- ============================================================================
-- SECTION 14: EXCLUDE COLUMNS TEST
-- ============================================================================
-- Objective: Test column exclusion feature

SELECT '=== TEST CASE 12: EXCLUDE COLUMNS ===' AS TEST;

-- Create test with sensitive columns
CREATE OR REPLACE TABLE D_BRONZE.SALES.EMPLOYEES (
    EMP_ID NUMBER PRIMARY KEY,
    EMP_NAME VARCHAR(255),
    SALARY DECIMAL(12,2),  -- Sensitive
    SSN VARCHAR(11),       -- Sensitive
    DEPARTMENT VARCHAR(100),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE D_SILVER.SALES.EMPLOYEES (
    EMP_ID NUMBER PRIMARY KEY,
    EMP_NAME VARCHAR(255),
    DEPARTMENT VARCHAR(100),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO D_BRONZE.SALES.EMPLOYEES VALUES
(1, 'Alice Manager', 85000.00, '123-45-6789', 'IT', CURRENT_TIMESTAMP()),
(2, 'Bob Developer', 75000.00, '987-65-4321', 'IT', CURRENT_TIMESTAMP());

-- Register with excluded columns
CALL CDC_FRAMEWORK.CONFIG.SP_REGISTER_CDC_TABLE(
    'D_BRONZE', 'SALES', 'EMPLOYEES',
    'D_SILVER', 'SALES', 'EMPLOYEES',
    'EMP_ID',
    'FULL',
    NULL,
    'SALARY,SSN',  -- Exclude sensitive columns
    'Employees with sensitive columns excluded'
);

-- Run CDC (DRY RUN to see SQL)
CALL CDC_FRAMEWORK.PROCESSING.SP_PROCESS_CDC_GENERIC(NULL, 'EMPLOYEES', TRUE);

-- Expected: MERGE SQL should NOT include SALARY or SSN columns

-- ============================================================================
-- SECTION 15: FINAL VALIDATION SUMMARY
-- ============================================================================

SELECT '=== FINAL VALIDATION SUMMARY ===' AS TEST;

-- Configuration count
SELECT 'CONFIGURATIONS' AS METRIC, COUNT(*) AS VALUE FROM CDC_FRAMEWORK.CONFIG.CDC_TABLE_CONFIG
UNION ALL
SELECT 'ACTIVE_CONFIGS', COUNT(*) FROM CDC_FRAMEWORK.CONFIG.CDC_TABLE_CONFIG WHERE IS_ACTIVE = TRUE
UNION ALL
SELECT 'TOTAL_RUNS', COUNT(*) FROM CDC_FRAMEWORK.MONITORING.CDC_PROCESSING_LOG
UNION ALL
SELECT 'SUCCESSFUL_RUNS', COUNT(*) FROM CDC_FRAMEWORK.MONITORING.CDC_PROCESSING_LOG WHERE STATUS = 'SUCCESS'
UNION ALL
SELECT 'FAILED_RUNS', COUNT(*) FROM CDC_FRAMEWORK.MONITORING.CDC_PROCESSING_LOG WHERE STATUS = 'FAILED'
UNION ALL
SELECT 'WATERMARKS_TRACKED', COUNT(*) FROM CDC_FRAMEWORK.CONFIG.CDC_WATERMARKS;

-- Full dashboard view
SELECT * FROM CDC_FRAMEWORK.MONITORING.V_CDC_DASHBOARD ORDER BY CONFIG_ID;

-- ============================================================================
-- CLEANUP (Optional - Uncomment to clean up test data)
-- ============================================================================
/*
DROP DATABASE IF EXISTS D_BRONZE;
DROP DATABASE IF EXISTS D_SILVER;
DELETE FROM CDC_FRAMEWORK.CONFIG.CDC_WATERMARKS;
DELETE FROM CDC_FRAMEWORK.MONITORING.CDC_PROCESSING_LOG;
DELETE FROM CDC_FRAMEWORK.MONITORING.CDC_ERROR_LOG;
DELETE FROM CDC_FRAMEWORK.CONFIG.CDC_TABLE_CONFIG;
*/

-- ============================================================================
-- END OF TEST SCENARIOS
-- ============================================================================
