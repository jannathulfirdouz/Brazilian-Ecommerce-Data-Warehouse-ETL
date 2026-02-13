-- =====================================================
-- OLIST E-COMMERCE DATA WAREHOUSE & ETL PIPELINE
-- PART 1: CREATE STAGING TABLES (RAW DATA LAYER)
-- =====================================================
-- Author: Jannathul Firdouz
-- Project: Brazilian E-Commerce Data Warehouse
-- Purpose: Import raw CSV data into staging tables
-- =====================================================

-- Create a separate schema for staging (raw) data
CREATE SCHEMA IF NOT EXISTS staging;

-- Set search path to staging schema
SET search_path TO staging;

-- =====================================================
-- TABLE 1: CUSTOMERS (Staging)
-- =====================================================
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(5)
);

COMMENT ON TABLE staging.customers IS 'Raw customer data from CSV - uncleaned';

-- =====================================================
-- TABLE 2: ORDERS (Staging)
-- =====================================================
CREATE TABLE IF NOT EXISTS staging.orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

COMMENT ON TABLE staging.orders IS 'Raw orders data from CSV - uncleaned';

-- =====================================================
-- TABLE 3: ORDER ITEMS (Staging)
-- =====================================================
CREATE TABLE IF NOT EXISTS staging.order_items (
    order_id VARCHAR(50),
    order_item_id INTEGER,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2)
);

COMMENT ON TABLE staging.order_items IS 'Raw order items data from CSV - uncleaned';

-- =====================================================
-- TABLE 4: PRODUCTS (Staging)
-- =====================================================
CREATE TABLE IF NOT EXISTS staging.products (
    product_id VARCHAR(50),
    product_category_name VARCHAR(100),
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

COMMENT ON TABLE staging.products IS 'Raw products data from CSV - uncleaned';

-- =====================================================
-- TABLE 5: SELLERS (Staging)
-- =====================================================
CREATE TABLE IF NOT EXISTS staging.sellers (
    seller_id VARCHAR(50),
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(5)
);

COMMENT ON TABLE staging.sellers IS 'Raw sellers data from CSV - uncleaned';

-- =====================================================
-- TABLE 6: ORDER PAYMENTS (Staging)
-- =====================================================
CREATE TABLE IF NOT EXISTS staging.order_payments (
    order_id VARCHAR(50),
    payment_sequential INTEGER,
    payment_type VARCHAR(50),
    payment_installments INTEGER,
    payment_value DECIMAL(10,2)
);

COMMENT ON TABLE staging.order_payments IS 'Raw payment data from CSV - uncleaned';

-- =====================================================
-- TABLE 7: ORDER REVIEWS (Staging)
-- =====================================================
CREATE TABLE IF NOT EXISTS staging.order_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

COMMENT ON TABLE staging.order_reviews IS 'Raw reviews data from CSV - uncleaned';

-- =====================================================
-- TABLE 8: GEOLOCATION (Staging)
-- =====================================================
CREATE TABLE IF NOT EXISTS staging.geolocation (
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat DECIMAL(10,8),
    geolocation_lng DECIMAL(11,8),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(5)
);

COMMENT ON TABLE staging.geolocation IS 'Raw geolocation data from CSV - uncleaned';

-- =====================================================
-- TABLE 9: PRODUCT CATEGORY TRANSLATION (Staging)
-- =====================================================
CREATE TABLE IF NOT EXISTS staging.product_category_translation (
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100)
);

COMMENT ON TABLE staging.product_category_translation IS 'Product category translation Portuguese to English';

-- =====================================================
-- VERIFICATION QUERY
-- =====================================================
-- Run this to verify all tables were created successfully
SELECT 
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema = 'staging'
ORDER BY table_name;

