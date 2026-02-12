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


-- =====================================================
-- IMPORT 1: CUSTOMERS DATA
-- =====================================================
COPY staging.customers(
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
)
FROM 'C:/Users\JannathulFirdouz/Downloads/Dataset/olist_customers_dataset.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

-- Verify import
SELECT COUNT(*) as customer_count FROM staging.customers;

-- =====================================================
-- IMPORT 2: ORDERS DATA
-- =====================================================
COPY staging.orders(
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
)
FROM 'C:/Users\JannathulFirdouz/Downloads/Dataset/olist_orders_dataset.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

-- Verify import
SELECT COUNT(*) as orders_count FROM staging.orders;

-- =====================================================
-- IMPORT 3: ORDER ITEMS DATA
-- =====================================================
COPY staging.order_items(
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
)
FROM 'C:/Users\JannathulFirdouz/Downloads/Dataset/olist_order_items_dataset.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

-- Verify import
SELECT COUNT(*) as order_items_count FROM staging.order_items;

-- =====================================================
-- IMPORT 4: PRODUCTS DATA
-- =====================================================
COPY staging.products(
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
FROM 'C:/Users\JannathulFirdouz/Downloads/Dataset/olist_products_dataset.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

-- Verify import
SELECT COUNT(*) as products_count FROM staging.products;

-- =====================================================
-- IMPORT 5: SELLERS DATA
-- =====================================================
COPY staging.sellers(
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
)
FROM 'C:/Users\JannathulFirdouz/Downloads/Dataset/olist_sellers_dataset.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

-- Verify import
SELECT COUNT(*) as sellers_count FROM staging.sellers;

-- =====================================================
-- IMPORT 6: ORDER PAYMENTS DATA
-- =====================================================
COPY staging.order_payments(
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
)
FROM 'C:/Users\JannathulFirdouz/Downloads/Dataset/olist_order_payments_dataset.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

-- Verify import
SELECT COUNT(*) as payments_count FROM staging.order_payments;

-- =====================================================
-- IMPORT 7: ORDER REVIEWS DATA
-- =====================================================
COPY staging.order_reviews(
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
)
FROM C:/Users\JannathulFirdouz/Downloads/Dataset/olist_order_reviews_dataset.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

-- Verify import
SELECT COUNT(*) as reviews_count FROM staging.order_reviews;

-- =====================================================
-- IMPORT 8: GEOLOCATION DATA
-- =====================================================
COPY staging.geolocation(
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
)
FROM 'C:/Users\JannathulFirdouz/Downloads/Dataset/olist_geolocation_dataset.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

-- Verify import
SELECT COUNT(*) as geolocation_count FROM staging.geolocation;

-- =====================================================
-- IMPORT 9: PRODUCT CATEGORY TRANSLATION
-- =====================================================
COPY staging.product_category_translation(
    product_category_name,
    product_category_name_english
)
FROM 'C:/Users\JannathulFirdouz/Downloads/Dataset/product_category_name_translation.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

-- Verify import
SELECT COUNT(*) as category_translation_count FROM staging.product_category_translation;

-- =====================================================
-- FINAL VERIFICATION: ALL TABLES ROW COUNTS
-- =====================================================
SELECT 'customers' as table_name, COUNT(*) as row_count FROM staging.customers
UNION ALL
SELECT 'orders', COUNT(*) FROM staging.orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM staging.order_items
UNION ALL
SELECT 'products', COUNT(*) FROM staging.products
UNION ALL
SELECT 'sellers', COUNT(*) FROM staging.sellers
UNION ALL
SELECT 'order_payments', COUNT(*) FROM staging.order_payments
UNION ALL
SELECT 'order_reviews', COUNT(*) FROM staging.order_reviews
UNION ALL
SELECT 'geolocation', COUNT(*) FROM staging.geolocation
UNION ALL
SELECT 'product_category_translation', COUNT(*) FROM staging.product_category_translation
ORDER BY table_name;

-- =====================================================
-- RESULTS :
-- customers: ~99,000 rows
-- orders: ~99,000 rows
-- order_items: ~112,000 rows
-- products: ~32,000 rows
-- sellers: ~3,000 rows
-- order_payments: ~103,000 rows
-- order_reviews: ~99,000 rows
-- geolocation: ~1,000,000 rows
-- product_category_translation: ~71 rows
-- =====================================================
