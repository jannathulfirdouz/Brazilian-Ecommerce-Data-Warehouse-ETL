-- =====================================================
-- OLIST E-COMMERCE DATA WAREHOUSE & ETL PIPELINE
-- PART 2: IMPORT CSV DATA INTO STAGING TABLES
-- =====================================================
-- Purpose: Load raw CSV files into staging tables
-- =====================================================

-- Set the file path (UPDATE THIS IF YOUR PATH IS DIFFERENT)
-- Note: Use forward slashes (/) in PostgreSQL, even on Windows

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
