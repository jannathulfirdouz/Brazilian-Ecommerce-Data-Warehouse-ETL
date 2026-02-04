-- =====================================================
-- OLIST E-COMMERCE DATA WAREHOUSE & ETL PIPELINE
-- PART 3: DATA QUALITY CHECKS
-- =====================================================
-- Purpose: Identify data quality issues before transformation
-- Key Areas: NULLs, Duplicates, Orphans, Invalid Values
-- =====================================================

-- =====================================================
-- SECTION 1: NULL VALUE ANALYSIS
-- =====================================================

-- 1.1 Check NULL values in CUSTOMERS table
SELECT 
    'customers' as table_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_id,
    SUM(CASE WHEN customer_unique_id IS NULL THEN 1 ELSE 0 END) as null_unique_id,
    SUM(CASE WHEN customer_zip_code_prefix IS NULL THEN 1 ELSE 0 END) as null_zip,
    SUM(CASE WHEN customer_city IS NULL THEN 1 ELSE 0 END) as null_city,
    SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END) as null_state
FROM staging.customers;

/* INTERPRETATION:
   - customer_id should NEVER be NULL (primary key)
   - Check if zip, city, state have NULLs (address issues)
*/

-- 1.2 Check NULL values in ORDERS table
SELECT 
    'orders' as table_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_id,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_id,
    SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END) as null_status,
    SUM(CASE WHEN order_purchase_timestamp IS NULL THEN 1 ELSE 0 END) as null_purchase_date,
    SUM(CASE WHEN order_approved_at IS NULL THEN 1 ELSE 0 END) as null_approved_date,
    SUM(CASE WHEN order_delivered_customer_date IS NULL THEN 1 ELSE 0 END) as null_delivered_date
FROM staging.orders;

/* INTERPRETATION:
   - order_approved_at might be NULL for cancelled/processing orders
   - order_delivered_customer_date NULL for pending/cancelled orders
   - These NULLs might be EXPECTED, not errors
*/

-- 1.3 Check NULL values in PRODUCTS table
SELECT 
    'products' as table_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_product_id,
    SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END) as null_category,
    SUM(CASE WHEN product_weight_g IS NULL THEN 1 ELSE 0 END) as null_weight,
    SUM(CASE WHEN product_length_cm IS NULL THEN 1 ELSE 0 END) as null_length,
    SUM(CASE WHEN product_height_cm IS NULL THEN 1 ELSE 0 END) as null_height,
    SUM(CASE WHEN product_width_cm IS NULL THEN 1 ELSE 0 END) as null_width
FROM staging.products;

/* INTERPRETATION:
   - Category NULLs = uncategorized products
   - Dimension NULLs = missing product specifications
   - These need cleaning/imputation
*/

-- 1.4 Check NULL values in ORDER_ITEMS table
SELECT 
    'order_items' as table_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_id,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_product_id,
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) as null_seller_id,
    SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) as null_price,
    SUM(CASE WHEN freight_value IS NULL THEN 1 ELSE 0 END) as null_freight
FROM staging.order_items;

/* INTERPRETATION:
   - price and freight should NEVER be NULL
   - These are critical for revenue calculations
*/


-- =====================================================
-- SECTION 2: DUPLICATE ANALYSIS
-- =====================================================

-- 2.1 Check for duplicate customers (by customer_id)
SELECT 
    customer_id,
    COUNT(*) as duplicate_count
FROM staging.customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 10;

/* INTERPRETATION:
   - If results found = data quality issue
   - customer_id should be unique
   - Need deduplication strategy
*/

-- 2.2 Check for duplicate orders (by order_id)
SELECT 
    order_id,
    COUNT(*) as duplicate_count
FROM staging.orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 10;

/* INTERPRETATION:
   - order_id should be unique
   - Duplicates indicate data loading errors
*/

-- 2.3 Check customers with multiple unique IDs (data inconsistency)
SELECT 
    customer_unique_id,
    COUNT(DISTINCT customer_id) as different_customer_ids
FROM staging.customers
GROUP BY customer_unique_id
HAVING COUNT(DISTINCT customer_id) > 1
ORDER BY different_customer_ids DESC
LIMIT 10;

/* INTERPRETATION:
   - One person might have multiple accounts
   - Important for customer analytics (LTV, retention)
*/


-- =====================================================
-- SECTION 3: REFERENTIAL INTEGRITY CHECKS (ORPHAN RECORDS)
-- =====================================================

-- 3.1 Orders without matching customers (orphan orders)
SELECT 
    COUNT(*) as orphan_orders_count
FROM staging.orders o
LEFT JOIN staging.customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

/* INTERPRETATION:
   - Should be 0
   - If > 0, data integrity issue
   - Orders exist but customers don't
*/

-- 3.2 Order items without matching orders (orphan items)
SELECT 
    COUNT(*) as orphan_items_count
FROM staging.order_items oi
LEFT JOIN staging.orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

/* INTERPRETATION:
   - Should be 0
   - Items sold without parent order = problem
*/

-- 3.3 Order items with products not in products table
SELECT 
    COUNT(*) as items_with_missing_products
FROM staging.order_items oi
LEFT JOIN staging.products p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

/* INTERPRETATION:
   - Products sold but not in catalog
   - Might be deleted/discontinued products
*/

-- 3.4 Order items with sellers not in sellers table
SELECT 
    COUNT(*) as items_with_missing_sellers
FROM staging.order_items oi
LEFT JOIN staging.sellers s ON oi.seller_id = s.seller_id
WHERE s.seller_id IS NULL;

/* INTERPRETATION:
   - Items sold by unknown sellers
   - Seller might have been removed from platform
*/


-- =====================================================
-- SECTION 4: INVALID DATA VALUES
-- =====================================================

-- 4.1 Check for negative prices or freight values
SELECT 
    'Negative Prices' as issue,
    COUNT(*) as count
FROM staging.order_items
WHERE price < 0

UNION ALL

SELECT 
    'Negative Freight' as issue,
    COUNT(*) as count
FROM staging.order_items
WHERE freight_value < 0

UNION ALL

SELECT 
    'Zero Prices' as issue,
    COUNT(*) as count
FROM staging.order_items
WHERE price = 0;

/* INTERPRETATION:
   - Negative values = data errors
   - Zero prices might be promotions/gifts (valid but unusual)
*/

-- 4.2 Check for invalid order statuses
SELECT 
    order_status,
    COUNT(*) as status_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM staging.orders
GROUP BY order_status
ORDER BY status_count DESC;

/* INTERPRETATION:
   - Shows distribution of order statuses
   - Identifies any unusual/invalid statuses
   - Expected: delivered, shipped, cancelled, processing, etc.
*/

-- 4.3 Check for future order dates (impossible timestamps)
SELECT 
    'Future Purchase Dates' as issue,
    COUNT(*) as count
FROM staging.orders
WHERE order_purchase_timestamp > CURRENT_TIMESTAMP;

/* INTERPRETATION:
   - Should be 0
   - Future dates = data entry errors
*/

-- 4.4 Check for delivery before purchase (logical errors)
SELECT 
    COUNT(*) as illogical_delivery_count
FROM staging.orders
WHERE order_delivered_customer_date < order_purchase_timestamp;

/* INTERPRETATION:
   - Impossible: delivered before ordered
   - Data quality issue needing correction
*/

-- 4.5 Check review scores outside valid range (1-5)
SELECT 
    'Invalid Review Scores' as issue,
    COUNT(*) as count
FROM staging.order_reviews
WHERE review_score NOT BETWEEN 1 AND 5 
   OR review_score IS NULL;

/* INTERPRETATION:
   - Valid range: 1-5 stars
   - Outside range = data errors
*/


-- =====================================================
-- SECTION 5: DATA COMPLETENESS SUMMARY
-- =====================================================

-- 5.1 Overall data completeness report
SELECT 
    'Orders with all timestamps' as metric,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM staging.orders), 2) as percentage
FROM staging.orders
WHERE order_purchase_timestamp IS NOT NULL
  AND order_approved_at IS NOT NULL
  AND order_delivered_carrier_date IS NOT NULL
  AND order_delivered_customer_date IS NOT NULL

UNION ALL

SELECT 
    'Products with complete specs' as metric,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM staging.products), 2) as percentage
FROM staging.products
WHERE product_category_name IS NOT NULL
  AND product_weight_g IS NOT NULL
  AND product_length_cm IS NOT NULL
  AND product_height_cm IS NOT NULL
  AND product_width_cm IS NOT NULL

UNION ALL

SELECT 
    'Customers with complete address' as metric,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM staging.customers), 2) as percentage
FROM staging.customers
WHERE customer_zip_code_prefix IS NOT NULL
  AND customer_city IS NOT NULL
  AND customer_state IS NOT NULL;

/* INTERPRETATION:
   - Shows % of records with complete data
   - Low percentage = need imputation/cleaning strategy
   - High percentage = good data quality
*/


-- =====================================================
-- SECTION 6: DISTRIBUTION ANALYSIS
-- =====================================================

-- 6.1 Top states by customer count
SELECT 
    customer_state,
    COUNT(*) as customer_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM staging.customers
WHERE customer_state IS NOT NULL
GROUP BY customer_state
ORDER BY customer_count DESC
LIMIT 10;

/* INTERPRETATION:
   - Geographic distribution of customers
   - Identifies key markets
   - SP (São Paulo) likely dominant
*/

-- 6.2 Product category distribution
SELECT 
    p.product_category_name,
    t.product_category_name_english,
    COUNT(*) as product_count
FROM staging.products p
LEFT JOIN staging.product_category_translation t 
    ON p.product_category_name = t.product_category_name
GROUP BY p.product_category_name, t.product_category_name_english
ORDER BY product_count DESC
LIMIT 10;

/* INTERPRETATION:
   - Most common product categories
   - NULL categories need attention
   - Guides business focus areas
*/

-- 6.3 Payment type distribution
SELECT 
    payment_type,
    COUNT(*) as payment_count,
    ROUND(AVG(payment_value), 2) as avg_payment_value,
    ROUND(SUM(payment_value), 2) as total_value
FROM staging.order_payments
GROUP BY payment_type
ORDER BY payment_count DESC;

/* INTERPRETATION:
   - Preferred payment methods
   - Credit card likely most common
   - Average transaction size per payment type
*/


-- =====================================================
-- SUMMARY: KEY DATA QUALITY ISSUES TO ADDRESS
-- =====================================================
-- Based on the queries above, document:
-- 1. Critical NULLs that need handling
-- 2. Duplicates requiring deduplication
-- 3. Orphan records needing resolution
-- 4. Invalid values requiring correction
-- 5. Completeness gaps needing imputation
--
-- NEXT STEPS:
-- - Review all query results
-- - Document findings in a separate report
-- - Create data cleaning strategy (Part 4)
-- =====================================================