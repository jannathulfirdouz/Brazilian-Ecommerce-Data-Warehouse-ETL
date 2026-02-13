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

-- =====================================================
-- COMPREHENSIVE NULL ANALYSIS - ALL TABLES
-- =====================================================
-- Shows NULL counts and percentages for all columns in all staging tables
-- =====================================================

-- CUSTOMERS TABLE - NULL Analysis
SELECT 
    'customers' as table_name,
    'customer_id' as column_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_count,
    ROUND(100.0 * SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as null_percentage
FROM staging.customers

UNION ALL

SELECT 'customers', 'customer_unique_id', COUNT(*), 
    SUM(CASE WHEN customer_unique_id IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN customer_unique_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.customers

UNION ALL

SELECT 'customers', 'customer_zip_code_prefix', COUNT(*), 
    SUM(CASE WHEN customer_zip_code_prefix IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN customer_zip_code_prefix IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.customers

UNION ALL

SELECT 'customers', 'customer_city', COUNT(*), 
    SUM(CASE WHEN customer_city IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN customer_city IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.customers

UNION ALL

SELECT 'customers', 'customer_state', COUNT(*), 
    SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.customers

-- ORDERS TABLE - NULL Analysis
UNION ALL

SELECT 'orders', 'order_id', COUNT(*), 
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.orders

UNION ALL

SELECT 'orders', 'customer_id', COUNT(*), 
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.orders

UNION ALL

SELECT 'orders', 'order_status', COUNT(*), 
    SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.orders

UNION ALL

SELECT 'orders', 'order_purchase_timestamp', COUNT(*), 
    SUM(CASE WHEN order_purchase_timestamp IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN order_purchase_timestamp IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.orders

UNION ALL

SELECT 'orders', 'order_approved_at', COUNT(*), 
    SUM(CASE WHEN order_approved_at IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN order_approved_at IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.orders

UNION ALL

SELECT 'orders', 'order_delivered_carrier_date', COUNT(*), 
    SUM(CASE WHEN order_delivered_carrier_date IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN order_delivered_carrier_date IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.orders

UNION ALL

SELECT 'orders', 'order_delivered_customer_date', COUNT(*), 
    SUM(CASE WHEN order_delivered_customer_date IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN order_delivered_customer_date IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.orders

UNION ALL

SELECT 'orders', 'order_estimated_delivery_date', COUNT(*), 
    SUM(CASE WHEN order_estimated_delivery_date IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN order_estimated_delivery_date IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.orders

-- PRODUCTS TABLE - NULL Analysis
UNION ALL

SELECT 'products', 'product_id', COUNT(*), 
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.products

UNION ALL

SELECT 'products', 'product_category_name', COUNT(*), 
    SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.products

UNION ALL

SELECT 'products', 'product_name_lenght', COUNT(*), 
    SUM(CASE WHEN product_name_lenght IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN product_name_lenght IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.products

UNION ALL

SELECT 'products', 'product_description_lenght', COUNT(*), 
    SUM(CASE WHEN product_description_lenght IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN product_description_lenght IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.products

UNION ALL

SELECT 'products', 'product_photos_qty', COUNT(*), 
    SUM(CASE WHEN product_photos_qty IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN product_photos_qty IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.products

UNION ALL

SELECT 'products', 'product_weight_g', COUNT(*), 
    SUM(CASE WHEN product_weight_g IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN product_weight_g IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.products

UNION ALL

SELECT 'products', 'product_length_cm', COUNT(*), 
    SUM(CASE WHEN product_length_cm IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN product_length_cm IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.products

UNION ALL

SELECT 'products', 'product_height_cm', COUNT(*), 
    SUM(CASE WHEN product_height_cm IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN product_height_cm IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.products

UNION ALL

SELECT 'products', 'product_width_cm', COUNT(*), 
    SUM(CASE WHEN product_width_cm IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN product_width_cm IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.products

-- ORDER_ITEMS TABLE - NULL Analysis
UNION ALL

SELECT 'order_items', 'order_id', COUNT(*), 
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.order_items

UNION ALL

SELECT 'order_items', 'product_id', COUNT(*), 
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.order_items

UNION ALL

SELECT 'order_items', 'seller_id', COUNT(*), 
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.order_items

UNION ALL

SELECT 'order_items', 'price', COUNT(*), 
    SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.order_items

UNION ALL

SELECT 'order_items', 'freight_value', COUNT(*), 
    SUM(CASE WHEN freight_value IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN freight_value IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.order_items

-- SELLERS TABLE - NULL Analysis
UNION ALL

SELECT 'sellers', 'seller_id', COUNT(*), 
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.sellers

UNION ALL

SELECT 'sellers', 'seller_zip_code_prefix', COUNT(*), 
    SUM(CASE WHEN seller_zip_code_prefix IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN seller_zip_code_prefix IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.sellers

UNION ALL

SELECT 'sellers', 'seller_city', COUNT(*), 
    SUM(CASE WHEN seller_city IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN seller_city IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.sellers

UNION ALL

SELECT 'sellers', 'seller_state', COUNT(*), 
    SUM(CASE WHEN seller_state IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN seller_state IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.sellers

-- ORDER_PAYMENTS TABLE - NULL Analysis
UNION ALL

SELECT 'order_payments', 'order_id', COUNT(*), 
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.order_payments

UNION ALL

SELECT 'order_payments', 'payment_type', COUNT(*), 
    SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.order_payments

UNION ALL

SELECT 'order_payments', 'payment_value', COUNT(*), 
    SUM(CASE WHEN payment_value IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN payment_value IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.order_payments

-- ORDER_REVIEWS TABLE - NULL Analysis
UNION ALL

SELECT 'order_reviews', 'review_id', COUNT(*), 
    SUM(CASE WHEN review_id IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN review_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.order_reviews

UNION ALL

SELECT 'order_reviews', 'order_id', COUNT(*), 
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.order_reviews

UNION ALL

SELECT 'order_reviews', 'review_score', COUNT(*), 
    SUM(CASE WHEN review_score IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN review_score IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.order_reviews

UNION ALL

SELECT 'order_reviews', 'review_comment_title', COUNT(*), 
    SUM(CASE WHEN review_comment_title IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN review_comment_title IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.order_reviews

UNION ALL

SELECT 'order_reviews', 'review_comment_message', COUNT(*), 
    SUM(CASE WHEN review_comment_message IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN review_comment_message IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM staging.order_reviews

ORDER BY table_name, column_name;

-- =====================================================
-- INTERPRETATION:
-- - null_count = 0: Perfect, no missing data
-- - null_percentage < 5%: Acceptable, can impute or remove
-- - null_percentage 5-20%: Moderate issue, needs strategy
-- - null_percentage > 20%: Serious issue, investigate why
-- =====================================================

-- Export this result to update your README.md with actual numbers!
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
  Duplicate Customers (by customer_id) — Interpretation

Result: No rows returned

Meaning: There are no duplicate customer_id values in the staging.customers table

Assessment: ✅ Data quality passed
customer_id can be safely treated as a primary key
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

/* Duplicate Orders (by order_id) — Interpretation

Result: No rows returned

Meaning: There are no duplicate order_id values in the staging.orders table

Assessment: ✅ Data quality passed
order_id functions as a true primary key
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
   Customers with Multiple customer_id per customer_unique_id — Interpretation

Result: Multiple rows returned

Meaning: A single customer_unique_id is associated with multiple distinct customer_id values

Observed Range: Up to 17 different customer_ids for one customer_unique_id

Assessment: 🟡 Expected behavior, not a data error
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
   Orphan Orders (Orders Without Matching Customers) — Interpretation

Result: orphan_orders_count = 0

Meaning: Every order in staging.orders has a valid matching customer in staging.customers

Assessment: ✅ Referential integrity passed
*/

-- 3.2 Order items without matching orders (orphan items)
SELECT 
    COUNT(*) as orphan_items_count
FROM staging.order_items oi
LEFT JOIN staging.orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

/* INTERPRETATION:
Orphan Order Items (Items Without Matching Orders) — Interpretation

Result: orphan_items_count = 0

Meaning: Every record in staging.order_items is linked to a valid parent order in staging.orders

Assessment: ✅ Referential integrity passed
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
Order Items with Missing Products — Interpretation

Result: items_with_missing_products = 0

Meaning: Every order item references a valid product in staging.products

Assessment: ✅ Referential integrity passed'
No products were sold without existing product records
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
Price & Freight Value Validation — Interpretation

Result:

Negative Prices = 0

Negative Freight = 0

Zero Prices = 0

Meaning: All monetary values in staging.order_items are strictly positive and valid

Assessment: ✅ Data quality passed
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
  Meaning: Majority of orders (97%) are delivered, which is expected for completed transactions.

Minor statuses exist for orders in different stages or anomalies:

shipped, canceled, unavailable, invoiced, processing — low percentages, normal for staging dataset

created and approved — extremely rare, likely incomplete or test records

Assessment: 🟢 Mostly clean, but edge statuses should be reviewed if needed for analytics
*/

-- 4.3 Check for future order dates (impossible timestamps)
SELECT 
    'Fuesture Purchase Dat' as issue,
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