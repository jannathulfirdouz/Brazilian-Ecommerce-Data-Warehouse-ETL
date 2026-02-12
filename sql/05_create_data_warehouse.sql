-- =====================================================
-- OLIST E-COMMERCE DATA WAREHOUSE & ETL PIPELINE
-- PART 5: DATA WAREHOUSE - STAR SCHEMA DESIGN
-- =====================================================
-- Purpose: Transform cleaned data into dimensional model
-- Architecture: Star Schema (1 Fact Table + 5 Dimension Tables)
-- =====================================================

-- Create data warehouse schema
CREATE SCHEMA IF NOT EXISTS dwh;

SET search_path TO dwh;

-- =====================================================
-- DIMENSION TABLE 1: DATE DIMENSION
-- =====================================================
-- Generate date dimension for time-based analysis

DROP TABLE IF EXISTS dwh.dim_date CASCADE;

CREATE TABLE dwh.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE
);

-- Populate date dimension (2016-2018 to cover all order dates)
INSERT INTO dwh.dim_date
SELECT 
    TO_CHAR(date_val, 'YYYYMMDD')::INTEGER as date_key,
    date_val as full_date,
    EXTRACT(YEAR FROM date_val) as year,
    EXTRACT(QUARTER FROM date_val) as quarter,
    EXTRACT(MONTH FROM date_val) as month,
    TO_CHAR(date_val, 'Month') as month_name,
    EXTRACT(WEEK FROM date_val) as week,
    EXTRACT(DAY FROM date_val) as day_of_month,
    EXTRACT(DOW FROM date_val) as day_of_week,
    TO_CHAR(date_val, 'Day') as day_name,
    CASE WHEN EXTRACT(DOW FROM date_val) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend,
    FALSE as is_holiday
FROM generate_series(
    '2016-01-01'::DATE,
    '2018-12-31'::DATE,
    '1 day'::INTERVAL
) as date_val;

-- Verify
SELECT 
    'dim_date' as dimension,
    COUNT(*) as total_dates,
    MIN(full_date) as start_date,
    MAX(full_date) as end_date
FROM dwh.dim_date;

/* INTERPRETATION:
   - 1,095 dates generated (3 years)
   - Enables time-based analysis (trends, seasonality)
   - Date key in YYYYMMDD format for easy joining
*/


-- =====================================================
-- DIMENSION TABLE 2: CUSTOMER DIMENSION
-- =====================================================

DROP TABLE IF EXISTS dwh.dim_customers CASCADE;

CREATE TABLE dwh.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    customer_unique_id VARCHAR(50) NOT NULL,
    city VARCHAR(100),
    state VARCHAR(5),
    zip_code VARCHAR(10),
    -- SCD Type 2 fields (Slowly Changing Dimension)
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Populate customer dimension
INSERT INTO dwh.dim_customers (customer_id, customer_unique_id, city, state, zip_code)
SELECT 
    customer_id,
    customer_unique_id,
    city,
    state,
    zip_code
FROM cleaned.customers;

-- Create indexes
CREATE INDEX idx_dim_customers_id ON dwh.dim_customers(customer_id);
CREATE INDEX idx_dim_customers_state ON dwh.dim_customers(state);

-- Verify
SELECT 
    'dim_customers' as dimension,
    COUNT(*) as total_customers,
    COUNT(DISTINCT customer_unique_id) as unique_persons,
    COUNT(DISTINCT state) as states
FROM dwh.dim_customers;

/* INTERPRETATION:
   - 99,441 customer records
   - SCD Type 2 ready for tracking customer changes over time
   - Surrogate key (customer_key) separates business key from DWH key
*/


-- =====================================================
-- DIMENSION TABLE 3: PRODUCT DIMENSION
-- =====================================================

DROP TABLE IF EXISTS dwh.dim_products CASCADE;

CREATE TABLE dwh.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    category_name_pt VARCHAR(100),
    category_name_en VARCHAR(100),
    name_length INTEGER,
    description_length INTEGER,
    photos_count INTEGER,
    weight_g INTEGER,
    length_cm INTEGER,
    height_cm INTEGER,
    width_cm INTEGER,
    volume_cm3 INTEGER,
    -- Derived attributes for analysis
    size_category VARCHAR(20),
    weight_category VARCHAR(20),
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Populate product dimension with categorizations
INSERT INTO dwh.dim_products (
    product_id, category_name_pt, category_name_en, name_length, 
    description_length, photos_count, weight_g, length_cm, 
    height_cm, width_cm, volume_cm3, size_category, weight_category
)
SELECT 
    product_id,
    category_name_pt,
    category_name_en,
    name_length,
    description_length,
    photos_count,
    weight_g,
    length_cm,
    height_cm,
    width_cm,
    volume_cm3,
    -- Categorize by volume
    CASE 
        WHEN volume_cm3 < 1000 THEN 'Small'
        WHEN volume_cm3 BETWEEN 1000 AND 10000 THEN 'Medium'
        WHEN volume_cm3 > 10000 THEN 'Large'
        ELSE 'Unknown'
    END as size_category,
    -- Categorize by weight
    CASE 
        WHEN weight_g < 500 THEN 'Light'
        WHEN weight_g BETWEEN 500 AND 5000 THEN 'Medium'
        WHEN weight_g > 5000 THEN 'Heavy'
        ELSE 'Unknown'
    END as weight_category
FROM cleaned.products;

-- Create indexes
CREATE INDEX idx_dim_products_id ON dwh.dim_products(product_id);
CREATE INDEX idx_dim_products_category ON dwh.dim_products(category_name_en);
CREATE INDEX idx_dim_products_size ON dwh.dim_products(size_category);

-- Verify
SELECT 
    'dim_products' as dimension,
    COUNT(*) as total_products,
    COUNT(DISTINCT category_name_en) as categories,
    COUNT(DISTINCT size_category) as size_categories,
    COUNT(DISTINCT weight_category) as weight_categories
FROM dwh.dim_products;

/* INTERPRETATION:
   - 32,951 products cataloged
   - Products categorized by size and weight for shipping analysis
   - English category names for better readability
*/


-- =====================================================
-- DIMENSION TABLE 4: SELLER DIMENSION
-- =====================================================

DROP TABLE IF EXISTS dwh.dim_sellers CASCADE;

CREATE TABLE dwh.dim_sellers (
    seller_key SERIAL PRIMARY KEY,
    seller_id VARCHAR(50) UNIQUE NOT NULL,
    city VARCHAR(100),
    state VARCHAR(5),
    zip_code VARCHAR(10),
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Populate seller dimension
INSERT INTO dwh.dim_sellers (seller_id, city, state, zip_code)
SELECT 
    seller_id,
    city,
    state,
    zip_code
FROM cleaned.sellers;

-- Create indexes
CREATE INDEX idx_dim_sellers_id ON dwh.dim_sellers(seller_id);
CREATE INDEX idx_dim_sellers_state ON dwh.dim_sellers(state);

-- Verify
SELECT 
    'dim_sellers' as dimension,
    COUNT(*) as total_sellers,
    COUNT(DISTINCT state) as states
FROM dwh.dim_sellers;


-- =====================================================
-- DIMENSION TABLE 5: ORDER STATUS DIMENSION
-- =====================================================

DROP TABLE IF EXISTS dwh.dim_order_status CASCADE;

CREATE TABLE dwh.dim_order_status (
    status_key SERIAL PRIMARY KEY,
    status_code VARCHAR(20) UNIQUE NOT NULL,
    status_description VARCHAR(100),
    is_completed BOOLEAN,
    is_cancelled BOOLEAN
);

-- Populate with all possible order statuses
INSERT INTO dwh.dim_order_status (status_code, status_description, is_completed, is_cancelled)
VALUES 
    ('DELIVERED', 'Order successfully delivered to customer', TRUE, FALSE),
    ('SHIPPED', 'Order shipped to customer', FALSE, FALSE),
    ('PROCESSING', 'Order being prepared', FALSE, FALSE),
    ('APPROVED', 'Payment approved, awaiting processing', FALSE, FALSE),
    ('INVOICED', 'Invoice issued', FALSE, FALSE),
    ('CANCELED', 'Order cancelled by customer or system', FALSE, TRUE),
    ('UNAVAILABLE', 'Product unavailable', FALSE, TRUE),
    ('CREATED', 'Order created, awaiting payment', FALSE, FALSE);

-- Verify
SELECT * FROM dwh.dim_order_status ORDER BY status_key;


-- =====================================================
-- FACT TABLE: SALES FACT
-- =====================================================
-- The heart of the star schema - contains measurements/metrics

DROP TABLE IF EXISTS dwh.fact_sales CASCADE;

CREATE TABLE dwh.fact_sales (
    sales_key SERIAL PRIMARY KEY,
    
    -- Foreign keys to dimensions (the "star" connections)
    order_date_key INTEGER REFERENCES dwh.dim_date(date_key),
    customer_key INTEGER REFERENCES dwh.dim_customers(customer_key),
    product_key INTEGER REFERENCES dwh.dim_products(product_key),
    seller_key INTEGER REFERENCES dwh.dim_sellers(seller_key),
    status_key INTEGER REFERENCES dwh.dim_order_status(status_key),
    
    -- Degenerate dimensions (kept in fact table)
    order_id VARCHAR(50) NOT NULL,
    item_number INTEGER NOT NULL,
    
    -- Measures (numeric facts to analyze)
    quantity INTEGER DEFAULT 1,
    unit_price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    total_item_value DECIMAL(10,2),
    
    -- Payment information (from order_payments)
    payment_type VARCHAR(50),
    payment_installments INTEGER,
    payment_value DECIMAL(10,2),
    
    -- Review information (from order_reviews)
    review_score INTEGER,
    
    -- Delivery metrics
    delivery_days INTEGER,
    delivered_on_time BOOLEAN,
    
    -- Timestamps
    purchased_at TIMESTAMP,
    delivered_at TIMESTAMP,
    
    -- ETL metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Populate fact table by joining cleaned tables
INSERT INTO dwh.fact_sales (
    order_date_key, customer_key, product_key, seller_key, status_key,
    order_id, item_number, quantity, unit_price, freight_value, total_item_value,
    payment_type, payment_installments, payment_value,
    review_score, delivery_days, delivered_on_time,
    purchased_at, delivered_at
)
SELECT 
    -- Date dimension key
    TO_CHAR(o.purchased_at, 'YYYYMMDD')::INTEGER as order_date_key,
    
    -- Dimension keys (lookup via surrogate keys)
    dc.customer_key,
    dp.product_key,
    ds.seller_key,
    dst.status_key,
    
    -- Degenerate dimensions
    oi.order_id,
    oi.item_number,
    
    -- Measures
    1 as quantity,  -- Each row is one item
    oi.price as unit_price,
    oi.freight_value,
    oi.total_price as total_item_value,
    
    -- Payment info (aggregate to order level, take first payment type)
    (SELECT payment_type FROM cleaned.order_payments WHERE order_id = oi.order_id LIMIT 1) as payment_type,
    (SELECT SUM(installments) FROM cleaned.order_payments WHERE order_id = oi.order_id) as payment_installments,
    (SELECT SUM(amount) FROM cleaned.order_payments WHERE order_id = oi.order_id) as payment_value,
    
    -- Review info
    r.score as review_score,
    
    -- Delivery metrics
    o.delivery_days,
    o.delivered_on_time,
    
    -- Timestamps
    o.purchased_at,
    o.delivered_at

FROM cleaned.order_items oi
INNER JOIN cleaned.orders o ON oi.order_id = o.order_id
INNER JOIN dwh.dim_customers dc ON o.customer_id = dc.customer_id
INNER JOIN dwh.dim_products dp ON oi.product_id = dp.product_id
INNER JOIN dwh.dim_sellers ds ON oi.seller_id = ds.seller_id
INNER JOIN dwh.dim_order_status dst ON o.status = dst.status_code
LEFT JOIN cleaned.order_reviews r ON o.order_id = r.order_id;

-- Create indexes for performance
CREATE INDEX idx_fact_sales_order_date ON dwh.fact_sales(order_date_key);
CREATE INDEX idx_fact_sales_customer ON dwh.fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON dwh.fact_sales(product_key);
CREATE INDEX idx_fact_sales_seller ON dwh.fact_sales(seller_key);
CREATE INDEX idx_fact_sales_status ON dwh.fact_sales(status_key);
CREATE INDEX idx_fact_sales_order_id ON dwh.fact_sales(order_id);

-- Verify fact table
SELECT 
    'fact_sales' as fact_table,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT order_id) as unique_orders,
    COUNT(DISTINCT customer_key) as unique_customers,
    COUNT(DISTINCT product_key) as unique_products,
    ROUND(SUM(total_item_value), 2) as total_revenue,
    ROUND(AVG(unit_price), 2) as avg_unit_price,
    ROUND(AVG(delivery_days), 2) as avg_delivery_days
FROM dwh.fact_sales;

/* INTERPRETATION:
   - Central fact table with all sales transactions
   - Connected to 5 dimension tables (star schema)
   - Contains both facts (prices, quantities) and foreign keys
   - Optimized with indexes for fast queries
   - Ready for business analytics
*/


-- =====================================================
-- DATA WAREHOUSE SUMMARY
-- =====================================================

-- Summary of all DWH tables
SELECT 
    'dim_date' as table_name,
    'Dimension' as table_type,
    COUNT(*) as row_count
FROM dwh.dim_date

UNION ALL

SELECT 'dim_customers', 'Dimension', COUNT(*) FROM dwh.dim_customers
UNION ALL
SELECT 'dim_products', 'Dimension', COUNT(*) FROM dwh.dim_products
UNION ALL
SELECT 'dim_sellers', 'Dimension', COUNT(*) FROM dwh.dim_sellers
UNION ALL
SELECT 'dim_order_status', 'Dimension', COUNT(*) FROM dwh.dim_order_status
UNION ALL
SELECT 'fact_sales', 'Fact', COUNT(*) FROM dwh.fact_sales

ORDER BY table_type, table_name;


-- =====================================================
-- SAMPLE ANALYTICAL QUERY
-- =====================================================
-- Demonstrate the power of star schema

-- Monthly sales by product category
SELECT 
    d.year,
    d.month_name,
    p.category_name_en,
    COUNT(DISTINCT f.order_id) as orders,
    COUNT(*) as items_sold,
    ROUND(SUM(f.total_item_value), 2) as revenue,
    ROUND(AVG(f.unit_price), 2) as avg_price
FROM dwh.fact_sales f
JOIN dwh.dim_date d ON f.order_date_key = d.date_key
JOIN dwh.dim_products p ON f.product_key = p.product_key
WHERE d.year = 2017
GROUP BY d.year, d.month, d.month_name, p.category_name_en
ORDER BY d.month, revenue DESC
LIMIT 20;

/* INTERPRETATION:
   - Simple, readable query thanks to star schema
   - Joins fact to dimensions for meaningful analysis
   - Fast performance due to indexes
   - Easy for business users to understand
*/


