-- =====================================================
-- OLIST E-COMMERCE DATA WAREHOUSE & ETL PIPELINE
-- PART 6: BUSINESS ANALYTICS & INSIGHTS
-- =====================================================
-- Purpose: Answer key business questions using the data warehouse
-- Demonstrates: Advanced SQL, business acumen, analytical thinking
-- =====================================================

SET search_path TO dwh;

-- =====================================================
-- SECTION 1: CUSTOMER ANALYTICS
-- =====================================================

-- 1.1 RFM Analysis (Recency, Frequency, Monetary) - FIXED v2
WITH customer_rfm AS (
    SELECT 
        c.customer_key,
        c.customer_id,
        c.state,
        -- Recency: Days since last purchase (FIXED)
        (CURRENT_DATE - MAX(f.purchased_at)::DATE)::INTEGER as recency_days,
        -- Frequency: Number of orders
        COUNT(DISTINCT f.order_id) as frequency,
        -- Monetary: Total spend
        SUM(f.total_item_value) as monetary_value
    FROM dwh.fact_sales f
    JOIN dwh.dim_customers c ON f.customer_key = c.customer_key
    GROUP BY c.customer_key, c.customer_id, c.state
),
rfm_scores AS (
    SELECT 
        *,
        -- Score 1-5 for each dimension (5 = best)
        NTILE(5) OVER (ORDER BY recency_days DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) as f_score,
        NTILE(5) OVER (ORDER BY monetary_value ASC) as m_score
    FROM customer_rfm
)
SELECT 
    CASE 
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2 THEN 'New Customers'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost Customers'
        ELSE 'Other'
    END as customer_segment,
    COUNT(*) as customer_count,
    ROUND(AVG(recency_days), 0) as avg_recency_days,
    ROUND(AVG(frequency), 1) as avg_orders,
    ROUND(AVG(monetary_value), 2) as avg_lifetime_value,
    ROUND(SUM(monetary_value), 2) as total_segment_revenue
FROM rfm_scores
GROUP BY customer_segment
ORDER BY total_segment_revenue DESC;

/* BUSINESS INSIGHT:
   - Identifies high-value customer segments
   - "Champions" deserve VIP treatment and retention programs
   - "At Risk" customers need re-engagement campaigns
   - "Lost Customers" may need win-back offers
   ACTION: Focus marketing spend on Champions and At Risk segments
*/

-- 1.2 Customer Lifetime Value (CLV) - Top 20
SELECT 
    c.customer_id,
    c.city,
    c.state,
    COUNT(DISTINCT f.order_id) as total_orders,
    COUNT(*) as total_items,
    ROUND(SUM(f.total_item_value), 2) as lifetime_value,
    ROUND(AVG(f.total_item_value), 2) as avg_order_value,
    MIN(f.purchased_at)::DATE as first_purchase,
    MAX(f.purchased_at)::DATE as last_purchase,
    (MAX(f.purchased_at)::DATE - MIN(f.purchased_at)::DATE) as customer_tenure_days
FROM dwh.fact_sales f
JOIN dwh.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_id, c.city, c.state
ORDER BY lifetime_value DESC
LIMIT 20;

/* BUSINESS INSIGHT:
   - Top 20 customers by lifetime value
   - Shows geographic concentration of high-value customers
   - Customer tenure indicates loyalty
   ACTION: Create loyalty program targeting high CLV customers
*/


-- 1.3 Customer Acquisition Trend by Month (FIXED)
WITH first_purchase AS (
    SELECT 
        c.customer_key,
        TO_CHAR(MIN(f.purchased_at), 'YYYY-MM') as cohort_month,
        MIN(f.purchased_at) as first_purchase_date
    FROM dwh.fact_sales f
    JOIN dwh.dim_customers c ON f.customer_key = c.customer_key
    GROUP BY c.customer_key
)
SELECT 
    cohort_month,
    COUNT(DISTINCT customer_key) as new_customers,
    SUM(COUNT(DISTINCT customer_key)) OVER (ORDER BY cohort_month) as cumulative_customers
FROM first_purchase
GROUP BY cohort_month
ORDER BY cohort_month;

/* BUSINESS INSIGHT:
   - Monthly customer acquisition trend
   - First order value indicates customer quality
   - Cumulative growth shows business trajectory
   ACTION: Identify successful acquisition months and replicate strategies
*/


-- =====================================================
-- SECTION 2: SALES ANALYTICS
-- =====================================================

-- 2.1 Monthly Revenue Trend with Growth Rate
WITH monthly_sales AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        COUNT(DISTINCT f.order_id) as orders,
        COUNT(*) as items_sold,
        ROUND(SUM(f.total_item_value), 2) as revenue,
        ROUND(AVG(f.total_item_value), 2) as avg_order_value
    FROM dwh.fact_sales f
    JOIN dwh.dim_date d ON f.order_date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
)
SELECT 
    year,
    month,
    month_name,
    orders,
    items_sold,
    revenue,
    avg_order_value,
    -- Month-over-month growth
    ROUND(((revenue - LAG(revenue) OVER (ORDER BY year, month)) / 
           NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0) * 100), 2) as mom_growth_pct,
    -- Running total
    SUM(revenue) OVER (ORDER BY year, month) as cumulative_revenue
FROM monthly_sales
ORDER BY year, month;

/* BUSINESS INSIGHT:
   - Monthly revenue trends show seasonality
   - Month-over-month growth identifies peaks/valleys
   - Cumulative revenue tracks overall business health
   ACTION: Plan inventory and marketing around high-growth months
*/


-- 2.2 Top 10 Product Categories by Revenue
SELECT 
    p.category_name_en,
    COUNT(DISTINCT f.order_id) as orders,
    COUNT(*) as items_sold,
    ROUND(SUM(f.total_item_value), 2) as total_revenue,
    ROUND(AVG(f.unit_price), 2) as avg_unit_price,
    ROUND(AVG(f.review_score), 2) as avg_review_score,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_total_items
FROM dwh.fact_sales f
JOIN dwh.dim_products p ON f.product_key = p.product_key
GROUP BY p.category_name_en
ORDER BY total_revenue DESC
LIMIT 10;

/* BUSINESS INSIGHT:
   - Revenue concentration by category
   - Average price and review score show quality vs volume trade-offs
   - Percentage of total shows category importance
   ACTION: Focus marketing on top categories, investigate low performers
*/


-- 2.3 Day of Week Sales Pattern
SELECT 
    d.day_name,
    d.day_of_week,
    COUNT(DISTINCT f.order_id) as orders,
    ROUND(SUM(f.total_item_value), 2) as revenue,
    ROUND(AVG(f.total_item_value), 2) as avg_order_value,
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type
FROM dwh.fact_sales f
JOIN dwh.dim_date d ON f.order_date_key = d.date_key
GROUP BY d.day_name, d.day_of_week, d.is_weekend
ORDER BY d.day_of_week;

/* BUSINESS INSIGHT:
   - Purchase patterns by day of week
   - Weekend vs weekday behavior
   ACTION: Schedule promotions on high-traffic days
*/


-- =====================================================
-- SECTION 3: OPERATIONAL ANALYTICS
-- =====================================================

-- 3.1 Delivery Performance by State
SELECT 
    c.state,
    COUNT(DISTINCT f.order_id) as total_orders,
    ROUND(AVG(f.delivery_days), 1) as avg_delivery_days,
    ROUND(100.0 * SUM(CASE WHEN f.delivered_on_time THEN 1 ELSE 0 END) / 
          NULLIF(SUM(CASE WHEN f.delivered_on_time IS NOT NULL THEN 1 ELSE 0 END), 0), 2) as on_time_pct,
    MIN(f.delivery_days) as fastest_delivery,
    MAX(f.delivery_days) as slowest_delivery
FROM dwh.fact_sales f
JOIN dwh.dim_customers c ON f.customer_key = c.customer_key
WHERE f.delivery_days IS NOT NULL
GROUP BY c.state
HAVING COUNT(DISTINCT f.order_id) >= 100  -- States with significant volume
ORDER BY on_time_pct DESC;

/* BUSINESS INSIGHT:
   - Delivery performance varies by state
   - States with poor performance need logistics improvement
   - On-time delivery impacts customer satisfaction
   ACTION: Improve logistics in underperforming states
*/


-- 3.2 Top Sellers Performance Comparison
SELECT 
    s.seller_id,
    s.city,
    s.state,
    COUNT(DISTINCT f.order_id) as orders_fulfilled,
    COUNT(*) as items_sold,
    ROUND(SUM(f.total_item_value), 2) as total_revenue,
    ROUND(AVG(f.unit_price), 2) as avg_item_price,
    ROUND(AVG(f.review_score), 2) as avg_review_score,
    COUNT(DISTINCT p.category_name_en) as categories_sold
FROM dwh.fact_sales f
JOIN dwh.dim_sellers s ON f.seller_key = s.seller_key
JOIN dwh.dim_products p ON f.product_key = p.product_key
GROUP BY s.seller_id, s.city, s.state
HAVING COUNT(*) >= 50  -- Active sellers only
ORDER BY total_revenue DESC
LIMIT 20;

/* BUSINESS INSIGHT:
   - Top-performing sellers by revenue
   - Geographic distribution of successful sellers
   - Diversification (categories sold) indicates seller sophistication
   ACTION: Partner closely with top sellers, learn best practices
*/


-- 3.3 Payment Method Analysis
SELECT 
    f.payment_type,
    COUNT(DISTINCT f.order_id) as orders,
    ROUND(SUM(f.payment_value), 2) as total_payment_value,
    ROUND(AVG(f.payment_value), 2) as avg_payment_value,
    ROUND(AVG(f.payment_installments), 1) as avg_installments,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_orders
FROM dwh.fact_sales f
WHERE f.payment_type IS NOT NULL
GROUP BY f.payment_type
ORDER BY total_payment_value DESC;

/* BUSINESS INSIGHT:
   - Payment method preferences
   - Installment usage indicates affordability concerns
   ACTION: Optimize checkout for preferred payment methods
*/


-- =====================================================
-- SECTION 4: PRODUCT ANALYTICS
-- =====================================================

-- 4.1 Product Performance - Best Sellers vs High Margin
SELECT 
    p.product_id,
    p.category_name_en,
    p.size_category,
    p.weight_category,
    COUNT(*) as units_sold,
    ROUND(SUM(f.total_item_value), 2) as total_revenue,
    ROUND(AVG(f.unit_price), 2) as avg_price,
    ROUND(AVG(f.freight_value), 2) as avg_freight,
    ROUND(AVG(f.review_score), 2) as avg_rating,
    -- Rank by revenue and volume
    RANK() OVER (ORDER BY SUM(f.total_item_value) DESC) as revenue_rank,
    RANK() OVER (ORDER BY COUNT(*) DESC) as volume_rank
FROM dwh.fact_sales f
JOIN dwh.dim_products p ON f.product_key = p.product_key
GROUP BY p.product_id, p.category_name_en, p.size_category, p.weight_category
HAVING COUNT(*) >= 10
ORDER BY total_revenue DESC
LIMIT 30;

/* BUSINESS INSIGHT:
   - Best-selling products by revenue and volume
   - Size/weight impact on freight costs
   - Rating indicates customer satisfaction
   ACTION: Stock top sellers, optimize pricing on high-margin items
*/


-- 4.2 Review Score Impact on Sales
WITH reviews AS (
    SELECT
        CASE 
            WHEN review_score >= 5 THEN '5 Stars'
            WHEN review_score >= 4 THEN '4 Stars'
            WHEN review_score >= 3 THEN '3 Stars'
            WHEN review_score >= 2 THEN '2 Stars'
            WHEN review_score >= 1 THEN '1 Star'
            ELSE 'No Review'
        END AS rating_category,
        unit_price,
        delivery_days
    FROM dwh.fact_sales
)
SELECT
    rating_category,
    COUNT(*) AS order_count,
    ROUND(AVG(unit_price), 2) AS avg_price,
    ROUND(AVG(delivery_days), 1) AS avg_delivery_days,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_orders
FROM reviews
GROUP BY rating_category
ORDER BY
    CASE rating_category
        WHEN '5 Stars' THEN 1
        WHEN '4 Stars' THEN 2
        WHEN '3 Stars' THEN 3
        WHEN '2 Stars' THEN 4
        WHEN '1 Star' THEN 5
        ELSE 6
    END;

/* BUSINESS INSIGHT:
   - Review score distribution
   - Correlation between price, delivery time, and ratings
   ACTION: Improve delivery speed to boost ratings
*/


-- 4.3 Product Category Trend Over Time
WITH category_revenue AS (
    SELECT 
        d.year,
        d.quarter,
        p.category_name_en,
        COUNT(*) AS items_sold,
        ROUND(SUM(f.total_item_value), 2) AS revenue
    FROM dwh.fact_sales f
    JOIN dwh.dim_date d ON f.order_date_key = d.date_key
    JOIN dwh.dim_products p ON f.product_key = p.product_key
    GROUP BY d.year, d.quarter, p.category_name_en
),
ranked_categories AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY year, quarter
            ORDER BY revenue DESC
        ) AS category_rank
    FROM category_revenue
)
SELECT *
FROM ranked_categories
WHERE category_rank <= 5
ORDER BY year, quarter, category_rank;


/* BUSINESS INSIGHT:
   - Top 5 categories per quarter
   - Seasonal category shifts
   ACTION: Adjust inventory based on seasonal trends
*/


-- =====================================================
-- SECTION 5: COHORT ANALYSIS
-- =====================================================

-- 5.1 Customer Retention by Cohort
WITH customer_cohorts AS (
    SELECT 
        c.customer_key,
        TO_CHAR(MIN(d.full_date), 'YYYY-MM') as cohort_month,
        MIN(d.full_date) as first_purchase_date
    FROM dwh.fact_sales f
    JOIN dwh.dim_customers c ON f.customer_key = c.customer_key
    JOIN dwh.dim_date d ON f.order_date_key = d.date_key
    GROUP BY c.customer_key
),
cohort_activity AS (
    SELECT 
        cc.cohort_month,
        TO_CHAR(d.full_date, 'YYYY-MM') as activity_month,
        DATE_PART('month', AGE(d.full_date, cc.first_purchase_date)) as months_since_first,
        COUNT(DISTINCT f.customer_key) as active_customers
    FROM dwh.fact_sales f
    JOIN customer_cohorts cc ON f.customer_key = cc.customer_key
    JOIN dwh.dim_date d ON f.order_date_key = d.date_key
    GROUP BY cc.cohort_month, activity_month, months_since_first
)
SELECT 
    cohort_month,
    months_since_first,
    active_customers,
    FIRST_VALUE(active_customers) OVER (PARTITION BY cohort_month ORDER BY months_since_first) as cohort_size,
    ROUND(100.0 * active_customers / 
          FIRST_VALUE(active_customers) OVER (PARTITION BY cohort_month ORDER BY months_since_first), 2) as retention_pct
FROM cohort_activity
WHERE months_since_first <= 12  -- First 12 months
ORDER BY cohort_month, months_since_first;

/* BUSINESS INSIGHT:
   - Customer retention by acquisition cohort
   - Shows how many customers return month-over-month
   - Identifies strongest and weakest cohorts
   ACTION: Improve onboarding for new customers to boost retention
*/


-- =====================================================
-- EXECUTIVE SUMMARY DASHBOARD
-- =====================================================

-- Key Business Metrics Summary
SELECT 
    'Total Revenue' as metric,
    TO_CHAR(SUM(total_item_value), 'R$ 999,999,999.99') as value
FROM dwh.fact_sales

UNION ALL

SELECT 'Total Orders', TO_CHAR(COUNT(DISTINCT order_id), '999,999')
FROM dwh.fact_sales

UNION ALL

SELECT 'Unique Customers', TO_CHAR(COUNT(DISTINCT customer_key), '999,999')
FROM dwh.fact_sales

UNION ALL

SELECT 'Average Order Value', TO_CHAR(AVG(total_item_value), 'R$ 999.99')
FROM dwh.fact_sales

UNION ALL

SELECT 'Average Delivery Days', TO_CHAR(AVG(delivery_days), '99.9')
FROM dwh.fact_sales

UNION ALL

SELECT 'On-Time Delivery Rate', 
    TO_CHAR(ROUND(100.0 * SUM(CASE WHEN delivered_on_time THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN delivered_on_time IS NOT NULL THEN 1 ELSE 0 END), 0), 2), '99.99%')
FROM dwh.fact_sales

UNION ALL

SELECT 'Average Review Score', TO_CHAR(AVG(review_score), '9.99')
FROM dwh.fact_sales WHERE review_score IS NOT NULL;

/* BUSINESS INSIGHT:
   - One-page executive summary
   - All key metrics in one view
   - Ready for dashboard/presentation
*/

-- =====================================================
-- END OF ANALYTICS QUERIES
-- =====================================================
-- These queries demonstrate:
-- ✅ Business acumen - answering real business questions
-- ✅ Advanced SQL - CTEs, window functions, complex joins
-- ✅ Analytical thinking - deriving insights from data
-- ✅ Communication - clear interpretations and actions
--
-- NEXT STEPS:
-- - Export results to CSV for visualization
-- - Create dashboards in Tableau/PowerBI
-- - Present findings to stakeholders
-- =====================================================