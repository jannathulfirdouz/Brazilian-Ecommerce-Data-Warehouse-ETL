# Star Schema ER Diagram
```mermaid
erDiagram
    dim_date ||--o{ fact_sales : "order_date_key"
    dim_customers ||--o{ fact_sales : "customer_key"
    dim_products ||--o{ fact_sales : "product_key"
    dim_sellers ||--o{ fact_sales : "seller_key"
    dim_order_status ||--o{ fact_sales : "status_key"

    dim_date {
        int date_key PK
        date full_date
        int year
        int quarter
        int month
        varchar month_name
        int week
        int day_of_month
        int day_of_week
        varchar day_name
        boolean is_weekend
    }

    dim_customers {
        serial customer_key PK
        varchar customer_id UK
        varchar customer_unique_id
        varchar city
        varchar state
        varchar zip_code
        timestamp valid_from
        timestamp valid_to
        boolean is_current
    }

    dim_products {
        serial product_key PK
        varchar product_id UK
        varchar category_name_pt
        varchar category_name_en
        int weight_g
        int length_cm
        int height_cm
        int width_cm
        int volume_cm3
        varchar size_category
        varchar weight_category
        timestamp valid_from
        timestamp valid_to
        boolean is_current
    }

    dim_sellers {
        serial seller_key PK
        varchar seller_id UK
        varchar city
        varchar state
        varchar zip_code
        timestamp valid_from
        timestamp valid_to
        boolean is_current
    }

    dim_order_status {
        serial status_key PK
        varchar status_code UK
        varchar status_description
        boolean is_completed
        boolean is_cancelled
    }

    fact_sales {
        serial sales_key PK
        int order_date_key FK
        int customer_key FK
        int product_key FK
        int seller_key FK
        int status_key FK
        varchar order_id
        int item_number
        decimal unit_price
        decimal freight_value
        decimal total_item_value
        varchar payment_type
        int payment_installments
        decimal payment_value
        int review_score
        int delivery_days
        boolean delivered_on_time
        timestamp purchased_at
        timestamp delivered_at
    }
```



Star Schema Characteristics
Design Pattern: STAR SCHEMA

1 Central Fact Table (fact_sales) - contains measurements/metrics
5 Dimension Tables (surrounding) - contains descriptive attributes
Optimized for Analytics - fast aggregations, simple joins

Fact Table: fact_sales
Grain: One row per order item

Keys: 5 foreign keys linking to dimensions
Measures: Numeric facts (price, freight, payment, delivery days)
Degenerate Dimensions: order_id, item_number stored in fact

Dimension Tables
DimensionTypeSCDPurposedim_dateConformedNoTime-based analysisdim_customersType 2YesCustomer attributes & historydim_productsType 2YesProduct catalog & categorizationdim_sellersType 2YesSeller information & locationdim_order_statusType 1NoOrder status lookup
Key Features
✅ Surrogate Keys: SERIAL primary keys (customer_key, product_key, etc.)

Protects from source system changes
Enables SCD Type 2 tracking
Improves join performance

✅ Slowly Changing Dimensions (SCD Type 2):

valid_from / valid_to timestamps
is_current flag
Tracks historical changes

✅ Derived Attributes:

size_category (Small/Medium/Large)
weight_category (Light/Medium/Heavy)
volume_cm3 (calculated dimension)
Business-friendly categorizations

✅ Performance Optimization:

Indexes on all foreign keys
Indexes on frequently filtered columns
Optimized for SELECT queries

Sample Analytical Query
sql-- Monthly revenue by product category
SELECT 
    d.year,
    d.month_name,
    p.category_name_en,
    COUNT(DISTINCT f.order_id) as orders,
    SUM(f.total_item_value) as revenue
FROM fact_sales f
JOIN dim_date d ON f.order_date_key = d.date_key
JOIN dim_products p ON f.product_key = p.product_key
WHERE d.year = 2018
GROUP BY d.year, d.month, d.month_name, p.category_name_en
ORDER BY d.month, revenue DESC;
Why This Works Well:

Simple 2-table joins (fact + 2 dimensions)
Fast execution due to indexes
Easy to understand for business users
Flexible for different time periods/categories

Benefits of This Design
BenefitDescriptionQuery PerformanceStar schema optimized for aggregationsSimplicityEasy to understand, one hop from fact to dimensionFlexibilityAdd dimensions without changing fact tableScalabilityCan handle millions of fact recordsBusiness-FriendlyDimension names match business languageHistorical TrackingSCD Type 2 preserves history