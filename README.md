# Brazilian E-Commerce Data Warehouse & ETL Pipeline

##  Project Overview
End-to-end ETL pipeline and data warehouse implementation for analyzing Brazilian e-commerce operations. This project demonstrates data engineering best practices including data quality assessment, dimensional modeling, and advanced SQL analytics.

##  Business Objective
Build a scalable data warehouse solution to analyze customer behavior, sales trends, and operational efficiency for a Brazilian e-commerce platform serving 99,000+ customers across multiple states.

##  Technologies Used
- **Database:** PostgreSQL 18
- **Visualization:** Power BI Desktop
- **ETL Tools:** SQL (COPY commands, custom transformations)
- **Version Control:** Git & GitHub
- **Data Volume:** 1.5M+ records across 9 tables
- **Dataset Source:** [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## 📁 Project Structure
```
Brazilian-Ecommerce-Data-Warehouse-ETL/
├── README.md                              # Project documentation
├── INSIGHTS.md                            # Business insights summary
├── sql/                                   # SQL scripts
│   ├── 01_create_staging_tables.sql       # Staging layer setup
│   ├── 02_import_csv_data.sql             # Data ingestion
│   ├── 03_data_quality_checks.sql         # Quality validation
│   ├── 04_data_cleaning_transformation.sql # Data cleaning & ETL
│   ├── 05_create_data_warehouse.sql       # Star schema implementation
│   └── 06_business_analytics.sql          # Advanced analytics queries
├── docs/                                  # Documentation & diagrams
│   ├── architecture.md                    # Star schema architecture
│   └── star_schema_er_diagram.md          # ER diagram
├── screenshots/                           # Dashboard visualizations
│   ├── Executive_dashboard.png            # Executive KPI dashboard
│   └── Customer_operations_dashboard.png  # Customer analytics dashboard
├── Dashboard_Screenshots.pdf              # Dashboard export
└── Dataset/                               # Raw CSV files (9 files)
```

##  Dataset Overview
The Olist dataset contains real Brazilian e-commerce data with:
- **99,441** customers
- **99,441** orders (2016-2018)
- **112,650** order items
- **32,951** products
- **3,095** sellers
- **103,886** payment transactions
- **99,224** customer reviews
- **1,000,163** geolocation records

##  ETL Pipeline Architecture

### Phase 1: Data Ingestion
- Created staging schema for raw data isolation
- Loaded 9 CSV files into PostgreSQL using COPY commands
- Preserved original data types and formats
- Total records ingested: 1.5M+

### Phase 2: Data Quality Assessment

Comprehensive quality checks performed across 1.5M+ records:

## Key Data Quality Findings

### Critical Issues Identified:
## Detailed Data Quality Assessment Results

| Category | Check | Result | Verdict | Notes |
|----------|-------|--------|---------|-------|
| **NULL Analysis** | Critical ID columns | 0% NULLs |  Pass | Core identifiers complete |
| **Duplicates** | Duplicate customer_id | None |  Pass | Safe primary key |
| **Duplicates** | Duplicate order_id | None |  Pass | Orders uniquely identified |
| **Consistency** | Multiple accounts per person | Present |  Expected | Business pattern |
| **Referential Integrity** | Orders without customers | 0 |  Pass | No orphans |
| **Referential Integrity** | Items without orders | 0 |  Pass | Transaction chain intact |
| **Referential Integrity** | Items with missing products | 0 |  Pass | Product catalog complete |
| **Referential Integrity** | Items with missing sellers | 0 |  Pass | Seller dimension complete |
| **Invalid Values** | Negative prices | 0 |  Pass | No corrupted financial data |
| **Invalid Values** | Negative freight | 0 |  Pass | Freight costs valid |
| **Invalid Values** | Zero prices | 0 |  Pass | No mispriced items |
| **Temporal Integrity** | Future purchase dates | 0 |  Pass | No impossible timestamps |
| **Temporal Integrity** | Delivery before purchase | 0 |  Pass | Chronologically consistent |
| **Review Validity** | Scores outside 1-5 | 0 |  Pass | Customer feedback clean |
| **Completeness** | Products with complete specs | 98.15% |  High | Minor gaps, imputable |
| **Completeness** | Complete customer addresses | 100% |  Perfect | Geo analysis supported |
| **Completeness** | Orders with all timestamps | 97% |  High | Missing due to cancellations |

*(Run 03_data_quality_checks.sql)*

**Verdict:** Dataset is exceptionally clean with minimal cleaning required.

### Phase 3: Data Cleaning & Transformation
-Handled NULL values with intelligent imputation strategies
-Standardized data formats (uppercase, trimming)
-Removed 823 invalid records (0.05% of dataset)
-Created derived business attributes
-Final retention rate: 99.95%

### Phase 4: Data Warehouse Design 

Implemented enterprise-grade **Star Schema** for optimal analytical performance:

**Architecture Overview:**
```
                    dim_date (1,096 dates)
                         |
                         |
    dim_customers ---- FACT_SALES ---- dim_products
    (99,441)          (112,960)         (32,951)
                         |
                         |
    dim_sellers ---- dim_order_status
    (3,095)              (8)
```

**Dimension Tables:**
- **dim_date**: Complete date dimension (2016-2018) with year, quarter, month, week, day attributes
- **dim_customers**: Customer master with geographic attributes (27 states)
- **dim_products**: Product catalog with 72 categories, size/weight classifications, volume calculations
- **dim_sellers**: Seller directory across 23 Brazilian states
- **dim_order_status**: Order status lookup with completion/cancellation flags

**Fact Table:**
- **fact_sales**: Grain = One order item
  - 112,960 sales transactions
  - R$ 15.87M total revenue
  - Links to all 5 dimension tables
  - Includes measures: price, freight, payment, review score, delivery metrics
  - Optimized with indexes on all foreign keys

**Design Features:**
-  Surrogate keys (SERIAL) for all dimensions
-  SCD Type 2 ready (valid_from, valid_to, is_current)
-  Degenerate dimensions (order_id in fact table)
-  Derived attributes (size_category, weight_category)
-  Performance indexes on all join columns
-  Business-friendly naming conventions

**Key Metrics from Data Warehouse:**
- Average order value: R$ 120.57
- Average delivery time: 12 days
- On-time delivery rate: 91.89%
- Active customer base: 98,666 unique customers
- Product catalog: 32,951 active SKUs
  
# Phase 5: Business Analytics
This phase involved generating 12+ analytical queries to answer key business questions across multiple domains.
---
## Customer Analytics

- RFM segmentation (Champions, Loyal, At Risk, Lost)  
- Customer Lifetime Value analysis  
- Customer acquisition trends  

---
## Revenue Analytics

- Monthly revenue trends (including a 53% Black Friday spike)  
- Top product categories by revenue  
- Day of week sales patterns  

---

## Operational Analytics

- Delivery performance by state  
- Seller performance rankings  
- Payment method preferences  

---

## Product Analytics

- Best-selling products analysis  
- Review score impact on sales  
- Seasonal category trends  

---

## Key Findings

- 34.7% of revenue comes from **“At Risk” customers** — retention should be a priority  
- Delivery time strongly correlates with review scores (10 days → 5 stars, 19 days → 1 star)  
- São Paulo delivers 86% faster than other states  
- Credit card is the dominant payment method (75.63%) with an average of 3.7 installments  

---

For complete business analysis and detailed insights, see the [INSIGHTS.md](INSIGHTS.md) file.

##  Business Insights 
- Geographic distribution of customers
- Top performing product categories
- Payment method preferences
- Seasonal sales patterns
- Customer lifetime value analysis

##  Visualizations

Interactive Power BI dashboards available showcasing:
- Executive KPIs and revenue trends
- Customer segmentation analysis (RFM)
- Geographic delivery performance
- Product category analytics

**View Dashboards:**
- [Executive Summary](./screenshots/executive_dashboard.png)
- [Customer & Operations](./screenshots/customer_operations_dashboard.png)

##  How to Run This Project

### Prerequisites
- PostgreSQL 12+ installed
- pgAdmin 4 or any PostgreSQL client
- Git (for cloning repository)
- Power BI Desktop (optional, for viewing dashboards)

### Setup Instructions

1. **Clone the repository:**
```bash
git clone https://github.com/jannathulfirdouz/Brazilian-Ecommerce-Data-Warehouse-ETL.git
cd Brazilian-Ecommerce-Data-Warehouse-ETL
```

2. **Download the dataset:**
   - Visit [Kaggle Olist Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
   - Download and extract CSV files to `Dataset/` folder

3. **Create database:**
```sql
CREATE DATABASE olist_ecommerce;
```

4. **Execute SQL scripts in order:**
```bash
# Run in pgAdmin Query Tool
01_create_staging_tables.sql
02_import_csv_data.sql
03_data_quality_checks.sql
04_data_cleaning_transformation.sql
05_create_data_warehouse.sql
06_business_analytics.sql
```

5. **Update file paths:**
   - Edit `02_import_csv_data.sql`
   - Update CSV file paths to match your local directory

## View results:

 - Run analytical queries from 06_business_analytics.sql
 - View Power BI dashboards in screenshots/ folder
 - Read business insights in INSIGHTS.md

#  Skills Demonstrated

- **SQL Proficiency:** Complex queries, CTEs, window functions, aggregations, joins  
- **Data Quality:** Comprehensive validation and cleansing strategies (99.95% quality)  
- **ETL Design:** Three-layer architecture (staging → cleaned → data warehouse)  
- **Data Modeling:** Dimensional modeling, star schema design, SCD Type 2  
- **Business Intelligence:** RFM segmentation, cohort analysis, trend analysis  
- **Visualization:** Power BI dashboard creation and design  
- **Problem Solving:** Handling real-world messy data  
- **Documentation:** Clear code comments, architecture diagrams, business insights  
- **Version Control:** Git workflow, professional GitHub repository  

---


# 💡 Key Project Highlights

### Scale
- 1.5M+ records processed  
- R$ 15.87M revenue analyzed  
- 98,666 unique customers  
- 112,960 transactions in fact table  

### Quality
- 99.95% data retention rate  
- Zero duplicate or orphan records  
- 100% referential integrity  
- Comprehensive validation rules  

### Performance
- Star schema optimized for analytics  
- Indexed foreign keys for fast queries  
- Efficient ETL pipeline design  
- Query response time < 1 second  

### Business Value
- 6 strategic recommendations with ROI projections  
- Identified R$5.5M "At Risk" customer segment  
- Discovered 87% delivery time correlation with ratings  
- Revealed 53% Black Friday revenue spike opportunity  


##  Author
**Jannathul Firdouz Sahul Hameed**
- GitHub: [@jannathulfirdouz](https://github.com/jannathulfirdouz)
- LinkedIn:(https://www.linkedin.com/in/jannathul-firdouz-58298b321)
- Email: jannathul.firdouz@icloud.com

##  License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

##  Acknowledgments
- Dataset provided by [Olist](https://olist.com/) via Kaggle
- Brazilian e-commerce market insights
- PostgreSQL community for excellent documentation

---

** If you found this project helpful, please consider giving it a star!**

*Last Updated: February 2026*
