# Brazilian E-Commerce Data Warehouse & ETL Pipeline

##  Project Overview
End-to-end ETL pipeline and data warehouse implementation for analyzing Brazilian e-commerce operations. This project demonstrates data engineering best practices including data quality assessment, dimensional modeling, and advanced SQL analytics.

##  Business Objective
Build a scalable data warehouse solution to analyze customer behavior, sales trends, and operational efficiency for a Brazilian e-commerce platform serving 99,000+ customers across multiple states.

##  Technologies Used
- **Database:** PostgreSQL 18
- **ETL Tools:** SQL (COPY commands, custom transformations)
- **Version Control:** Git & GitHub
- **Data Volume:** 1.5M+ records across 9 tables
- **Dataset Source:** [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

##  Project Structure
```
Brazilian-Ecommerce-Data-Warehouse-ETL/
├── README.md                          # Project documentation
├── sql/                               # SQL scripts
│   ├── 01_create_staging_tables.sql   # Staging layer setup
│   ├── 02_import_csv_data.sql         # Data ingestion
│   ├── 03_data_quality_checks.sql     # Quality validation
│   ├── 04_data_cleaning.sql           # (Coming soon)
│   ├── 05_create_data_warehouse.sql   # (Coming soon)
│   └── 06_analysis_queries.sql        # (Coming soon)
├── docs/                              # Documentation & diagrams
├── screenshots/                       # Query results & visualizations
└── Dataset/                           # Raw CSV files (9 files)
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
- Handle NULL values (imputation strategies)
- Remove duplicates
- Standardize data formats
- Create business rules for data validation

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
### Phase 5: Business Analytics
- 12+ analytical queries
- Customer segmentation (RFM analysis)
- Sales trend analysis
- Product performance metrics
- Seller analytics
- Delivery performance tracking

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
```

5. **Update file paths:**
   - Edit `02_import_csv_data.sql`
   - Update CSV file paths to match your local directory

##  Project Status

- [x] Phase 1: Data Ingestion (1.5M records imported)
- [x] Phase 2: Data Quality Assessment (99.95% data quality)
- [x] Phase 3: Data Cleaning & Transformation (823 invalid records removed)
- [x] Phase 4: Data Warehouse Design (Star Schema - 5 dimensions + 1 fact)
- [x] Phase 5: Business Analytics & Insights
- [x] Phase 6: Visualization & Final Documentation

##  Skills Demonstrated
- **SQL Proficiency:** Complex queries, CTEs, window functions, aggregations
- **Data Quality:** Comprehensive validation and cleansing strategies
- **ETL Design:** Staging layer, transformation logic, error handling
- **Data Modeling:** Dimensional modeling, star schema design
- **Problem Solving:** Handling real-world messy data
- **Documentation:** Clear code comments and project documentation

##  Future Enhancements
- [ ] Implement stored procedures for automation
- [ ] Create views for business users
- [ ] Add incremental data loading
- [ ] Build Tableau/PowerBI dashboards
- [ ] Implement data lineage tracking
- [ ] Add unit tests for transformations

##  Author
**Jannathul Firdouz Sahul Hameed**
- GitHub: [@jannathulfirdouz](https://github.com/jannathulfirdouz)
- LinkedIn: [Add your LinkedIn]
- Email: [Add your email]

##  License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

##  Acknowledgments
- Dataset provided by [Olist](https://olist.com/) via Kaggle
- Brazilian e-commerce market insights
- PostgreSQL community for excellent documentation

---

** If you found this project helpful, please consider giving it a star!**

*Last Updated: February 2026*
