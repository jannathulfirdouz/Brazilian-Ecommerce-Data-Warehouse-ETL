# Brazilian E-Commerce Data Warehouse & ETL Pipeline

## 📊 Project Overview
End-to-end ETL pipeline and data warehouse implementation for analyzing Brazilian e-commerce operations. This project demonstrates data engineering best practices including data quality assessment, dimensional modeling, and advanced SQL analytics.

## 🎯 Business Objective
Build a scalable data warehouse solution to analyze customer behavior, sales trends, and operational efficiency for a Brazilian e-commerce platform serving 99,000+ customers across multiple states.

## 🛠️ Technologies Used
- **Database:** PostgreSQL 18
- **ETL Tools:** SQL (COPY commands, custom transformations)
- **Version Control:** Git & GitHub
- **Data Volume:** 1.5M+ records across 9 tables
- **Dataset Source:** [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## 📁 Project Structure
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

## 📊 Dataset Overview
The Olist dataset contains real Brazilian e-commerce data with:
- **99,441** customers
- **99,441** orders (2016-2018)
- **112,650** order items
- **32,951** products
- **3,095** sellers
- **103,886** payment transactions
- **99,224** customer reviews
- **1,000,163** geolocation records

## 🏗️ ETL Pipeline Architecture

### Phase 1: Data Ingestion (Completed ✅)
- Created staging schema for raw data isolation
- Loaded 9 CSV files into PostgreSQL using COPY commands
- Preserved original data types and formats
- Total records ingested: 1.5M+

### Phase 2: Data Quality Assessment

Comprehensive quality checks performed across 1.5M+ records:

** Data Integrity (Perfect):**
- Zero duplicate records across all primary keys
- 100% referential integrity - no orphan records
- All financial data valid (no negative prices/freight)
- All timestamps chronologically consistent
- Review scores within valid range (1-5)

**  High Data Quality:**
- 98.15% products with complete specifications
- 100% customers with complete addresses  
- 97% orders with complete timestamps
- Valid order statuses across all records

**  Minor Issues (Expected):**
- 81.85% products missing category names (610 products need "Unknown" category)
- 88.34% reviews missing comment titles (optional field - expected)
- 3% orders missing delivery dates (cancelled/pending orders - expected)
- Multiple customer accounts per person detected (business pattern - valid)

**Verdict:** Dataset is exceptionally clean with minimal cleaning required.

### Phase 3: Data Cleaning & Transformation (In Progress 🔄)
- Handle NULL values (imputation strategies)
- Remove duplicates
- Standardize data formats
- Create business rules for data validation

### Phase 4: Data Warehouse Design (Planned 📋)
- Implement star schema with fact and dimension tables
- Create surrogate keys
- Build slowly changing dimensions (SCD Type 2)
- Optimize with indexes and partitioning

### Phase 5: Business Analytics (Planned 📋)
- Customer segmentation (RFM analysis)
- Sales trend analysis
- Product performance metrics
- Seller analytics
- Delivery performance tracking

## 🔍 Key Data Quality Findings

### Critical Issues Identified:
## 🔍 Detailed Data Quality Assessment Results

| Category | Check | Result | Verdict | Notes |
|----------|-------|--------|---------|-------|
| **NULL Analysis** | Critical ID columns | 0% NULLs | ✅ Pass | Core identifiers complete |
| **Duplicates** | Duplicate customer_id | None | ✅ Pass | Safe primary key |
| **Duplicates** | Duplicate order_id | None | ✅ Pass | Orders uniquely identified |
| **Consistency** | Multiple accounts per person | Present | 🟡 Expected | Business pattern |
| **Referential Integrity** | Orders without customers | 0 | ✅ Pass | No orphans |
| **Referential Integrity** | Items without orders | 0 | ✅ Pass | Transaction chain intact |
| **Referential Integrity** | Items with missing products | 0 | ✅ Pass | Product catalog complete |
| **Referential Integrity** | Items with missing sellers | 0 | ✅ Pass | Seller dimension complete |
| **Invalid Values** | Negative prices | 0 | ✅ Pass | No corrupted financial data |
| **Invalid Values** | Negative freight | 0 | ✅ Pass | Freight costs valid |
| **Invalid Values** | Zero prices | 0 | ✅ Pass | No mispriced items |
| **Temporal Integrity** | Future purchase dates | 0 | ✅ Pass | No impossible timestamps |
| **Temporal Integrity** | Delivery before purchase | 0 | ✅ Pass | Chronologically consistent |
| **Review Validity** | Scores outside 1-5 | 0 | ✅ Pass | Customer feedback clean |
| **Completeness** | Products with complete specs | 98.15% | 🟢 High | Minor gaps, imputable |
| **Completeness** | Complete customer addresses | 100% | ✅ Perfect | Geo analysis supported |
| **Completeness** | Orders with all timestamps | 97% | 🟢 High | Missing due to cancellations |

*(Run 03_data_quality_checks.sql)*

## 💡 Business Insights (Coming Soon)
- Geographic distribution of customers
- Top performing product categories
- Payment method preferences
- Seasonal sales patterns
- Customer lifetime value analysis

## 🚀 How to Run This Project

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

## 📈 Project Status

- [x] Phase 1: Data Ingestion
- [x] Phase 2: Data Quality Assessment
- [ ] Phase 3: Data Cleaning & Transformation
- [ ] Phase 4: Data Warehouse Design
- [ ] Phase 5: Business Analytics
- [ ] Phase 6: Documentation & Visualization

## 🎓 Skills Demonstrated
- **SQL Proficiency:** Complex queries, CTEs, window functions, aggregations
- **Data Quality:** Comprehensive validation and cleansing strategies
- **ETL Design:** Staging layer, transformation logic, error handling
- **Data Modeling:** Dimensional modeling, star schema design
- **Problem Solving:** Handling real-world messy data
- **Documentation:** Clear code comments and project documentation

## 📝 Future Enhancements
- [ ] Implement stored procedures for automation
- [ ] Create views for business users
- [ ] Add incremental data loading
- [ ] Build Tableau/PowerBI dashboards
- [ ] Implement data lineage tracking
- [ ] Add unit tests for transformations

## 👤 Author
**Jannathul Firdouz Sahul Hameed**
- GitHub: [@jannathulfirdouz](https://github.com/jannathulfirdouz)
- LinkedIn: [Add your LinkedIn]
- Email: [Add your email]

## 📄 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments
- Dataset provided by [Olist](https://olist.com/) via Kaggle
- Brazilian e-commerce market insights
- PostgreSQL community for excellent documentation

---

**⭐ If you found this project helpful, please consider giving it a star!**

*Last Updated: February 2026*
