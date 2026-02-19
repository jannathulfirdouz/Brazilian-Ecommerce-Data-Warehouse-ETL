##  Data Warehouse Architecture

The data warehouse follows a **3-layer architecture** to ensure data quality, traceability, and analytical performance.

```
Raw Layer (staging schema)
   ↓
Cleaned Layer (cleaned schema)
   ↓
Data Warehouse Layer (dwh schema)
```

---

### 1️ Raw Layer — `staging` Schema

This layer stores the **original source data** exactly as received.

**Characteristics**

* Direct import from CSV files
* No transformations
* Preserves raw data for auditing and recovery
* All columns stored as `VARCHAR`

**Purpose**

* Data backup and traceability
* Prevents corruption of original source data
* Enables reprocessing if ETL logic changes

---

### 2️ Cleaned Layer — `cleaned` Schema

This layer performs **data cleaning and preparation** before loading into the warehouse.

**Transformations Applied**

* NULL handling and imputation
* Data type conversion (VARCHAR → numeric/date types)
* Standardization:

  * `UPPER()`
  * `TRIM()`
* Business rule validation
* Duplicate removal
* Data quality checks

**Purpose**

* Improve data reliability
* Prepare data for dimensional modeling
* Ensure consistent format across sources

---

### 3️ Data Warehouse Layer — `dwh` Schema

This is the **analytical layer** used by reporting tools and BI dashboards.

**Features**

* Star schema dimensional model
* Surrogate keys generated
* Slowly Changing Dimensions (SCD Type 2)
* Indexed tables for fast queries
* Optimized for aggregations and reporting

**Usage**

* Business intelligence dashboards
* Analytical queries
* KPI reporting
* Decision support

---

## 🔧 Technical Implementation

**Database System:** PostgreSQL 18
**Schema Flow:** `staging → cleaned → dwh`
**Data Volume:** ~1.5 million records across all layers

---

### Key Technologies Used

* Dimensional Modeling (**Kimball methodology**)
* Slowly Changing Dimensions (**Type 1 & Type 2**)
* Surrogate Key generation (`SERIAL`)
* Index optimization
* Foreign key constraints
* ETL processing using SQL scripts

---

## Why Layered Architecture?

| Layer   | Main Benefit                 |
| ------- | ---------------------------- |
| Staging | Data recovery and auditing   |
| Cleaned | Data quality and consistency |
| DWH     | High-performance analytics   |

This layered approach separates **raw data ingestion**, **data preparation**, and **analytics**, improving maintainability and performance.