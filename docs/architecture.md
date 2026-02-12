Data Warehouse Layers
┌─────────────────────────────────────────────┐
│         Raw Layer (staging schema)          │
│  • Original CSV structure                   │
│  • All data types as VARCHAR                │
│  • No transformations applied               │
└────────────────┬────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────┐
│       Cleaned Layer (cleaned schema)        │
│  • NULL handling & imputation               │
│  • Data type optimization                   │
│  • Standardization (UPPER, TRIM)            │
│  • Business rules applied                   │
└────────────────┬────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────┐
│    Data Warehouse Layer (dwh schema)        │
│  • Star schema dimensional model            │
│  • Surrogate keys generated                 │
│  • SCD Type 2 implemented                   │
│  • Optimized for analytics                  │
│  • Indexed for performance                  │
└─────────────────────────────────────────────┘
Technical Implementation
Database: PostgreSQL 18
Schema Separation: staging → cleaned → dwh
Total Storage: ~1.5M records across all layers
Key Technologies:

Dimensional modeling (Kimball methodology)
Slowly Changing Dimensions (Type 1 & Type 2)
Surrogate key generation (SERIAL)
Index optimization
Foreign key constraints