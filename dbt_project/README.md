# FMCG dbt Analytics Project

A comprehensive dbt analytics project designed to transform and analyze Fast-Moving Consumer Goods (FMCG) data. This project implements a medallion architecture (Bronze → Silver → Gold) to deliver production-ready analytics on customer behavior, product performance, and order analytics.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Models](#data-models)
- [Setup & Installation](#setup--installation)
- [Running dbt](#running-dbt)
- [Data Quality & Testing](#data-quality--testing)
- [Lineage & Dependencies](#lineage--dependencies)
- [Troubleshooting](#troubleshooting)

## Overview

This dbt project transforms raw FMCG transactional data into clean, analytical datasets. It handles:

- **Customer Analytics**: Deduplicated customer records with standardized location data
- **Product Analytics**: Cleaned product information with category standardization and business division mapping
- **Order Analytics**: Aggregated order facts with incremental updates
- **Pricing Analytics**: Validated and cleaned pricing information with currency conversion utilities

**Key Statistics:**
- 4 staging models (Silver layer)
- 5 mart models (Gold layer)
- 2 dbt packages for data quality and utilities
- Multi-database adapter support (Databricks, PostgreSQL, BigQuery, Fabric)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Analytics)                   │
│  ┌──────────────┐  ┌─────────────┐  ┌──────────────────┐   │
│  │ dim_customers│  │ dim_products│  │ dim_gross_price  │   │
│  └──────────────┘  └─────────────┘  └──────────────────┘   │
│              ↑            ↑                   ↑              │
│              └────┬───────┴─────────────────┬┘              │
│                   │                         │               │
│         ┌─────────▼──────────────────┐     │               │
│         │   fact_orders (Fact)       │     │               │
│         │  (Incremental/Merge)       │     │               │
│         └─────────┬──────────────────┘     │               │
│                   │                         │               │
│         ┌─────────▼──────────────────────┬─┴─────────────┐ │
│         │ vw_fact_orders_enriched (View) │ (Analytics)   │ │
│         │ All dimensions + metrics      │               │ │
│         └────────────────────────────────┴───────────────┘ │
└─────────────────────────────────────────────────────────────┘
                          ↑
┌─────────────────────────┼───────────────────────────────────┐
│              SILVER LAYER (Staging/Cleaning)                │
│  ┌──────────────┐  ┌─────────────┐  ┌──────────────────┐   │
│  │ stg_customers│  │ stg_products│  │ stg_orders       │   │
│  │  - Dedupe    │  │  - Clean    │  │  - Aggregate     │   │
│  │  - Cities    │  │  - Division │  │  - By Month      │   │
│  │  - Normalize │  │  - Variants │  │                  │   │
│  └──────────────┘  └─────────────┘  └──────────────────┘   │
│                                                              │
│  ┌──────────────────────────┐  ┌──────────────────────┐    │
│  │   stg_gross_price        │  │   customers_py       │    │
│  │  - Validate pricing      │  │  (Python model)      │    │
│  │  - Month aggregation     │  │                      │    │
│  └──────────────────────────┘  └──────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                          ↑
┌─────────────────────────┼───────────────────────────────────┐
│             BRONZE LAYER (Raw/Source Data)                  │
│    Source: fmcg_bronze schema                              │
│  ┌──────────────┐  ┌─────────────┐  ┌──────────────────┐   │
│  │  customers   │  │  products   │  │  gross_price     │   │
│  │              │  │             │  │                  │   │
│  └──────────────┘  └─────────────┘  └──────────────────┘   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │               orders (fact records)                  │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Project Structure

```
dbt_project/
├── dbt_project.yml              # Project configuration
├── packages.yml                 # dbt package dependencies
├── README.md                    # This file
│
├── models/
│   ├── staging/                 # Silver layer - cleaned, deduplicated data
│   │   ├── sources.yml          # Source definitions (Bronze layer)
│   │   ├── schema.yml           # Data quality tests and column descriptions
│   │   ├── stg_customers.sql    # Customer deduplication and cleaning
│   │   ├── stg_products.sql     # Product cleaning and classification
│   │   ├── stg_orders.sql       # Order aggregation by month
│   │   ├── stg_gross_price.sql  # Price data cleaning and validation
│   │   └── customers_py.py      # Python transformation model
│   │
│   └── marts/                   # Gold layer - analytics-ready tables
│       ├── dim_customers.sql    # Customer dimension
│       ├── dim_products.sql     # Product dimension with division mapping
│       ├── dim_gross_price.sql  # Price dimension
│       ├── fact_orders.sql      # Orders fact table (incremental)
│       └── vw_fact_orders_enriched.sql  # Enriched view with all dimensions
│
├── macros/
│   ├── cents_to_dollars.sql     # Currency conversion utility
│   └── generate_schema_name.sql # Schema naming logic
│
├── seeds/                       # Reference/lookup data
│   ├── dim_customers_seed.csv
│   ├── dim_products_seed.csv
│   ├── dim_gross_price_seed.csv
│   └── fact_orders_seed.csv
│
└── dbt_packages/                # Installed packages (generated)
    ├── dbt_utils/               # Generic macros and utilities
    └── dbt_expectations/        # Advanced data quality tests
```

## Data Models

### Staging Models (Silver Layer)

#### [`stg_customers`](dbt_project/models/staging/stg_customers.sql)
**Purpose:** Clean and standardize customer data

**Transformations:**
- Deduplication (keeps most recent record per customer_id)
- City name standardization (Bengaluru, Hyderabad, New Delhi)
- Customer name title-casing
- Generation of unique `customer_code` via SHA256 hash

**Key Columns:**
- `customer_code` (PK): SHA256 hash of cleaned customer name
- `customer_id`: Original customer identifier
- `customer_name`: Cleaned and standardized name
- `city`: Standardized city name

**Tests:** Uniqueness and NOT NULL constraints on customer_code, city domain validation

---

#### [`stg_products`](dbt_project/models/staging/stg_products.sql)
**Purpose:** Clean product data and apply business logic

**Transformations:**
- Deduplication (keeps most recent record per product_id)
- Spelling corrections (Protien → Protein)
- Category title-casing
- **Division mapping**: Creates standardized business divisions from categories
  - Energy Bars, Protein Bars → **Nutrition Bars**
  - Granola & Cereals → **Breakfast Foods**
  - Recovery Dairy → **Dairy & Recovery**
  - Electrolyte Mix → **Hydration & Electrolytes**
  - Others → **Other**
- Variant extraction from product names
- Generation of unique `product_code` via SHA256 hash

**Key Columns:**
- `product_code` (PK): SHA256 hash of cleaned product name
- `product_id`: Original product ID (cleaned to numeric, else 999999)
- `division`: Standardized business division
- `category`: Cleaned product category
- `variant`: Product weight/quantity variant

**Tests:** Uniqueness and NOT NULL on product_code, regex validation on product_id, accepted values on division

---

#### [`stg_orders`](dbt_project/models/staging/stg_orders.sql)
**Purpose:** Aggregate orders at monthly-customer-product grain

**Transformations:**
- Monthly truncation of order dates
- Aggregation of sold quantities
- Linking to customer and product codes

**Key Columns:**
- `date`: Month start date
- `customer_id`: Customer identifier
- `product_code`: Product identifier
- `sold_quantity`: Total quantity sold in the month

---

#### [`stg_gross_price`](dbt_project/models/staging/stg_gross_price.sql)
**Purpose:** Clean and validate pricing data

**Transformations:**
- Month aggregation
- Date format validation and standardization
- Absolute value enforcement (handles negative prices)
- Price validation (ensures >= 0)

**Key Columns:**
- `month`: Standardized month date
- `gross_price`: Validated price value

**Tests:** NOT NULL on month and gross_price, expression validation for non-negative values

---

#### [`customers_py`](dbt_project/models/staging/customers_py.py)
**Purpose:** Python-based customer transformation model

Demonstrates dbt's Python capabilities for complex data processing tasks.

---

### Mart Models (Gold Layer)

#### [`dim_customers`](dbt_project/models/marts/dim_customers.sql)
**Purpose:** Customer dimension table for analytics

**Source:** `stg_customers`

**Grain:** One row per unique customer

**Key Attributes:**
- Customer codes and names
- Geographic information (city/market)
- Business segments (platform, channel)

---

#### [`dim_products`](dbt_project/models/marts/dim_products.sql)
**Purpose:** Product dimension table with business classification

**Source:** `stg_products`

**Grain:** One row per unique product

**Key Attributes:**
- Product codes and names
- Category and division
- Weight/quantity variants
- Business segment mapping

---

#### [`dim_gross_price`](dbt_project/models/marts/dim_gross_price.sql)
**Purpose:** Pricing dimension table

**Source:** `stg_gross_price`

**Grain:** One row per product-month combination

**Key Attributes:**
- Monthly price points
- INR currency (with conversion utilities available)

---

#### [`fact_orders`](dbt_project/models/marts/fact_orders.sql)
**Purpose:** Central fact table for order analytics

**Materialization:** Incremental (Merge strategy)

**Unique Key:** `[date, product_code, customer_code]`

**Grain:** One row per customer-product-month combination

**Key Metrics:**
- `sold_quantity`: Monthly quantity sold

**Special Features:**
- Uses incremental merge for efficient updates
- Seeds historical data from `fact_orders_seed.csv`
- Supports full refresh for complete data reprocessing

---

#### [`vw_fact_orders_enriched`](dbt_project/models/marts/vw_fact_orders_enriched.sql)
**Purpose:** Enriched analytical view combining all dimensions

**Materialization:** View

**Joins:**
- `fact_orders` → `dim_customers` (on customer_code)
- `fact_orders` → `dim_products` (on product_code)
- `fact_orders` → `dim_gross_price` (implicit pricing lookup)
- External `dim_date` (assumes external date dimension in Gold schema)

**Key Analytical Metrics:**
- `sold_quantity`: Volume sold
- `price_inr`: Unit price in Indian Rupees
- `total_amount_inr`: Calculated revenue (quantity × price)

**Available Dimensions:**
- **Temporal:** Year, month, quarter, date attributes
- **Customer:** Name, market, platform, channel
- **Product:** Division, category, product name, variant

---

## Setup & Installation

### Prerequisites

- **dbt** (v1.3+): [Installation guide](https://docs.getdbt.com/docs/core/installation)
- **Supported Data Warehouse:**
  - Databricks (primary)
  - PostgreSQL
  - Google BigQuery
  - Microsoft Fabric
- **Python** (v3.8+) for Python models

### Step 1: Clone & Configure

```bash
# Navigate to project directory
cd dbt_project

# Install dbt packages
dbt deps

# Create profiles.yml for your database
# Location: ~/.dbt/profiles.yml
# Template for Databricks (example):
# fmcg_dbt:
#   target: dev
#   outputs:
#     dev:
#       type: databricks
#       catalog: fmcg
#       schema: dev
#       host: [your-workspace-url]
#       http_path: /sql/1.0/warehouses/[warehouse-id]
#       token: [your-token]
```

### Step 2: Verify Connection

```bash
dbt debug
```

Expected output: `All checks passed!`

### Step 3: Load Seed Data

```bash
dbt seed
```

This loads reference data from CSV files into the `raw` schema:
- `dim_customers_seed`
- `dim_products_seed`
- `dim_gross_price_seed`
- `fact_orders_seed`

### Step 4: Run Models

```bash
# Build entire project (seeds, models, tests)
dbt build

# Or run models only (skip tests)
dbt run
```

## Running dbt

### Common Commands

```bash
# Refresh entire project
dbt build

# Run only staging models
dbt run --select staging

# Run only mart models
dbt run --select marts

# Run and test a specific model
dbt build --select dim_customers

# Full refresh of fact_orders (rebuilds incremental table)
dbt run --select fact_orders --full-refresh

# Generate documentation
dbt docs generate
dbt docs serve

# Run all tests
dbt test

# Run tests for specific model
dbt test --select stg_products

# Preview model output (doesn't build)
dbt parse
```

### Incremental Model Strategy

The `fact_orders` table uses an **incremental merge strategy**:

```sql
-- During incremental runs: Only processes recent data
-- During full refresh: Rebuilds from scratch and seeds historical data

dbt run --select fact_orders              # Incremental run
dbt run --select fact_orders --full-refresh  # Full rebuild
```

## Data Quality & Testing

This project implements comprehensive data quality testing using:

### Testing Packages

1. **[dbt_utils](https://github.com/dbt-labs/dbt-utils)** (≥1.3.0)
   - Generic tests for common patterns
   - Expression validation tests
   - Utilities for SQL generation

2. **[dbt_expectations](https://github.com/metaplane/dbt_expectations)** (0.10.10)
   - Advanced statistical tests
   - Pattern matching tests
   - Distributional validation

### Key Tests

**stg_customers:**
- `unique`: customer_code must be unique
- `not_null`: customer_code and city required
- `accepted_values`: city ∈ {Bengaluru, Hyderabad, New Delhi, Unknown}

**stg_products:**
- `unique`: product_code must be unique
- `not_null`: product_code and division required
- `expect_column_values_to_match_regex`: product_id must be numeric
- `accepted_values`: division ∈ {Nutrition Bars, Breakfast Foods, Dairy & Recovery, Healthy Snacks, Hydration & Electrolytes, Other}

**stg_gross_price:**
- `not_null`: month and gross_price required
- `expression_is_true`: gross_price ≥ 0

### Running Tests

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select stg_products

# Run specific test
dbt test --select stg_customers.unique

# Show test results with details
dbt test --show
```

## Lineage & Dependencies

### Model Dependencies

```
Bronze (Sources)
├── customers → stg_customers → dim_customers → fact_orders → vw_fact_orders_enriched
├── products → stg_products → dim_products → fact_orders → vw_fact_orders_enriched
├── orders → stg_orders → fact_orders → vw_fact_orders_enriched
└── gross_price → stg_gross_price → dim_gross_price → vw_fact_orders_enriched
```

### Generate Documentation

```bash
# Generate dbt docs
dbt docs generate

# Serve docs locally (http://localhost:8000)
dbt docs serve
```

Access the interactive data lineage diagram and model documentation in the dbt docs interface.

## Macros

### [`cents_to_dollars`](dbt_project/macros/cents_to_dollars.sql)

Converts monetary values from cents to dollars with database-specific implementations.

**Usage:**
```sql
SELECT 
    order_id,
    {{ cents_to_dollars('amount_cents') }} AS amount_usd
FROM orders
```

**Supported Databases:**
- **Default (PostgreSQL):** `(column / 100)::numeric(16, 2)`
- **BigQuery:** `round(cast(column / 100 as numeric), 2)`
- **Databricks:** `cast(column / 100 as numeric(16,2))`
- **Fabric:** `cast(column / 100 as numeric(16,2))`

### [`generate_schema_name`](dbt_project/macros/generate_schema_name.sql)

Custom schema naming logic for Databricks environments.

## Troubleshooting

### Issue: "Profile not found"

```bash
# Verify profiles.yml exists
cat ~/.dbt/profiles.yml

# Debug database connection
dbt debug
```

### Issue: "Source not found in fmcg_bronze"

```bash
# Verify bronze schema tables exist
SELECT * FROM fmcg.bronze.INFORMATION_SCHEMA.TABLES;

# Confirm source definitions in sources.yml match actual table names
```

### Issue: "fact_orders incremental model not updating"

```bash
# Clear state and rebuild
dbt run --select fact_orders --full-refresh

# For incremental debugging
dbt run --select fact_orders --debug
```

### Issue: Tests failing after model changes

```bash
# Regenerate tests with latest schema
dbt test --select [model_name] --show

# Debug specific test
dbt test --select [model_name].[test_name] --show
```

### Issue: Python model (`customers_py`) execution error

```bash
# Verify Python environment
python --version

# Check dbt Python version requirement
dbt --version

# Install additional Python dependencies if needed
pip install -r requirements.txt
```

## Project Configuration

### Key Settings (dbt_project.yml)

```yaml
name: fmcg_dbt
profile: fmcg_dbt

# Directories
seed-paths: ["seeds"]
model-paths: ["models"]
macro-paths: ["macros"]

# Schema management
seeds:
  fmcg_dbt:
    +schema: raw        # Seeds load to raw schema

models:
  fmcg_dbt:
    staging:
      +schema: silver   # Staging models use silver schema
      +materialized: table
    marts:
      +schema: gold     # Mart models use gold schema
      +materialized: table
```

### Three-Schema Approach

- **Bronze** (`fmcg.bronze`): Raw source data
- **Silver** (`fmcg.silver`): Cleaned, deduplicated data
- **Gold** (`fmcg.gold`): Analytics-ready aggregated data

## Support & Contribution

For issues, questions, or improvements:

1. Review the model YAML files for documentation
2. Check dbt docs: `dbt docs serve`
3. Examine model SQL for transformation logic
4. Review test configurations in `schema.yml`

---

**Last Updated:** 2024
**dbt Version:** 1.3+
**Supported Data Warehouses:** Databricks, PostgreSQL, BigQuery, Fabric
