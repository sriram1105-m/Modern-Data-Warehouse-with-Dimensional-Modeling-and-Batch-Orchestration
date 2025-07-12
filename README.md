# Modern-Data-Warehouse-with-Dimensional-Modeling-and-Batch-Orchestration

A batch-oriented data warehouse project with SCD Type 2, data validation, and KPI reporting built using Spark SQL and Delta Lake.

# 🧱 From Raw to Insights: Building a Real-World Data Pipeline with SQL, Delta Lake & Dimensional Modeling

This project simulates a real-world batch data pipeline that ingests raw transactional data, models it into a dimensional warehouse, applies data quality checks, and produces clean, validated outputs for business reporting.

It’s designed in the way I’d build a pipeline in production: clean structure, modular SQL, auditability, and traceable transformations across every layer.

## Why I built this

I wanted to demonstrate how to take raw data and shape it into something analysts, BI tools, and decision makers can rely on.  
Not just working SQL, but a pipeline you can explain, schedule, test, and hand off to another engineer confidently.

This is not a demo project. It reflects the level of rigor and structure I bring to production pipelines I’m responsible for.

## Project Structure

This follows a layered approach, from ingestion to insights:
bronze → raw data from CSV, no changes
silver → cleaned, typed, dimensionally modeled
silver_validated → filtered and audited for DQ
gold → final KPIs, aggregated for analytics

Each layer is modular. Every transformation step is isolated and repeatable.  
If a task fails, you fix the logic and rerun just that part. This is how real pipelines should work.

## 🧱 Dimensional Model (Star Schema)
```text
                             +-----------------------------------------+
                             |        tp_silver.dim_customer          |
                             |-----------------------------------------|
                             | customer_id (PK)                        |
                             | customer_name                           |
                             | customer_email                          |
                             | customer_region                         |
                             | valid_from                              |
                             | valid_to                                |
                             | is_current                              |
                             +-----------------------------------------+
                                           ▲
                                           |
                                           |
+-------------------------------+          |          +-------------------------------+
|     tp_silver.dim_date        |          |          |     tp_silver.dim_product     |
|-------------------------------|          |          |-------------------------------|
| date_key (PK)                 |◄─────────┼─────────►| product_id (PK)               |
| order_date                    |          |          | product_name                  |
| day                           |                     | brand                         |
| month                         |                     +-------------------------------+
| year                          |
+-------------------------------+

                            ▲
                            |
                            |
             +----------------------------------------------+
             |          tp_silver.fact_sales                |
             |----------------------------------------------|
             | date_key (FK)                                |
             | customer_id (FK)                             |
             | product_id (FK)                              |
             | quantity                                     |
             | unit_price                                   |
             | total_price                                  |
             | payment_method                               |
             | channel                                      |
             | created_at                                   |
             +----------------------------------------------+
```




## What’s included

### Bronze

- Raw ingestion of a CSV file into a Delta table
- No cleaning, just loading

### Silver

- `dim_customer`: includes SCD Type 2 logic with `valid_from`, `valid_to`, and `is_current`
- `dim_product`: uses hashed IDs from product and brand
- `dim_date`: standard calendar dimension
- `fact_sales`: joins everything into a modeled fact table

### Silver Validated

- Filters out records with null FKs, zero quantities, and other DQ issues
- Adds `row_hash`, `batch_id`, and `validated_at` for tracking

### Gold

- `monthly_revenue_by_channel`
- `top_customers_by_revenue`
- `top_products_by_revenue`
- `brand_performance_summary`

All outputs are ready for dashboards, exports, or further analysis.

## How it works

Each script is a logical task, like you’d see in a DAG. You can run them one by one, or automate them in dbt, Airflow, or a notebook job.

No hidden dependencies.  
No magic functions.  
Every transformation is visible in SQL.

# 🧩 Production-Oriented Design and Simulated Orchestration
This project is built with real-world pipeline architecture in mind. While orchestration tools like Airflow, dbt, or Dagster aren't used directly, the modular structure mirrors how you’d design a modern DAG.
# Simulated Task Mapping
| Pipeline Layer   | SQL Script Used            | Equivalent DAG Node                     |
| ---------------- | -------------------------- | --------------------------------------- |
| Bronze Ingestion | `load_raw_sales.sql`       | Airflow BashOperator or Spark Job       |
| Dim Customer     | `dim_customer.sql`         | SCD update logic node                   |
| Dim Product      | `dim_product.sql`          | dbt model or transformation task        |
| Dim Date         | `dim_date.sql`             | Calendar dimension loader               |
| Fact Sales       | `fact_sales.sql`           | Join logic and surrogate key resolution |
| Validated Layer  | `fact_sales_validated.sql` | Data Quality step                       |
| Gold KPIs        | `gold_*.sql`               | Metric aggregations and outputs         |

In Airflow, a DAG for this project might look like:
dim_customer >> fact_sales >> fact_sales_validated >> gold_kpis

# How it would run in production:
- Scheduled daily/weekly batch jobs
- Retry logic on failure
- SLAs and alerting for critical failures
- Integrated with CI/CD for versioning and testing
- Tagged with batch IDs and timestamps for traceability

# Designed for deployment:
Even without Airflow/dbt, this project is ready to plug into orchestration layers because:
- SQL is modular and atomic
- Logic is cleanly separated by layer
- Naming is standardized
- Audit fields and lineage are tracked

## Key engineering details

- SCD Type 2 is handled using email + region to track customer changes
- All surrogate keys are generated using MD5 for consistency
- `fact_sales_validated` includes DQ rules and full audit tracking
- KPIs are cleanly derived from validated records only
- Table naming and structure are consistent with how real teams organize layers

## 🧠 Why This Pipeline Matters

Most demo projects stop at loading some data and running a few queries.  
This pipeline goes much deeper:

- Built with **real warehouse design** principles
- Implements **SCD logic**, not just upserts
- Tracks **data quality violations**, not just assumes clean input
- Uses **audit hashes** and **timestamps** to simulate traceability
- Structures code for **Airflow/dbt compatibility** without relying on tools

This mirrors what production systems look like in companies that take data seriously.

## 🔍 Core Engineering Practices Applied

| Concept                | How it’s used here                            |
|------------------------|-----------------------------------------------|
| SCD Type 2             | Full version tracking on `dim_customer`       |
| Star Schema            | Clear separation of dimensions and fact table |
| Auditability           | `row_hash`, `batch_id`, `validated_at` fields |
| Validation             | Only clean rows enter gold layer              |
| DAG Design             | Every SQL file is atomic and reusable         |
| Hash-based IDs         | Consistent surrogate key generation           |

# 🤖 Machine Learning and Data Science Use
This pipeline is not just for dashboards and reports. It also prepares high-quality, structured data that machine learning and data science workflows can use directly without extra cleanup or reshaping.
Most ML models fail before they start because the data is messy, inconsistent, or untraceable. This pipeline solves that problem by creating a stable, auditable, and well-modeled foundation that supports training, evaluation, and deployment workflows.

Here’s how:

- Dimensions enable reliable feature engineering
  dim_customer tracks changes using SCD Type 2, which allows historical context for training.
  dim_product provides normalized product and brand information.
  dim_date makes it easy to extract temporal features like week of year or month.
  These dimensions are cleaned, deduplicated, and ready for use as input features.

- Validated fact table ensures trustworthy labels and events
  fact_sales_validated only includes rows that pass strict data quality rules.
  All keys are resolved, quantities are positive, and prices are properly typed.
  Each row includes a hash and batch ID for tracking and reproducibility.
  This makes it safe to use for supervised learning, aggregation, or time-series modeling.

- Time-based modeling is supported by design
  Every transaction is tied to a dim_date entry.
  You can easily slice data by order month or week for training and evaluation.
  The structure supports building time-aware models like forecasting or churn prediction.

- Modular structure makes experimentation easier
  You can use the silver layer for granular, row-level modeling.
  You can use the gold layer for pre-aggregated KPIs or features.
  The pipeline produces consistent outputs across batches, which supports repeatable experiments.

## This project shows

- How I model data to serve business intelligence, analytics, machine learning, and data science needs
- How I apply validation, auditing, and structure to ensure clean, trustworthy data
- How I build pipelines that are modular, testable, and easy to maintain
- How I support both reporting workflows and machine learning pipelines with reliable, well-modeled inputs
- How I prevent surprises by addressing data quality issues early and making transformations fully transparent

# ✍️ Author
I build data systems that people can trust. Not just pipelines that move rows, but processes that support decisions, analysis, and long-term reliability.
This project shows how I think about data engineering. Clean design, clear logic, and business awareness built into every step.

If you're working on something where data matters and quality cannot be an afterthought, let's connect. I'm always up for building meaningful, production-grade systems.
