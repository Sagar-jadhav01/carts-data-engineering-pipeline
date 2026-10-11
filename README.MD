# Carts Data Engineering Pipeline

## Project Overview

This project demonstrates an end-to-end data engineering pipeline that processes cart and product data using AWS S3, Databricks, Apache Airflow, and Snowflake.

## Architecture

```text
Carts JSON Files
       |
       v
    AWS S3 (Raw)
       |
       v
Databricks + PySpark
       |
       v
Bronze → Silver → Gold
       |
       v
    AWS S3 (Parquet exports)
       |
       v
Apache Airflow
       |
       v
    Snowflake
       |
       v
Silver and Gold Tables
```

## Technologies Used

- **Python** — scripting and data handling
- **AWS S3** — raw data storage and processed-data exports
- **Databricks** — data processing using PySpark
- **Apache Airflow** — pipeline orchestration
- **Snowflake** — analytical data warehouse
- **SQL** — table creation, data loading, and validation
- **Git and GitHub** — version control and project sharing

## Data Processing

- **Bronze:** Ingest cart data from JSON files.
- **Silver:** Clean and transform cart and product records.
- **Gold:** Aggregate product performance metrics, including quantity sold, number of carts, sales value, and average product price.

## Snowflake Tables

- `CARTS_DB.SILVER.SILVER_CARTS`
- `CARTS_DB.GOLD.GOLD_PRODUCT_PERFORMANCE`

## Project Structure

```text
data-engering-project/
├── airflow/
│   └── carts_s3_to_snowflake.py
├── aws/
│   └── upload_to_s3.py
├── databricks/
│   └── carts_pipeline.py
├── snowflake/
│   └── carts_setup_and_load.sql
├── .gitignore
└── README.md
```

## Learning Objectives

- Build a data pipeline using cloud and data engineering tools.
- Process JSON data and create Bronze, Silver, and Gold layers.
- Orchestrate pipeline tasks with Apache Airflow.
- Load Parquet data into Snowflake.
- Validate data using SQL queries.
