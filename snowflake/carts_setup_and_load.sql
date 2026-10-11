# Carts Data Engineering Project — Snowflake SQL

-- =====================================================
-- 1. CREATE DATABASE AND WAREHOUSE
-- =====================================================

```sql
CREATE DATABASE IF NOT EXISTS CARTS_DB;

CREATE WAREHOUSE IF NOT EXISTS CARTS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;
```

-- =====================================================
-- 2. CREATE SCHEMAS
-- =====================================================

```sql
CREATE SCHEMA IF NOT EXISTS CARTS_DB.SILVER;
CREATE SCHEMA IF NOT EXISTS CARTS_DB.GOLD;

SHOW SCHEMAS IN DATABASE CARTS_DB;
```

-- =====================================================
-- 3. CHECK EXISTING STAGES AND STORAGE INTEGRATION
-- =====================================================

```sql
SHOW STAGES IN ACCOUNT;

DESC STAGE
    SNS_SQS_PRACTICE_DB.PUBLIC.SNOWFLAKE_S3_PRACTICE_STAGE;

DESC INTEGRATION SNOWFLAKE_S3_PRACTICE_INTEGRATION;

LIST @SNS_SQS_PRACTICE_DB.PUBLIC.SNOWFLAKE_S3_PRACTICE_STAGE;

LIST @SNS_SQS_PRACTICE_DB.PUBLIC.SNOWFLAKE_S3_PRACTICE_STAGE/silver/;
```

-- =====================================================
-- 4. CONFIGURE ALLOWED S3 LOCATIONS
-- =====================================================

```sql
ALTER STORAGE INTEGRATION SNOWFLAKE_S3_PRACTICE_INTEGRATION
SET STORAGE_ALLOWED_LOCATIONS = (
    's3://emr--test-bucket/raw/',
    's3://emr--test-bucket/silver/',
    's3://emr--test-bucket/gold/'
);
```

-- =====================================================
-- 5. CREATE SILVER TABLE
-- =====================================================

```sql
CREATE TABLE IF NOT EXISTS CARTS_DB.SILVER.SILVER_CARTS (
    CART_ID                  NUMBER,
    USER_ID                  NUMBER,
    CART_TOTAL               FLOAT,
    CART_DISCOUNTED_TOTAL    FLOAT,
    TOTAL_PRODUCTS           NUMBER,
    TOTAL_QUANTITY           NUMBER,
    PRODUCT_ID               NUMBER,
    PRODUCT_TITLE            VARCHAR,
    PRODUCT_PRICE            FLOAT,
    QUANTITY                 NUMBER,
    DISCOUNT_PERCENTAGE      FLOAT,
    PRODUCT_DISCOUNTED_TOTAL FLOAT,
    PRODUCT_TOTAL            FLOAT,
    THUMBNAIL                VARCHAR
);
```

-- =====================================================
-- 6. CREATE GOLD TABLE
-- =====================================================

```sql
CREATE TABLE IF NOT EXISTS CARTS_DB.GOLD.GOLD_PRODUCT_PERFORMANCE (
    PRODUCT_ID             NUMBER,
    PRODUCT_TITLE          VARCHAR,
    TOTAL_QUANTITY_SOLD    NUMBER,
    NUMBER_OF_CARTS        NUMBER,
    TOTAL_SALES_VALUE      FLOAT,
    AVERAGE_PRODUCT_PRICE  FLOAT
);
```

-- =====================================================
-- 7. CREATE SNOWFLAKE STAGE FOR SILVER DATA
-- =====================================================

```sql
CREATE STAGE IF NOT EXISTS CARTS_DB.SILVER.CARTS_SILVER_STAGE
    URL = 's3://emr--test-bucket/silver/'
    STORAGE_INTEGRATION = SNOWFLAKE_S3_PRACTICE_INTEGRATION
    FILE_FORMAT = (TYPE = PARQUET);

LIST @CARTS_DB.SILVER.CARTS_SILVER_STAGE;
```

-- =====================================================
-- 8. CREATE SNOWFLAKE STAGE FOR GOLD DATA
-- =====================================================

```sql
CREATE STAGE IF NOT EXISTS CARTS_DB.GOLD.CARTS_GOLD_STAGE
    URL = 's3://emr--test-bucket/gold/'
    STORAGE_INTEGRATION = SNOWFLAKE_S3_PRACTICE_INTEGRATION
    FILE_FORMAT = (TYPE = PARQUET);

LIST @CARTS_DB.GOLD.CARTS_GOLD_STAGE;
```

-- =====================================================
-- 9. LOAD SILVER DATA FROM S3
-- =====================================================

```sql
COPY INTO CARTS_DB.SILVER.SILVER_CARTS
FROM @CARTS_DB.SILVER.CARTS_SILVER_STAGE/silver_carts/
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*[.]parquet'
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

-- =====================================================
-- 10. LOAD GOLD DATA FROM S3
-- =====================================================

```sql
COPY INTO CARTS_DB.GOLD.GOLD_PRODUCT_PERFORMANCE
FROM @CARTS_DB.GOLD.CARTS_GOLD_STAGE/gold_product_performance/
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*[.]parquet'
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

-- =====================================================
-- 11. VERIFY SILVER AND GOLD DATA
-- =====================================================

```sql
SELECT COUNT(*) AS SILVER_ROWS
FROM CARTS_DB.SILVER.SILVER_CARTS;

SELECT COUNT(*) AS GOLD_ROWS
FROM CARTS_DB.GOLD.GOLD_PRODUCT_PERFORMANCE;

SELECT *
FROM CARTS_DB.GOLD.GOLD_PRODUCT_PERFORMANCE
LIMIT 10;
```

-- =====================================================
-- 12. CHECK DUPLICATES IN SILVER DATA
-- =====================================================

```sql
SELECT
    COUNT(*) AS TOTAL_ROWS,
    COUNT(
        DISTINCT CONCAT(
            CART_ID, '-',
            PRODUCT_ID, '-',
            PRODUCT_TITLE
        )
    ) AS UNIQUE_CART_PRODUCTS
FROM CARTS_DB.SILVER.SILVER_CARTS;
```
