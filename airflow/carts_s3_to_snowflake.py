
from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

with DAG(
    dag_id="carts_s3_to_snowflake",
    start_date=datetime(2026, 10, 10),
    schedule=None,
    catchup=False,
) as dag:

    # Task 1: Run the Databricks pipeline
    run_carts_pipeline = DatabricksSubmitRunOperator(
        task_id="run_carts_pipeline",
        databricks_conn_id="databricks_connection",
        pipeline_task={
            "pipeline_id": "199ee93d-f4e4-4fd5-88d3-c7bd2b9b597d"
        },
    )

    # Task 2: Replace Snowflake Silver with the latest full snapshot
    load_silver = SQLExecuteQueryOperator(
        task_id="load_silver_to_snowflake",
        conn_id="movies_snowflake_connection",
        sql="""
        TRUNCATE TABLE CARTS_DB.SILVER.SILVER_CARTS;

        COPY INTO CARTS_DB.SILVER.SILVER_CARTS
        FROM @CARTS_DB.SILVER.CARTS_SILVER_STAGE/silver_carts/
        FILE_FORMAT = (TYPE = PARQUET)
        PATTERN = '.*[.]parquet'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        FORCE = TRUE;
        """,
    )

    # Task 3: Replace Snowflake Gold with the latest full snapshot
    load_gold = SQLExecuteQueryOperator(
        task_id="load_gold_to_snowflake",
        conn_id="movies_snowflake_connection",
        sql="""
        TRUNCATE TABLE CARTS_DB.GOLD.GOLD_PRODUCT_PERFORMANCE;

        COPY INTO CARTS_DB.GOLD.GOLD_PRODUCT_PERFORMANCE
        FROM @CARTS_DB.GOLD.CARTS_GOLD_STAGE/gold_product_performance/
        FILE_FORMAT = (TYPE = PARQUET)
        PATTERN = '.*[.]parquet'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        FORCE = TRUE;
        """,
    )

    # Execute tasks in order
    run_carts_pipeline >> load_silver >> load_gold
