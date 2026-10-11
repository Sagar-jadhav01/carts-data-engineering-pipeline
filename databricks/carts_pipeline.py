
from pyspark import pipelines as dp
from pyspark.sql.functions import explode, col
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    countDistinct,
    avg
)


@dp.table
def bronze_carts():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("multiLine", "true")
        .load("s3://emr--test-bucket/raw/")
    )

    return (
        df
        .select(explode(col("carts")).alias("cart"))
        .select("cart.*")
    )
    
    

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, trim, lower

@dp.table(name="silver_carts")
def silver_carts():

    # Step 1: Read Bronze data
    df = spark.read.table("bronze_carts")

    # Step 2: Flatten the products array
    df = (
        df.select(
            col("id").alias("cart_id"),
            col("userId").alias("user_id"),
            col("total").alias("cart_total"),
            col("discountedTotal").alias("cart_discounted_total"),
            col("totalProducts").alias("total_products"),
            col("totalQuantity").alias("total_quantity"),
            explode(col("products")).alias("product")
        )
        .select(
            "cart_id",
            "user_id",
            "cart_total",
            "cart_discounted_total",
            "total_products",
            "total_quantity",
            col("product.id").alias("product_id"),
            col("product.title").alias("product_title"),
            col("product.price").alias("product_price"),
            col("product.quantity").alias("quantity"),
            col("product.discountPercentage").alias("discount_percentage"),
            col("product.discountedTotal").alias("product_discounted_total"),
            col("product.total").alias("product_total"),
            col("product.thumbnail").alias("thumbnail")
        )
    )

    # Step 3: Clean text columns
    df = (
        df.withColumn("product_title", trim(col("product_title")))
          .withColumn("thumbnail", trim(col("thumbnail")))
    )

    # Step 4: Remove rows missing essential information
    df = df.filter(
        col("cart_id").isNotNull() &
        col("user_id").isNotNull() &
        col("product_id").isNotNull() &
        col("product_title").isNotNull() &
        (col("product_title") != "") &
        col("product_price").isNotNull() &
        col("quantity").isNotNull()
    )

    # Step 5: Remove invalid numeric values
    df = df.filter(
        (col("product_price") >= 0) &
        (col("quantity") > 0) &
        (col("discount_percentage") >= 0) &
        (col("discount_percentage") <= 100) &
        (col("product_discounted_total") >= 0) &
        (col("product_total") >= 0) &
        (col("cart_total") >= 0) &
        (col("cart_discounted_total") >= 0)
    )

    # Step 6: Remove duplicate product rows
    df = df.dropDuplicates([
        "cart_id",
        "product_id",
        "product_title",
        "product_price",
        "quantity"
    ])

    # Step 7: Return cleaned Silver data
    return df
@dp.table(name="gold_product_performance")
def gold_product_performance():

    df = spark.read.table("silver_carts")

    return (
        df.groupBy("product_id", "product_title")
        .agg(
            spark_sum("quantity").alias("total_quantity_sold"),
            countDistinct("cart_id").alias("number_of_carts"),
            spark_sum("product_total").alias("total_sales_value"),
            avg("product_price").alias("average_product_price")
        )
    )    


