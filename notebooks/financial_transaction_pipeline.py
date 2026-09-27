```python
# Databricks notebook source

# ============================================================
# Financial Lakehouse & AI Copilot
# Financial Transaction Pipeline
# ============================================================
#
# Architecture:
# Bronze → Data Quality → Silver → Incremental MERGE → Gold
#
# Technologies:
# Databricks, PySpark, Delta Lake, Unity Catalog
# ============================================================


# ============================================================
# 1. Generate Financial Transaction Data
# ============================================================

from pyspark.sql.functions import rand, expr, col, sum, avg, count


transactions_df = (
    spark.range(1, 100001)
    .withColumnRenamed("id", "transaction_id")

    .withColumn(
        "customer_id",
        (rand() * 8 + 101).cast("int")
    )

    .withColumn(
        "transaction_date",
        expr(
            "date_sub(current_date(), cast(rand() * 365 as int))"
        )
    )

    .withColumn(
        "product",
        expr("""
            CASE
                WHEN rand() < 0.4 THEN 'ETF'
                WHEN rand() < 0.7 THEN 'Mutual Fund'
                WHEN rand() < 0.9 THEN 'Bond'
                ELSE 'Equity'
            END
        """)
    )

    .withColumn(
        "amount",
        (rand() * 49000 + 1000).cast("decimal(12,2)")
    )

    .withColumn(
        "status",
        expr("""
            CASE
                WHEN rand() < 0.85 THEN 'SUCCESS'
                WHEN rand() < 0.95 THEN 'PENDING'
                ELSE 'REJECTED'
            END
        """)
    )

    .withColumn(
        "country",
        expr("""
            CASE
                WHEN customer_id IN (101,102,106) THEN 'India'
                WHEN customer_id IN (103,105) THEN 'USA'
                WHEN customer_id IN (104,108) THEN 'UK'
                ELSE 'Germany'
            END
        """)
    )
)

display(transactions_df.limit(20))


# ============================================================
# 2. Create Bad Records for Data Quality Testing
# ============================================================

bad_transactions = [
    (100001, None, "2026-09-01", "ETF", 25000.0, "SUCCESS", "India"),
    (100002, 102, "2026-09-01", "Bond", -5000.0, "SUCCESS", "India"),
    (100003, 103, "2026-09-01", "ETF", 15000.0, "INVALID", "USA"),
    (100004, 104, "2026-09-01", "Equity", 20000.0, "SUCCESS", "UK"),
    (100004, 104, "2026-09-01", "Equity", 20000.0, "SUCCESS", "UK")
]

bad_columns = [
    "transaction_id",
    "customer_id",
    "transaction_date",
    "product",
    "amount",
    "status",
    "country"
]

bad_df = spark.createDataFrame(
    bad_transactions,
    bad_columns
)

all_transactions_df = transactions_df.unionByName(bad_df)

print("Total Bronze input records:", all_transactions_df.count())

display(all_transactions_df.limit(20))


# ============================================================
# 3. Bronze Layer
# ============================================================
#
# Bronze stores the incoming data with minimal transformation.
# ============================================================

all_transactions_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("financial_bronze_transactions")


bronze_transactions_df = spark.read.table(
    "financial_bronze_transactions"
)

print(
    "Bronze records:",
    bronze_transactions_df.count()
)

display(bronze_transactions_df.limit(20))


# ============================================================
# 4. Data Quality Checks
# ============================================================

# Check 1: NULL customer IDs

null_customer_count = (
    bronze_transactions_df
    .filter(col("customer_id").isNull())
    .count()
)

print(
    "NULL customer_id records:",
    null_customer_count
)


# Check 2: Negative transaction amounts

negative_amount_count = (
    bronze_transactions_df
    .filter(col("amount") < 0)
    .count()
)

print(
    "Negative amount records:",
    negative_amount_count
)


# Check 3: Invalid transaction status

invalid_status_count = (
    bronze_transactions_df
    .filter(
        ~col("status").isin(
            "SUCCESS",
            "PENDING",
            "REJECTED"
        )
    )
    .count()
)

print(
    "Invalid status records:",
    invalid_status_count
)


# Check 4: Duplicate transaction IDs

duplicate_transactions = (
    bronze_transactions_df
    .groupBy("transaction_id")
    .count()
    .filter(col("count") > 1)
)

display(duplicate_transactions)


# ============================================================
# 5. Silver Layer
# ============================================================
#
# Silver contains cleaned and validated transaction data.
# ============================================================

silver_transactions_df = (
    bronze_transactions_df

    # Remove records without customer ID
    .filter(col("customer_id").isNotNull())

    # Remove negative transaction amounts
    .filter(col("amount") >= 0)

    # Keep only valid statuses
    .filter(
        col("status").isin(
            "SUCCESS",
            "PENDING",
            "REJECTED"
        )
    )

    # Remove duplicate transaction IDs
    .dropDuplicates(["transaction_id"])
)

display(silver_transactions_df.limit(20))

print(
    "Silver records:",
    silver_transactions_df.count()
)


# Save Silver table

silver_transactions_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("financial_silver_transactions")


# Validate Silver table

silver_check_df = spark.read.table(
    "financial_silver_transactions"
)

print(
    "Stored Silver records:",
    silver_check_df.count()
)


# ============================================================
# 6. Day-2 Incremental Data
# ============================================================
#
# Simulates new data arriving after the initial load.
#
# Includes:
# - New transactions
# - Existing transactions that need updates
# - Invalid records
# - Duplicate records
# ============================================================

day2_transactions = [

    # New transactions
    (100006, 101, "2026-09-02", "ETF", 55000.0, "SUCCESS", "India"),
    (100007, 104, "2026-09-02", "Bond", 22000.0, "SUCCESS", "UK"),
    (100008, 105, "2026-09-02", "Mutual Fund", 78000.0, "PENDING", "USA"),
    (100009, 102, "2026-09-02", "ETF", 35000.0, "SUCCESS", "India"),
    (100010, 108, "2026-09-02", "Equity", 41000.0, "SUCCESS", "UK"),
    (100011, 106, "2026-09-02", "ETF", 16000.0, "SUCCESS", "India"),
    (100012, 107, "2026-09-02", "Bond", 12500.0, "PENDING", "Germany"),
    (100013, 103, "2026-09-02", "Mutual Fund", 98000.0, "SUCCESS", "USA"),

    # Existing transactions for MERGE updates
    (500, 103, "2026-09-02", "ETF", 75000.0, "SUCCESS", "USA"),
    (1200, 105, "2026-09-02", "Bond", 18500.0, "REJECTED", "USA"),
    (3500, 101, "2026-09-02", "ETF", 63000.0, "SUCCESS", "India"),
    (9999, 102, "2026-09-02", "Mutual Fund", 44000.0, "SUCCESS", "India"),

    # Invalid records
    (200001, None, "2026-09-02", "ETF", 20000.0, "SUCCESS", "India"),
    (200002, 102, "2026-09-02", "ETF", -3000.0, "SUCCESS", "India"),
    (200003, 103, "2026-09-02", "ETF", 25000.0, "INVALID", "USA"),

    # Duplicate record
    (200004, 104, "2026-09-02", "Bond", 18000.0, "SUCCESS", "UK"),
    (200004, 104, "2026-09-02", "Bond", 18000.0, "SUCCESS", "UK"),

    # Additional valid transaction
    (200005, 108, "2026-09-02", "Equity", 51000.0, "SUCCESS", "UK")
]

day2_columns = [
    "transaction_id",
    "customer_id",
    "transaction_date",
    "product",
    "amount",
    "status",
    "country"
]

day2_df = spark.createDataFrame(
    day2_transactions,
    day2_columns
)

print(
    "Day-2 records:",
    day2_df.count()
)

display(day2_df)


# ============================================================
# 7. Clean Day-2 Data
# ============================================================

day2_clean_df = (
    day2_df

    .filter(col("customer_id").isNotNull())

    .filter(col("amount") >= 0)

    .filter(
        col("status").isin(
            "SUCCESS",
            "PENDING",
            "REJECTED"
        )
    )

    .dropDuplicates(["transaction_id"])
)

print(
    "Clean Day-2 records:",
    day2_clean_df.count()
)

display(day2_clean_df)


# ============================================================
# 8. Delta MERGE — Incremental Processing
# ============================================================
#
# Existing transaction IDs → UPDATE
# New transaction IDs      → INSERT
# ============================================================

day2_clean_df.createOrReplaceTempView(
    "day2_transactions"
)


spark.sql("""
MERGE INTO financial_silver_transactions AS target

USING day2_transactions AS source

ON target.transaction_id = source.transaction_id

WHEN MATCHED THEN
    UPDATE SET
        target.customer_id = source.customer_id,
        target.transaction_date = source.transaction_date,
        target.product = source.product,
        target.amount = source.amount,
        target.status = source.status,
        target.country = source.country

WHEN NOT MATCHED THEN
    INSERT (
        transaction_id,
        customer_id,
        transaction_date,
        product,
        amount,
        status,
        country
    )

    VALUES (
        source.transaction_id,
        source.customer_id,
        source.transaction_date,
        source.product,
        source.amount,
        source.status,
        source.country
    )
""")


# Validate final Silver table

final_silver_df = spark.read.table(
    "financial_silver_transactions"
)

print(
    "Final Silver records:",
    final_silver_df.count()
)


# ============================================================
# 9. Gold Layer — Country KPIs
# ============================================================

silver_df = spark.read.table(
    "financial_silver_transactions"
)

gold_country_df = (
    silver_df

    .groupBy("country")

    .agg(
        sum("amount").alias(
            "total_transaction_amount"
        ),

        count("transaction_id").alias(
            "transaction_count"
        ),

        avg("amount").alias(
            "average_transaction_amount"
        )
    )

    .orderBy("country")
)

display(gold_country_df)


# Save Gold country KPI table

gold_country_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "financial_gold_country_kpi"
    )


# Validate Gold table

gold_country_check_df = spark.read.table(
    "financial_gold_country_kpi"
)

display(gold_country_check_df)


# ============================================================
# 10. Gold Layer — Product KPIs
# ============================================================

gold_product_df = (
    silver_df

    .groupBy("product")

    .agg(
        sum("amount").alias(
            "total_amount"
        ),

        count("transaction_id").alias(
            "transaction_count"
        ),

        avg("amount").alias(
            "average_amount"
        )
    )

    .orderBy("product")
)

display(gold_product_df)


# Save Gold product KPI table

gold_product_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "financial_gold_product_kpi"
    )


# Validate Gold product table

gold_product_check_df = spark.read.table(
    "financial_gold_product_kpi"
)

display(gold_product_check_df)


# ============================================================
# END OF FINANCIAL TRANSACTION PIPELINE
# ============================================================
#
# Final flow:
#
# Raw Financial Data
#        ↓
# Bronze Delta
#        ↓
# Data Quality Checks
#        ↓
# Silver Delta
#        ↓
# Delta MERGE
#        ↓
# Gold KPIs
#
# Gold tables:
# - financial_gold_country_kpi
# - financial_gold_product_kpi
# ============================================================
```
