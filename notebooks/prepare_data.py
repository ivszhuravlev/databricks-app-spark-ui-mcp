# Databricks notebook source
# MAGIC %md
# MAGIC # Prepare reporting extracts
# MAGIC
# MAGIC Build working tables used by the daily USD KPI job.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text(
    "source_txn",
    "hive_metastore.spark_tuning_test.transactions",
)
dbutils.widgets.text(
    "schema",
    "hive_metastore.spark_tuning_work",
)

SOURCE_TXN = dbutils.widgets.get("source_txn").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
REQUIRED_COLS = ["transaction_description", "category", "country", "currency"]
BUSINESS_START = "2024-01-01"
BUSINESS_DAYS = 120
STORE_COUNT = 25
SCALE_FACTOR = 20
USA_SHARE = 0.80
OUTPUT_FILES = 8

spark.sql("CREATE SCHEMA IF NOT EXISTS {0}".format(SCHEMA))

TXN_TABLE = "{0}.transactions".format(SCHEMA)
STORES_TABLE = "{0}.stores".format(SCHEMA)
FX_TABLE = "{0}.fx_rates".format(SCHEMA)

src = spark.table(SOURCE_TXN)
missing = [c for c in REQUIRED_COLS if c not in src.columns]
if missing:
    raise ValueError("{0} missing {1}".format(SOURCE_TXN, missing))

fact = (
    src.withColumn("txn_id", F.concat(F.lit("TXN-"), F.monotonically_increasing_id().cast("string")))
    .withColumn(
        "store_id",
        F.concat(
            F.lit("ST-"),
            F.lpad(
                (F.floor(F.rand(101) * STORE_COUNT) + F.lit(1)).cast("int").cast("string"),
                4,
                "0",
            ),
        ),
    )
    .withColumn(
        "business_date",
        F.date_add(
            F.lit(BUSINESS_START).cast("date"),
            F.floor(F.rand(103) * BUSINESS_DAYS).cast("int"),
        ),
    )
    .withColumn("amount", (F.rand(104) * 245.0 + F.lit(5.0)).cast("double"))
    .withColumn("source_system", F.lit("core_batch"))
    .select(
        "txn_id",
        "store_id",
        "business_date",
        "amount",
        "source_system",
        *REQUIRED_COLS,
    )
)

if SCALE_FACTOR > 1:
    fact = (
        fact.crossJoin(spark.range(SCALE_FACTOR).withColumnRenamed("id", "rep"))
        .withColumn(
            "txn_id",
            F.concat(F.col("txn_id"), F.lit("-"), F.col("rep").cast("string")),
        )
        .drop("rep")
    )

fact = fact.withColumn(
    "country",
    F.when(F.rand(105) < USA_SHARE, F.lit("USA")).otherwise(F.col("country")),
)

spark.sql("DROP TABLE IF EXISTS {0}".format(TXN_TABLE))
(
    fact.coalesce(OUTPUT_FILES)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TXN_TABLE)
)
txn_n = spark.table(TXN_TABLE).count()
txn_files = spark.sql("DESCRIBE DETAIL {0}".format(TXN_TABLE)).collect()[0]
print("wrote {0} rows={1} files={2}".format(TXN_TABLE, txn_n, txn_files.numFiles))
spark.table(TXN_TABLE).groupBy("store_id").count().orderBy(F.desc("count")).show(5, truncate=False)
print("usa_rows={0}".format(spark.table(TXN_TABLE).filter("country = 'USA'").count()))

stores = spark.createDataFrame(
    [("ST-{0:04d}".format(i), "region-{0}".format((i % 5) + 1)) for i in range(1, STORE_COUNT + 1)],
    ["store_id", "region"],
)
spark.sql("DROP TABLE IF EXISTS {0}".format(STORES_TABLE))
stores.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STORES_TABLE)
print("wrote {0} rows={1}".format(STORES_TABLE, spark.table(STORES_TABLE).count()))

base_rates = spark.createDataFrame(
    [
        ("USD", 1.0),
        ("GBP", 1.27),
        ("CAD", 0.74),
        ("AUD", 0.66),
        ("INR", 0.012),
        ("EUR", 1.08),
        ("JPY", 0.0067),
        ("MXN", 0.058),
    ],
    ["currency", "base_rate"],
)
currencies = (
    spark.table(TXN_TABLE)
    .select("currency")
    .distinct()
    .union(base_rates.select("currency"))
    .distinct()
)
rate_dates = spark.range(BUSINESS_DAYS).select(
    F.date_add(F.lit(BUSINESS_START).cast("date"), F.col("id").cast("int")).alias("rate_date")
)

fx_rates = (
    currencies.crossJoin(rate_dates)
    .join(base_rates, "currency", "left")
    .withColumn("base_rate", F.coalesce(F.col("base_rate"), F.lit(1.0)))
    .withColumn(
        "usd_rate",
        F.when(F.col("currency") == "USD", F.lit(1.0)).otherwise(
            (
                F.col("base_rate")
                * (F.lit(1.0) + (F.rand(201) - F.lit(0.5)) * F.lit(0.03))
            ).cast("double")
        ),
    )
    .select("currency", "rate_date", "usd_rate")
)

spark.sql("DROP TABLE IF EXISTS {0}".format(FX_TABLE))
fx_rates.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FX_TABLE)
print("wrote {0} rows={1}".format(FX_TABLE, spark.table(FX_TABLE).count()))
