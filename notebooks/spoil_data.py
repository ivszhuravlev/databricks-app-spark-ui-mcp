# Databricks notebook source
# MAGIC %md
# MAGIC # Rewrite extracts for file layout
# MAGIC
# MAGIC Rebuild transactions and FX extracts from the current work tables.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text(
    "schema",
    "hive_metastore.spark_tuning_work",
)

SCHEMA = dbutils.widgets.get("schema").strip()
STORE_COUNT = 25
MAX_RECORDS_PER_FILE = 800
WRITE_PARTITIONS = 200
FX_VENDORS = [
    "bloomberg",
    "reuters",
    "ecb_ref",
    "internal_ops",
    "morningstar",
    "refinitiv",
]

TXN_TABLE = "{0}.transactions".format(SCHEMA)
FX_TABLE = "{0}.fx_rates".format(SCHEMA)

other_store = F.concat(
    F.lit("ST-"),
    F.lpad(
        (F.floor(F.rand(102) * (STORE_COUNT - 1)) + F.lit(2)).cast("int").cast("string"),
        4,
        "0",
    ),
)

fact = (
    spark.table(TXN_TABLE)
    .withColumn(
        "store_id",
        F.when(F.rand(101) < 0.96, F.lit("ST-0001")).otherwise(other_store),
    )
    .repartition(WRITE_PARTITIONS)
)
fact.cache()
fact.count()

spark.sql("DROP TABLE IF EXISTS {0}".format(TXN_TABLE))
(
    fact.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
    .saveAsTable(TXN_TABLE)
)
txn_n = spark.table(TXN_TABLE).count()
txn_files = spark.sql("DESCRIBE DETAIL {0}".format(TXN_TABLE)).collect()[0]
print("wrote {0} rows={1} files={2}".format(TXN_TABLE, txn_n, txn_files.numFiles))
spark.table(TXN_TABLE).groupBy("store_id").count().orderBy(F.desc("count")).show(5, truncate=False)

fx = spark.table(FX_TABLE)
if "vendor" in fx.columns:
    fx = fx.drop("vendor")
fx = fx.dropDuplicates(["currency", "rate_date"])
vendors = spark.createDataFrame([(v,) for v in FX_VENDORS], ["vendor"])
fx_rates = fx.crossJoin(vendors).select("currency", "rate_date", "usd_rate", "vendor")
fx_rates.cache()
fx_rates.count()

spark.sql("DROP TABLE IF EXISTS {0}".format(FX_TABLE))
fx_rates.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FX_TABLE)
print("wrote {0} rows={1}".format(FX_TABLE, spark.table(FX_TABLE).count()))
