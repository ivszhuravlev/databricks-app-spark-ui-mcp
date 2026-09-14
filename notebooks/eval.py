# Databricks notebook source
# MAGIC %md
# MAGIC # eval
# MAGIC
# MAGIC Daily USD KPIs. USA store activity converted to USD and rolled up by store and day.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("schema", "hive_metastore.spark_tuning_work")
SCHEMA = dbutils.widgets.get("schema").strip()

TXN_TABLE = "{0}.transactions".format(SCHEMA)
STORES_TABLE = "{0}.stores".format(SCHEMA)
FX_TABLE = "{0}.fx_rates".format(SCHEMA)
KPI_TABLE = "{0}.daily_usd_kpi".format(SCHEMA)

txn = spark.table(TXN_TABLE).alias("t")
stores = spark.table(STORES_TABLE).alias("s")
fx = spark.table(FX_TABLE).alias("f")

enriched = (
    txn.join(stores, F.col("t.store_id") == F.col("s.store_id"))
    .join(
        fx,
        (F.col("t.currency") == F.col("f.currency"))
        & (F.col("t.business_date") == F.col("f.rate_date")),
    )
    .where(F.col("t.country") == "USA")
)

daily_usd = enriched.groupBy(
    F.col("t.store_id").alias("store_id"),
    F.col("s.region").alias("region"),
    F.col("t.business_date").alias("business_date"),
    F.col("t.currency").alias("currency"),
).agg(
    F.count("*").alias("txn_count"),
    F.sum(F.col("t.amount") * F.col("f.usd_rate")).alias("usd_amount"),
)

daily_usd.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(KPI_TABLE)
print("wrote {0} rows={1}".format(KPI_TABLE, spark.table(KPI_TABLE).count()))
