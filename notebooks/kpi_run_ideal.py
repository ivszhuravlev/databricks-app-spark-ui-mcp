# Databricks notebook source
# MAGIC %md
# MAGIC # Daily USD KPIs (ceiling)
# MAGIC
# MAGIC Same tables as the eval pipeline. Session-only fixes. Does not rewrite source tables.

# COMMAND ----------

from pyspark.sql import functions as F

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")
spark.conf.set("spark.sql.files.openCostInBytes", "0")
spark.conf.set("spark.sql.shuffle.partitions", "8")

dbutils.widgets.text("schema", "hive_metastore.spark_tuning_work")
SCHEMA = dbutils.widgets.get("schema").strip()

TXN_TABLE = "{0}.transactions".format(SCHEMA)
STORES_TABLE = "{0}.stores".format(SCHEMA)
FX_TABLE = "{0}.fx_rates".format(SCHEMA)
KPI_TABLE = "{0}.daily_usd_kpi_ideal".format(SCHEMA)

txn = (
    spark.table(TXN_TABLE)
    .where(F.col("country") == "USA")
    .select("store_id", "business_date", "currency", "amount")
)
stores = F.broadcast(spark.table(STORES_TABLE).select("store_id", "region"))
fx = F.broadcast(
    spark.table(FX_TABLE)
    .dropDuplicates(["currency", "rate_date"])
    .select("currency", "rate_date", "usd_rate")
)

enriched = (
    txn.alias("t")
    .join(stores.alias("s"), F.col("t.store_id") == F.col("s.store_id"))
    .join(
        fx.alias("f"),
        (F.col("t.currency") == F.col("f.currency"))
        & (F.col("t.business_date") == F.col("f.rate_date")),
    )
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
