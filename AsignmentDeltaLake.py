# Databricks notebook source
bucket_name = "ttdatabricksstorage"
mount_name = "/mnt/gcs/"
dbutils.fs.mount(
  f"gs://{bucket_name}",
  f"/mnt/databricks/{mount_name}",
  extra_configs = {"fs.gs.project.id": "dataengproj-443211"}
)


# COMMAND ----------

spark.conf.set('spark.sql.legacy.parquet.nanosAsLong','true')

# COMMAND ----------

sale_df = spark.read \
.format("parquet") \
.load("/mnt/databricks/gcs/DKHousingPrices.parquet")

# COMMAND ----------


