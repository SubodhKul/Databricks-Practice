# Databricks notebook source
dbutils.fs.mount(
    source = "wasbs://ttcontainer1@ttdatabricks101.blob.core.windows.net/",
    mount_point = "/mnt/danishdata",
    extra_configs = {"fs.azure.account.key.ttdatabricks101.blob.core.windows.net":dbutils.secrets.get(scope = "azstorage", key = "sa101key")})

# COMMAND ----------

spark.conf.set('spark.sql.legacy.parquet.nanosAsLong','true')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/danishdata

# COMMAND ----------

sale_df = spark.read \
.format("parquet") \
.load("dbfs:/mnt/danishdata/")

# COMMAND ----------

from pyspark.sql.functions import *
sale_df_new = sale_df.withColumn("date", from_unixtime(col("date").cast("string")[0:8])) \
    .withColumn('Quarter',quarter(from_unixtime(col('quarter')))) \
    .withColumn("Quarter",concat(year('Date'),lit("Q"),col("Quarter")))

# COMMAND ----------


