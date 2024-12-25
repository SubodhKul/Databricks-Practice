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

display(sale_df)

# COMMAND ----------

from pyspark.sql.functions import *
sale_df_new = sale_df.withColumn("date", to_date(from_unixtime(col("date")/1e9))) \
    .withColumn('Quarter',quarter('date')) \
    .withColumn("Quarter",concat(year('date'),lit("Q"),col("Quarter")))

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists DRH_Records;

# COMMAND ----------

display(sale_df_new)

# COMMAND ----------

sale_df.write \
    .format("parquet") \
    .partitionBy("region") \
    .mode("overwrite") \
    .save("/mnt/danishdata/parquet")

# COMMAND ----------

sale_df.write.format("delta").mode("overwrite").option("path","/mnt/danishdata/").saveAsTable("DRH_Records.salesdeltatable")

# COMMAND ----------


