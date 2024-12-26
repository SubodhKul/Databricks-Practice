# Databricks notebook source
dbutils.fs.mount(
    source = "wasbs://ttcontainer1@ttdatabricks101.blob.core.windows.net/",
    mount_point = "/mnt/danishdata",
    extra_configs = {"fs.azure.account.key.ttdatabricks101.blob.core.windows.net":dbutils.secrets.get(scope = "Azure-key-vault", key = "sa-ttdatabricks101-key")})

# COMMAND ----------

spark.conf.set('spark.sql.legacy.parquet.nanosAsLong','true')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/danishdata

# COMMAND ----------

sale_df = spark.read \
.format("parquet") \
.load("dbfs:/mnt/danishdata/DKHousingPrices.parquet")

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

sale_df_new.write \
    .format("parquet") \
    .partitionBy("region") \
    .option("overwriteSchema","true") \
    .mode("overwrite") \
    .option('path',"/mnt/danishdata/parquet") \
    .saveAsTable("DRH_Records.salesparquet")

# COMMAND ----------

sale_df_new.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("region") \
    .option("overwriteSchema","true") \
    .option("path","/mnt/danishdata/delta") \
    .saveAsTable("DRH_Records.salesdeltatable")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in drh_records;

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE drh_records.salesparquet

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY drh_records.salesparquet

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY drh_records.salesdeltatable

# COMMAND ----------


