# Databricks notebook source
# MAGIC %sql
# MAGIC create table orders(
# MAGIC   order_id int,
# MAGIC   order_date string, 
# MAGIC   customer_id int,
# MAGIC   order_status string
# MAGIC )
# MAGIC using delta
# MAGIC TBlproperties(delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders
# MAGIC VALUES
# MAGIC   (1, '2025-01-01 09:15:00', 101, 'Completed'),
# MAGIC   (2, '2025-01-02 11:30:00', 102, 'Pending'),
# MAGIC   (3, '2025-01-03 14:45:00', 103, 'Cancelled'),
# MAGIC   (4, '2025-01-04 16:00:00', 104, 'Completed'),
# MAGIC   (5, '2025-01-05 19:20:00', 105, 'Pending');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes("orders",1)

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from orders where order_id = 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from table_changes('orders',2);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('orders',1);

# COMMAND ----------

# MAGIC %sql
# MAGIC update orders set order_status = 'COMPLETE' where order_id = 4

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('orders',3)

# COMMAND ----------


