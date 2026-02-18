# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window as w

# COMMAND ----------

df = spark.read.format('parquet')\
            .load('abfss://bronze@databricks451.dfs.core.windows.net/orders')

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop('_rescued_data')

# COMMAND ----------

df = df.withColumn('year', year(col('order_date')))\
       .withColumn('order_date', to_timestamp(col('order_date')))

# COMMAND ----------

windowSpec = w.partitionBy('year').orderBy(desc('total_amount'))
df1 = df.withColumn('flag', dense_rank().over(windowSpec))

# COMMAND ----------

df1.display()

# COMMAND ----------

windowSpec = w.partitionBy('year').orderBy(desc('total_amount'))
df1 = df.withColumn('row_flag', row_number().over(windowSpec))

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

df.write.format('delta')\
        .mode('overwrite')\
        .save('abfss://silver@databricks451.dfs.core.windows.net/orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.orders_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databricks451.dfs.core.windows.net/orders'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.silver.orders_silver

# COMMAND ----------

