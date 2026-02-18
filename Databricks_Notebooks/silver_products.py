# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df = spark.read.format('parquet')\
               .load('abfss://bronze@databricks451.dfs.core.windows.net/products')

df.display()

# COMMAND ----------

df = df.drop('_rescued_data')

# COMMAND ----------

df.createOrReplaceTempView('products')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databricks_cata.bronze.discount_func(p_price double)
# MAGIC returns double
# MAGIC language sql
# MAGIC return p_price * 0.9

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, price, databricks_cata.bronze.discount_func(price) as discounted_price from products

# COMMAND ----------

df = df.withColumn('discounted_price', expr('databricks_cata.bronze.discount_func(price)'))

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databricks_cata.bronze.upper_brand(p_brand string)
# MAGIC returns string
# MAGIC language python
# MAGIC as
# MAGIC   $$
# MAGIC     return p_brand.upper()
# MAGIC   $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, brand, databricks_cata.bronze.upper_brand(brand) as upper_brand from products

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('abfss://silver@databricks451.dfs.core.windows.net/products')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.products_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databricks451.dfs.core.windows.net/products'