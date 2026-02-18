# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.table('databricks_cata.bronze.regions')

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop('_rescued_data')

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('abfss://silver@databricks451.dfs.core.windows.net/regions')

# COMMAND ----------

df = spark.read.format('delta')\
               .load('abfss://silver@databricks451.dfs.core.windows.net/regions')

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.regions_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databricks451.dfs.core.windows.net/regions'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.silver.regions_silver