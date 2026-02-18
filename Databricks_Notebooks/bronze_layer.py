# Databricks notebook source
dbutils.widgets.text('file_name', 'orders')

# COMMAND ----------

p_file_name = dbutils.widgets.get('file_name')
print(p_file_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

# df = spark.read.format('parquet').load('abfss://bronze@databricks451.dfs.core.windows.net/products')
# df.display()

# COMMAND ----------

df = spark.readStream.format('cloudFiles')\
                     .option('cloudFiles.format', 'parquet')\
                     .option('cloudFiles.schemaLocation', f'abfss://bronze@databricks451.dfs.core.windows.net/schema_{p_file_name}')\
                     .load(f'abfss://source@databricks451.dfs.core.windows.net/{p_file_name}')

# COMMAND ----------

df.writeStream.format('parquet')\
                      .outputMode('append')\
                      .option('checkpointLocation', f'abfss://bronze@databricks451.dfs.core.windows.net/checkpoint_{p_file_name}')\
                      .option('path', f'abfss://bronze@databricks451.dfs.core.windows.net/{p_file_name}')\
                      .trigger(once=True)\
                      .start()