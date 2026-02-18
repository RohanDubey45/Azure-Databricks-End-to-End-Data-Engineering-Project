# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format('parquet')\
               .load('abfss://bronze@databricks451.dfs.core.windows.net/customers')

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop(
        col('_rescued_data')
    )

# COMMAND ----------

df = df.withColumn(
        'domain',
        split(col('email'), '@')[1]
    )

df.display()

# COMMAND ----------

df_count = df.groupBy(
    'domain',
).agg(
    count('customer_id').alias("total_customers")
).sort(
    'total_customers', ascending=False
)


# COMMAND ----------

df_gmail = df.filter(
    col('domain') == 'gmail.com'
)

# COMMAND ----------

df_hotmail = df.filter(
    col('domain') == 'hotmail.com'
)

# COMMAND ----------

df_yahoo = df.filter(
    col('domain') == 'yahoo.com'
)

# COMMAND ----------

df = df.withColumn(
    'full_name',
    concat(col('first_name'), lit(' '), col('last_name'))
)

df = df.drop('first_name', 'last_name')
df.display()

# COMMAND ----------

df.write.format('delta')\
        .mode('overwrite')\
        .save('abfss://silver@databricks451.dfs.core.windows.net/customers')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.customers_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databricks451.dfs.core.windows.net/customers'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.silver.customers_silver

# COMMAND ----------

