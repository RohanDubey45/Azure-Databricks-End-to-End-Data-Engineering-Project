# Databricks notebook source
# MAGIC %md
# MAGIC ### init_load_flag
# MAGIC ### 0 - Means table does not exists so initial load
# MAGIC ### 1 - Means table exists 

# COMMAND ----------

# init_load_flag = dbutils.widgets.get('init_load_flag')
# init_load_flag = int(init_load_flag)
# print(init_load_flag)
# print(type(init_load_flag))

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.sql('select * from databricks_cata.silver.customers_silver')

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.dropDuplicates(subset = ['customer_id'])

# COMMAND ----------

if spark.catalog.tableExists('databricks_cata.gold.dimcustomers'):
# if init_load_flag == 1: # table already exists
    print(init_load_flag)
    df_old = spark.sql('''select DimCustomerKey, customer_id, create_date, update_date 
                          from databricks_cata.gold.DimCustomers''')
    
else: # initial state load the table
    print(init_load_flag)
    df_old = spark.sql('''select 0 DimCustomerKey, 0 customer_id, 0 create_date, 0 update_date 
                          from databricks_cata.silver.customers_silver where 1=0''')

# COMMAND ----------

df_old = df_old.withColumnRenamed('DimCustomerKey', 'old_dim_customers_key')\
                  .withColumnRenamed('customer_id', 'old_customer_id')\
                  .withColumnRenamed('create_date', 'old_create_date')\
                  .withColumnRenamed('update_date', 'old_update_date')

# COMMAND ----------

df_old.display()

# COMMAND ----------

df_join = df.join(df_old, df.customer_id == df_old.old_customer_id, 'left')

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Separating New vs Old Records

# COMMAND ----------

df_new = df_join.filter(df_join['old_dim_customers_key'].isNull())

# COMMAND ----------

df_old = df_join.filter(col('old_dim_customers_key').isNotNull())
df_old.display()

# COMMAND ----------

# Drop columns you no longer need
df_old = df_old.drop('old_customer_id', 'old_update_date')

df_old = df_old.withColumnRenamed('old_dim_customers_key', 'DimCustomerKey')

# Rename old_create_date to create_date
df_old = df_old.withColumnRenamed("old_create_date", "create_date")
df_old = df_old.withColumn('create_date', to_timestamp(col('create_date'))) # cast to timestamp 

# Recreate update_date with current timestamp
df_old = df_old.withColumn("update_date", current_timestamp())

# COMMAND ----------

df_new.display()

# COMMAND ----------

# Drop columns you no longer need
df_new = df_new.drop('old_dim_customers_key', 'old_customer_id', 'old_update_date', 'old_create_date')

# Recreate update_date with current timestamp
df_new = df_new.withColumn("create_date", current_timestamp())
df_new = df_new.withColumn("update_date", current_timestamp())

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Surrogate key for new records

# COMMAND ----------

df_new = df_new.withColumn('DimCustomerKey', monotonically_increasing_id()+lit(1))
df_new.display()

# COMMAND ----------

if spark.catalog.tableExists('databricks_cata.gold.dimcustomers'):
    df_maxsur = spark.sql('select max("DimCustomerKey") as max_surrogate_key from databricks_cata.gold.DimCustomers')
    max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']
    print(max_surrogate_key)

else:
    max_surrogate_key = 0
    

# COMMAND ----------

df_new = df_new.withColumn('DimCustomerKey', lit(max_surrogate_key) + col('DimCustomerKey'))

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_old.printSchema()

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #**SCD Type 1**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('databricks_cata.gold.DimCustomers'):
    print(True) 
else: 
    print(False)

# COMMAND ----------

if spark.catalog.tableExists('databricks_cata.gold.DimCustomers'):
    del_obj = DeltaTable.forPath(
        spark,
        'abfss://gold@databricks451.dfs.core.windows.net/DimCustomers'
    )

    trg = del_obj.alias('trg')
    src = df_final.alias('src')

    trg.merge(src, 'trg.DimCustomerkey = src.DimCustomerKey')\
       .whenMatchedUpdateAll()\
       .whenNotMatchedInsertAll()\
       .execute()

else:
  df_final.write.format('delta')\
                .mode('overwrite')\
                .option('path', 'abfss://gold@databricks451.dfs.core.windows.net/DimCustomers')\
                .saveAsTable('databricks_cata.gold.DimCustomers')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.DimCustomers;