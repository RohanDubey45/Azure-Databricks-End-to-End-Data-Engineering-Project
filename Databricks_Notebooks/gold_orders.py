# Databricks notebook source
# MAGIC %md
# MAGIC #**Fact Orders**

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Reading**

# COMMAND ----------

df = spark.sql('select * from databricks_cata.silver.orders_silver')
df.display()

# COMMAND ----------

df_dimcus = spark.sql('select DimCustomerKey, customer_id as dim_cust_id from databricks_cata.gold.dimcustomers')

df_dimpro = spark.sql('select product_id DimProductKey, product_id as dim_product_id from databricks_cata.gold.dimproducts')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Fact Dataframe

# COMMAND ----------

df_fact = df.join(df_dimcus, df['customer_id'] == df_dimcus['dim_cust_id'], how="left")
df_fact  = df_fact.join(df_dimpro, df_fact['product_id'] == df_dimpro['dim_product_id'], how="left")

df_fact = df_fact.drop('dim_cust_id', 'dim_product_id', 'customer_id', 'product_id')

df_fact.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##**Upsert**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('databricks_cata.gold.FactOrders'):
  
      del_obj = DeltaTable.forName(spark, 'databricks_cata.gold.FactOrders')

      src = df_fact.alias('src')
      trg = del_obj.alias('trg')

      trg.merge(src, '''src.order_id = trg.order_id and 
                        src.DimCustomerKey = trg.DimCustomerKey and
                        src.DimProductKey = trg.DimProductKey
                     ''').whenMatchedUpdateAll()\
                         .whenNotMatchedInsertAll()

else:
  df_fact.write.format('delta')\
               .option('path', 'abfss://gold@databricks451.dfs.core.windows.net/FactOrders')\
               .saveAsTable('databricks_cata.gold.FactOrders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.FactOrders