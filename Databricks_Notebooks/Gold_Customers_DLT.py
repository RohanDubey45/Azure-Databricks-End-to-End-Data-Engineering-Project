# Databricks notebook source
import dlt
from pyspark.sql.functions import * 

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Streming Table**

# COMMAND ----------

@dlt.table(name="dim_customers_stage")

def stage():
    
    df = spark.readStream('databricks_cata.silver.customers_silver')

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Streaming View**

# COMMAND ----------

@dlt.view

def dim_customers_view():
    df = spark.readStream.table('Live.dim_customers_stage')

# COMMAND ----------

# MAGIC %md
# MAGIC ##**DimCustomers**

# COMMAND ----------

dlt.create_streaming_table('DimCustomers')

# COMMAND ----------

dlt.apply_changes(
    target = "DimCustomers",
    source = "Live.dim_customers_view",
    keys = ["product_id"],
    sequence_by = "updated_at",
    stored_as_scd_type = 1,
)