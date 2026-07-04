# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Build Silver Table
# MAGIC Cleans and enriches the bronze table: adds a computed `total_amount`
# MAGIC column and filters out any bad rows.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail_sales_dab_dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = spark.table(f"{catalog}.{schema}.bronze_sales")

silver_df = (
    bronze_df
    .filter((F.col("unit_price") > 0) & (F.col("quantity") > 0))
    .withColumn("total_amount", F.round(F.col("unit_price") * F.col("quantity"), 2))
    .withColumn("order_month", F.date_trunc("month", F.col("order_date")))
)

silver_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_sales")

print(f"Wrote {silver_df.count()} rows to {catalog}.{schema}.silver_sales")
display(silver_df.limit(10))
