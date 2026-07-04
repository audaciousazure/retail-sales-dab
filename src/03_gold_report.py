# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Build Gold Report Table
# MAGIC Aggregates silver data into a region x month summary table, ready
# MAGIC for a dashboard or BI tool.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail_sales_dab_dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

from pyspark.sql import functions as F

silver_df = spark.table(f"{catalog}.{schema}.silver_sales")

gold_df = (
    silver_df
    .groupBy("region", "order_month")
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
        F.countDistinct("order_id").alias("num_orders"),
    )
    .withColumn("avg_order_value", F.round(F.col("total_revenue") / F.col("num_orders"), 2))
    .orderBy("order_month", "region")
)

gold_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.gold_sales_summary")

print(f"Wrote {gold_df.count()} rows to {catalog}.{schema}.gold_sales_summary")
display(gold_df)
