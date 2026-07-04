# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Generate Bronze Data
# MAGIC Creates the catalog/schema (if needed) and writes a synthetic
# MAGIC "raw sales" table. No external internet access required, which
# MAGIC keeps this friendly to Databricks Free Edition's restricted egress.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail_sales_dab_dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

from pyspark.sql import functions as F
import random

random.seed(42)

regions = ["North", "South", "East", "West"]
products = ["Widget", "Gadget", "Gizmo", "Doohickey", "Thingamajig"]

rows = []
for i in range(5000):
    rows.append((
        i,
        random.choice(regions),
        random.choice(products),
        round(random.uniform(5.0, 500.0), 2),
        random.randint(1, 20),
        f"2026-{random.randint(1,6):02d}-{random.randint(1,28):02d}",
    ))

bronze_df = spark.createDataFrame(
    rows,
    schema="order_id INT, region STRING, product STRING, unit_price DOUBLE, quantity INT, order_date STRING"
).withColumn("order_date", F.to_date("order_date"))

bronze_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_sales")

print(f"Wrote {bronze_df.count()} rows to {catalog}.{schema}.bronze_sales")
display(bronze_df.limit(10))
