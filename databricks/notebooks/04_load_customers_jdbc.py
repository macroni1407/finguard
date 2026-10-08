# Databricks notebook source
# MAGIC %md
# MAGIC # Load customers from Postgres (JDBC) — alternative to Lakeflow Connect
# MAGIC
# MAGIC The tutorial ingests `customers` with **Lakeflow Connect** (configured in the UI, no code).
# MAGIC Use this notebook instead when Lakeflow Connect is not available on your workspace.
# MAGIC
# MAGIC - Connection details: secret `finguard-scope/postgres_connection_details`
# MAGIC   (JSON with host, port, database, user, password: `scripts/setup_secrets.sh`).
# MAGIC - Writes a full snapshot to `finguard_dev.bronze.customers`.
# COMMAND ----------

import json

pg = json.loads(dbutils.secrets.get(scope="finguard-scope", key="postgres_connection_details"))
jdbc_url = f"jdbc:postgresql://{pg['host']}:{pg.get('port', '5432')}/{pg['database']}?sslmode=require"

customers = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "public.customers")
    .option("user", pg["user"])
    .option("password", pg["password"])
    .load()
)
print("rows read:", customers.count())

# COMMAND ----------

(customers.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("finguard_dev.bronze.customers"))

display(spark.table("finguard_dev.bronze.customers").limit(10))
