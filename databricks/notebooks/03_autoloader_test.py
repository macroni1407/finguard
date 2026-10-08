# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader test
# MAGIC Reads the watchlist JSON files written by `watchlist_generator/fraud_watchlist_data_generator.py`
# MAGIC with Auto Loader (`cloudFiles`), outside the pipeline, to see how schema inference and
# MAGIC "only new files" work. The test table and checkpoint can be removed afterwards (last cell).

# COMMAND ----------

source_path = "/Volumes/finguard_dev/source/fraud_watchlist/source_data/"
test_root = "/Volumes/finguard_dev/source/fraud_watchlist/_autoloader_test/"

display(dbutils.fs.ls(source_path))

# COMMAND ----------

from pyspark.sql import functions as F

input_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", test_root + "schema/")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(source_path)
)

transformed_df = input_stream.select(
    "*",
    F.col("_metadata.file_path").alias("file_path"),
    F.current_timestamp().alias("ingestion_timestamp"),
)

# COMMAND ----------

streaming_query = (
    transformed_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", test_root + "checkpoint/")
    .trigger(availableNow=True)
    .toTable("finguard_dev.bronze.fraud_watchlist_autoloader_test")
)
streaming_query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard_dev.bronze.fraud_watchlist_autoloader_test

# COMMAND ----------

# Clean up
# spark.sql("DROP TABLE IF EXISTS finguard_dev.bronze.fraud_watchlist_autoloader_test")
# dbutils.fs.rm(test_root, recurse=True)
