# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka connection test
# MAGIC Checks that Databricks can read the Confluent topic, first as a batch, then as a stream.
# MAGIC Credentials come from the secret scope (`scripts/setup_secrets.sh`), never from this notebook.
# MAGIC The `*_test` tables created here can be dropped afterwards (last cell).

# COMMAND ----------

import json

from pyspark.sql.functions import col

kafka_config = json.loads(dbutils.secrets.get(scope="finguard-scope", key="kafka_connection_details"))

jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{kafka_config["api_key"]}" password="{kafka_config["api_secret"]}";'
)
kafka_options = {
    "kafka.bootstrap.servers": kafka_config["bootstrap_servers"],
    "subscribe": kafka_config["topic"],
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": jaas_config,
    "startingOffsets": "earliest",
}

# COMMAND ----------

# Batch read: everything currently in the topic
sample_batch = spark.read.format("kafka").options(**kafka_options).load()
print("messages in topic:", sample_batch.count())

parsed_batch = sample_batch.select(
    col("key").cast("string"), col("value").cast("string"),
    "topic", "partition", "offset", "timestamp", "timestampType",
)
display(parsed_batch)

# COMMAND ----------

# Streaming read: process what is available now, then stop
streaming_query = (
    spark.readStream.format("kafka").options(**kafka_options).load()
    .select(col("key").cast("string"), col("value").cast("string"),
            "topic", "partition", "offset", "timestamp", "timestampType")
    .writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/finguard_dev/source/fraud_watchlist/_checkpoints/kafka_streaming_test/")
    .trigger(availableNow=True)
    .toTable("finguard_dev.bronze.transactions_streaming_test")
)
streaming_query.awaitTermination()
display(spark.table("finguard_dev.bronze.transactions_streaming_test"))

# COMMAND ----------

# Clean up the test table
# spark.sql("DROP TABLE IF EXISTS finguard_dev.bronze.transactions_streaming_test")
