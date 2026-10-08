"""
Transformation logic as plain PySpark functions: DataFrame in, DataFrame out.

The pipeline files (decorated with @dp.table) only read their inputs and call these functions,
so the logic can be unit-tested with a local SparkSession (tests/) and reused outside Lakeflow.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .schemas import CUSTOMER_COLUMNS, TRANSACTION_SCHEMA, WATCHLIST_COLUMNS

WATERMARK_DELAY = "5 minutes"


# ---------- Kafka ----------

def kafka_read_options(config: dict) -> dict:
    """Spark Kafka source options for Confluent Cloud (SASL_SSL / PLAIN) from the secret JSON."""
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{config["api_key"]}" password="{config["api_secret"]}";'
    )
    return {
        "kafka.bootstrap.servers": config["bootstrap_servers"],
        "subscribe": config["topic"],
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas,
        "startingOffsets": "earliest",
    }


def kafka_to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Keep the raw message (key, value as strings) and its Kafka metadata."""
    return kafka_df.select(
        F.col("key").cast("string"),
        F.col("value").cast("string"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp"),
        F.col("timestampType"),
        F.current_timestamp().alias("ingestion_timestamp"),
    )


# ---------- Transactions ----------

def parse_transactions(bronze_df: DataFrame) -> DataFrame:
    """Parse the JSON value into columns; keep Kafka lineage columns."""
    return bronze_df.select(
        F.from_json(F.col("value"), TRANSACTION_SCHEMA).alias("data"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
    ).select(
        F.col("data.*"),
        "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "bronze_ingestion_timestamp",
        F.current_timestamp().alias("silver_ingestion_timestamp"),
    )


# ---------- Watchlist ----------

def watchlist_to_bronze(files_df: DataFrame) -> DataFrame:
    """Auto Loader output: watchlist columns, rescued data and source file."""
    return files_df.select(
        *[F.col(c) for c in WATCHLIST_COLUMNS],
        F.col("_rescued_data"),
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("ingestion_timestamp"),
    )


def clean_watchlist(bronze_df: DataFrame) -> DataFrame:
    """Upper-case identifiers and categorical values, parse effective_from."""
    return bronze_df.select(
        F.upper("watchlist_id").alias("watchlist_id"),
        F.col("watch_type"),
        F.upper("entity_id").alias("entity_id"),
        F.upper("risk_level").alias("risk_level"),
        F.upper("action").alias("action"),
        F.col("reason_code"),
        F.col("reason_description"),
        F.col("status"),
        F.to_timestamp("effective_from", "dd-MMM-yyyy HH:mm:ss").alias("effective_from"),
        F.col("reported_by"),
        F.col("reported_source"),
        F.col("country"),
        F.col("city"),
        F.col("source_file"),
        F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        F.current_timestamp().alias("silver_ingestion_timestamp"),
    )


# ---------- Customers ----------

def clean_customers(bronze_df: DataFrame) -> DataFrame:
    """Select customer columns, parse account_open_date."""
    columns = [
        F.to_date("account_open_date", "yyyy-MM-dd").alias("account_open_date")
        if c == "account_open_date" else F.col(c)
        for c in CUSTOMER_COLUMNS
    ]
    return bronze_df.select(*columns, F.current_timestamp().alias("silver_ingestion_timestamp"))


# ---------- Alerts ----------

def high_value_alerts(transactions: DataFrame, customers: DataFrame) -> DataFrame:
    """Stream-static join: transactions above the customer's transaction_limit."""
    t, c = transactions.alias("t"), customers.alias("c")
    return (
        t.join(c, F.col("t.customer_id") == F.col("c.customer_id"), "left")
        .filter(F.col("t.amount") > F.col("c.transaction_limit"))
        .select(
            F.concat_ws("-", F.lit("ALERT"), F.col("t.transaction_id")).alias("alert_id"),
            F.lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
            F.current_timestamp().alias("alert_timestamp"),
            F.col("t.transaction_id"),
            F.col("t.customer_id"),
            F.col("c.email").alias("customer_email"),
            F.concat_ws(" ", F.col("c.first_name"), F.col("c.last_name")).alias("customer_name"),
            F.col("t.amount").alias("transaction_amount"),
            F.col("c.transaction_limit"),
            F.col("t.currency"),
            F.col("t.merchant_name"),
            F.col("t.merchant_category"),
            F.col("t.transaction_type"),
            F.col("t.payment_channel"),
            F.col("t.city"),
            F.col("t.country"),
            F.col("t.is_international"),
            F.col("t.transaction_timestamp"),
            F.col("t.status"),
        )
    )


def fraud_card_alerts(transactions: DataFrame, watchlist: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Transactions whose card is on the fraud watchlist.
    """
    t = transactions.withWatermark("transaction_timestamp", WATERMARK_DELAY).alias("t")
    w = watchlist.withWatermark("effective_from", WATERMARK_DELAY).alias("w")
    c = customers.alias("c")
    return (
        t.join(w, F.col("t.card_number") == F.col("w.entity_id"), "inner")
        .join(c, F.col("t.customer_id") == F.col("c.customer_id"), "left")
        .select(
            F.concat_ws("-", F.lit("FRAUD"), F.col("t.transaction_id"), F.col("w.watchlist_id")).alias("alert_id"),
            F.lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
            F.current_timestamp().alias("alert_timestamp"),
            F.col("t.transaction_id"),
            F.col("t.customer_id"),
            F.col("c.email").alias("customer_email"),
            F.concat_ws(" ", F.col("c.first_name"), F.col("c.last_name")).alias("customer_name"),
            F.col("t.card_number"),
            F.col("t.amount"),
            F.col("t.currency"),
            F.col("t.merchant_id"),
            F.col("t.merchant_name"),
            F.col("t.merchant_category"),
            F.col("t.transaction_type"),
            F.col("t.payment_channel"),
            F.col("t.device_id"),
            F.col("t.city").alias("transaction_city"),
            F.col("t.country").alias("transaction_country"),
            F.col("t.transaction_timestamp"),
            F.col("t.is_international"),
            F.col("t.status").alias("transaction_status"),
            F.col("w.watchlist_id"),
            F.col("w.watch_type"),
            F.col("w.risk_level"),
            F.col("w.action"),
            F.col("w.reason_code"),
            F.col("w.reason_description"),
            F.col("w.effective_from").alias("watchlist_effective_from"),
            F.col("w.reported_by"),
            F.col("w.reported_source"),
            F.col("w.city").alias("watchlist_city"),
            F.col("w.country").alias("watchlist_country"),
        )
    )


# ---------- Aggregations ----------

def transaction_counts(transactions: DataFrame, window_duration: str, slide_duration: str = None) -> DataFrame:
    """Transactions per event-time window: tumbling if slide_duration is None, sliding otherwise."""
    window = (F.window("transaction_timestamp", window_duration, slide_duration) if slide_duration
              else F.window("transaction_timestamp", window_duration))
    return (
        transactions.withWatermark("transaction_timestamp", WATERMARK_DELAY)
        .groupBy(window)
        .agg(F.count("*").alias("transaction_count"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("transaction_count"),
        )
    )
