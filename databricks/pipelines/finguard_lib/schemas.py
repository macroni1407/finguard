"""Schemas and table names shared by the pipelines and the tests."""
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CATALOG = "finguard_dev"
SECRET_SCOPE = "finguard-scope"

# Fully qualified table names
BRONZE_TRANSACTIONS = f"{CATALOG}.bronze.transactions"
BRONZE_WATCHLIST = f"{CATALOG}.bronze.fraud_watchlist"
BRONZE_CUSTOMERS = f"{CATALOG}.bronze.customers"
SILVER_TRANSACTIONS = f"{CATALOG}.silver.transactions"
SILVER_WATCHLIST = f"{CATALOG}.silver.fraud_watchlist"
SILVER_CUSTOMERS = f"{CATALOG}.silver.customers"
GOLD_HIGH_VALUE_ALERTS = f"{CATALOG}.gold.high_value_transactions_alert"
GOLD_FRAUD_CARD_ALERTS = f"{CATALOG}.gold.fraud_card_alert"
GOLD_COUNT_BY_MINUTE = f"{CATALOG}.gold.transaction_count_by_minute"
GOLD_COUNT_SLIDING = f"{CATALOG}.gold.transaction_count_by_minute_sliding_window"

WATCHLIST_SOURCE_PATH = "/Volumes/finguard_dev/source/fraud_watchlist/source_data/"

# JSON payload sent by producer/*.py (fraud_score and fraud_reason are removed before sending)
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("card_number", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_name", StringType()),
    StructField("merchant_category", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("transaction_type", StringType()),
    StructField("payment_channel", StringType()),
    StructField("device_id", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("transaction_timestamp", TimestampType()),
    StructField("is_international", BooleanType()),
    StructField("status", StringType()),
])

WATCHLIST_COLUMNS = [
    "watchlist_id", "watch_type", "entity_id", "risk_level", "action", "reason_code",
    "reason_description", "status", "effective_from", "reported_by", "reported_source",
    "country", "city",
]

CUSTOMER_COLUMNS = [
    "customer_id", "first_name", "last_name", "gender", "age", "city", "state", "country",
    "annual_income", "customer_segment", "account_open_date", "risk_score",
    "preferred_spending_min", "preferred_spending_max", "preferred_city", "preferred_country",
    "trusted_device_id", "card_number", "card_type", "email", "transaction_limit",
]
