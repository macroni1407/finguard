import json
from datetime import date, datetime

import pytest
from pyspark.sql import Row

from finguard_lib import transforms

TXN = {
    "transaction_id": "TXN000001", "customer_id": "CUST000001", "card_number": "5008514036965665",
    "merchant_id": "MER0001", "merchant_name": "Shop", "merchant_category": "grocery",
    "amount": 150000.0, "currency": "INR", "transaction_type": "PURCHASE", "payment_channel": "ONLINE",
    "device_id": "DEV1", "city": "Mumbai", "country": "India",
    "transaction_timestamp": "2026-10-01T10:00:30", "is_international": False, "status": "APPROVED",
}


def bronze_row(payload, offset=0):
    return Row(key="k", value=payload if isinstance(payload, str) else json.dumps(payload),
               topic="credit_card_transactions", partition=0, offset=offset,
               timestamp=datetime(2026, 10, 1, 10, 0, 31), timestampType=0,
               ingestion_timestamp=datetime(2026, 10, 1, 10, 0, 32))


def transactions_df(spark, *payloads):
    return transforms.parse_transactions(spark.createDataFrame([bronze_row(p, i) for i, p in enumerate(payloads)]))


def test_kafka_read_options_use_secret_values():
    options = transforms.kafka_read_options(
        {"bootstrap_servers": "host:9092", "topic": "t", "api_key": "KEY", "api_secret": "SECRET"})
    assert options["kafka.bootstrap.servers"] == "host:9092"
    assert options["subscribe"] == "t"
    assert options["kafka.security.protocol"] == "SASL_SSL"
    assert 'username="KEY"' in options["kafka.sasl.jaas.config"]
    assert 'password="SECRET"' in options["kafka.sasl.jaas.config"]


def test_parse_transactions_extracts_fields_and_lineage(spark):
    row = transactions_df(spark, TXN).collect()[0]
    assert row.transaction_id == "TXN000001"
    assert row.amount == 150000.0
    assert row.transaction_timestamp == datetime(2026, 10, 1, 10, 0, 30)
    assert row.is_international is False
    assert row.kafka_topic == "credit_card_transactions" and row.kafka_offset == 0


def test_parse_transactions_bad_json_gives_null_fields(spark):
    row = transactions_df(spark, "not json").collect()[0]
    assert row.transaction_id is None   # dropped later by the silver expectations


def watchlist_bronze(spark, **overrides):
    record = {"watchlist_id": "wl000001", "watch_type": "CARD", "entity_id": "5008514036965665",
              "risk_level": "high", "action": "add", "reason_code": "PHISHING", "reason_description": "x",
              "status": "ACTIVE", "effective_from": "18-Jun-2026 09:00:00", "reported_by": "SOC",
              "reported_source": "Manual", "country": "India", "city": "Hyderabad",
              "_rescued_data": None, "_metadata": Row(file_path="/Volumes/x/f1.json")}
    record.update(overrides)
    schema = ("watchlist_id string, watch_type string, entity_id string, risk_level string, action string, "
              "reason_code string, reason_description string, status string, effective_from string, "
              "reported_by string, reported_source string, country string, city string, "
              "_rescued_data string, _metadata struct<file_path:string>")
    return transforms.watchlist_to_bronze(spark.createDataFrame([record], schema))


def test_watchlist_bronze_keeps_source_file(spark):
    row = watchlist_bronze(spark).collect()[0]
    assert row.source_file == "/Volumes/x/f1.json"


def test_clean_watchlist_normalises_case_and_parses_date(spark):
    row = transforms.clean_watchlist(watchlist_bronze(spark)).collect()[0]
    assert row.watchlist_id == "WL000001"
    assert row.risk_level == "HIGH" and row.action == "ADD"
    assert row.effective_from == datetime(2026, 6, 18, 9, 0, 0)


def customers_df(spark, limit=100000.0, customer_id="CUST000001"):
    from finguard_lib.schemas import CUSTOMER_COLUMNS
    values = {c: None for c in CUSTOMER_COLUMNS}
    values.update(customer_id=customer_id, first_name="Asha", last_name="Rao", email="alerts@example.com",
                  transaction_limit=limit, account_open_date="2025-10-27", card_number="5008514036965665")
    schema = ", ".join(f"{c} {'double' if c == 'transaction_limit' else 'string'}" for c in CUSTOMER_COLUMNS)
    return transforms.clean_customers(spark.createDataFrame([values], schema))


def test_clean_customers_parses_open_date(spark):
    row = customers_df(spark).collect()[0]
    assert row.account_open_date == date(2025, 10, 27)
    assert row.transaction_limit == 100000.0


def test_high_value_alert_only_above_limit(spark):
    small = dict(TXN, transaction_id="TXN000002", amount=500.0)
    alerts = transforms.high_value_alerts(transactions_df(spark, TXN, small), customers_df(spark)).collect()
    assert [a.transaction_id for a in alerts] == ["TXN000001"]
    alert = alerts[0]
    assert alert.alert_id == "ALERT-TXN000001" and alert.alert_type == "HIGH_VALUE_TRANSACTION"
    assert alert.customer_name == "Asha Rao" and alert.customer_email == "alerts@example.com"


def test_high_value_alert_unknown_customer_is_silently_dropped(spark):
    # Known tutorial issue (fixed in step 2.8): no customer -> limit is null -> no alert
    unknown = dict(TXN, customer_id="CUST999999")
    assert transforms.high_value_alerts(transactions_df(spark, unknown), customers_df(spark)).count() == 0


def test_fraud_card_alert_matches_watchlisted_card(spark):
    other = dict(TXN, transaction_id="TXN000003", card_number="4111111111111111")
    watchlist = transforms.clean_watchlist(watchlist_bronze(spark))
    alerts = transforms.fraud_card_alerts(transactions_df(spark, TXN, other), watchlist, customers_df(spark)).collect()
    assert [a.transaction_id for a in alerts] == ["TXN000001"]
    assert alerts[0].alert_id == "FRAUD-TXN000001-WL000001"
    assert alerts[0].risk_level == "HIGH" and alerts[0].customer_name == "Asha Rao"


@pytest.mark.parametrize("slide, expected_windows", [(None, 1), ("1 minute", 5)])
def test_transaction_counts_tumbling_and_sliding(spark, slide, expected_windows):
    txns = transactions_df(spark, TXN, dict(TXN, transaction_id="TXN000004", transaction_timestamp="2026-10-01T10:00:50"))
    duration = "1 minute" if slide is None else "5 minutes"
    rows = transforms.transaction_counts(txns, duration, slide).collect()
    assert len(rows) == expected_windows
    assert all(r.transaction_count == 2 for r in rows)
