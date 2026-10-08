from datetime import datetime

from finguard_lib import alert_emails

FRAUD_ROW = {
    "alert_id": "FRAUD-TXN1-WL000001", "alert_type": "FRAUD_WATCHLIST_MATCH", "alert_timestamp": datetime(2026, 10, 1),
    "customer_name": "Asha Rao", "transaction_id": "TXN1", "card_number": "5008514036965665", "amount": 1234.5,
    "currency": "INR", "merchant_name": "Shop", "merchant_category": "grocery", "transaction_type": "PURCHASE",
    "payment_channel": "ONLINE", "transaction_city": "Mumbai", "transaction_country": "India",
    "transaction_timestamp": datetime(2026, 10, 1), "is_international": False, "transaction_status": "APPROVED",
    "watchlist_id": "WL000001", "watch_type": "CARD", "risk_level": "HIGH", "action": "ADD",
    "reason_description": "phishing", "reported_by": "SOC", "reported_source": "Manual",
    "watchlist_city": None, "watchlist_country": "India", "watchlist_effective_from": datetime(2026, 6, 18, 9),
}


def test_mask_card_keeps_last_four():
    assert alert_emails.mask_card("5008514036965665") == "5665"
    assert alert_emails.mask_card(None) == "****"


def test_fraud_email_never_contains_full_card_number():
    data = alert_emails.fraud_card_alert_data(FRAUD_ROW)
    body = alert_emails.fraud_card_alert_email_body(data)
    assert "5008514036965665" not in body
    assert "****5665" in body
    assert data["watchlist_city"] == "N/A"


def test_high_value_email_body_shows_amount_and_limit():
    row = {"alert_id": "ALERT-TXN1", "customer_name": "Asha Rao", "transaction_id": "TXN1",
           "transaction_amount": 150000.0, "currency": "INR", "transaction_limit": 100000.0,
           "merchant_name": "Shop", "merchant_category": "grocery",
           "transaction_timestamp": datetime(2026, 10, 1), "city": "Mumbai", "country": "India"}
    body = alert_emails.high_value_alert_email_body(alert_emails.high_value_alert_data(row))
    assert "ALERT-TXN1" in body and "Asha Rao" in body
