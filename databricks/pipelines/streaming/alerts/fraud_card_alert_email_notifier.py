"""
Email sink for fraud-watchlist alerts (foreachBatch).

Settings:
  - sender address: pipeline configuration key `finguard.email_from` (pipeline settings → Configuration)
  - Gmail app password: secret `finguard-scope/gmail_api_key`
"""
from pyspark import pipelines as dp

from finguard_lib import alert_emails, schemas

EMAIL_FROM = spark.conf.get("finguard.email_from", None) 
try:
    APP_PASSWORD = dbutils.secrets.get(schemas.SECRET_SCOPE, "gmail_api_key") 
except Exception as e:
    print(f"Failed to retrieve Gmail app password from secrets: {e}")
    APP_PASSWORD = None


@dp.foreach_batch_sink(name="fraud_email_notifier_sink")
def send_fraud_alert_emails(df, batch_id):
    """Send one email per fraud-watchlist alert in the micro-batch."""
    if not EMAIL_FROM or APP_PASSWORD is None:
        print(f"Batch {batch_id}: finguard.email_from or gmail_api_key not set, skipping emails")
        return

    rows = df.collect()
    print(f"Batch {batch_id}: processing {len(rows)} fraud alert(s)")
    sent = failed = 0
    for row in rows:
        try:
            data = alert_emails.fraud_card_alert_data(row.asDict())
            subject = f"FRAUD ALERT - {data['risk_level']} Risk - {data['alert_id']}"
            alert_emails.send_email(row.customer_email, subject, alert_emails.fraud_card_alert_email_body(data),
                                    EMAIL_FROM, APP_PASSWORD)
            sent += 1
        except Exception as e:
            failed += 1
            print(f"  error on alert {row.alert_id}: {e}")
    print(f"Batch {batch_id}: {sent} sent, {failed} failed")


@dp.append_flow(target="fraud_email_notifier_sink")
def fraud_card_alert_stream():
    return spark.readStream.table(schemas.GOLD_FRAUD_CARD_ALERTS)
