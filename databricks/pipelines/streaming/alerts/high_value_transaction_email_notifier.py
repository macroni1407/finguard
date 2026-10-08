"""
Email sink for high-value alerts (foreachBatch).

Settings:
  - sender address: pipeline configuration key `finguard.email_from` (pipeline settings → Configuration)
  - Gmail app password: secret `finguard-scope/gmail_api_key`
"""
from pyspark import pipelines as dp

from finguard_lib import alert_emails, schemas

# Read configuration outside the sink function so it is not re-read per batch
EMAIL_FROM = spark.conf.get("finguard.email_from", None)
try:
    APP_PASSWORD = dbutils.secrets.get(schemas.SECRET_SCOPE, "gmail_api_key")
except Exception as e:
    print(f"Failed to retrieve Gmail app password from secrets: {e}")
    APP_PASSWORD = None


@dp.foreach_batch_sink(name="email_notifier_sink")
def send_high_value_alert_emails(df, batch_id):
    """Send one email per high-value alert in the micro-batch."""
    if not EMAIL_FROM or APP_PASSWORD is None:
        print(f"Batch {batch_id}: finguard.email_from or gmail_api_key not set, skipping emails")
        return

    rows = df.collect()
    print(f"Batch {batch_id}: processing {len(rows)} alert(s)")
    sent = failed = 0
    for row in rows:
        try:
            data = alert_emails.high_value_alert_data(row.asDict())
            subject = f"High Value Transaction Alert - {data['alert_id']}"
            alert_emails.send_email(row.customer_email, subject, alert_emails.high_value_alert_email_body(data),
                                    EMAIL_FROM, APP_PASSWORD)
            sent += 1
        except Exception as e:
            failed += 1
            print(f"  error on alert {row.alert_id}: {e}")
    print(f"Batch {batch_id}: {sent} sent, {failed} failed")


@dp.append_flow(target="email_notifier_sink")
def high_value_alert_stream():
    return spark.readStream.table(schemas.GOLD_HIGH_VALUE_ALERTS)
