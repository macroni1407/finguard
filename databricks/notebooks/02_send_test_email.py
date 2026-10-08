# Databricks notebook source
# DBTITLE 1,Create test email body
body = """
<html>
<body>

<h2>Test Email from Databricks</h2>

<p>This is a test email sent from Databricks notebook.</p>

<p>If you receive this, the email configuration is working correctly.</p>

<br>

<p>
Thanks<br>
<b>FinGuard Support Team</b>
</p>

</body>
</html>
"""

print(body)

# COMMAND ----------

# DBTITLE 1,Create test subject
subject = "Test Email from Databricks"

print(subject)

# COMMAND ----------

gmail_api_key = dbutils.secrets.get(
            scope="finguard-scope",
            key="gmail_api_key"
        )

# COMMAND ----------

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Gmail account that sends alerts (same value as the pipeline setting finguard.email_from)
dbutils.widgets.text("email_from", "", "Sender Gmail address")
dbutils.widgets.text("email_to", "", "Test recipient")
EMAIL = dbutils.widgets.get("email_from")
APP_PASSWORD = gmail_api_key

# Receiver
to_email = dbutils.widgets.get("email_to") or EMAIL
if not EMAIL:
    raise ValueError("Fill in the email_from widget at the top of the notebook")

# Create message
msg = MIMEMultipart()
msg["From"] = EMAIL
msg["To"] = to_email
msg["Subject"] = subject

msg.attach(MIMEText(body, "html"))

try:
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(EMAIL, APP_PASSWORD)
        server.send_message(msg)

    print("✅ Email sent successfully!")

except Exception as e:
    print("❌ Error sending email:", e)
