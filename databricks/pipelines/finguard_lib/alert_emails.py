"""Alert emails: HTML bodies and sending over SMTP (Gmail). Pure Python, no Spark: unit-testable."""
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_email(to_email, subject, body, from_email, app_password):
    """Sends an email using Gmail SMTP server."""
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
    
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(from_email, app_password)
        server.send_message(msg)


def high_value_alert_email_body(alert_data):
    """Creates the HTML email body for a high-value transaction alert."""
    return f"""<html><body style="font-family: Arial, sans-serif;">
<h2 style="color: #d9534f;">⚠️ High Value Transaction Alert</h2>
<p>Dear {alert_data['customer_name']},</p>
<p>We detected a transaction that exceeded your configured limit:</p>
<div style="background-color: #f8f9fa; padding: 15px; border-left: 4px solid #d9534f; margin: 20px 0;">
<h3 style="margin-top: 0;">Transaction Details</h3>
<ul style="list-style-type: none; padding-left: 0;">
<li><strong>Transaction ID:</strong> {alert_data['transaction_id']}</li>
<li><strong>Amount:</strong> {alert_data['currency']} {alert_data['transaction_amount']:,.2f}</li>
<li><strong>Your Limit:</strong> {alert_data['currency']} {alert_data['transaction_limit']:,.2f}</li>
<li><strong>Merchant:</strong> {alert_data['merchant_name']} ({alert_data['merchant_category']})</li>
<li><strong>Location:</strong> {alert_data['city']}, {alert_data['country']}</li>
<li><strong>Date/Time:</strong> {alert_data['transaction_timestamp']}</li>
</ul></div>
<p><strong>Action Required:</strong></p>
<ul>
<li>If you authorized this transaction, no action is needed.</li>
<li>If you did NOT authorize this, contact our fraud department immediately.</li>
</ul>
<p>For your security, we're monitoring your account closely.</p>
<hr style="margin: 20px 0; border: none; border-top: 1px solid #ddd;">
<p style="font-size: 12px; color: #6c757d;">
Alert ID: {alert_data['alert_id']}<br>
This is an automated message from FinGuard Transaction Monitoring System.
</p>
</body></html>"""


def fraud_card_alert_email_body(alert_data):
    """Creates the HTML email body for a fraud card alert."""
    return f"""<html><body style="font-family: Arial, sans-serif;">
<h2 style="color: #dc3545;">🚨 FRAUD ALERT - Suspicious Activity Detected</h2>
<p>Dear {alert_data['customer_name']},</p>
<p><strong style="color: #dc3545;">URGENT:</strong> We detected suspicious activity on your card that matches our fraud watchlist:</p>
<div style="background-color: #fff3cd; padding: 15px; border-left: 4px solid #dc3545; margin: 20px 0;">
<h3 style="margin-top: 0; color: #dc3545;">⚠️ Fraud Details</h3>
<ul style="list-style-type: none; padding-left: 0;">
<li><strong>Alert Type:</strong> {alert_data['alert_type']}</li>
<li><strong>Risk Level:</strong> <span style="color: #dc3545; font-weight: bold;">{alert_data['risk_level']}</span></li>
<li><strong>Watchlist Type:</strong> {alert_data['watch_type']}</li>
<li><strong>Reason:</strong> {alert_data['reason_description']}</li>
<li><strong>Action Required:</strong> <span style="color: #dc3545; font-weight: bold;">{alert_data['action']}</span></li>
</ul></div>
<div style="background-color: #f8f9fa; padding: 15px; border-left: 4px solid #6c757d; margin: 20px 0;">
<h3 style="margin-top: 0;">Transaction Details</h3>
<ul style="list-style-type: none; padding-left: 0;">
<li><strong>Transaction ID:</strong> {alert_data['transaction_id']}</li>
<li><strong>Card Number:</strong> ****{alert_data['card_number_masked']}</li>
<li><strong>Amount:</strong> {alert_data['currency']} {alert_data['amount']:,.2f}</li>
<li><strong>Merchant:</strong> {alert_data['merchant_name']} ({alert_data['merchant_category']})</li>
<li><strong>Transaction Type:</strong> {alert_data['transaction_type']}</li>
<li><strong>Payment Channel:</strong> {alert_data['payment_channel']}</li>
<li><strong>Location:</strong> {alert_data['transaction_city']}, {alert_data['transaction_country']}</li>
<li><strong>International:</strong> {'Yes' if alert_data['is_international'] else 'No'}</li>
<li><strong>Date/Time:</strong> {alert_data['transaction_timestamp']}</li>
<li><strong>Status:</strong> {alert_data['transaction_status']}</li>
</ul></div>
<div style="background-color: #f8d7da; padding: 15px; border-left: 4px solid #dc3545; margin: 20px 0;">
<h3 style="margin-top: 0; color: #721c24;">🔒 IMMEDIATE ACTION REQUIRED</h3>
<ol style="color: #721c24;">
<li><strong>If you DID NOT authorize this transaction:</strong>
<ul>
<li>Call our fraud hotline immediately: 1-800-FRAUD-HELP</li>
<li>Your card has been flagged for security review</li>
<li>Do not use this card until you speak with our fraud team</li>
</ul>
</li>
<li><strong>If you DID authorize this transaction:</strong>
<ul>
<li>Reply to this email or call us to confirm</li>
<li>Be prepared to verify your identity</li>
</ul>
</li>
</ol>
</div>
<div style="background-color: #e7f3ff; padding: 15px; border-left: 4px solid #0d6efd; margin: 20px 0;">
<h3 style="margin-top: 0; color: #084298;">Watchlist Information</h3>
<ul style="list-style-type: none; padding-left: 0;">
<li><strong>Watchlist ID:</strong> {alert_data['watchlist_id']}</li>
<li><strong>Reported By:</strong> {alert_data['reported_by']}</li>
<li><strong>Reported Source:</strong> {alert_data['reported_source']}</li>
<li><strong>Watchlist Location:</strong> {alert_data['watchlist_city']}, {alert_data['watchlist_country']}</li>
<li><strong>Effective From:</strong> {alert_data['watchlist_effective_from']}</li>
</ul>
</div>
<p style="color: #dc3545; font-weight: bold;">Your account security is our top priority. We're monitoring your account closely.</p>
<hr style="margin: 20px 0; border: none; border-top: 1px solid #ddd;">
<p style="font-size: 12px; color: #6c757d;">
Alert ID: {alert_data['alert_id']}<br>
Alert Timestamp: {alert_data['alert_timestamp']}<br>
This is an automated fraud alert from FinGuard Transaction Monitoring System.<br>
<strong>Never share this email or your card details with anyone.</strong>
</p>
</body></html>"""


def mask_card(card_number):
    """Last 4 digits only: the full card number never goes into an email."""
    return str(card_number)[-4:] if card_number else "****"


def high_value_alert_data(row):
    """Fields for the high-value email body, from a gold.high_value_transactions_alert row (as a dict)."""
    return {
        "alert_id": row["alert_id"],
        "customer_name": row["customer_name"],
        "transaction_id": row["transaction_id"],
        "transaction_amount": float(row["transaction_amount"]),
        "currency": row["currency"],
        "transaction_limit": float(row["transaction_limit"]),
        "merchant_name": row["merchant_name"],
        "merchant_category": row["merchant_category"],
        "transaction_timestamp": str(row["transaction_timestamp"]),
        "city": row["city"],
        "country": row["country"],
    }


def fraud_card_alert_data(row):
    """Fields for the fraud-card email body, from a gold.fraud_card_alert row (as a dict)."""
    return {
        "alert_id": row["alert_id"],
        "alert_type": row["alert_type"],
        "alert_timestamp": str(row["alert_timestamp"]),
        "customer_name": row["customer_name"],
        "transaction_id": row["transaction_id"],
        "card_number_masked": mask_card(row["card_number"]),
        "amount": float(row["amount"]),
        "currency": row["currency"],
        "merchant_name": row["merchant_name"],
        "merchant_category": row["merchant_category"],
        "transaction_type": row["transaction_type"],
        "payment_channel": row["payment_channel"],
        "transaction_city": row["transaction_city"],
        "transaction_country": row["transaction_country"],
        "transaction_timestamp": str(row["transaction_timestamp"]),
        "is_international": row["is_international"],
        "transaction_status": row["transaction_status"],
        "watchlist_id": row["watchlist_id"],
        "watch_type": row["watch_type"],
        "risk_level": row["risk_level"],
        "action": row["action"],
        "reason_description": row["reason_description"],
        "reported_by": row["reported_by"],
        "reported_source": row["reported_source"],
        "watchlist_city": row["watchlist_city"] or "N/A",
        "watchlist_country": row["watchlist_country"] or "N/A",
        "watchlist_effective_from": str(row["watchlist_effective_from"]),
    }
