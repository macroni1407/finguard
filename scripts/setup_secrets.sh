#!/usr/bin/env bash
# Create the Databricks secret scope used by the pipelines, with the Databricks CLI.
# Values are read from environment variables (e.g. `set -a; source producer/.env; set +a`),
# so no secret is ever written into a notebook or committed to git.
#
# Required:  BOOTSTRAP_SERVERS, API_KEY, API_SECRET          (Confluent Cloud)
# Optional:  TOPIC_NAME (default credit_card_transactions)
#            GMAIL_APP_PASSWORD                              (only for email alerts)
#            POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
#                                                            (only for databricks/notebooks/04_load_customers_jdbc.py)
#
# Usage:
#   databricks auth login --host https://<your-workspace>    # once
#   set -a; source producer/.env; set +a
#   export GMAIL_APP_PASSWORD=...                             # optional
#   ./scripts/setup_secrets.sh
set -euo pipefail

SCOPE="${SECRET_SCOPE:-finguard-scope}"

: "${BOOTSTRAP_SERVERS:?set BOOTSTRAP_SERVERS}"
: "${API_KEY:?set API_KEY}"
: "${API_SECRET:?set API_SECRET}"
TOPIC_NAME="${TOPIC_NAME:-credit_card_transactions}"

if databricks secrets list-scopes | grep -qw "$SCOPE"; then
  echo "Scope $SCOPE already exists"
else
  databricks secrets create-scope "$SCOPE"
  echo "Created scope $SCOPE"
fi

kafka_json=$(python3 -c 'import json, os; print(json.dumps({
    "bootstrap_servers": os.environ["BOOTSTRAP_SERVERS"],
    "topic": os.environ.get("TOPIC_NAME", "credit_card_transactions"),
    "api_key": os.environ["API_KEY"],
    "api_secret": os.environ["API_SECRET"]}))')
databricks secrets put-secret "$SCOPE" kafka_connection_details --string-value "$kafka_json"
echo "Stored kafka_connection_details"

if [[ -n "${GMAIL_APP_PASSWORD:-}" ]]; then
  databricks secrets put-secret "$SCOPE" gmail_api_key --string-value "$GMAIL_APP_PASSWORD"
  echo "Stored gmail_api_key"
fi

if [[ -n "${POSTGRES_HOST:-}" ]]; then
  pg_json=$(python3 -c 'import json, os; print(json.dumps({
      "host": os.environ["POSTGRES_HOST"], "port": os.environ.get("POSTGRES_PORT", "5432"),
      "database": os.environ["POSTGRES_DB"], "user": os.environ["POSTGRES_USER"],
      "password": os.environ["POSTGRES_PASSWORD"]}))')
  databricks secrets put-secret "$SCOPE" postgres_connection_details --string-value "$pg_json"
  echo "Stored postgres_connection_details"
fi

echo "Keys in $SCOPE:"
databricks secrets list-secrets "$SCOPE"
