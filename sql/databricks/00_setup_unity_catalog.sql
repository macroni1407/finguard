-- FinGuard: Unity Catalog objects. Run once in the Databricks SQL editor (or a notebook with %sql).
-- Tables themselves are created by the Lakeflow pipelines; this only creates their containers.

CREATE CATALOG IF NOT EXISTS finguard_dev;

CREATE SCHEMA IF NOT EXISTS finguard_dev.bronze COMMENT 'Raw data as ingested: Kafka messages, watchlist files, customers';
CREATE SCHEMA IF NOT EXISTS finguard_dev.silver COMMENT 'Parsed and cleaned data';
CREATE SCHEMA IF NOT EXISTS finguard_dev.gold   COMMENT 'Alerts and aggregates for consumption';
CREATE SCHEMA IF NOT EXISTS finguard_dev.source COMMENT 'Landing zone for files (UC volumes)';

-- Landing zone for fraud watchlist JSON files (Auto Loader reads source_data/)
CREATE VOLUME IF NOT EXISTS finguard_dev.source.fraud_watchlist
  COMMENT 'Fraud watchlist feed: one JSON file per watchlist change';
