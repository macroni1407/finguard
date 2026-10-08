# Transaction producer

Simulates credit card transactions and publishes them as JSON to a Kafka topic (Confluent Cloud).

## Setup
```bash
cd producer
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # fill in BOOTSTRAP_SERVERS, API_KEY, API_SECRET
```
Keep `TOTAL_CUSTOMERS=1000` and `RANDOM_SEED=42`: `sql/postgres/customers_historic.sql` contains exactly
the customers generated with these values (same IDs and card numbers), so transactions join to customers.

## Scripts
| Script | What it sends | Use |
|---|---|---|
| `producer_normal.py` | Normal transactions, continuously, `TRANSACTIONS_PER_SECOND` per second, amount 1–99,999.99 | Background traffic. Never exceeds the 100,000 transaction limit of the sample customers |
| `producer_fraud_transaction.py` | **One** transaction with amount ≥ 100,001 | Triggers a **high-value** alert |
| `producer_fraud_card.py` | **One** transaction with card `5008514036965665` (watchlist entry `wl000001`) | Triggers a **watchlist** alert (the watchlist file must already be ingested) |
| `consumer.py` | Prints messages from the topic | Check that messages arrive |

```bash
python producer_normal.py            # Ctrl+C to stop
python producer_fraud_transaction.py
python producer_fraud_card.py
python consumer.py
```

## Modules
| File | Role |
|---|---|
| `config.py` | Settings from `.env`, validated |
| `models.py` | Dataclasses: customer, merchant, transaction |
| `customer_generator.py`, `merchant_generator.py` | Deterministic customers and merchants (seeded), written to `data/*.csv` |
| `transaction_generator.py` | Transactions consistent with each customer's profile |
| `fraud_engine.py` | Rule-based fraud score and reason (high value, impossible travel, new device, risky/blacklisted merchant, international, velocity, card testing) |
| `utils.py` | Helpers (IDs, JSON validation) |

