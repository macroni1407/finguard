from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from finguard_lib import schemas, transforms


@dp.table(
    name=schemas.GOLD_FRAUD_CARD_ALERTS,
    comment="Transactions made with a card on the fraud watchlist",
)
def fraud_card_alert() -> DataFrame:
    transactions = spark.readStream.table(schemas.SILVER_TRANSACTIONS)
    watchlist = spark.readStream.table(schemas.SILVER_WATCHLIST)
    customers = spark.read.table(schemas.SILVER_CUSTOMERS)
    return transforms.fraud_card_alerts(transactions, watchlist, customers)
