from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from finguard_lib import schemas, transforms


@dp.table(
    name=schemas.GOLD_HIGH_VALUE_ALERTS,
    comment="Transactions above the customer's configured transaction limit (stream-static join)",
)
def high_value_transactions_alert() -> DataFrame:
    transactions = spark.readStream.table(schemas.SILVER_TRANSACTIONS)
    customers = spark.read.table(schemas.SILVER_CUSTOMERS)
    return transforms.high_value_alerts(transactions, customers)
