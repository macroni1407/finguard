from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from finguard_lib import schemas, transforms


@dp.table(
    name=schemas.GOLD_COUNT_SLIDING,
    comment="Transactions per 5-minute window, sliding every minute",
)
def transaction_count_by_minute_sliding_window() -> DataFrame:
    transactions = spark.readStream.table(schemas.SILVER_TRANSACTIONS)
    return transforms.transaction_counts(transactions, "5 minutes", "1 minute")
