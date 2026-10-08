from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from finguard_lib import schemas, transforms


@dp.table(name=schemas.GOLD_COUNT_BY_MINUTE, comment="Transactions per 1-minute tumbling window")
def transaction_count_by_minute() -> DataFrame:
    return transforms.transaction_counts(spark.readStream.table(schemas.SILVER_TRANSACTIONS), "1 minute")
