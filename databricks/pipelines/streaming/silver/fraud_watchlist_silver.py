from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from finguard_lib import schemas, transforms


@dp.table(name=schemas.SILVER_WATCHLIST, comment="Cleaned fraud watchlist")
def fraud_watchlist_silver() -> DataFrame:
    return transforms.clean_watchlist(spark.readStream.table(schemas.BRONZE_WATCHLIST))
