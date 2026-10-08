from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from finguard_lib import schemas, transforms


@dp.table(name=schemas.BRONZE_WATCHLIST, comment="Fraud watchlist JSON files ingested by Auto Loader")
def fraud_watchlist_bronze() -> DataFrame:
    files_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(schemas.WATCHLIST_SOURCE_PATH)
    )
    return transforms.watchlist_to_bronze(files_df)
