from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from finguard_lib import schemas, transforms


@dp.table(name=schemas.SILVER_TRANSACTIONS, comment="Parsed and cleaned transactions")
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number", "card_number IS NOT NULL")
@dp.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
@dp.expect("valid_amount", "amount > 0")   
def transactions_silver() -> DataFrame:
    return transforms.parse_transactions(spark.readStream.table(schemas.BRONZE_TRANSACTIONS))
