from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from finguard_lib import schemas, transforms


@dp.table(name=schemas.SILVER_CUSTOMERS, comment="Parsed and cleaned customer data")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def customers_silver() -> DataFrame:
    return transforms.clean_customers(spark.readStream.table(schemas.BRONZE_CUSTOMERS)) 
