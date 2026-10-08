import json

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from finguard_lib import schemas, transforms


@dp.table(name=schemas.BRONZE_TRANSACTIONS, comment="Raw transaction messages from Kafka, with Kafka metadata")
def transactions_bronze() -> DataFrame:
    config = json.loads(dbutils.secrets.get(scope=schemas.SECRET_SCOPE, key="kafka_connection_details"))  
    kafka_df = spark.readStream.format("kafka").options(**transforms.kafka_read_options(config)).load()  
    return transforms.kafka_to_bronze(kafka_df)
