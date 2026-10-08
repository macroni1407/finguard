import os
import sys
import time
from pathlib import Path

import pytest

# finguard_lib lives in the pipelines' root folder
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "databricks" / "pipelines"))


@pytest.fixture(scope="session")
def spark():
    # Spark and Python must agree on the time zone, or collected timestamps shift by the local offset
    os.environ["TZ"] = "UTC"
    time.tzset()
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("finguard-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
