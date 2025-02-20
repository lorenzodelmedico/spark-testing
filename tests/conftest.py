# tests/conftest.py
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("pytest-pyspark-testing")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()
