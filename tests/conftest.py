from pyspark import SparkContext
from pyspark.sql import SQLContext
import pytest
from app.dependencies.spark import start_spark


@pytest.fixture(scope='session')
def spark():
    spark, log = start_spark(app_name='testing')
    yield spark
    spark.stop()
