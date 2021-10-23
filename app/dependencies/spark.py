"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""
import __main__

from os import environ
from pyspark.sql import SparkSession

from app.dependencies import logging


def start_spark(
        app_name='my_spark_app',
        master='local[*]',
        log_level='ERROR',
        jar_packages=[],
        spark_config={}):
    """Creates spark

    Args:
        app_name:
        master:
        log_level:
        jar_packages:
        spark_config:

    Returns:

    """

    # detect execution environment
    flag_repl = not(hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()

    if not (flag_repl or flag_debug):
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .appName(app_name))
    else:
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .master(master)
            .appName(app_name))

        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        # add other config params
        for key, val in spark_config.items():
            spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark_sess, level=log_level)
    spark_sess.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    return spark_sess, spark_logger
