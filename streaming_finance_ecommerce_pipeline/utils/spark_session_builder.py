"""
Spark Session Builder Utility

Provides a centralized, production-grade SparkSession factory configured
with Kafka streaming connectors, Snowflake JDBC/Spark plugins, Kryo serialization,
and Adaptive Query Execution (AQE).
"""

import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def create_spark_streaming_session(
    app_name: str = "Realtime_Streaming_Pipeline",
    spark_master: str = "local[*]",
    include_packages: bool = True
) -> SparkSession:
    """
    Constructs and returns an optimized SparkSession for Kafka and Snowflake streaming.

    Args:
        app_name: Name of the Spark application.
        spark_master: Master URL (defaults to 'local[*]' for local development).
        include_packages: Whether to attach Maven coordinates for Kafka and Snowflake jars.

    Returns:
        SparkSession instance.
    """
    builder = SparkSession.builder.appName(app_name)

    if spark_master:
        builder = builder.master(spark_master)

    # Core connector Maven coordinates:
    # 1. Apache Kafka streaming connector for Spark SQL
    # 2. Snowflake JDBC Driver
    # 3. Spark-Snowflake ecosystem connector
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "net.snowflake:snowflake-jdbc:3.14.0",
        "net.snowflake:spark-snowflake_2.12:2.13.0-spark_3.5"
    ]

    if include_packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    # Performance and Streaming Optimizations
    builder = (
        builder
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")  # Low partition count for lightweight local/dev streaming
    )

    logger.info(f"Initializing SparkSession: '{app_name}'")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"SparkSession created successfully. Spark Version: {spark.version}")
    return spark
