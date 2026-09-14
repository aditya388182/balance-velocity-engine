from __future__ import annotations

from typing import Any, Dict, List

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# PySpark 3.5.1 bundles Hadoop 3.3.4, and hadoop-aws must match the bundled
# Hadoop exactly; aws-java-sdk-bundle 1.12.262 is the version hadoop-aws 3.3.4 was
# built against. Bumping one without the other yields NoClassDefFoundError.
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


def build_spark(cfg: Dict[str, Any], app_name: str = "balance-engine",
                streaming: bool = True) -> SparkSession:
    sp = cfg["spark"]
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(sp["master"])
        .config("spark.driver.memory", sp["driver_memory"])
        #  Delta 
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        #  S3A to MinIO 
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", cfg["s3"]["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", cfg["s3"]["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", cfg["s3"]["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        #  Arrow: applyInPandasWithState requires it 
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "2000")
        #  shuffle partitions 
        # The default of 200 means 200 RocksDB state store instances PER stateful
        # operator. On 16 GB laptop that is the difference between a 5-second
        # micro-batch and a job that never finishes its first batch.
        .config("spark.sql.shuffle.partitions", str(sp["shuffle_partitions"]))
        .config("spark.ui.showConsoleProgress", "false")
    )

    if streaming:
        builder = (
            builder
            .config("spark.sql.streaming.stateStore.providerClass",
                    "org.apache.spark.sql.execution.streaming.state."
                    "RocksDBStateStoreProvider")
            .config("spark.sql.streaming.stateStore.rocksdb.formatVersion", "5")
            .config("spark.sql.streaming.stateStore.rocksdb."
                    "changelogCheckpointing.enabled", "true")
            .config("spark.sql.streaming.stateStore.rocksdb.blockCacheSizeMB", "256")
            .config("spark.sql.streaming.stateStore.rocksdb.writeBufferSizeMB", "64")
            .config("spark.sql.streaming.stateStore.rocksdb.trackTotalNumberOfRows", "true")
            .config("spark.sql.streaming.metricsEnabled", "true")
        )

    packages: List[str] = [HADOOP_AWS, AWS_SDK] + ([KAFKA_PKG] if streaming else [])
    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def assert_state_store_configured(spark: SparkSession) -> None:
    provider = spark.conf.get("spark.sql.streaming.stateStore.providerClass", "")
    if "RocksDB" not in provider:
        raise RuntimeError(f"state store provider is not RocksDB: {provider!r}")
    changelog = spark.conf.get(
        "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "false")
    if changelog != "true":
        raise RuntimeError("RocksDB changelog checkpointing is not enabled")
    print(f"[session] state store OK: {provider.rsplit('.', 1)[-1]}, "
          f"changelog={changelog}, "
          f"shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')}, "
          f"arrow={spark.conf.get('spark.sql.execution.arrow.pyspark.enabled')}")
