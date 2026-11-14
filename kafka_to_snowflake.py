"""
kafka_to_snowflake.py

PySpark Structured Streaming job: reads JSON events from a Kafka topic
and loads them into a Snowflake table (micro-batches via foreachBatch).

Run (example):
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4,net.snowflake:snowflake-jdbc:3.16.1 \
    kafka_to_snowflake.py

All connection settings come from environment variables (never hard-code secrets).
"""

import os
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("kafka_to_snowflake")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
def env(name: str, default: str | None = None, required: bool = True) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP_SERVERS")            # e.g. broker1:9092,broker2:9092
KAFKA_TOPIC = env("KAFKA_TOPIC")                            # e.g. orders
KAFKA_STARTING_OFFSETS = env("KAFKA_STARTING_OFFSETS", "latest")
KAFKA_SECURITY_PROTOCOL = env("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT", required=False)
KAFKA_SASL_MECHANISM = env("KAFKA_SASL_MECHANISM", "", required=False)
KAFKA_SASL_JAAS = env("KAFKA_SASL_JAAS_CONFIG", "", required=False)

SF_OPTIONS = {
    "sfURL": env("SNOWFLAKE_URL"),                          # <account>.snowflakecomputing.com
    "sfUser": env("SNOWFLAKE_USER"),
    "sfPassword": env("SNOWFLAKE_PASSWORD"),
    "sfDatabase": env("SNOWFLAKE_DATABASE"),
    "sfSchema": env("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": env("SNOWFLAKE_WAREHOUSE"),
    "sfRole": env("SNOWFLAKE_ROLE", "", required=False),
}
SF_TABLE = env("SNOWFLAKE_TABLE")                           # e.g. RAW_ORDERS

CHECKPOINT_DIR = env("CHECKPOINT_DIR", "/tmp/checkpoints/kafka_to_snowflake")
TRIGGER_INTERVAL = env("TRIGGER_INTERVAL", "1 minute")
SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"

# Schema of the JSON payload in the Kafka message value -- adjust to your data.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_time", TimestampType(), True),
])


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("kafka-to-snowflake")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka(spark: SparkSession) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL)
    )
    if KAFKA_SASL_MECHANISM:
        reader = reader.option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
    if KAFKA_SASL_JAAS:
        reader = reader.option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS)
    return reader.load()


def transform(raw: DataFrame) -> DataFrame:
    """Parse the Kafka value as JSON and add Kafka metadata columns."""
    parsed = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    return (
        parsed
        .filter(F.col("data").isNotNull())        # drop malformed JSON
        .select("data.*", "kafka_key", "kafka_topic", "kafka_partition",
                "kafka_offset", "kafka_timestamp")
        .withColumn("load_ts", F.current_timestamp())
    )


def write_to_snowflake(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink: append each micro-batch to Snowflake."""
    if batch_df.rdd.isEmpty():
        log.info("Batch %s empty, skipping", batch_id)
        return
    # Snowflake identifiers default to upper case
    out = batch_df.toDF(*[c.upper() for c in batch_df.columns])
    options = {k: v for k, v in SF_OPTIONS.items() if v}
    (
        out.write.format(SNOWFLAKE_SOURCE)
        .options(**options)
        .option("dbtable", SF_TABLE)
        .mode("append")
        .save()
    )
    log.info("Batch %s written to %s (%d rows)", batch_id, SF_TABLE, out.count())


def main() -> None:
    spark = build_spark()
    events = transform(read_kafka(spark))

    query = (
        events.writeStream
        .foreachBatch(write_to_snowflake)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    log.info("Streaming %s -> Snowflake %s", KAFKA_TOPIC, SF_TABLE)
    query.awaitTermination()


if __name__ == "__main__":
    main()
