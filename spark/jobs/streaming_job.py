from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, to_timestamp, trim
from pyspark.sql.types import LongType, StringType, StructField, StructType

from config.settings import settings

# Keep the Kafka payload schema explicit so Spark can parse and validate
# the incoming JSON without relying on schema inference.
station_status_schema = StructType(
    [
        StructField("station_id", StringType(), True),
        StructField("num_bikes_available", LongType(), True),
        StructField("num_bikes_disabled", LongType(), True),
        StructField("num_docks_available", LongType(), True),
        StructField("num_docks_disabled", LongType(), True),
        StructField("is_installed", LongType(), True),
        StructField("is_renting", LongType(), True),
        StructField("is_returning", LongType(), True),
        StructField("last_reported", LongType(), True),
        StructField("snapshot_id", StringType(), True),
        StructField("poll_time", StringType(), True),
        StructField("feed_last_updated", LongType(), True),
        StructField("feed_ttl", LongType(), True),
    ]
)


spark = SparkSession.builder.appName(settings.spark_app_name).getOrCreate()


kafka_events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
    .option("subscribe", settings.kafka_station_status_topic)
    .option("startingOffsets", "earliest")
    .load()
)


# Parse the JSON payload and keep Kafka metadata for traceability
# and downstream technical deduplication.
parsed_events = kafka_events.select(
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
    from_json(
        col("value").cast("string"),
        station_status_schema,
    ).alias("station"),
).select(
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "station.*",
)


# Only apply basic structural checks here.
# Business rules and deduplication are handled downstream in dbt.
structured_events = parsed_events.filter(
    col("station_id").isNotNull()
    & (trim(col("station_id")) != "")
    & col("last_reported").isNotNull()
    & col("num_bikes_available").isNotNull()
    & col("num_docks_available").isNotNull()
    & col("is_installed").isNotNull()
    & col("is_renting").isNotNull()
    & col("is_returning").isNotNull()
)


def write_raw_batch(batch_df, _batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    # Convert the producer timestamp to a warehouse timestamp
    # and record when Spark writes the batch to BigQuery.
    raw_batch = batch_df.withColumn(
        "poll_time",
        to_timestamp("poll_time"),
    ).withColumn(
        "ingested_at",
        current_timestamp(),
    )

    (
        raw_batch.write.format("bigquery")
        .option(
            "table",
            f"{settings.gcp_project_id}."
            f"{settings.bq_raw_dataset}."
            "station_status",
        )
        .option("temporaryGcsBucket", settings.gcs_temp_bucket)
        .mode("append")
        .save()
    )


# foreachBatch lets each Spark micro-batch be written to BigQuery
# while the checkpoint keeps track of streaming progress.
stream_query = (
    structured_events.writeStream.foreachBatch(write_raw_batch)
    .option(
        "checkpointLocation",
        settings.spark_checkpoint_path,
    )
    .start()
)

stream_query.awaitTermination()
