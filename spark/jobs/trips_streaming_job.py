from __future__ import annotations

import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, date_trunc, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "trips.raw")
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints/trips")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "trips")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

schema = StructType([
    StructField("city", StringType()),
    StructField("region", StringType()),
    StructField("origin_lat", DoubleType()),
    StructField("origin_lon", DoubleType()),
    StructField("destination_lat", DoubleType()),
    StructField("destination_lon", DoubleType()),
    StructField("trip_datetime", StringType()),
    StructField("time_of_day", StringType()),
    StructField("origin_cell", StringType()),
    StructField("destination_cell", StringType()),
    StructField("datasource", StringType()),
    StructField("ingestion_job_id", StringType()),
    StructField("event_hash", StringType()),
])


def upsert_batch(df, batch_id: int) -> None:
    if df.rdd.isEmpty():
        return

    props = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    clean_df = df.select(
        "city",
        "region",
        "origin_lat",
        "origin_lon",
        "destination_lat",
        "destination_lon",
        "origin_cell",
        "destination_cell",
        "trip_datetime",
        "time_of_day",
        "datasource",
        "event_hash",
        "ingestion_job_id",
    )

    clean_df.write.jdbc(url=JDBC_URL, table="trips_clean", mode="append", properties=props)

    grouped_df = (
        clean_df.groupBy("city", "region", "origin_cell", "destination_cell", "time_of_day")
        .agg(count("*").alias("trip_count"))
    )
    grouped_df.write.jdbc(url=JDBC_URL, table="trip_group_summary_staging", mode="overwrite", properties=props)

    weekly_df = (
        clean_df.withColumn("week_start", date_trunc("week", col("trip_datetime")))
        .groupBy("city", "region", "week_start")
        .agg(count("*").alias("trip_count"))
    )
    weekly_df.write.jdbc(url=JDBC_URL, table="weekly_region_stats_staging", mode="overwrite", properties=props)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("TripsStreamingJob")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        raw_df.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
        .withColumn("trip_datetime", col("trip_datetime").cast("timestamp"))
    )

    query = (
        parsed_df.writeStream.foreachBatch(upsert_batch)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    query.awaitTermination()
