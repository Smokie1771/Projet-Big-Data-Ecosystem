"""
Spark Structured Streaming - Real-Time Weather Alert System
Consumes from Kafka topic 'weather-raw', detects extreme conditions in real time,
and writes alerts + aggregated micro-batches to output.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)
import sys


# Schema matching the JSON records produced by the Kafka producer
WEATHER_SCHEMA = StructType([
    StructField("station_id", StringType()),
    StructField("station_name", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("temperature_c", DoubleType()),
    StructField("humidity_pct", DoubleType()),
    StructField("pressure_hpa", DoubleType()),
    StructField("wind_speed_kmh", DoubleType()),
    StructField("wind_direction", StringType()),
    StructField("precipitation_mm", DoubleType()),
    StructField("is_anomaly", BooleanType()),
])

# Alert thresholds
TEMP_HIGH = 40.0
TEMP_LOW = -10.0
WIND_HIGH = 80.0
PRESSURE_LOW = 980.0


def create_spark_session():
    """Initialize Spark session with Kafka packages."""
    import os
    master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
    spark = SparkSession.builder \
        .appName("WeatherStreamingAlerts") \
        .master(master) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark, kafka_broker="kafka:9092", topic="weather-raw"):
    """Read streaming data from Kafka topic."""
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON from Kafka value
    parsed = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(F.from_json(F.col("json_str"), WEATHER_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

    return parsed


def detect_alerts(stream_df):
    """Apply real-time alert rules for extreme weather conditions."""
    alerts = stream_df.filter(
        (F.col("temperature_c") > TEMP_HIGH) |
        (F.col("temperature_c") < TEMP_LOW) |
        (F.col("wind_speed_kmh") > WIND_HIGH) |
        (F.col("pressure_hpa") < PRESSURE_LOW)
    ).withColumn("alert_type",
        F.when(F.col("temperature_c") > TEMP_HIGH, "EXTREME_HEAT")
         .when(F.col("temperature_c") < TEMP_LOW, "EXTREME_COLD")
         .when(F.col("wind_speed_kmh") > WIND_HIGH, "DANGEROUS_WIND")
         .when(F.col("pressure_hpa") < PRESSURE_LOW, "LOW_PRESSURE_STORM")
         .otherwise("UNKNOWN")
    ).withColumn("alert_severity",
        F.when(F.col("alert_type").isin("EXTREME_HEAT", "DANGEROUS_WIND"), "CRITICAL")
         .otherwise("WARNING")
    )

    return alerts


def windowed_aggregation(stream_df):
    """Compute 5-minute sliding window aggregations per station."""
    windowed = stream_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            F.window("event_time", "5 minutes", "1 minute"),
            "station_id", "station_name"
        ).agg(
            F.round(F.avg("temperature_c"), 2).alias("avg_temp"),
            F.round(F.max("temperature_c"), 2).alias("max_temp"),
            F.round(F.min("temperature_c"), 2).alias("min_temp"),
            F.round(F.avg("humidity_pct"), 2).alias("avg_humidity"),
            F.round(F.avg("wind_speed_kmh"), 2).alias("avg_wind"),
            F.count("*").alias("reading_count"),
        )

    return windowed


def main():
    kafka_broker = sys.argv[1] if len(sys.argv) > 1 else "kafka:9092"
    output_base = sys.argv[2] if len(sys.argv) > 2 else "/opt/data/output/streaming"

    spark = create_spark_session()

    print("=" * 60)
    print("  WEATHER STREAMING ALERT SYSTEM - STARTING")
    print("=" * 60)

    # Read from Kafka
    weather_stream = read_kafka_stream(spark, kafka_broker)

    # --- Stream 1: Real-time alerts to console ---
    alerts = detect_alerts(weather_stream)
    alert_query = alerts \
        .select("station_id", "station_name", "timestamp",
                "temperature_c", "wind_speed_kmh", "pressure_hpa",
                "alert_type", "alert_severity") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .queryName("weather_alerts") \
        .trigger(processingTime="10 seconds") \
        .start()

    # --- Stream 2: Windowed aggregations to Parquet ---
    windowed = windowed_aggregation(weather_stream)
    windowed_query = windowed \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .queryName("windowed_stats") \
        .trigger(processingTime="30 seconds") \
        .start()

    # --- Stream 3: All parsed data to Parquet for later batch analysis ---
    raw_query = weather_stream \
        .select("station_id", "station_name", "event_time",
                "temperature_c", "humidity_pct", "pressure_hpa",
                "wind_speed_kmh", "precipitation_mm") \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"{output_base}/raw_parquet") \
        .option("checkpointLocation", f"{output_base}/checkpoints/raw") \
        .queryName("raw_archive") \
        .trigger(processingTime="30 seconds") \
        .start()

    print("Streaming queries started. Waiting for termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
