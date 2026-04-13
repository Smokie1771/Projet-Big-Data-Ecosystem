"""
Spark Batch Processing - Historical Weather Data Analysis
Reads CSV from HDFS, performs aggregations and anomaly detection, saves results.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import os


def create_spark_session():
    """Initialize Spark session (Hive support optional)."""
    import os
    master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
    builder = SparkSession.builder \
        .appName("WeatherBatchProcessing") \
        .master(master) \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    try:
        spark = builder.enableHiveSupport().getOrCreate()
    except Exception:
        spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_data(spark, input_path):
    """Load the historical weather CSV into a DataFrame."""
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df = df.withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("date", F.to_date("timestamp"))
    df = df.withColumn("hour", F.hour("timestamp"))
    df = df.withColumn("month", F.month("timestamp"))
    print(f"Loaded {df.count()} records from {input_path}")
    df.printSchema()
    return df


def daily_aggregations(df):
    """Compute daily statistics per station."""
    daily = df.groupBy("station_id", "station_name", "date").agg(
        F.round(F.avg("temperature_c"), 2).alias("avg_temp"),
        F.round(F.min("temperature_c"), 2).alias("min_temp"),
        F.round(F.max("temperature_c"), 2).alias("max_temp"),
        F.round(F.avg("humidity_pct"), 2).alias("avg_humidity"),
        F.round(F.avg("pressure_hpa"), 2).alias("avg_pressure"),
        F.round(F.avg("wind_speed_kmh"), 2).alias("avg_wind_speed"),
        F.round(F.sum("precipitation_mm"), 2).alias("total_precipitation"),
        F.count("*").alias("num_readings"),
        F.sum(F.when(F.col("is_anomaly") == True, 1).otherwise(0)).alias("anomaly_count")
    ).orderBy("station_id", "date")

    print(f"\n=== Daily Aggregations ({daily.count()} rows) ===")
    daily.show(20, truncate=False)
    return daily


def monthly_aggregations(df):
    """Compute monthly statistics across all stations."""
    monthly = df.groupBy("month").agg(
        F.round(F.avg("temperature_c"), 2).alias("avg_temp"),
        F.round(F.min("temperature_c"), 2).alias("min_temp"),
        F.round(F.max("temperature_c"), 2).alias("max_temp"),
        F.round(F.avg("humidity_pct"), 2).alias("avg_humidity"),
        F.round(F.sum("precipitation_mm"), 2).alias("total_precipitation"),
        F.count("*").alias("num_readings")
    ).orderBy("month")

    print(f"\n=== Monthly Aggregations ===")
    monthly.show(12, truncate=False)
    return monthly


def station_ranking(df):
    """Rank stations by average temperature."""
    ranking = df.groupBy("station_id", "station_name").agg(
        F.round(F.avg("temperature_c"), 2).alias("avg_temp"),
        F.round(F.avg("wind_speed_kmh"), 2).alias("avg_wind"),
        F.round(F.sum("precipitation_mm"), 2).alias("total_precip"),
    ).orderBy(F.desc("avg_temp"))

    print(f"\n=== Station Ranking by Average Temperature ===")
    ranking.show(10, truncate=False)
    return ranking


def detect_anomalies(df):
    """Detect anomalies using statistical thresholds (Z-score > 3)."""
    # Compute station-level stats
    stats = df.groupBy("station_id").agg(
        F.avg("temperature_c").alias("mean_temp"),
        F.stddev("temperature_c").alias("std_temp"),
        F.avg("wind_speed_kmh").alias("mean_wind"),
        F.stddev("wind_speed_kmh").alias("std_wind"),
    )

    df_with_stats = df.join(stats, on="station_id")

    anomalies = df_with_stats.filter(
        (F.abs(F.col("temperature_c") - F.col("mean_temp")) > 3 * F.col("std_temp")) |
        (F.abs(F.col("wind_speed_kmh") - F.col("mean_wind")) > 3 * F.col("std_wind"))
    ).select(
        "station_id", "station_name", "timestamp", "temperature_c",
        "wind_speed_kmh", "pressure_hpa", "is_anomaly"
    )

    print(f"\n=== Anomalies Detected: {anomalies.count()} ===")
    anomalies.show(20, truncate=False)
    return anomalies


def hourly_patterns(df):
    """Analyze hourly temperature patterns (diurnal cycle)."""
    hourly = df.groupBy("hour").agg(
        F.round(F.avg("temperature_c"), 2).alias("avg_temp"),
        F.round(F.avg("humidity_pct"), 2).alias("avg_humidity"),
        F.round(F.avg("wind_speed_kmh"), 2).alias("avg_wind"),
    ).orderBy("hour")

    print(f"\n=== Hourly Temperature Patterns ===")
    hourly.show(24, truncate=False)
    return hourly


def save_to_hive(df, table_name, mode="overwrite"):
    """Save DataFrame as a Hive table."""
    df.write.mode(mode).saveAsTable(table_name)
    print(f"Saved to Hive table: {table_name}")


def save_to_parquet(df, output_path, mode="overwrite"):
    """Save DataFrame as Parquet files on HDFS."""
    df.write.mode(mode).parquet(output_path)
    print(f"Saved Parquet to: {output_path}")


def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/data/sample/weather_historical.csv"
    output_base = sys.argv[2] if len(sys.argv) > 2 else "/opt/data/output"

    spark = create_spark_session()

    print("=" * 60)
    print("  WEATHER BATCH PROCESSING PIPELINE")
    print("=" * 60)

    # Step 1: Load data
    df = load_data(spark, input_path)
    df.cache()

    # Step 2: Daily aggregations
    daily = daily_aggregations(df)
    save_to_parquet(daily, f"{output_base}/daily_aggregations")

    # Step 3: Monthly aggregations
    monthly = monthly_aggregations(df)
    save_to_parquet(monthly, f"{output_base}/monthly_aggregations")

    # Step 4: Station ranking
    ranking = station_ranking(df)
    save_to_parquet(ranking, f"{output_base}/station_rankings")

    # Step 5: Anomaly detection
    anomalies = detect_anomalies(df)
    save_to_parquet(anomalies, f"{output_base}/anomalies")

    # Step 6: Hourly patterns
    hourly = hourly_patterns(df)
    save_to_parquet(hourly, f"{output_base}/hourly_patterns")

    # Step 7: Save raw data to Hive for SQL queries
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS weather_db")
        save_to_hive(df, "weather_db.raw_readings")
        save_to_hive(daily, "weather_db.daily_stats")
        print("Hive tables created successfully.")
    except Exception as e:
        print(f"Hive save skipped (metastore may not be available): {e}")

    # Summary
    print("\n" + "=" * 60)
    print("  BATCH PROCESSING COMPLETE")
    print("=" * 60)
    print(f"  Total records processed  : {df.count():,}")
    print(f"  Daily aggregation rows   : {daily.count():,}")
    print(f"  Anomalies detected       : {anomalies.count():,}")
    print(f"  Output directory         : {output_base}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
