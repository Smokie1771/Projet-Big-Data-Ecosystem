/**
 * WeatherAnalysis.scala - Bonus: Spark Batch Processing in Scala
 * Demonstrates the same pipeline logic using a different language (Scala).
 * 
 * Build with: sbt package  (or submit directly with spark-submit)
 * Run with:
 *   spark-submit --class WeatherAnalysis --master spark://spark-master:7077 \
 *     target/scala-2.12/weather-analysis_2.12-1.0.jar \
 *     /opt/data/sample/weather_historical.csv /opt/data/output/scala
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object WeatherAnalysis {

  def main(args: Array[String]): Unit = {
    val inputPath = if (args.length > 0) args(0) else "/opt/data/sample/weather_historical.csv"
    val outputPath = if (args.length > 1) args(1) else "/opt/data/output/scala"

    val spark = SparkSession.builder()
      .appName("WeatherAnalysis-Scala")
      .getOrCreate()

    import spark.implicits._

    println("=" * 60)
    println("  SCALA WEATHER BATCH ANALYSIS")
    println("=" * 60)

    // Load CSV
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)
      .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("date", to_date(col("timestamp")))
      .withColumn("hour", hour(col("timestamp")))
      .withColumn("month", month(col("timestamp")))

    rawDF.cache()
    val totalRecords = rawDF.count()
    println(s"Loaded $totalRecords records")

    // Daily statistics per station
    val dailyStats = rawDF.groupBy("station_id", "station_name", "date")
      .agg(
        round(avg("temperature_c"), 2).alias("avg_temp"),
        round(min("temperature_c"), 2).alias("min_temp"),
        round(max("temperature_c"), 2).alias("max_temp"),
        round(avg("humidity_pct"), 2).alias("avg_humidity"),
        round(avg("wind_speed_kmh"), 2).alias("avg_wind"),
        round(sum("precipitation_mm"), 2).alias("total_precip"),
        count("*").alias("readings")
      )
      .orderBy("station_id", "date")

    println(s"\n=== Daily Stats (Scala): ${dailyStats.count()} rows ===")
    dailyStats.show(10, truncate = false)

    // Temperature extremes per station
    val extremes = rawDF.groupBy("station_id", "station_name")
      .agg(
        round(min("temperature_c"), 2).alias("record_low"),
        round(max("temperature_c"), 2).alias("record_high"),
        round(avg("temperature_c"), 2).alias("mean_temp"),
        round(stddev("temperature_c"), 2).alias("stddev_temp")
      )
      .orderBy(desc("record_high"))

    println("\n=== Temperature Extremes (Scala) ===")
    extremes.show(10, truncate = false)

    // Wind analysis - gustiest days
    val gustyDays = rawDF
      .filter(col("wind_speed_kmh") > 40)
      .groupBy("station_name", "date")
      .agg(
        round(max("wind_speed_kmh"), 1).alias("max_gust"),
        count("*").alias("high_wind_readings")
      )
      .orderBy(desc("max_gust"))

    println("\n=== Gustiest Days (Scala) ===")
    gustyDays.show(10, truncate = false)

    // Save results
    dailyStats.write.mode("overwrite").parquet(s"$outputPath/daily_stats_scala")
    extremes.write.mode("overwrite").parquet(s"$outputPath/extremes_scala")
    println(s"\nScala output saved to: $outputPath")

    println("=" * 60)
    println("  SCALA ANALYSIS COMPLETE")
    println("=" * 60)

    spark.stop()
  }
}
