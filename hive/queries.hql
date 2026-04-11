-- ============================================================
-- Hive Queries for Weather Data Warehouse
-- Run after batch processing has populated Hive tables
-- ============================================================

-- Create database
CREATE DATABASE IF NOT EXISTS weather_db;
USE weather_db;

-- ============================================================
-- Table definitions (if not created by Spark)
-- ============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS raw_readings (
    station_id       STRING,
    station_name     STRING,
    latitude         DOUBLE,
    longitude        DOUBLE,
    timestamp        TIMESTAMP,
    temperature_c    DOUBLE,
    humidity_pct     DOUBLE,
    pressure_hpa     DOUBLE,
    wind_speed_kmh   DOUBLE,
    wind_direction   STRING,
    precipitation_mm DOUBLE,
    is_anomaly       BOOLEAN,
    date             DATE,
    hour             INT,
    month            INT
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/weather_db.db/raw_readings';

CREATE EXTERNAL TABLE IF NOT EXISTS daily_stats (
    station_id         STRING,
    station_name       STRING,
    date               DATE,
    avg_temp           DOUBLE,
    min_temp           DOUBLE,
    max_temp           DOUBLE,
    avg_humidity       DOUBLE,
    avg_pressure       DOUBLE,
    avg_wind_speed     DOUBLE,
    total_precipitation DOUBLE,
    num_readings       BIGINT,
    anomaly_count      BIGINT
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/weather_db.db/daily_stats';


-- ============================================================
-- Query 1: Top 5 hottest days across all stations
-- ============================================================
SELECT station_name, `date`, max_temp
FROM daily_stats
ORDER BY max_temp DESC
LIMIT 5;


-- ============================================================
-- Query 2: Monthly average temperature trend
-- ============================================================
SELECT month,
       ROUND(AVG(temperature_c), 2) AS avg_temp,
       ROUND(MIN(temperature_c), 2) AS min_temp,
       ROUND(MAX(temperature_c), 2) AS max_temp,
       COUNT(*) AS total_readings
FROM raw_readings
GROUP BY month
ORDER BY month;


-- ============================================================
-- Query 3: Stations with most anomalies
-- ============================================================
SELECT station_id, station_name,
       SUM(anomaly_count) AS total_anomalies,
       ROUND(AVG(avg_temp), 2) AS avg_daily_temp
FROM daily_stats
GROUP BY station_id, station_name
ORDER BY total_anomalies DESC;


-- ============================================================
-- Query 4: Rainy days analysis (precipitation > 5mm)
-- ============================================================
SELECT station_name, `date`, total_precipitation, avg_temp, avg_humidity
FROM daily_stats
WHERE total_precipitation > 5.0
ORDER BY total_precipitation DESC
LIMIT 20;


-- ============================================================
-- Query 5: Average conditions by station
-- ============================================================
SELECT station_name,
       ROUND(AVG(avg_temp), 2) AS overall_avg_temp,
       ROUND(AVG(avg_humidity), 2) AS overall_avg_humidity,
       ROUND(AVG(avg_wind_speed), 2) AS overall_avg_wind,
       ROUND(SUM(total_precipitation), 2) AS total_rain,
       SUM(anomaly_count) AS total_anomalies
FROM daily_stats
GROUP BY station_name
ORDER BY overall_avg_temp DESC;


-- ============================================================
-- Query 6: Hourly temperature patterns (diurnal cycle)
-- ============================================================
SELECT hour,
       ROUND(AVG(temperature_c), 2) AS avg_temp,
       ROUND(AVG(humidity_pct), 2) AS avg_humidity,
       ROUND(AVG(wind_speed_kmh), 2) AS avg_wind
FROM raw_readings
GROUP BY hour
ORDER BY hour;


-- ============================================================
-- Query 7: Day-over-day temperature change per station
-- ============================================================
SELECT a.station_name,
       a.`date` AS current_date,
       a.avg_temp AS current_avg,
       b.avg_temp AS previous_avg,
       ROUND(a.avg_temp - b.avg_temp, 2) AS temp_change
FROM daily_stats a
JOIN daily_stats b
  ON a.station_id = b.station_id
  AND a.`date` = DATE_ADD(b.`date`, 1)
WHERE ABS(a.avg_temp - b.avg_temp) > 5
ORDER BY ABS(a.avg_temp - b.avg_temp) DESC
LIMIT 20;
