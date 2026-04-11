#!/bin/bash
# ============================================================
# Full Pipeline Orchestration Script
# Runs all stages of the Big Data weather pipeline
# ============================================================

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

echo "============================================================"
echo "  SMART CITY WEATHER DATA PIPELINE"
echo "  $(date)"
echo "============================================================"
echo ""

# ----- Step 1: Generate data -----
echo ">>> STEP 1: Generating weather data..."
python3 data/generator/generate_weather_data.py 90 500
echo ""

# ----- Step 2: Start infrastructure -----
echo ">>> STEP 2: Starting Docker infrastructure..."
docker-compose up -d
echo "Waiting 30s for services to initialize..."
sleep 30
echo ""

# ----- Step 3: Upload to HDFS -----
echo ">>> STEP 3: Uploading data to HDFS..."
docker exec namenode bash -c "hdfs dfs -mkdir -p /user/weather/raw"
docker cp data/sample/weather_historical.csv namenode:/tmp/weather_historical.csv
docker exec namenode bash -c "hdfs dfs -put -f /tmp/weather_historical.csv /user/weather/raw/"
echo "HDFS upload complete."
echo ""

# ----- Step 4: Create Kafka topic -----
echo ">>> STEP 4: Creating Kafka topic..."
docker exec kafka kafka-topics --create \
    --bootstrap-server kafka:9092 \
    --topic weather-raw \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists
echo ""

# ----- Step 5: Run Kafka producer -----
echo ">>> STEP 5: Starting Kafka producer (background)..."
docker exec -d spark-master bash -c "
    pip install kafka-python 2>/dev/null;
    cd /opt/spark-jobs && python3 ../data/../spark-jobs/../data/../spark-jobs/../kafka_producer_wrapper.py
" 2>/dev/null || echo "Kafka producer will be started manually."
echo ""

# ----- Step 6: Run Spark batch processing -----
echo ">>> STEP 6: Running Spark batch processing..."
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-jobs/batch_processing.py \
    /opt/data/sample/weather_historical.csv \
    /opt/data/output
echo ""

# ----- Step 7: Run Spark streaming (background) -----
echo ">>> STEP 7: Starting Spark streaming (background)..."
docker exec -d spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/spark-jobs/streaming_processing.py \
    kafka:9092 \
    /opt/data/output/streaming
echo ""

# ----- Step 8: Run Hive queries -----
echo ">>> STEP 8: Running Hive queries..."
docker exec hive-server bash -c "
    beeline -u 'jdbc:hive2://localhost:10000' -f /opt/hive/queries.hql
" 2>/dev/null || echo "Hive queries will be run manually (metastore may need time)."
echo ""

echo "============================================================"
echo "  PIPELINE EXECUTION COMPLETE"
echo "============================================================"
echo ""
echo "Access points:"
echo "  HDFS Web UI      : http://localhost:9870"
echo "  Spark Master UI  : http://localhost:8080"
echo "  Hive Server      : jdbc:hive2://localhost:10000"
echo ""
echo "To stop all services: docker-compose down"
