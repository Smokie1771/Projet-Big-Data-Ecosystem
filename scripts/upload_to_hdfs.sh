#!/bin/bash
# ============================================================
# Upload weather data to HDFS
# Run from inside the namenode container or a container with
# Hadoop client configured to reach namenode:9000
# ============================================================

set -e

HDFS_CMD="hdfs dfs"
DATA_DIR="/opt/data/sample"
HDFS_TARGET="/user/weather/raw"

echo "=== Uploading weather data to HDFS ==="

# Create directories
$HDFS_CMD -mkdir -p $HDFS_TARGET

# Upload historical CSV
if [ -f "$DATA_DIR/weather_historical.csv" ]; then
    echo "Uploading weather_historical.csv ..."
    $HDFS_CMD -put -f "$DATA_DIR/weather_historical.csv" "$HDFS_TARGET/"
    echo "Done. Verifying:"
    $HDFS_CMD -ls "$HDFS_TARGET/"
else
    echo "ERROR: $DATA_DIR/weather_historical.csv not found."
    echo "Run the data generator first: python data/generator/generate_weather_data.py"
    exit 1
fi

echo ""
echo "=== HDFS Upload Complete ==="
echo "Data available at: $HDFS_TARGET/weather_historical.csv"
$HDFS_CMD -du -s -h "$HDFS_TARGET/"
