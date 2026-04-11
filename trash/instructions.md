# Instructions — How to Run the Weather Pipeline

## Prerequisites

Install the following before anything else:

- **Docker Desktop** — https://www.docker.com/products/docker-desktop (engine + compose included)
- **Python 3.8+** — https://www.python.org/downloads/

Verify both are available:

```
docker --version
docker compose version
python --version
```

Install the Python dependencies (needed for data generation and tests):

```
pip install -r requirements.txt
```

---

## Project Structure Quick Reference

```
BigData_project/
├── data/generator/         → Python data generator
├── data/sample/            → Generated CSV and JSONL files (created on Step 1)
├── kafka/                  → Kafka producer and consumer scripts
├── spark/                  → PySpark batch/streaming jobs + Scala bonus
├── hive/                   → HiveQL analytical queries
├── scripts/                → Helper shell scripts
└── tests/                  → Unit tests and integration tests
```

---

## Step-by-Step Execution

### Step 1 — Generate the Data

Run the data generator from the project root. This creates the historical CSV and the streaming JSONL file inside `data/sample/`.

```
python data/generator/generate_weather_data.py
```

Default output:
- `data/sample/weather_historical.csv` — 86,400 records (90 days, 10 stations, 15-min intervals)
- `data/sample/weather_streaming.jsonl` — 500 simulated real-time records

Optional arguments to control the size:

```
python data/generator/generate_weather_data.py <days> <stream_count>

# Example: 30 days of history and 200 stream records
python data/generator/generate_weather_data.py 30 200
```

---

### Step 2 — Start the Infrastructure

Start all Docker containers (HDFS, Kafka, Spark, Hive, ZooKeeper, PostgreSQL):

```
docker compose up -d
```

Wait approximately **60 seconds** for all services to fully initialize. You can check status with:

```
docker compose ps
```

All containers should show status `Up` or `running`.

**Web interfaces available after startup:**

| Service          | URL                        |
|------------------|----------------------------|
| HDFS NameNode UI | http://localhost:9870       |
| Spark Master UI  | http://localhost:8080       |
| Hive Server UI   | http://localhost:10002      |

---

### Step 3 — Upload Data to HDFS

Copy the historical CSV into the namenode container and upload it to HDFS:

```
docker cp data/sample/weather_historical.csv namenode:/tmp/weather_historical.csv
docker exec namenode hdfs dfs -mkdir -p /user/weather/raw
docker exec namenode hdfs dfs -put -f /tmp/weather_historical.csv /user/weather/raw/
```

Verify the upload:

```
docker exec namenode hdfs dfs -ls /user/weather/raw/
```

---

### Step 4 — Create the Kafka Topic

```
docker exec kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic weather-raw \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists
```

List topics to confirm:

```
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

---

### Step 5 — Run the Kafka Producer

The producer reads `weather_streaming.jsonl` and publishes each record to the `weather-raw` topic.

```
docker exec spark-master bash -c "pip install kafka-python && python3 /opt/spark-jobs/../../kafka/producer.py file /opt/data/sample/weather_streaming.jsonl"
```

**Alternative — live mode** (generates and streams fresh records in real time):

```
docker exec spark-master bash -c "python3 /opt/spark-jobs/../../kafka/producer.py live 200"
```

To verify messages are arriving, run the consumer in a separate terminal:

```
docker exec spark-master bash -c "pip install kafka-python && python3 /opt/spark-jobs/../../kafka/consumer.py 100"
```

---

### Step 6 — Run Spark Batch Processing

Submit the PySpark batch job to the Spark cluster. This reads the historical CSV and produces daily/monthly aggregations, station rankings, and anomaly detection results.

```
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/batch_processing.py \
  /opt/data/sample/weather_historical.csv \
  /opt/data/output
```

Output is written to `/opt/data/output/` inside the container (mapped to `data/output/` on your machine):
- `daily_aggregations/` — Parquet
- `monthly_aggregations/` — Parquet
- `station_rankings/` — Parquet
- `anomalies/` — Parquet
- `hourly_patterns/` — Parquet

---

### Step 7 — Run Spark Structured Streaming

Start the streaming job. It consumes live data from Kafka, detects extreme weather alerts, and writes windowed aggregations and archived Parquet files. This runs continuously in the background.

```
docker exec -d spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-jobs/streaming_processing.py \
  kafka:9092 \
  /opt/data/output/streaming
```

To see the streaming output (alerts printed to console):

```
docker logs -f spark-master
```

Stop following logs with `Ctrl+C` (the streaming job keeps running in the background).

---

### Step 8 — Initialize the Hive Metastore Schema (first run only)

On the very first run, the Hive metastore requires a one-time schema initialization in PostgreSQL. Run this before starting HiveServer2:

```
docker exec hive-server /opt/hive/bin/schematool -initSchema -dbType postgres
```

Expected last line: `schemaTool completed`

Then restart the metastore and server so they pick up the initialized schema:

```
docker compose restart hive-metastore
```

Wait ~15 seconds, then:

```
docker compose restart hive-server
```

Wait ~30 seconds for HiveServer2 to fully start before running queries.

> This step is only needed **once**. On subsequent `docker compose up`, the schema already exists in the PostgreSQL volume.

---

### Step 9 — Run Hive Queries

Once the batch job has populated the Hive tables, open a Beeline session and execute the analytical queries:

```
docker exec -it hive-server beeline -u "jdbc:hive2://localhost:10000"
```

Inside the Beeline shell, paste or source the queries:

```sql
!run /opt/hive/queries.hql
```

Or run a single query directly from the terminal:

```
docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT station_name, date, max_temp FROM weather_db.daily_stats ORDER BY max_temp DESC LIMIT 5;"
```

---

## Running the Tests

### Unit Tests (no Docker required)

Runs 17 tests covering data generation, field types, schema compatibility, seasonal/diurnal patterns, and anomaly injection.

```
python -m unittest tests.test_pipeline -v
```

Expected output: `Ran 17 tests in ~0.5s — OK`

### Integration Tests (Docker must be running)

Tests connectivity and correctness of HDFS, Kafka, and Spark inside the containers.

```
python -m unittest tests.test_integration -v
```

---

## Bonus — Scala Job

The Scala job (`spark/WeatherAnalysis.scala`) performs daily aggregations and temperature extremes analysis using Spark's native Scala API. To run it, the `.scala` file must first be compiled into a JAR with `sbt`:

```
# Inside the spark/ directory (requires sbt installed)
sbt package

# Submit the JAR
docker exec spark-master spark-submit \
  --class WeatherAnalysis \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/target/scala-2.12/weather-analysis_2.12-1.0.jar \
  /opt/data/sample/weather_historical.csv \
  /opt/data/output/scala
```

---

## Running the Entire Pipeline in One Command

A shell script orchestrates all steps automatically (Linux/macOS/WSL):

```
bash scripts/run_pipeline.sh
```

On Windows (without WSL), run each step manually as described above.

---

## Stopping Everything

Stop and remove all containers (data volumes are preserved):

```
docker compose down
```

Stop containers **and delete all data volumes** (full reset):

```
docker compose down -v
```

---

## Troubleshooting

| Problem | Solution |
|---|---|
| Container not starting | Run `docker compose logs <service>` to see error details |
| HDFS upload fails | Wait a few more seconds for the namenode to exit safe mode, then retry |
| Kafka producer can't connect | Ensure the `kafka` container is `Up` before running the producer |
| Spark job fails with memory error | Increase Docker Desktop memory to at least 4 GB in Settings → Resources |
| Hive queries fail with `ParseException` on `date` | `date` is a reserved keyword — always wrap it in backticks: `` `date` `` |
| Hive metastore exits immediately on first run | Run the schema init command in Step 8 before restarting |
| Port already in use | Stop any conflicting local services using ports 9092, 8080, or 9870 |
