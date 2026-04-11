"""
Kafka Consumer - Reads weather data from 'weather-raw' topic for validation.
Demonstrates message consumption and prints statistics.
"""

import json
import sys
import os
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC = "weather-raw"


def create_consumer(retries=10, delay=5):
    """Create Kafka consumer with retry logic."""
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id="weather-consumer-group",
                consumer_timeout_ms=15000,
            )
            print(f"Connected to Kafka, consuming from topic '{TOPIC}'")
            return consumer
        except NoBrokersAvailable:
            print(f"Attempt {attempt + 1}/{retries}: Kafka not ready, retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple retries.")


def consume_and_report(max_messages=None):
    """Consume messages and print summary stats."""
    consumer = create_consumer()
    count = 0
    stations_seen = set()
    anomaly_count = 0
    temp_sum = 0.0

    for message in consumer:
        record = message.value
        count += 1
        stations_seen.add(record.get("station_id"))
        temp_sum += record.get("temperature_c", 0)
        if record.get("is_anomaly"):
            anomaly_count += 1

        if count % 50 == 0:
            print(f"Consumed {count} messages | Stations: {len(stations_seen)} | Anomalies: {anomaly_count}")

        if max_messages and count >= max_messages:
            break

    consumer.close()

    avg_temp = round(temp_sum / count, 2) if count > 0 else 0
    print(f"\n--- Consumer Summary ---")
    print(f"Total messages consumed : {count}")
    print(f"Unique stations         : {len(stations_seen)}")
    print(f"Anomalies detected      : {anomaly_count}")
    print(f"Average temperature     : {avg_temp}°C")
    return count


if __name__ == "__main__":
    max_msg = int(sys.argv[1]) if len(sys.argv) > 1 else None
    consume_and_report(max_messages=max_msg)
