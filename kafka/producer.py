"""
Kafka Producer - Streams weather sensor data to Kafka topic 'weather-raw'.
Reads from the generated JSONL file and publishes each record.
"""

import json
import sys
import time
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC = "weather-raw"


def create_producer(retries=10, delay=5):
    """Create Kafka producer with retry logic for container startup."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"Connected to Kafka broker at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            print(f"Attempt {attempt + 1}/{retries}: Kafka not ready, retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple retries.")


def stream_from_file(filepath, rate_limit=0.1):
    """Read JSONL file and stream each record to Kafka."""
    producer = create_producer()
    sent = 0

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line.strip())
            producer.send(TOPIC, value=record)
            sent += 1
            if sent % 50 == 0:
                print(f"Sent {sent} records to topic '{TOPIC}'")
            time.sleep(rate_limit)

    producer.flush()
    producer.close()
    print(f"Finished: {sent} records sent to topic '{TOPIC}'")
    return sent


def stream_live(count=200, interval=2):
    """Generate and stream live weather data directly to Kafka."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "data", "generator"))
    from generate_weather_data import generate_reading, STATIONS
    import random
    from datetime import datetime

    producer = create_producer()
    sent = 0

    for i in range(count):
        station = random.choice(STATIONS)
        record = generate_reading(station, datetime.now())
        producer.send(TOPIC, value=record)
        sent += 1
        if sent % 10 == 0:
            print(f"Live: sent {sent}/{count} records")
        time.sleep(interval)

    producer.flush()
    producer.close()
    print(f"Live streaming finished: {sent} records sent.")
    return sent


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "file"

    if mode == "file":
        data_file = sys.argv[2] if len(sys.argv) > 2 else "/opt/data/sample/weather_streaming.jsonl"
        stream_from_file(data_file)
    elif mode == "live":
        count = int(sys.argv[2]) if len(sys.argv) > 2 else 200
        stream_live(count=count)
    else:
        print(f"Usage: python producer.py [file|live] [filepath|count]")
