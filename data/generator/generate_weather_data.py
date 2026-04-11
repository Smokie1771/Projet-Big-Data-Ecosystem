"""
Weather Sensor Data Generator
Generates realistic weather data from multiple IoT sensors across a smart city.
Produces both a historical CSV dataset (batch) and real-time streaming records (Kafka).
"""

import csv
import json
import random
import os
import sys
from datetime import datetime, timedelta

# ---------- Configuration ----------
STATIONS = [
    {"station_id": "ST001", "name": "Downtown",       "lat": 48.8566, "lon": 2.3522},
    {"station_id": "ST002", "name": "Airport",         "lat": 48.7262, "lon": 2.3652},
    {"station_id": "ST003", "name": "Industrial_Zone", "lat": 48.9062, "lon": 2.2872},
    {"station_id": "ST004", "name": "Riverside",       "lat": 48.8400, "lon": 2.3760},
    {"station_id": "ST005", "name": "University",      "lat": 48.8470, "lon": 2.3440},
    {"station_id": "ST006", "name": "Park_Area",       "lat": 48.8650, "lon": 2.3130},
    {"station_id": "ST007", "name": "Suburb_North",    "lat": 48.9200, "lon": 2.3400},
    {"station_id": "ST008", "name": "Suburb_South",    "lat": 48.7900, "lon": 2.3300},
    {"station_id": "ST009", "name": "Harbor",          "lat": 48.8300, "lon": 2.4100},
    {"station_id": "ST010", "name": "Hilltop",         "lat": 48.8800, "lon": 2.2600},
]

# Seasonal base temperatures (monthly averages for Paris-like city)
MONTHLY_BASE_TEMP = {
    1: 3.5, 2: 4.5, 3: 8.0, 4: 11.0, 5: 15.0, 6: 18.5,
    7: 20.5, 8: 20.0, 9: 16.5, 10: 12.0, 11: 7.5, 12: 4.5
}


def generate_reading(station, timestamp):
    """Generate a single realistic weather sensor reading."""
    month = timestamp.month
    hour = timestamp.hour
    base_temp = MONTHLY_BASE_TEMP[month]

    # Diurnal variation: cooler at night, warmer afternoon
    diurnal = -3.0 + 6.0 * max(0, min(1, (hour - 6) / 8)) if hour < 14 else \
              3.0 - 6.0 * max(0, min(1, (hour - 14) / 10))

    temperature = round(base_temp + diurnal + random.gauss(0, 1.5), 1)
    humidity = round(max(10, min(100, 60 + random.gauss(0, 15) - diurnal * 2)), 1)
    pressure = round(random.gauss(1013.25, 5), 1)
    wind_speed = round(max(0, random.gauss(12, 6)), 1)
    wind_direction = random.choice(["N", "NE", "E", "SE", "S", "SW", "W", "NW"])
    precipitation = round(max(0, random.gauss(0, 0.8)), 2) if random.random() < 0.3 else 0.0

    # Inject occasional anomalies (~2% of readings) for detection tests
    is_anomaly = False
    if random.random() < 0.02:
        anomaly_type = random.choice(["temp_spike", "pressure_drop", "wind_gust"])
        if anomaly_type == "temp_spike":
            temperature += random.choice([-15, 15])
            is_anomaly = True
        elif anomaly_type == "pressure_drop":
            pressure -= 30
            is_anomaly = True
        elif anomaly_type == "wind_gust":
            wind_speed = round(random.uniform(80, 130), 1)
            is_anomaly = True

    return {
        "station_id": station["station_id"],
        "station_name": station["name"],
        "latitude": station["lat"],
        "longitude": station["lon"],
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "temperature_c": temperature,
        "humidity_pct": humidity,
        "pressure_hpa": pressure,
        "wind_speed_kmh": wind_speed,
        "wind_direction": wind_direction,
        "precipitation_mm": precipitation,
        "is_anomaly": is_anomaly
    }


def generate_batch_csv(output_path, days=90):
    """Generate historical weather data CSV for batch processing."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    start_date = datetime(2025, 1, 1, 0, 0, 0)
    total_records = 0

    fieldnames = [
        "station_id", "station_name", "latitude", "longitude", "timestamp",
        "temperature_c", "humidity_pct", "pressure_hpa",
        "wind_speed_kmh", "wind_direction", "precipitation_mm", "is_anomaly"
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for day_offset in range(days):
            current_date = start_date + timedelta(days=day_offset)
            for hour in range(24):
                for minute in [0, 15, 30, 45]:  # readings every 15 min
                    ts = current_date.replace(hour=hour, minute=minute)
                    for station in STATIONS:
                        record = generate_reading(station, ts)
                        writer.writerow(record)
                        total_records += 1

    print(f"Generated {total_records} records over {days} days -> {output_path}")
    return total_records


def generate_streaming_json(output_path, count=500):
    """Generate a JSON-lines file simulating real-time sensor data for Kafka."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    now = datetime.now()
    records = []

    for i in range(count):
        ts = now + timedelta(seconds=i * 5)  # 1 reading every 5 seconds
        station = random.choice(STATIONS)
        record = generate_reading(station, ts)
        records.append(record)

    with open(output_path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    print(f"Generated {count} streaming records -> {output_path}")
    return count


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    sample_dir = os.path.join(base_dir, "..", "sample")

    batch_path = os.path.join(sample_dir, "weather_historical.csv")
    stream_path = os.path.join(sample_dir, "weather_streaming.jsonl")

    days = int(sys.argv[1]) if len(sys.argv) > 1 else 90
    stream_count = int(sys.argv[2]) if len(sys.argv) > 2 else 500

    total_batch = generate_batch_csv(batch_path, days=days)
    total_stream = generate_streaming_json(stream_path, count=stream_count)

    print(f"\nSummary:")
    print(f"  Batch  : {total_batch:,} records  ({days} days, 10 stations, 15-min intervals)")
    print(f"  Stream : {total_stream:,} records  (simulated real-time)")
    print(f"  Total  : {total_batch + total_stream:,} records")
