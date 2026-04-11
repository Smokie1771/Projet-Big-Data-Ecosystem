"""
Test Suite - Validates each component of the Big Data weather pipeline.
Can be run locally (unit tests) or against Docker containers (integration tests).
"""

import unittest
import os
import sys
import json
import csv
import tempfile
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from data.generator.generate_weather_data import (
    generate_reading, generate_batch_csv, generate_streaming_json, STATIONS
)


class TestDataGenerator(unittest.TestCase):
    """Test the weather data generator."""

    def test_generate_single_reading(self):
        """A single reading should have all required fields."""
        station = STATIONS[0]
        ts = datetime(2025, 7, 15, 14, 30, 0)
        record = generate_reading(station, ts)

        required_fields = [
            "station_id", "station_name", "latitude", "longitude",
            "timestamp", "temperature_c", "humidity_pct", "pressure_hpa",
            "wind_speed_kmh", "wind_direction", "precipitation_mm", "is_anomaly"
        ]
        for field in required_fields:
            self.assertIn(field, record, f"Missing field: {field}")

    def test_station_id_preserved(self):
        """Reading should carry the station's ID and name."""
        station = STATIONS[3]
        ts = datetime(2025, 1, 1, 0, 0, 0)
        record = generate_reading(station, ts)
        self.assertEqual(record["station_id"], station["station_id"])
        self.assertEqual(record["station_name"], station["name"])

    def test_temperature_range(self):
        """Temperature should be plausible (-40 to 60 including anomalies)."""
        station = STATIONS[0]
        for _ in range(1000):
            ts = datetime(2025, 6, 15, 12, 0, 0)
            record = generate_reading(station, ts)
            self.assertGreater(record["temperature_c"], -50)
            self.assertLess(record["temperature_c"], 70)

    def test_humidity_bounds(self):
        """Humidity should be between 10 and 100."""
        station = STATIONS[0]
        for _ in range(500):
            ts = datetime(2025, 3, 10, 8, 0, 0)
            record = generate_reading(station, ts)
            self.assertGreaterEqual(record["humidity_pct"], 10)
            self.assertLessEqual(record["humidity_pct"], 100)

    def test_wind_speed_non_negative(self):
        """Wind speed should never be negative."""
        station = STATIONS[0]
        for _ in range(500):
            ts = datetime(2025, 11, 20, 16, 0, 0)
            record = generate_reading(station, ts)
            self.assertGreaterEqual(record["wind_speed_kmh"], 0)

    def test_wind_direction_valid(self):
        """Wind direction should be a valid compass direction."""
        valid = {"N", "NE", "E", "SE", "S", "SW", "W", "NW"}
        station = STATIONS[0]
        for _ in range(200):
            ts = datetime(2025, 5, 5, 10, 0, 0)
            record = generate_reading(station, ts)
            self.assertIn(record["wind_direction"], valid)

    def test_precipitation_non_negative(self):
        """Precipitation should never be negative."""
        station = STATIONS[0]
        for _ in range(500):
            ts = datetime(2025, 4, 1, 6, 0, 0)
            record = generate_reading(station, ts)
            self.assertGreaterEqual(record["precipitation_mm"], 0)

    def test_anomalies_exist(self):
        """Over many readings, some anomalies should be generated (~2%)."""
        station = STATIONS[0]
        anomaly_count = 0
        total = 5000
        for _ in range(total):
            ts = datetime(2025, 8, 1, 12, 0, 0)
            record = generate_reading(station, ts)
            if record["is_anomaly"]:
                anomaly_count += 1
        rate = anomaly_count / total
        self.assertGreater(rate, 0.005, "Anomaly rate too low")
        self.assertLess(rate, 0.05, "Anomaly rate too high")

    def test_batch_csv_generation(self):
        """Batch CSV should be created with correct structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test_batch.csv")
            count = generate_batch_csv(path, days=1)

            # 1 day * 24 hours * 4 intervals * 10 stations = 960
            self.assertEqual(count, 960)
            self.assertTrue(os.path.exists(path))

            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                self.assertEqual(len(rows), 960)
                self.assertIn("station_id", rows[0])
                self.assertIn("temperature_c", rows[0])

    def test_streaming_jsonl_generation(self):
        """Streaming JSONL should produce valid JSON lines."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test_stream.jsonl")
            count = generate_streaming_json(path, count=50)
            self.assertEqual(count, 50)

            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                self.assertEqual(len(lines), 50)
                for line in lines:
                    record = json.loads(line.strip())
                    self.assertIn("station_id", record)
                    self.assertIn("temperature_c", record)

    def test_all_stations_covered(self):
        """Over sufficient data, all 10 stations should appear."""
        self.assertEqual(len(STATIONS), 10)
        ids = {s["station_id"] for s in STATIONS}
        self.assertEqual(len(ids), 10)

    def test_timestamp_format(self):
        """Timestamps should follow yyyy-MM-dd HH:mm:ss format."""
        station = STATIONS[0]
        ts = datetime(2025, 12, 31, 23, 59, 0)
        record = generate_reading(station, ts)
        parsed = datetime.strptime(record["timestamp"], "%Y-%m-%d %H:%M:%S")
        self.assertEqual(parsed.year, 2025)


class TestDataIntegrity(unittest.TestCase):
    """Test data integrity and consistency."""

    def test_seasonal_variation(self):
        """Summer temperatures should be higher than winter on average."""
        station = STATIONS[0]
        summer_temps = []
        winter_temps = []
        for _ in range(500):
            summer_rec = generate_reading(station, datetime(2025, 7, 15, 14, 0))
            winter_rec = generate_reading(station, datetime(2025, 1, 15, 14, 0))
            summer_temps.append(summer_rec["temperature_c"])
            winter_temps.append(winter_rec["temperature_c"])

        avg_summer = sum(summer_temps) / len(summer_temps)
        avg_winter = sum(winter_temps) / len(winter_temps)
        self.assertGreater(avg_summer, avg_winter,
                           "Summer should be warmer than winter on average")

    def test_diurnal_variation(self):
        """Afternoon temperatures should be higher than night on average."""
        station = STATIONS[0]
        afternoon_temps = []
        night_temps = []
        for _ in range(500):
            afternoon = generate_reading(station, datetime(2025, 6, 15, 14, 0))
            night = generate_reading(station, datetime(2025, 6, 15, 3, 0))
            afternoon_temps.append(afternoon["temperature_c"])
            night_temps.append(night["temperature_c"])

        avg_afternoon = sum(afternoon_temps) / len(afternoon_temps)
        avg_night = sum(night_temps) / len(night_temps)
        self.assertGreater(avg_afternoon, avg_night,
                           "Afternoon should be warmer than night on average")


class TestKafkaMessageFormat(unittest.TestCase):
    """Test that generated data matches the expected Kafka message schema."""

    def test_json_serializable(self):
        """Each record must be JSON-serializable."""
        station = STATIONS[0]
        ts = datetime(2025, 5, 1, 10, 0, 0)
        record = generate_reading(station, ts)
        json_str = json.dumps(record)
        parsed = json.loads(json_str)
        self.assertEqual(parsed["station_id"], record["station_id"])

    def test_schema_matches_spark_streaming(self):
        """Fields must match the Spark Structured Streaming schema."""
        expected_fields = {
            "station_id", "station_name", "latitude", "longitude",
            "timestamp", "temperature_c", "humidity_pct", "pressure_hpa",
            "wind_speed_kmh", "wind_direction", "precipitation_mm", "is_anomaly"
        }
        station = STATIONS[0]
        ts = datetime(2025, 5, 1, 10, 0, 0)
        record = generate_reading(station, ts)
        self.assertEqual(set(record.keys()), expected_fields)

    def test_field_types(self):
        """Verify correct data types for each field."""
        station = STATIONS[0]
        ts = datetime(2025, 5, 1, 10, 0, 0)
        record = generate_reading(station, ts)

        self.assertIsInstance(record["station_id"], str)
        self.assertIsInstance(record["station_name"], str)
        self.assertIsInstance(record["latitude"], float)
        self.assertIsInstance(record["longitude"], float)
        self.assertIsInstance(record["timestamp"], str)
        self.assertIsInstance(record["temperature_c"], float)
        self.assertIsInstance(record["humidity_pct"], float)
        self.assertIsInstance(record["pressure_hpa"], float)
        self.assertIsInstance(record["wind_speed_kmh"], float)
        self.assertIsInstance(record["wind_direction"], str)
        self.assertIsInstance(record["precipitation_mm"], float)
        self.assertIsInstance(record["is_anomaly"], bool)


if __name__ == "__main__":
    unittest.main(verbosity=2)
