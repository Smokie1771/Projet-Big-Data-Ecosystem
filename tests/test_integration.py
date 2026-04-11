"""
Integration Tests - Validates Kafka, HDFS, and Spark connectivity.
These tests require the Docker infrastructure to be running.
Run with: python -m pytest tests/test_integration.py -v
"""

import unittest
import subprocess
import json
import os
import time


def docker_exec(container, command, timeout=30):
    """Execute a command inside a Docker container and return output."""
    result = subprocess.run(
        ["docker", "exec", container, "bash", "-c", command],
        capture_output=True, text=True, timeout=timeout
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


class TestHDFSIntegration(unittest.TestCase):
    """Test HDFS read/write operations."""

    def test_hdfs_available(self):
        """HDFS namenode should be reachable."""
        code, out, err = docker_exec("namenode", "hdfs dfs -ls /")
        self.assertEqual(code, 0, f"HDFS not available: {err}")

    def test_hdfs_write_read(self):
        """Write and read back a test file from HDFS."""
        test_content = "hello_bigdata_test"
        docker_exec("namenode", f"echo '{test_content}' > /tmp/hdfs_test.txt")
        docker_exec("namenode", "hdfs dfs -mkdir -p /tmp/test")
        docker_exec("namenode", "hdfs dfs -put -f /tmp/hdfs_test.txt /tmp/test/")
        code, out, _ = docker_exec("namenode", "hdfs dfs -cat /tmp/test/hdfs_test.txt")
        self.assertEqual(code, 0)
        self.assertIn(test_content, out)
        # Cleanup
        docker_exec("namenode", "hdfs dfs -rm -r /tmp/test")

    def test_weather_data_on_hdfs(self):
        """Historical weather CSV should be present on HDFS after upload."""
        code, out, _ = docker_exec(
            "namenode", "hdfs dfs -ls /user/weather/raw/weather_historical.csv"
        )
        if code != 0:
            self.skipTest("Weather data not yet uploaded to HDFS")
        self.assertIn("weather_historical.csv", out)

    def test_hdfs_replication(self):
        """Check that HDFS replication factor is configured."""
        code, out, _ = docker_exec("namenode", "hdfs dfs -stat '%r' /user/weather/raw/weather_historical.csv")
        if code != 0:
            self.skipTest("File not available for replication check")
        # Replication factor should be 1 (single datanode setup)
        self.assertEqual(out.strip(), "1")


class TestKafkaIntegration(unittest.TestCase):
    """Test Kafka broker and topic operations."""

    def test_kafka_broker_running(self):
        """Kafka broker should be reachable."""
        code, out, _ = docker_exec(
            "kafka",
            "kafka-broker-api-versions --bootstrap-server kafka:9092 2>&1 | head -1"
        )
        self.assertEqual(code, 0, "Kafka broker not reachable")

    def test_create_topic(self):
        """Should be able to create a test topic."""
        docker_exec("kafka", (
            "kafka-topics --create --bootstrap-server kafka:9092 "
            "--topic test-pipeline-validation --partitions 1 "
            "--replication-factor 1 --if-not-exists"
        ))
        code, out, _ = docker_exec("kafka", (
            "kafka-topics --list --bootstrap-server kafka:9092"
        ))
        self.assertEqual(code, 0)
        self.assertIn("test-pipeline-validation", out)

    def test_produce_consume_message(self):
        """Should be able to produce and consume a message."""
        test_msg = json.dumps({"test": "bigdata", "value": 42})
        # Produce
        docker_exec("kafka", (
            f"echo '{test_msg}' | kafka-console-producer "
            "--bootstrap-server kafka:9092 --topic test-pipeline-validation"
        ))
        # Consume
        code, out, _ = docker_exec("kafka", (
            "kafka-console-consumer --bootstrap-server kafka:9092 "
            "--topic test-pipeline-validation --from-beginning "
            "--timeout-ms 5000 --max-messages 1"
        ), timeout=15)
        self.assertEqual(code, 0)
        self.assertIn("bigdata", out)

    def test_weather_topic_exists(self):
        """The weather-raw topic should exist after pipeline setup."""
        code, out, _ = docker_exec("kafka", (
            "kafka-topics --list --bootstrap-server kafka:9092"
        ))
        if "weather-raw" not in out:
            self.skipTest("weather-raw topic not yet created")
        self.assertIn("weather-raw", out)


class TestSparkIntegration(unittest.TestCase):
    """Test Spark cluster connectivity."""

    def test_spark_master_running(self):
        """Spark master should be running and accessible."""
        code, out, _ = docker_exec("spark-master", "spark-submit --version 2>&1 | head -3")
        self.assertEqual(code, 0, "Spark master not reachable")

    def test_spark_worker_connected(self):
        """At least one Spark worker should be registered."""
        try:
            result = subprocess.run(
                ["curl", "-s", "http://localhost:8080/json/"],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0 and result.stdout:
                data = json.loads(result.stdout)
                workers = data.get("aliveworkers", 0)
                self.assertGreaterEqual(workers, 1, "No Spark workers connected")
        except Exception:
            self.skipTest("Cannot reach Spark Master UI")

    def test_spark_batch_output(self):
        """Batch processing should have produced output files."""
        code, out, _ = docker_exec(
            "spark-master", "ls -la /opt/data/output/ 2>/dev/null"
        )
        if code != 0:
            self.skipTest("Batch output not yet available")
        self.assertIn("daily_aggregations", out)


class TestEndToEnd(unittest.TestCase):
    """End-to-end pipeline validation."""

    def test_generated_data_exists(self):
        """Generated data files should exist on local filesystem."""
        base = os.path.join(os.path.dirname(__file__), "..", "data", "sample")
        csv_path = os.path.join(base, "weather_historical.csv")
        jsonl_path = os.path.join(base, "weather_streaming.jsonl")

        if not os.path.exists(csv_path):
            self.skipTest("Historical CSV not yet generated")
        self.assertTrue(os.path.exists(csv_path))

        if os.path.exists(jsonl_path):
            with open(jsonl_path, "r") as f:
                first_line = json.loads(f.readline())
                self.assertIn("station_id", first_line)


if __name__ == "__main__":
    unittest.main(verbosity=2)
