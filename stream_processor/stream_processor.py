 # StreamForge - Week 4
# Stateful Stream Processing with:
# 5-Minute Tumbling Windows
# RocksDB Persistent State
# Kafka Changelog Recovery
# Prometheus Monitoring

import json
import time
import os

from kafka import KafkaConsumer

from windowing import update_window
from state_store import StateStore
from changelog import ChangelogProducer
from recovery import recover_state
from metrics import MetricsManager


# Kafka configuration
BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "truck-telemetry"
CHANGELOG_TOPIC = "aggregation-changelog"
CONSUMER_GROUP = "streamforge-week3"


# Worker configuration
WORKER_ID = os.getenv("WORKER_ID", "worker-default")

STATE_DB_PATH = os.getenv(
    "STATE_DB_PATH",
    f"rocksdb_state_{WORKER_ID}"
)


class StreamProcessor:

    def __init__(self):

        print("Starting StreamForge Week 4 processor...")

        # Initialize Prometheus monitoring
        self.metrics = MetricsManager(port=8000)

        # Initialize RocksDB state store
        self.state_store = StateStore(STATE_DB_PATH)

        # Initialize Kafka changelog producer
        self.changelog = ChangelogProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=CHANGELOG_TOPIC
        )

        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            key_deserializer=lambda key:
                key.decode("utf-8") if key else None,
            value_deserializer=lambda value:
                json.loads(value.decode("utf-8"))
        )

        print("Kafka consumer connected.")
        print("Input topic:", INPUT_TOPIC)
        print("Consumer group:", CONSUMER_GROUP)
        print("Worker ID:", WORKER_ID)
        print("State DB:", STATE_DB_PATH)

        # Recover state from local RocksDB
        self.state = self.state_store.get_all()

        print(
            "Recovered states from RocksDB:",
            len(self.state)
        )

        # Recover state from Kafka changelog
        print()
        print("Starting Kafka state recovery...")

        kafka_state = recover_state()

        print(
            "Recovered states from Kafka changelog:",
            len(kafka_state)
        )

        # Merge Kafka recovered state into local RocksDB state
        for window_key, window_state in kafka_state.items():

            self.state[window_key] = window_state

            self.state_store.save(
                window_key,
                window_state
            )

        print(
            "Final recovered state:",
            len(self.state)
        )

    def get_timestamp(self, event):

        timestamp = event.get("timestamp")

        if timestamp is None:
            timestamp = time.time()

        timestamp = float(timestamp)

        # Convert milliseconds to seconds
        if timestamp > 10000000000:
            timestamp = timestamp / 1000

        return int(timestamp)

    def get_value(self, event):

        if "value" in event:
            return float(event["value"])

        if "amount" in event:
            return float(event["amount"])

        if "temperature" in event:
            return float(event["temperature"])

        if "price" in event:
            return float(event["price"])

        raise ValueError(
            "No numeric value field found in event."
        )

    def process_event(self, event):

        # Extract event timestamp
        timestamp = self.get_timestamp(event)

        # Extract numeric value
        value = self.get_value(event)

        # Update Prometheus metrics
        self.metrics.record_event(timestamp)

        # Update 5-minute tumbling window
        window = update_window(
            self.state,
            timestamp,
            value
        )

        # Get window key
        window_key = str(window["window_start"])

        # Save latest state to RocksDB
        self.state_store.save(
            window_key,
            window
        )

        # Backup latest state to Kafka changelog
        self.changelog.publish(
            window_key,
            window
        )

        # Display processing result
        print(
            f"Event processed | "
            f"window={window_key} | "
            f"value={value} | "
            f"count={window['count']} | "
            f"average={window['average']:.2f}"
        )

    def run(self):

        print()
        print("Waiting for events...")

        try:

            for message in self.consumer:

                try:

                    event = message.value

                    print()
                    print("Received event:", event)

                    self.process_event(event)

                except Exception as error:

                    print(
                        "Error processing event:",
                        error
                    )

        except KeyboardInterrupt:

            print()
            print("Processor stopped by user.")

        finally:

            self.close()

    def close(self):

        print()
        print("Closing StreamForge processor...")

        try:
            self.consumer.close()
        except Exception:
            pass

        try:
            self.changelog.close()
        except Exception:
            pass

        try:
            self.state_store.close()
        except Exception:
            pass

        print("Processor closed.")


def main():

    processor = StreamProcessor()

    processor.run()


if __name__ == "__main__":
    main()