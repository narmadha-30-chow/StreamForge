from confluent_kafka import Producer
import json
import random
import time

KAFKA_BROKER = "localhost:9092"
TOPIC = "truck-telemetry"

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER
})


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed:", err)
    else:
        print(
            f"Delivered to {msg.topic()} "
            f"[partition {msg.partition()}] "
            f"offset {msg.offset()}"
        )


def generate_telemetry():
    truck_id = f"TRUCK_{random.randint(1, 50000):05d}"
    temperature = round(random.uniform(15.0, 45.0), 2)
    timestamp = time.time()

    return {
        "truck_id": truck_id,
        "temperature": temperature,
        "timestamp": timestamp
    }


try:
    while True:
        data = generate_telemetry()

        producer.produce(
            TOPIC,
            key=data["truck_id"],
            value=json.dumps(data),
            callback=delivery_report
        )

        producer.poll(0)

        print("Sending:", data)

        time.sleep(0.1)

except KeyboardInterrupt:
    print("\nStopping producer...")

finally:
    producer.flush()
    # StreamForge - Week 1
# Kafka Telemetry Producer
#
# This producer generates simulated truck telemetry events
# and publishes them to the truck-telemetry Kafka topic.
#
# Each event contains:
# - truck_id
# - temperature
# - timestamp
#
# The producer is used to test real-time telemetry ingestion
# into the StreamForge stream-processing pipeline.
# StreamForge - Week 1
# Kafka Telemetry Producer
#
# Generates simulated truck telemetry events
# and publishes them to the truck-telemetry Kafka topic.
#
# Each event contains:
# truck_id
# temperature
# timestamp