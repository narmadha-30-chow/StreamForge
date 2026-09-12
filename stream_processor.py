import json
from datetime import datetime, timezone

from confluent_kafka import Consumer


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "truck-telemetry"
GROUP_ID = "streamforge-week2"


consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "latest",
})

consumer.subscribe([KAFKA_TOPIC])


# Store events for 5-minute tumbling windows
windows = {}


def get_window_start(timestamp):
    """
    Convert event timestamp into a 5-minute window.
    """
    window_seconds = 5 * 60
    return int(timestamp // window_seconds) * window_seconds


print("=" * 60)
print("STREAMFORGE - WEEK 2 STREAM PROCESSOR")
print("=" * 60)
print("Kafka Topic :", KAFKA_TOPIC)
print("Filter      : Temperature > 0")
print("Window      : 5 Minutes - Tumbling")
print("Grouping    : Truck ID")
print("Aggregation : Average Temperature")
print("=" * 60)


try:
    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Kafka Error:", msg.error())
            continue

        # -------------------------------------------------
        # 1. CONSUME
        # -------------------------------------------------

        data = json.loads(msg.value().decode("utf-8"))

        truck_id = data["truck_id"]
        temperature = float(data["temperature"])
        timestamp = float(data["timestamp"])

        # -------------------------------------------------
        # 2. FILTER - Temperature > 0
        # -------------------------------------------------

        if temperature <= 0:
            continue

        # -------------------------------------------------
        # 3. MAP / TRANSFORM
        # -------------------------------------------------

        event_time = datetime.fromtimestamp(
            timestamp,
            tz=timezone.utc
        )

        window_start = get_window_start(timestamp)
        window_end = window_start + 300

        # -------------------------------------------------
        # 4. GROUP BY TRUCK ID + 5-MINUTE WINDOW
        # -------------------------------------------------

        key = (truck_id, window_start)

        if key not in windows:
            windows[key] = {
                "sum": 0.0,
                "count": 0
            }

        windows[key]["sum"] += temperature
        windows[key]["count"] += 1

        # -------------------------------------------------
        # 5. CALCULATE CURRENT WINDOW AVERAGE
        # -------------------------------------------------

        average_temperature = (
            windows[key]["sum"] /
            windows[key]["count"]
        )

        print(
            f"Truck: {truck_id} | "
            f"Temp: {temperature:.2f}°C | "
            f"Event Time: {event_time} | "
            f"5-Min Window: {window_start} - {window_end} | "
            f"Average: {average_temperature:.2f}°C"
        )

except KeyboardInterrupt:

    print("\nStopping StreamForge Week 2 processor...")

finally:

    consumer.close()