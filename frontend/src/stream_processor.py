import json
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
from bytewax.connectors.kafka import operators as kop
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import EventClock, TumblingWindower, fold_window


# ---------------------------------------------------------
# Kafka Configuration
# ---------------------------------------------------------

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "truck-telemetry"


# ---------------------------------------------------------
# Create Bytewax Dataflow
# ---------------------------------------------------------

flow = Dataflow("streamforge_week2")


# ---------------------------------------------------------
# 1. CONSUME
# Kafka -> Bytewax
# ---------------------------------------------------------

kafka_input = kop.input(
    "consume_kafka",
    flow,
    brokers=[KAFKA_BROKER],
    topics=[KAFKA_TOPIC],
)


# ---------------------------------------------------------
# 2. MAP
# Convert Kafka message into Python dictionary
# ---------------------------------------------------------

def parse_message(message):
    data = json.loads(message.value.decode("utf-8"))

    return {
        "truck_id": data["truck_id"],
        "temperature": float(data["temperature"]),
        "timestamp": float(data["timestamp"]),
    }


parsed = op.map(
    "parse_json",
    kafka_input.oks,
    parse_message,
)


# ---------------------------------------------------------
# 3. FILTER
# Keep only temperature > 0
# ---------------------------------------------------------

filtered = op.filter(
    "temperature_greater_than_zero",
    parsed,
    lambda x: x["temperature"] > 0,
)


# ---------------------------------------------------------
# 4. MAP / TRANSFORM
# Convert Unix timestamp into UTC datetime
# ---------------------------------------------------------

def transform_event(event):
    event["event_time"] = datetime.fromtimestamp(
        event["timestamp"],
        tz=timezone.utc,
    )

    return event


transformed = op.map(
    "map_event",
    filtered,
    transform_event,
)


# ---------------------------------------------------------
# 5. GROUP BY TRUCK ID
# ---------------------------------------------------------

keyed = op.key_on(
    "group_by_truck",
    transformed,
    lambda event: event["truck_id"],
)


# ---------------------------------------------------------
# 6. EVENT-TIME CLOCK
# Use the event's timestamp, NOT processing time
# ---------------------------------------------------------

clock = EventClock(
    ts_getter=lambda event: event["event_time"],
    wait_for_system_duration=timedelta(seconds=10),
)


# ---------------------------------------------------------
# 7. 5-MINUTE TUMBLING WINDOW
# ---------------------------------------------------------

windower = TumblingWindower(
    length=timedelta(minutes=5),
    align_to=datetime(
        2026,
        1,
        1,
        tzinfo=timezone.utc,
    ),
)


# ---------------------------------------------------------
# 8. WINDOW AGGREGATION
# Calculate temperature sum and count
# ---------------------------------------------------------

def create_accumulator():
    return {
        "sum": 0.0,
        "count": 0,
    }


def add_temperature(accumulator, event):
    accumulator["sum"] += event["temperature"]
    accumulator["count"] += 1

    return accumulator


def merge_accumulators(acc1, acc2):
    return {
        "sum": acc1["sum"] + acc2["sum"],
        "count": acc1["count"] + acc2["count"],
    }


windowed = fold_window(
    "five_minute_window",
    keyed,
    clock,
    windower,
    create_accumulator,
    add_temperature,
    merge_accumulators,
)


# ---------------------------------------------------------
# 9. CALCULATE AVERAGE
# ---------------------------------------------------------

def calculate_average(item):
    truck_id, (window_id, accumulator) = item

    total = accumulator["sum"]
    count = accumulator["count"]

    average = total / count if count > 0 else 0

    return {
        "truck_id": truck_id,
        "window_id": window_id,
        "average_temperature": round(average, 2),
        "readings": count,
    }


results = op.map(
    "calculate_average",
    windowed.down,
    calculate_average,
)


# ---------------------------------------------------------
# 10. DISPLAY RESULTS
# ---------------------------------------------------------

op.output(
    "output_results",
    results,
    StdOutSink(),
)