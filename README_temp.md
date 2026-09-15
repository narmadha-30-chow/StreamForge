# StreamForge: Distributed Python Event Processor

## Overview

StreamForge is a distributed stream processing project designed to process large-scale IoT telemetry data using Apache Kafka and Bytewax.

The project simulates telemetry readings from a large fleet of trucks and processes the incoming temperature data in real time.

## Project Domain

**Distributed Systems & Big Data**

## Problem Statement

Modern IoT systems generate a continuous stream of data from thousands of devices. Processing this data efficiently requires scalable, fault-tolerant, and real-time stream processing techniques.

StreamForge addresses this problem by consuming truck telemetry data through Apache Kafka and processing it using a Python-based Bytewax streaming application.

## Use Case

The system represents an IoT fleet management scenario where approximately 50,000 trucks send temperature readings periodically.

Each telemetry record contains:

* Truck ID
* Temperature
* Timestamp

## Architecture

```text
Telemetry Producer
        |
        v
   Apache Kafka
        |
   +----+----+
   |         |
Partition 0 ... Partition 3
        |
        v
 Bytewax Stream Processor
        |
        +--> Filter: Temperature > 0
        |
        +--> Map / Transform
        |
        +--> Group by Truck ID
        |
        +--> 5-Minute Tumbling Window
        |
        v
Average Temperature
        |
        v
Console Output
```

## Week 1 - Kafka Foundation

Week 1 focuses on establishing the Kafka-based telemetry streaming foundation.

### Completed Features

* Local Apache Kafka setup using Docker
* Kafka topic creation
* Four Kafka partitions
* Python telemetry producer
* High-frequency telemetry generation
* JSON-formatted messages
* Truck ID generation
* Temperature generation
* Event timestamp generation
* Kafka message delivery verification

### Telemetry Format

```json
{
  "truck_id": "TRUCK_44327",
  "temperature": 19.52,
  "timestamp": 1789207012.9294739
}
```

## Week 2 - Stream Processing

Week 2 implements real-time stream processing using Bytewax.

### Processing Pipeline

```text
Kafka
  ↓
Consume
  ↓
Parse JSON
  ↓
Filter Temperature > 0
  ↓
Map / Transform
  ↓
Group by Truck ID
  ↓
5-Minute Tumbling Window
  ↓
Average Temperature
  ↓
Console Output
```

### Completed Features

* Bytewax Dataflow
* Kafka stream consumption
* JSON message parsing
* Temperature filtering
* Data transformation
* Truck-based grouping
* Event-time processing
* Five-minute tumbling windows
* Average temperature calculation
* Console result output

## Technologies Used

* Python
* Apache Kafka
* Bytewax
* Docker
* React
* React Flow
* Vite
* Git
* GitHub

## Project Structure

```text
StreamForge/
├── producer/
│   └── producer.py
├── stream_processor/
│   └── stream_processor.py
├── frontend/
├── kafka/
├── tests/
├── docker-compose.yml
├── README.md
└── .gitignore
```

## How to Run

### 1. Start Kafka

```bash
docker compose up -d
```

### 2. Run the Producer

Activate the Python environment containing `confluent-kafka`:

```bash
venv\Scripts\activate
```

Run:

```bash
python producer\producer.py
```

### 3. Run the Bytewax Processor

Activate the Week 2 environment:

```bash
week2_env\Scripts\activate
```

Run:

```bash
python -m bytewax.run stream_processor.stream_processor
```

## Expected Output

```text
[5-MIN WINDOW]
Truck: TRUCK_12345
Average Temperature: 27.45 C
Readings: 5
```

## Future Enhancements

* RocksDB-based state management
* Fault tolerance and state recovery
* Kafka partition rebalancing
* Exactly-once processing
* FastAPI backend
* React monitoring dashboard
* Real-time topology visualization
* Performance monitoring

## Author

Developed as an academic project on distributed stream processing and Big Data technologies.
## Week 4 – Monitoring and Performance Metrics

Week 4 focuses on monitoring the StreamForge distributed stream-processing system using Prometheus.

### Monitoring Features

- Prometheus metrics integrated into the stream processor
- Total events processed monitoring
- Processing lag monitoring
- Events-per-second monitoring
- Metrics exposed through HTTP endpoint on port 8000

### Implemented Metrics

| Metric | Description |
|---|---|
| `streamforge_events_processed_total` | Total telemetry events processed |
| `streamforge_processing_lag_seconds` | Current processing lag in seconds |
| `streamforge_events_per_second` | Current stream processing rate |

### Monitoring Endpoint

The Prometheus metrics are available at:

`http://localhost:8000/metrics`

### Week 4 Result

The StreamForge processor successfully processes Kafka telemetry events while exposing real-time monitoring metrics through Prometheus. Event count, processing lag, and processing rate can be monitored through the metrics endpoint.