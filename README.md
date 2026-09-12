# StreamForge: Distributed Python Event Processor

StreamForge is a distributed Python event processing system designed for real-time IoT telemetry data processing using Apache Kafka and Bytewax.

## Project Overview

The system collects real-time temperature readings from IoT-enabled trucks, streams the data through Apache Kafka, and processes the telemetry using Bytewax.

## Technologies Used

- Python
- Apache Kafka
- Bytewax
- Docker
- React
- React Flow

## Project Workflow

IoT Telemetry
    ↓
Kafka Producer
    ↓
Apache Kafka
    ↓
Kafka Partitions
    ↓
Bytewax Stream Processor
    ↓
5-Minute Tumbling Window
    ↓
Average Temperature

## Features

- Real-time IoT telemetry streaming
- Apache Kafka message processing
- Kafka partition-based streaming
- Temperature filtering
- Truck-wise data grouping
- Five-minute tumbling windows
- Average temperature calculation
- React Flow topology visualization

## Project Status

### Week 1
- Kafka foundation completed
- Telemetry producer implemented
- React Flow topology dashboard created

### Week 2
- Bytewax stream processor implemented
- Kafka consumption integrated
- Temperature filtering added
- Five-minute tumbling window implemented
- Average temperature calculation implemented
