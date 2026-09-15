# Week 4 – Monitoring and Performance Metrics

## 1. Overview

Week 4 of the StreamForge project focuses on monitoring the real-time stream processing system using Prometheus. Monitoring metrics are integrated into the stream processor to measure event processing activity, processing lag, and stream processing rate.

## 2. Prometheus Monitoring

Prometheus is used to collect and expose real-time performance metrics from the StreamForge stream processor. The metrics server runs on port 8000 and provides a `/metrics` endpoint that can be accessed through a web browser.

## 3. Implemented Metrics

### Events Processed

`streamforge_events_processed_total` is a counter that records the total number of telemetry events processed by the stream processor.

### Processing Lag

`streamforge_processing_lag_seconds` measures the time difference between the event timestamp and the current processing time.

### Events Per Second

`streamforge_events_per_second` represents the current rate at which telemetry events are processed.

## 4. Monitoring Workflow

The monitoring workflow follows these steps:

1. Telemetry events are received from the Kafka topic.
2. The stream processor processes each event.
3. Prometheus metrics are updated for every processed event.
4. Processing lag is calculated using the event timestamp.
5. The current processing rate is calculated.
6. Metrics are exposed through the Prometheus HTTP endpoint.
7. The metrics can be viewed using a web browser.

## 5. Testing

The Prometheus metrics server was successfully started on port 8000. Telemetry events were successfully processed by the stream processor, and monitoring metrics were updated during execution.

## 6. Week 4 Result

The Week 4 monitoring implementation successfully provides visibility into the StreamForge stream-processing workload. Event counts, processing lag, and processing rate can be monitored through the Prometheus metrics endpoint.
Test-NetConnection localhost -Port 9092
## 6. Monitoring Test Evidence

The StreamForge Week 4 processor was executed successfully with Prometheus monitoring enabled.

The Prometheus metrics endpoint was verified using:

`http://localhost:8000/metrics`

The following metrics were observed during testing:

- `streamforge_events_processed_total`
- `streamforge_processing_lag_seconds`
- `streamforge_events_per_second`

The metrics endpoint successfully returned the current monitoring values while Kafka telemetry events were being processed.

## 7. Conclusion

Week 4 successfully adds monitoring and performance visibility to the StreamForge stream-processing system. Prometheus provides real-time information about processed events, processing lag, and event-processing rate.