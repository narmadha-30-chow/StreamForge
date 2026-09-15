# StreamForge - Week 4
# Prometheus Monitoring Metrics
# StreamForge - Week 4
# Prometheus Metrics Manager
#
# Tracks processed events, processing lag,
# and events processed per second.

import time

from prometheus_client import Counter, Gauge, start_http_server


# Total number of processed events
events_processed = Counter(
    "streamforge_events_processed_total",
    "Total number of telemetry events processed"
)


# Current processing lag in seconds
processing_lag = Gauge(
    "streamforge_processing_lag_seconds",
    "Current event processing lag in seconds"
)


# Current events processed per second
events_per_second = Gauge(
    "streamforge_events_per_second",
    "Current stream processing rate"
)


class MetricsManager:
    """
    Manages Prometheus metrics for StreamForge.
    """

    def __init__(self, port=8000):

        self.port = port
        self.start_time = time.time()

        # Start Prometheus HTTP metrics server
        start_http_server(self.port)

        print(
            f"Prometheus metrics server started on port {self.port}"
        )

    def record_event(self, event_timestamp):

        # Increment processed event counter
        events_processed.inc()

        # Calculate event processing lag
        current_time = time.time()

        lag = current_time - float(event_timestamp)

        if lag < 0:
            lag = 0

        processing_lag.set(lag)

        # Calculate events per second
        elapsed_time = current_time - self.start_time

        if elapsed_time > 0:

            rate = events_processed._value.get() / elapsed_time

            events_per_second.set(rate)

        print(
            f"Metrics updated | "
            f"lag={lag:.2f}s | "
            f"events/sec={events_per_second._value.get():.2f}"
        )