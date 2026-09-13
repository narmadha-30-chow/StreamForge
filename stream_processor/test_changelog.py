# StreamForge - Week 3
# Test Kafka Aggregation Changelog

from changelog import ChangelogProducer


def main():
    print("Starting Kafka changelog test...")

    producer = ChangelogProducer(
        bootstrap_servers="localhost:9092",
        topic="aggregation-changelog"
    )

    test_state = {
        "window_start": 1757750400,
        "count": 10,
        "sum": 750.0,
        "average": 75.0
    }

    producer.publish(
        "test-window",
        test_state
    )

    producer.close()

    print("Aggregation state successfully sent to Kafka.")
    print(test_state)


if __name__ == "__main__":
    main()