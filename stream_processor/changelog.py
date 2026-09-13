# StreamForge - Week 3
# Kafka Aggregation Changelog

import json
from kafka import KafkaProducer


class ChangelogProducer:
    """
    Sends the latest aggregation state to a Kafka
    changelog topic for recovery.
    """

    def __init__(
        self,
        bootstrap_servers="localhost:9092",
        topic="aggregation-changelog"
    ):
        self.topic = topic

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda key: str(key).encode("utf-8"),
            value_serializer=lambda value:
                json.dumps(value).encode("utf-8")
        )

    def publish(self, key, state):
        """
        Publish the latest state to Kafka changelog.
        """

        self.producer.send(
            self.topic,
            key=key,
            value=state
        )

        self.producer.flush()

    def close(self):
        """
        Close Kafka producer.
        """

        self.producer.close()