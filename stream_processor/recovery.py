# StreamForge - Week 3
# Kafka Changelog State Recovery


# Replays aggregation state from the Kafka changelog topic
# after a worker restart or failure.
#
# This module restores the latest window state so that
# stream processing can continue without losing aggregation data.

import json
from kafka import KafkaConsumer, TopicPartition


BOOTSTRAP_SERVERS = "localhost:9092"
CHANGELOG_TOPIC = "aggregation-changelog"


def recover_state():
    """
    Recover the latest aggregation state from the Kafka changelog topic.
    """

    print("Starting Kafka changelog recovery...")
    print("Changelog topic:", CHANGELOG_TOPIC)

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        key_deserializer=lambda key:
            key.decode("utf-8") if key else None,
        value_deserializer=lambda value:
            json.loads(value.decode("utf-8"))
    )

    try:
        # Get all partitions of the changelog topic
        partitions = consumer.partitions_for_topic(CHANGELOG_TOPIC)

        if not partitions:
            print("No partitions found for changelog topic.")
            return {}

        topic_partitions = [
            TopicPartition(CHANGELOG_TOPIC, partition)
            for partition in sorted(partitions)
        ]

        print("Changelog partitions:", sorted(partitions))

        # Assign partitions manually
        consumer.assign(topic_partitions)

        # Start reading from the beginning
        consumer.seek_to_beginning(*topic_partitions)

        # Store the latest state for each window
        recovered_state = {}

        # Get the current end offset of each partition
        end_offsets = consumer.end_offsets(topic_partitions)

        print("Reading changelog records...")

        while True:

            records = consumer.poll(timeout_ms=1000)

            for partition, messages in records.items():

                for message in messages:

                    if message.key is None:
                        continue

                    key = str(message.key)

                    # Ignore test records
                    if not key.isdigit():
                        continue

                    # Latest record for the same window replaces old state
                    recovered_state[key] = message.value

                    print(
                        f"Recovered window={key} | "
                        f"count={message.value.get('count')} | "
                        f"average={message.value.get('average')}"
                    )

            # Check whether all available records were read
            finished = True

            for topic_partition in topic_partitions:
                current_position = consumer.position(topic_partition)

                if current_position < end_offsets[topic_partition]:
                    finished = False
                    break

            if finished:
                break

        print()
        print(
            f"Recovery completed. "
            f"Recovered {len(recovered_state)} window(s)."
        )

        return recovered_state

    finally:
        consumer.close()
        print("Kafka recovery consumer closed.")


def main():

    recovered_state = recover_state()

    print()
    print("========== RECOVERED STATE ==========")

    if not recovered_state:
        print("No aggregation state found.")
    else:
        for window_key, state in recovered_state.items():

            print(
                f"Window: {window_key} | "
                f"Count: {state.get('count')} | "
                f"Sum: {state.get('sum')} | "
                f"Average: {state.get('average')}"
            )

    print("=====================================")


if __name__ == "__main__":
    main()