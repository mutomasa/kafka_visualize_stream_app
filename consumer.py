import json
import os
from typing import Iterator

from kafka import KafkaConsumer


def create_consumer() -> KafkaConsumer:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
    topic = os.environ.get("KAFKA_TOPIC", "stream3d")
    group_id = os.environ.get("KAFKA_GROUP", "streamlit-viewer")
    offset_reset = os.environ.get("KAFKA_OFFSET_RESET", "earliest")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset=offset_reset,
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
        max_poll_records=50,
    )
    return consumer


def stream_events() -> Iterator[dict]:
    consumer = create_consumer()
    try:
        for msg in consumer:
            yield msg.value
    finally:
        consumer.close()


if __name__ == "__main__":
    for e in stream_events():
        print(e)


