import os
import json
import time
import random
from kafka import KafkaProducer, KafkaConsumer


def main():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
    topic = os.environ.get("KAFKA_TOPIC", "stream3d")
    print("bootstrap:", bootstrap, "topic:", topic, flush=True)

    # produce a few test messages
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    n = 10
    for i in range(n):
        producer.send(
            topic,
            {
                "probe": True,
                "i": i,
                "x": random.random(),
                "y": random.random(),
                "z": random.random(),
                "t": time.time(),
            },
        )
    producer.flush()
    print("produced", n, "messages", flush=True)

    # consume with a fresh group from earliest
    gid = f"probe-{int(time.time())}-{random.randint(0,9999)}"
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=gid,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=4000,
    )
    count = 0
    for _ in consumer:
        count += 1
    print("consumed", count, flush=True)


if __name__ == "__main__":
    main()


