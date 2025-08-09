import json
import os
import random
import time
from datetime import datetime

from kafka import KafkaProducer


def create_producer() -> KafkaProducer:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks="1",
    )


def generate_test_event(seq: int) -> dict:
    # ランダムな3D座標とカテゴリを生成
    return {
        "ts": datetime.utcnow().isoformat(),
        "seq": seq,
        "category": random.choice(["A", "B", "C", "D"]),
        "x": random.uniform(-10, 10),
        "y": random.uniform(-10, 10),
        "z": random.uniform(-10, 10),
        "value": random.uniform(0, 100),
    }


def main():
    topic = os.environ.get("KAFKA_TOPIC", "stream3d")
    producer = create_producer()
    seq = 0
    print(f"Producing to topic={topic}")
    while True:
        event = generate_test_event(seq)
        producer.send(topic, event)
        if seq % 50 == 0:
            producer.flush()
        seq += 1
        time.sleep(0.1)


if __name__ == "__main__":
    main()


