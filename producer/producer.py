"""Generate demo transaction events for Kafka."""
from __future__ import annotations
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer


def build_event(i: int) -> dict:
    return {
        "event_id": f"evt-{i}",
        "customer_id": f"cust-{random.randint(1, 1000)}",
        "event_type": random.choice(["purchase", "refund", "login"]),
        "amount": round(random.uniform(5, 500), 2),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def run(broker: str = "localhost:9092", topic: str = "transactions") -> None:
    producer = KafkaProducer(bootstrap_servers=broker, value_serializer=lambda v: json.dumps(v).encode())
    for i in range(1, 101):
        producer.send(topic, build_event(i))
        time.sleep(.1)
    producer.flush()


if __name__ == "__main__":
    run()
