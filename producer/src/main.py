"""Continuously publishes synthetic events to Kafka at a configurable rate."""
import json
import logging
import os
import time

from confluent_kafka import Producer

from generator import EventGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("producer")


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer({"bootstrap.servers": bootstrap_servers, "linger.ms": 20})


def delivery_report(err, msg) -> None:
    if err is not None:
        logger.error("delivery failed: %s", err)


def run() -> None:
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "events")
    events_per_sec = float(os.environ.get("EVENTS_PER_SEC", "50"))
    max_events = int(os.environ.get("MAX_EVENTS", "0"))  # 0 = unlimited

    producer = build_producer(bootstrap_servers)
    generator = EventGenerator()

    interval = 1.0 / events_per_sec if events_per_sec > 0 else 0
    sent = 0
    logger.info("starting producer -> topic=%s rate=%.1f/s", topic, events_per_sec)

    try:
        while max_events == 0 or sent < max_events:
            event = generator.next_event()
            producer.produce(
                topic,
                key=event["user_id"].encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            sent += 1
            if sent % 1000 == 0:
                logger.info("sent %d events", sent)
            if interval:
                time.sleep(interval)
    finally:
        producer.flush(10)
        logger.info("producer stopped after %d events", sent)


if __name__ == "__main__":
    run()
