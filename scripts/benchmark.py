"""Drives the producer's event generator directly against the running stack's
Kafka broker at increasing rate, then reads the real measured throughput/lag
back out of the `pipeline_metrics` Delta table via the backend API.

Run against a live `docker compose up` stack:
    python scripts/benchmark.py --duration 60 --rate 2000
"""
import argparse
import json
import sys
import time
from pathlib import Path

import requests
from confluent_kafka import Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "producer" / "src"))
from generator import EventGenerator  # noqa: E402


def run_benchmark(bootstrap_servers: str, topic: str, api_base: str, duration: int, rate: float) -> None:
    producer = Producer({"bootstrap.servers": bootstrap_servers, "linger.ms": 20})
    generator = EventGenerator()

    interval = 1.0 / rate if rate > 0 else 0
    sent = 0
    start = time.time()
    print(f"benchmark: producing at target {rate}/s for {duration}s ...")

    while time.time() - start < duration:
        event = generator.next_event()
        producer.produce(topic, key=event["user_id"].encode(), value=json.dumps(event).encode())
        producer.poll(0)
        sent += 1
        if interval:
            time.sleep(interval)
    producer.flush(10)

    elapsed = time.time() - start
    actual_rate = sent / elapsed
    print(f"produced {sent} events in {elapsed:.1f}s -> {actual_rate:.1f} events/sec (producer-side)")

    print("waiting 15s for the streaming job to drain the backlog ...")
    time.sleep(15)

    resp = requests.get(f"{api_base}/api/metrics/summary", timeout=10)
    resp.raise_for_status()
    summary = resp.json()
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:29099")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--api-base", default="http://localhost:8035")
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--rate", type=float, default=2000.0)
    args = parser.parse_args()

    run_benchmark(args.bootstrap_servers, args.topic, args.api_base, args.duration, args.rate)
