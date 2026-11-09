"""
Simulated Kafka consumer for e-commerce event streams.
In production, replace with actual Kafka/Confluent connectivity.
"""

import json
import random
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from loguru import logger


class SimulatedKafkaMessage:
    """Mimics a kafka-python ConsumerRecord."""

    def __init__(self, topic: str, key: str, value: dict, partition: int = 0):
        self.topic = topic
        self.partition = partition
        self.offset = random.randint(0, 10_000_000)
        self.key = key.encode()
        self.value = json.dumps(value).encode()
        self.timestamp = int(datetime.utcnow().timestamp() * 1000)


class EcommerceEventProducer:
    """
    Generates realistic e-commerce event messages for simulation.
    Replace produce() with actual Kafka KafkaProducer in production.
    """

    TOPICS = {
        "orders": "ecommerce.orders",
        "inventory": "ecommerce.inventory",
        "demand": "ecommerce.demand_signals",
    }

    def __init__(self, output_dir: str = "./data/raw/streaming"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._message_count = 0

    def _order_event(self) -> dict:
        return {
            "event_type": "ORDER_PLACED",
            "order_id": str(uuid.uuid4()),
            "product_id": f"PROD{random.randint(1, 200):04d}",
            "customer_id": f"CUST{random.randint(1, 10000):05d}",
            "quantity": random.randint(1, 10),
            "unit_price": round(random.uniform(5.0, 2000.0), 2),
            "channel": random.choice(["web", "mobile", "marketplace"]),
            "region": random.choice(["North", "South", "East", "West", "Central"]),
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _inventory_event(self) -> dict:
        product_id = f"PROD{random.randint(1, 200):04d}"
        stock = random.randint(0, 2000)
        return {
            "event_type": "INVENTORY_UPDATE",
            "product_id": product_id,
            "warehouse_id": random.choice(["WH-NORTH", "WH-SOUTH", "WH-EAST", "WH-WEST"]),
            "current_stock": stock,
            "reorder_point": random.randint(50, 300),
            "needs_reorder": stock < random.randint(50, 300),
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _demand_signal(self) -> dict:
        return {
            "event_type": "DEMAND_SIGNAL",
            "product_id": f"PROD{random.randint(1, 200):04d}",
            "search_volume": random.randint(100, 50_000),
            "social_mentions": random.randint(0, 5000),
            "market_demand_idx": round(random.uniform(0.5, 2.5), 2),
            "trend_direction": random.choice(["up", "down", "stable"]),
            "timestamp": datetime.utcnow().isoformat(),
        }

    def produce_batch(self, n: int = 100) -> list[dict]:
        """Produce a batch of mixed events and save to JSONL file."""
        generators = [self._order_event, self._inventory_event, self._demand_signal]
        weights = [0.60, 0.25, 0.15]
        events = []
        for _ in range(n):
            gen = random.choices(generators, weights=weights)[0]
            event = gen()
            events.append(event)
            self._message_count += 1

        # Persist to JSONL for Spark Structured Streaming readStream
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        out = self.output_dir / f"batch_{ts}.jsonl"
        with open(out, "w") as fh:
            for e in events:
                fh.write(json.dumps(e) + "\n")

        logger.debug(f"Produced {n} events → {out}")
        return events

    def stream(
        self,
        events_per_second: int = 10,
        duration_seconds: Optional[int] = None,
        callback: Optional[Callable[[dict], None]] = None,
    ) -> None:
        """
        Continuously produce events at the given rate.

        Args:
            events_per_second: Target throughput.
            duration_seconds: Stop after N seconds (None = run forever).
            callback: Optional function called for each event.
        """
        logger.info(f"Starting event stream at {events_per_second} events/s …")
        start = time.time()
        total = 0
        batch = max(1, events_per_second)
        sleep = 1.0

        try:
            while True:
                events = self.produce_batch(batch)
                total += len(events)
                if callback:
                    for e in events:
                        callback(e)

                elapsed = time.time() - start
                if duration_seconds and elapsed >= duration_seconds:
                    break
                time.sleep(sleep)

        except KeyboardInterrupt:
            logger.info("Stream interrupted by user.")
        finally:
            logger.success(f"Produced {total:,} events in {time.time() - start:.1f}s")


if __name__ == "__main__":
    producer = EcommerceEventProducer()
    producer.stream(events_per_second=5, duration_seconds=30)
