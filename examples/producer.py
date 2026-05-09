"""Synthetic e-commerce order producer.

Generates Avro-encoded orders, registers the schema with the local Schema
Registry, and pushes them to the `orders` topic at a configurable rate. Vivid
enough to demo retrieval queries like "high-value orders from California" or
"refunded apparel purchases" once they land in the vector store.

Usage:
    python examples/producer.py
    python examples/producer.py --rate 5 --count 1000

Requires the docker compose stack to be up:
    docker compose up -d
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "order.avsc"
DEFAULT_TOPIC = "orders"

REGIONS = ["US_WEST", "US_EAST", "EU", "APAC", "LATAM"]
STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled", "refunded"]
CHANNELS = ["web", "mobile", "in_store", "partner_api"]

CATALOG = [
    # (sku, name, category, base_price)
    ("SKU-1001", "Merino wool hoodie",         "apparel",     89.00),
    ("SKU-1002", "Cast-iron skillet 12in",     "kitchen",     45.00),
    ("SKU-1003", "Bamboo cutting board",       "kitchen",     22.50),
    ("SKU-1004", "Noise-cancelling headphones","electronics", 299.00),
    ("SKU-1005", "Mechanical keyboard 65%",    "electronics", 149.00),
    ("SKU-1006", "Yoga mat (cork)",            "fitness",     59.00),
    ("SKU-1007", "Trail running shoes",        "fitness",    139.00),
    ("SKU-1008", "Single origin coffee 1lb",   "grocery",     24.00),
    ("SKU-1009", "Dark chocolate bar",         "grocery",      6.50),
    ("SKU-1010", "Linen bed sheet set",        "home",       189.00),
    ("SKU-1011", "Espresso machine",           "kitchen",    549.00),
    ("SKU-1012", "Wireless mouse",             "electronics", 39.00),
    ("SKU-1013", "Down winter jacket",         "apparel",    349.00),
    ("SKU-1014", "Cycling jersey",             "fitness",     79.00),
    ("SKU-1015", "Cold brew carafe",           "kitchen",     34.00),
]

NOTE_SAMPLES = [
    None, None, None,  # most orders have no note
    "gift wrap please",
    "leave at door",
    "rush delivery requested",
    "address confirmed via email",
    "second attempt — first delivery missed",
    "promo code BLACKFRIDAY applied",
    "VIP customer — handle with care",
]


def load_schema() -> str:
    return SCHEMA_PATH.read_text()


def make_order() -> dict:
    n_items = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5])[0]
    items = []
    for _ in range(n_items):
        sku, name, category, base = random.choice(CATALOG)
        qty = random.choices([1, 2, 3], weights=[80, 15, 5])[0]
        # small per-order price jitter
        price = round(base * random.uniform(0.95, 1.05), 2)
        items.append({"sku": sku, "name": name, "category": category, "qty": qty, "price": price})
    total = round(sum(i["qty"] * i["price"] for i in items), 2)
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"cust-{random.randint(1, 5000):05d}",
        "region": random.choice(REGIONS),
        "status": random.choices(STATUSES, weights=[10, 35, 25, 20, 5, 5])[0],
        "currency": "USD",
        "total": total,
        "items": items,
        "channel": random.choices(CHANNELS, weights=[55, 30, 10, 5])[0],
        "notes": random.choice(NOTE_SAMPLES),
        "timestamp": int(time.time() * 1000),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] delivery failed: {err}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic order producer for streamcontext.")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers.")
    parser.add_argument("--schema-registry", default="http://localhost:8081")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--rate", type=float, default=2.0, help="Messages per second.")
    parser.add_argument("--count", type=int, default=0, help="Total messages to send (0 = forever).")
    args = parser.parse_args()

    sr = SchemaRegistryClient({"url": args.schema_registry})
    schema_str = load_schema()
    avro_ser = AvroSerializer(sr, schema_str)
    key_ser = StringSerializer("utf_8")

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 50})

    print(
        f"[producer] sending Avro orders to '{args.topic}' at {args.rate}/s "
        f"(count={'∞' if args.count == 0 else args.count}). Ctrl-C to stop.",
        file=sys.stderr,
    )

    stop = {"flag": False}

    def _sigint(_sig, _frm):
        stop["flag"] = True
        print("[producer] shutdown requested, flushing…", file=sys.stderr)

    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    sent = 0
    interval = 1.0 / args.rate if args.rate > 0 else 0
    while not stop["flag"]:
        order = make_order()
        try:
            value = avro_ser(
                order, SerializationContext(args.topic, MessageField.VALUE)
            )
            key = key_ser(order["customer_id"])
            producer.produce(
                topic=args.topic,
                key=key,
                value=value,
                on_delivery=delivery_report,
            )
            producer.poll(0)
            sent += 1
            if sent % 50 == 0:
                print(f"[producer] sent {sent} orders (last: {json.dumps(order, default=str)[:120]}…)", file=sys.stderr)
            if args.count and sent >= args.count:
                break
        except BufferError:
            producer.poll(0.5)
            continue
        except Exception as exc:
            print(f"[producer] error: {exc}", file=sys.stderr)
            time.sleep(1)

        if interval:
            time.sleep(interval)

    producer.flush(10)
    print(f"[producer] done. total sent={sent}", file=sys.stderr)


if __name__ == "__main__":
    main()
