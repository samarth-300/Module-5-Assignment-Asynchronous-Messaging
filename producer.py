"""Order Producer: publishes order messages to a durable RabbitMQ queue."""
import argparse
import json
import uuid
from datetime import datetime

import pika

QUEUE = "orders"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [producer] {msg}", flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=6, help="number of orders to publish")
    p.add_argument("--start", type=int, default=1, help="first order number (ORD-<n>)")
    p.add_argument("--duplicate", action="store_true",
                   help="publish the LAST message a second time with the SAME event_id")
    args = p.parse_args()

    conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)

    last = None
    for i in range(args.start, args.start + args.count):
        last = {
            "event_id": str(uuid.uuid4()),
            "order_id": f"ORD-{i}",
            "item_id": f"ITEM-{100 + i}",
            "quantity": (i % 5) + 1,
        }
        publish(ch, last)

    if args.duplicate and last:
        log("Re-publishing last message with the SAME event_id (simulated duplicate)")
        publish(ch, last)

    conn.close()


def publish(ch, order):
    ch.basic_publish(
        exchange="",
        routing_key=QUEUE,
        body=json.dumps(order),
        properties=pika.BasicProperties(delivery_mode=2),  # persistent
    )
    log(f"Published {order}")


if __name__ == "__main__":
    main()
