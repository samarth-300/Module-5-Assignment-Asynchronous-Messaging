"""Order Worker: consumes orders, ACKs after processing, ignores duplicate event_ids."""
import argparse
import json
import os
import time
from datetime import datetime

import pika

QUEUE = "orders"
PROCESSED_FILE = "processed_events.txt"   # shared by all workers


def make_logger(name):
    def log(msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [{name}] {msg}", flush=True)
    return log


def load_processed():
    if not os.path.exists(PROCESSED_FILE):
        return set()
    with open(PROCESSED_FILE) as f:
        return {line.strip() for line in f if line.strip()}


def mark_processed(event_id):
    with open(PROCESSED_FILE, "a") as f:
        f.write(event_id + "\n")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--name", required=True, help="worker name, e.g. worker-A")
    p.add_argument("--delay", type=float, default=2.0, help="seconds of simulated work")
    p.add_argument("--crash-on", default=None,
                   help="order_id (e.g. ORD-1): kill this process on that order BEFORE the ACK")
    p.add_argument("--crash-after-process", action="store_true",
                   help="with --crash-on: do the business action first, then die before ACK")
    args = p.parse_args()
    log = make_logger(args.name)

    def on_message(ch, method, props, body):
        order = json.loads(body)
        eid, oid = order["event_id"], order["order_id"]
        log(f"RECEIVED order={oid} event_id={eid[:8]} redelivered={method.redelivered}")

        crash_here = args.crash_on == oid

        if crash_here and not args.crash_after_process:
            log(f"!!! Simulating crash on {oid} BEFORE processing/ACK. Exiting now.")
            os._exit(1)  # connection drops with message unacked

        if eid in load_processed():
            log(f"DUPLICATE detected event_id={eid[:8]} order={oid} -> skipping business action")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            log(f"ACK sent (duplicate) order={oid}")
            return

        log(f"PROCESSING order={oid} item={order['item_id']} qty={order['quantity']}")
        time.sleep(args.delay)            # the "business action"
        mark_processed(eid)

        if crash_here and args.crash_after_process:
            log(f"!!! Simulating crash on {oid} AFTER processing but BEFORE ACK. Exiting now.")
            os._exit(1)

        log(f"DONE order={oid}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        log(f"ACK sent order={oid}")

    conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.basic_qos(prefetch_count=1)        # fair dispatch: one unacked message at a time
    ch.basic_consume(queue=QUEUE, on_message_callback=on_message, auto_ack=False)

    log("Waiting for messages. Ctrl+C to stop.")
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        log("Stopping.")
        conn.close()


if __name__ == "__main__":
    main()
