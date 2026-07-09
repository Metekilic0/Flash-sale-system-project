"""
Synthetic E-commerce Clickstream Generator
===========================================
Produces JSON events onto the Kafka topic configured via KAFKA_TOPIC, simulating:

  * NORMAL phase  -> steady traffic from many distinct, well-behaved users/IPs
  * FLASH_SALE phase -> a short burst of very high traffic that includes a
    configurable fraction of "bot/scalper" traffic: a handful of IPs hammering
    a small set of "hot" products far faster than any human could click.

The Spark Structured Streaming job (spark/jobs/streaming_job.py) is expected
to detect and filter out that bot-like behaviour per micro-batch.

Event schema (one JSON object per Kafka message):
    {
        "event_id":    "<uuid4 hex>",
        "user_id":     "user_00042",
        "action":      "VIEW" | "ADD_TO_CART" | "PURCHASE",
        "product_id":  "PRD-0007",
        "timestamp":   "2026-06-18T10:15:30.123456+00:00",
        "ip_address":  "203.0.113.42"
    }
"""

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer] %(levelname)s %(message)s",
)
log = logging.getLogger("producer")

# --------------------------------------------------------------------------- #
# Configuration (all overridable via environment variables / .env)
# --------------------------------------------------------------------------- #
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092").split(",")
TOPIC = os.environ.get("KAFKA_TOPIC", "clickstream-events")

MODE = os.environ.get("PRODUCER_MODE", "auto").lower()              # auto | manual
NORMAL_EPS = float(os.environ.get("PRODUCER_NORMAL_EPS", "100"))
BURST_EPS = float(os.environ.get("PRODUCER_BURST_EPS", "10000"))
NORMAL_DURATION_SEC = float(os.environ.get("PRODUCER_NORMAL_DURATION_SEC", "120"))
BURST_DURATION_SEC = float(os.environ.get("PRODUCER_BURST_DURATION_SEC", "30"))
BOT_RATIO = float(os.environ.get("PRODUCER_BOT_RATIO", "0.15"))
CATALOG_SIZE = int(os.environ.get("PRODUCER_CATALOG_SIZE", "200"))

CONTROL_DIR = "/control"
BURST_TRIGGER_FILE = os.path.join(CONTROL_DIR, "burst_trigger")
STOP_FILE = os.path.join(CONTROL_DIR, "stop")

TICK_SECONDS = 0.1  # how often we wake up to emit a slice of events

# --------------------------------------------------------------------------- #
# Synthetic population
# --------------------------------------------------------------------------- #
PRODUCT_IDS = [f"PRD-{i:04d}" for i in range(1, CATALOG_SIZE + 1)]
HOT_PRODUCT_IDS = PRODUCT_IDS[:5]  # the "flash sale" items everyone wants

NORMAL_USER_IDS = [f"user_{i:06d}" for i in range(1, 20001)]
NORMAL_IPS = [f"10.{a}.{b}.{c}" for a in range(1, 11) for b in range(0, 256, 4) for c in range(1, 250, 4)]

BOT_IPS = [f"198.51.100.{i}" for i in range(1, 9)]          # small fixed pool -> easy to spot
BOT_USER_IDS = [f"bot_{i:03d}" for i in range(1, 9)]

ACTIONS = ["VIEW", "ADD_TO_CART", "PURCHASE"]
ACTION_WEIGHTS_NORMAL = [0.70, 0.20, 0.10]
ACTION_WEIGHTS_BOT = [0.10, 0.20, 0.70]  # bots disproportionately try to "PURCHASE" (scalping)

_running = True


def _handle_signal(signum, _frame):
    global _running
    log.info("Received signal %s, shutting down gracefully...", signum)
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def make_producer() -> KafkaProducer:
    log.info("Connecting to Kafka brokers: %s", BOOTSTRAP_SERVERS)
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",                # wait for min.insync.replicas -> demonstrates durability
        retries=10,
        retry_backoff_ms=300,
        request_timeout_ms=30000,
        linger_ms=50,               # small batching window, important at high EPS
        batch_size=131072,
        max_in_flight_requests_per_connection=5,
    )


def random_timestamp_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def gen_legit_event() -> dict:
    action = random.choices(ACTIONS, weights=ACTION_WEIGHTS_NORMAL)[0]
    return {
        "event_id": uuid.uuid4().hex,
        "user_id": random.choice(NORMAL_USER_IDS),
        "action": action,
        "product_id": random.choice(PRODUCT_IDS),
        "timestamp": random_timestamp_now(),
        "ip_address": random.choice(NORMAL_IPS),
    }


def gen_bot_event() -> dict:
    action = random.choices(ACTIONS, weights=ACTION_WEIGHTS_BOT)[0]
    return {
        "event_id": uuid.uuid4().hex,
        "user_id": random.choice(BOT_USER_IDS),
        "action": action,
        "product_id": random.choice(HOT_PRODUCT_IDS),
        "timestamp": random_timestamp_now(),
        "ip_address": random.choice(BOT_IPS),
    }


def send_event(producer: KafkaProducer, event: dict):
    def on_error(exc: KafkaError):
        log.warning("Failed to deliver event %s: %s", event.get("event_id"), exc)

    producer.send(TOPIC, key=event["product_id"], value=event).add_errback(on_error)


def run_phase(producer: KafkaProducer, eps: float, duration_sec: float, bot_ratio: float, label: str):
    log.info("=== Phase '%s' starting: target=%.0f events/sec, duration=%.0fs, bot_ratio=%.2f ===",
              label, eps, duration_sec, bot_ratio)
    phase_start = time.time()
    end_time = phase_start + duration_sec
    sent_total = 0
    last_log = time.time()

    # Anchor on cumulative elapsed time (rather than accumulating per-tick
    # fractions) so small rounding errors never drift away from the target
    # rate, even for very low or very high EPS values.
    while _running and time.time() < end_time:
        tick_start = time.time()

        target_cumulative = int(eps * (tick_start - phase_start))
        n_events = max(0, target_cumulative - sent_total)

        n_bot = int(n_events * bot_ratio)
        n_legit = n_events - n_bot

        for _ in range(n_legit):
            send_event(producer, gen_legit_event())
        for _ in range(n_bot):
            send_event(producer, gen_bot_event())

        sent_total += n_events

        if time.time() - last_log >= 5:
            log.info("[%s] sent_total=%d (~%.0f events/sec target)", label, sent_total, eps)
            last_log = time.time()

        elapsed = time.time() - tick_start
        sleep_left = TICK_SECONDS - elapsed
        if sleep_left > 0:
            time.sleep(sleep_left)

    # Final top-up so the phase total matches eps * duration_sec exactly,
    # even if the loop's last iteration exited right at the boundary.
    target_final = int(eps * duration_sec)
    if target_final > sent_total:
        remaining = target_final - sent_total
        n_bot = int(remaining * bot_ratio)
        n_legit = remaining - n_bot
        for _ in range(n_legit):
            send_event(producer, gen_legit_event())
        for _ in range(n_bot):
            send_event(producer, gen_bot_event())
        sent_total += remaining

    producer.flush(timeout=10)
    log.info("=== Phase '%s' finished. Total events sent: %d ===", label, sent_total)


def manual_burst_pending() -> bool:
    return os.path.exists(BURST_TRIGGER_FILE)


def consume_manual_trigger():
    try:
        os.remove(BURST_TRIGGER_FILE)
    except FileNotFoundError:
        pass


def main():
    os.makedirs(CONTROL_DIR, exist_ok=True)
    producer = make_producer()
    log.info("Producer ready. Mode=%s", MODE)

    try:
        while _running:
            if os.path.exists(STOP_FILE):
                log.info("Stop file detected, exiting.")
                break

            if MODE == "auto":
                run_phase(producer, NORMAL_EPS, NORMAL_DURATION_SEC, bot_ratio=0.0, label="NORMAL")
                if not _running:
                    break
                run_phase(producer, BURST_EPS, BURST_DURATION_SEC, bot_ratio=BOT_RATIO, label="FLASH_SALE")
            else:  # manual
                if manual_burst_pending():
                    consume_manual_trigger()
                    run_phase(producer, BURST_EPS, BURST_DURATION_SEC, bot_ratio=BOT_RATIO, label="FLASH_SALE(manual)")
                else:
                    run_phase(producer, NORMAL_EPS, 5, bot_ratio=0.0, label="NORMAL(idle)")
    finally:
        log.info("Flushing and closing producer...")
        producer.flush(timeout=10)
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
