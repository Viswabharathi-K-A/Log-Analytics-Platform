"""
Log Producer — generates realistic application logs and sends them to Kafka.
Exposes Prometheus metrics on :8000/metrics.
"""

import json
import random
import time
import uuid
import os
import logging
from datetime import datetime, timezone

from kafka import KafkaProducer
from prometheus_client import Counter, Histogram, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MESSAGES_PRODUCED = Counter(
    "producer_messages_total",
    "Total log messages sent to Kafka",
    ["level", "service"],
)
PRODUCE_LATENCY = Histogram(
    "producer_send_duration_seconds",
    "Time taken to send a message to Kafka",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)
PRODUCE_ERRORS = Counter("producer_errors_total", "Failed Kafka sends")

SERVICES = ["auth-service", "payment-service", "api-gateway", "user-service", "notification-service"]
LOG_LEVELS = ["INFO", "INFO", "INFO", "WARNING", "ERROR"]
HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
ENDPOINTS = [
    "/api/v1/users",
    "/api/v1/auth/login",
    "/api/v1/payments/charge",
    "/api/v1/notifications",
    "/api/v1/products",
    "/health",
    "/metrics",
]
STATUS_CODES = {
    "INFO": [200, 201, 204, 301, 302],
    "WARNING": [400, 401, 403, 429],
    "ERROR": [500, 502, 503, 504],
}
ERROR_MESSAGES = [
    "Database connection timeout",
    "Redis cache miss — falling back to DB",
    "Payment gateway returned non-200",
    "JWT validation failed",
    "Rate limit exceeded",
    "Upstream service unavailable",
]


def generate_log_entry(service: str, level: str) -> dict:
    method = random.choice(HTTP_METHODS)
    endpoint = random.choice(ENDPOINTS)
    status = random.choice(STATUS_CODES[level])
    latency_ms = round(random.lognormvariate(3.5, 0.8), 2)

    entry = {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "level": level,
        "method": method,
        "endpoint": endpoint,
        "status_code": status,
        "latency_ms": latency_ms,
        "user_id": f"user_{random.randint(1000, 9999)}" if random.random() > 0.3 else None,
        "request_id": str(uuid.uuid4()),
        "region": random.choice(["us-east-1", "us-west-2", "eu-west-1"]),
    }

    if level == "ERROR":
        entry["error_message"] = random.choice(ERROR_MESSAGES)
        entry["stack_trace"] = f"Traceback (most recent call last):\n  ...\n{entry['error_message']}"

    return entry


def create_producer(bootstrap_servers: str, retries: int = 10) -> KafkaProducer:
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                compression_type="gzip",
            )
            logger.info("Connected to Kafka at %s", bootstrap_servers)
            return producer
        except Exception as exc:
            logger.warning("Kafka not ready (attempt %d/%d): %s", attempt + 1, retries, exc)
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after %d retries" % retries)


def main():
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "app-logs"
    produce_interval = 0.5

    start_http_server(8000)
    logger.info("Prometheus metrics available at :8000/metrics")

    producer = create_producer(kafka_bootstrap)

    logger.info("Starting log production -> topic '%s'", topic)
    while True:
        service = random.choice(SERVICES)
        level = random.choices(LOG_LEVELS, weights=[60, 60, 60, 15, 5])[0]
        log_entry = generate_log_entry(service, level)

        try:
            start = time.perf_counter()
            future = producer.send(topic, value=log_entry, key=service.encode())
            future.get(timeout=5)
            elapsed = time.perf_counter() - start

            PRODUCE_LATENCY.observe(elapsed)
            MESSAGES_PRODUCED.labels(level=level, service=service).inc()

            logger.debug("Sent [%s] %s %s -> %s", level, log_entry["method"], log_entry["endpoint"], service)

        except Exception as exc:
            PRODUCE_ERRORS.inc()
            logger.error("Failed to send message: %s", exc)

        time.sleep(produce_interval)


if __name__ == "__main__":
    main()