"""
Log Consumer — reads from Kafka topic, persists to PostgreSQL, caches aggregates in Redis.
Exposes Prometheus metrics on :8001/metrics.
"""

import json
import logging
import time

import psycopg2
import redis
from kafka import KafkaConsumer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MESSAGES_CONSUMED = Counter(
    "consumer_messages_total",
    "Total messages consumed from Kafka",
    ["level", "service"],
)
DB_WRITE_LATENCY = Histogram(
    "consumer_db_write_duration_seconds",
    "Latency of PostgreSQL writes",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5],
)
REDIS_WRITE_LATENCY = Histogram(
    "consumer_redis_write_duration_seconds",
    "Latency of Redis writes",
    buckets=[0.0001, 0.001, 0.005, 0.01, 0.05],
)
ERROR_RATE_GAUGE = Gauge("consumer_error_rate_1m", "Error log rate per service", ["service"])
PROCESSING_ERRORS = Counter("consumer_processing_errors_total", "Unhandled processing errors")

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "loganalytics",
    "user": "loguser",
    "password": "logpass",
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS logs (
    id          UUID PRIMARY KEY,
    timestamp   TIMESTAMPTZ NOT NULL,
    service     TEXT NOT NULL,
    level       TEXT NOT NULL,
    method      TEXT,
    endpoint    TEXT,
    status_code INTEGER,
    latency_ms  DOUBLE PRECISION,
    user_id     TEXT,
    request_id  UUID,
    region      TEXT,
    error_message TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_logs_service   ON logs (service);
CREATE INDEX IF NOT EXISTS idx_logs_level     ON logs (level);
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs (timestamp DESC);
"""

INSERT_LOG_SQL = """
INSERT INTO logs (id, timestamp, service, level, method, endpoint, status_code,
                  latency_ms, user_id, request_id, region, error_message)
VALUES (%(id)s, %(timestamp)s, %(service)s, %(level)s, %(method)s, %(endpoint)s,
        %(status_code)s, %(latency_ms)s, %(user_id)s, %(request_id)s, %(region)s,
        %(error_message)s)
ON CONFLICT (id) DO NOTHING;
"""


def connect_postgres(retries: int = 10):
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
            conn.commit()
            logger.info("Connected to PostgreSQL and schema ready")
            return conn
        except Exception as exc:
            logger.warning("Postgres not ready (attempt %d/%d): %s", attempt + 1, retries, exc)
            time.sleep(5)
    raise RuntimeError("Could not connect to PostgreSQL")


def write_to_postgres(conn, log_entry: dict):
    with DB_WRITE_LATENCY.time():
        try:
            with conn.cursor() as cur:
                cur.execute(INSERT_LOG_SQL, {
                    "id": log_entry["id"],
                    "timestamp": log_entry["timestamp"],
                    "service": log_entry["service"],
                    "level": log_entry["level"],
                    "method": log_entry.get("method"),
                    "endpoint": log_entry.get("endpoint"),
                    "status_code": log_entry.get("status_code"),
                    "latency_ms": log_entry.get("latency_ms"),
                    "user_id": log_entry.get("user_id"),
                    "request_id": log_entry.get("request_id"),
                    "region": log_entry.get("region"),
                    "error_message": log_entry.get("error_message"),
                })
            conn.commit()
        except Exception:
            conn.rollback()
            raise


REDIS_CONFIG = {"host": "localhost", "port": 6379, "decode_responses": True}


def connect_redis(retries: int = 10):
    for attempt in range(retries):
        try:
            r = redis.Redis(**REDIS_CONFIG)
            r.ping()
            logger.info("Connected to Redis")
            return r
        except Exception as exc:
            logger.warning("Redis not ready (attempt %d/%d): %s", attempt + 1, retries, exc)
            time.sleep(3)
    raise RuntimeError("Could not connect to Redis")


def update_redis_counters(r: redis.Redis, log_entry: dict):
    service = log_entry["service"]
    level = log_entry["level"]

    with REDIS_WRITE_LATENCY.time():
        pipe = r.pipeline()
        pipe.incr("logs:total")
        pipe.incr(f"logs:service:{service}:total")
        pipe.incr(f"logs:level:{level}:total")
        pipe.incr(f"logs:service:{service}:level:{level}")
        if level == "ERROR":
            key = f"errors:service:{service}:1m"
            pipe.incr(key)
            pipe.expire(key, 60)
        endpoint = log_entry.get("endpoint")
        if endpoint:
            pipe.zincrby("logs:endpoints:hits", 1, endpoint)
        latency = log_entry.get("latency_ms")
        if latency:
            pipe.lpush(f"latency:{service}", latency)
            pipe.ltrim(f"latency:{service}", 0, 999)
        pipe.execute()


def update_error_rate_gauges(r: redis.Redis):
    services = [
        "auth-service", "payment-service", "api-gateway",
        "user-service", "notification-service",
    ]
    for service in services:
        count = r.get(f"errors:service:{service}:1m") or 0
        ERROR_RATE_GAUGE.labels(service=service).set(int(count))


def create_consumer(bootstrap_servers: str, topic: str, retries: int = 10):
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id="log-analytics-consumer",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            logger.info("Kafka consumer ready, subscribed to '%s'", topic)
            return consumer
        except Exception as exc:
            logger.warning("Kafka not ready (attempt %d/%d): %s", attempt + 1, retries, exc)
            time.sleep(5)
    raise RuntimeError("Could not create Kafka consumer")


def main():
    start_http_server(8001)
    logger.info("Prometheus metrics available at :8001/metrics")

    pg_conn = connect_postgres()
    redis_client = connect_redis()
    consumer = create_consumer("localhost:9092", "app-logs")

    gauge_refresh_interval = 10
    last_gauge_refresh = time.time()
    processed = 0

    logger.info("Consuming logs...")
    for message in consumer:
        log_entry = message.value
        service = log_entry.get("service", "unknown")
        level = log_entry.get("level", "INFO")

        try:
            write_to_postgres(pg_conn, log_entry)
            update_redis_counters(redis_client, log_entry)

            MESSAGES_CONSUMED.labels(level=level, service=service).inc()
            processed += 1

            if processed % 100 == 0:
                logger.info("Processed %d messages (latest: %s [%s])", processed, service, level)

            if time.time() - last_gauge_refresh > gauge_refresh_interval:
                update_error_rate_gauges(redis_client)
                last_gauge_refresh = time.time()

        except Exception as exc:
            PROCESSING_ERRORS.inc()
            logger.error("Error processing message from %s: %s", service, exc)


if __name__ == "__main__":
    main()