# flink_app/main.py
import json
import os
import time
import threading
from typing import Dict, Any, Optional

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

# Sinks Python
import redis
import requests
import psycopg2


# ---------- Helpers: Postgres lookup with in-process cache ----------
class ContentLookup:
    """
    Tiny lookup helper that keeps a local LRU-ish cache for content metadata.
    Falls back to SELECT when miss.
    """
    def __init__(self):
        self.host = os.environ.get("PG_HOST", "postgres")
        self.db = os.environ.get("PG_DB", "streaming_db")
        self.user = os.environ.get("PG_USER", "postgres")
        self.password = os.environ.get("PG_PASSWORD", "postgres")
        self._conn = None
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def _connect(self):
        if self._conn is None:
            self._conn = psycopg2.connect(
                host=self.host, dbname=self.db, user=self.user, password=self.password
            )
            self._conn.autocommit = True

    def get(self, content_id: str) -> Optional[Dict[str, Any]]:
        # very small cache, good enough for the exercise
        with self._lock:
            if content_id in self._cache:
                return self._cache[content_id]
        # miss: query postgres
        self._connect()
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT content_type, length_seconds FROM content WHERE id = %s LIMIT 1",
                (content_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            data = {"content_type": row[0], "length_seconds": row[1]}
            with self._lock:
                self._cache[content_id] = data
            return data


# ---------- Sinks ----------
class RedisSink:
    """
    Writes rolling 10-min engagement to Redis using a sorted set.
    Key: engagement:10min
    Score: accumulated engagement_seconds by content_id
    TTL ~ 10 minutes (we reset TTL on each write for simplicity).
    """
    def __init__(self):
        self.client = redis.Redis(host=os.environ.get("REDIS_HOST", "redis"), port=6379)

    def write(self, content_id: str, engagement_seconds: float):
        key = "engagement:10min"
        # increment score
        self.client.zincrby(key, engagement_seconds, content_id)
        # keep a TTL so it's 10-min rolling approx
        self.client.expire(key, 600)


class ExternalSink:
    """
    Simple HTTP POST sink to an external endpoint.
    """
    def __init__(self):
        self.url = os.environ.get("EXTERNAL_URL", "")

    def write(self, payload: Dict[str, Any]):
        if not self.url:
            return
        try:
            requests.post(self.url, json=payload, timeout=3)
        except Exception:
            # swallow errors for the demo
            pass


# ---------- Main Flink job ----------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka Source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.environ.get("BOOTSTRAP_SERVERS", "kafka:29092"))
        .set_topics(os.environ.get("KAFKA_TOPIC", "pg.public.engagement_events"))
        .set_group_id("pyflink_consumer_group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka-debezium"
    )

    # Resources shared by sinks
    lookup = ContentLookup()
    redis_sink = RedisSink()
    ext_sink = ExternalSink()

    # Processing: parse Debezium → enrich → compute → fan out
    def process(raw: str):
        """
        raw: JSON string from Debezium (envelope)
        returns enriched dict or None if invalid
        """
        try:
            msg = json.loads(raw)
            after = msg.get("after") or {}
            # 'after' contains our actual row
            eid = after.get("id")
            user_id = after.get("user_id")
            content_id = after.get("content_id")
            duration_ms = after.get("duration_ms")
            event_ts = after.get("event_ts")  # ISO-8601 string ending with Z

            if not (eid and content_id and event_ts):
                return None

            # Lookup content metadata
            meta = lookup.get(str(content_id))
            content_type = meta["content_type"] if meta else None
            length_seconds = meta["length_seconds"] if meta else None

            engagement_seconds = None
            engagement_pct = None
            if duration_ms is not None:
                engagement_seconds = round(float(duration_ms) / 1000.0, 3)
                if length_seconds:
                    engagement_pct = round(engagement_seconds / float(length_seconds), 4)

            out = {
                "id": eid,
                "user_id": user_id,
                "content_id": content_id,
                "event_ts": event_ts,
                "duration_ms": duration_ms,
                "engagement_seconds": engagement_seconds,
                "engagement_pct": engagement_pct,
                "content_type": content_type,
                "length_seconds": length_seconds,
            }

            # Sinks
            if engagement_seconds:
                redis_sink.write(str(content_id), engagement_seconds)
            ext_sink.write(out)

            # Also print for debug
            print(json.dumps(out, ensure_ascii=False))
            return out
        except Exception as e:
            # swallow for the demo
            print(f"parse_error: {e} raw={raw[:200]}")
            return None

    stream.map(process)

    env.execute("PyFlink Debezium → Enrich → Redis + External")


if __name__ == "__main__":
    main()