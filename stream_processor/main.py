#!/usr/bin/env python3
import json
import os
import time
import threading
from typing import Dict, Any, Optional

import redis
import requests
import psycopg2
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "pg.public.engagement_events")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
EXTERNAL_URL = os.environ.get("EXTERNAL_URL", "http://external-system:5001/webhook")
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_DB = os.environ.get("PG_DB", "streaming_db")
PG_USER = os.environ.get("PG_USER", "user")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "password")

class ContentLookup:
    def __init__(self):
        self._conn = None
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        print("🔍 ContentLookup initialized")

    def _connect(self):
        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            try:
                if self._conn is None or self._conn.closed:
                    self._conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
                    self._conn.autocommit = True
                    print("✅ PostgreSQL connected")
                return
            except Exception as e:
                retry_count += 1
                print(f"❌ PostgreSQL attempt {retry_count}: {e}")
                time.sleep(2)

    def get(self, content_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            if content_id in self._cache:
                return self._cache[content_id]
        
        try:
            self._connect()
            with self._conn.cursor() as cur:
                cur.execute("SELECT content_type, length_seconds FROM content WHERE id = %s", (content_id,))
                row = cur.fetchone()
                if row:
                    data = {"content_type": row[0], "length_seconds": row[1]}
                    with self._lock:
                        self._cache[content_id] = data
                    return data
        except Exception as e:
            print(f"❌ Database lookup error for {content_id}: {e}")
        return None

class StreamProcessor:
    def __init__(self):
        self.content_lookup = ContentLookup()
        self._init_redis()
        self.processed_count = 0
        print("🚀 StreamProcessor initialized")
        
    def _init_redis(self):
        try:
            self.redis_client = redis.Redis(host=REDIS_HOST, port=6379)
            self.redis_client.ping()
            print("✅ Redis connected")
        except Exception as e:
            print(f"❌ Redis failed: {e}")
            self.redis_client = None

    def process_message(self, raw_message: str) -> Optional[Dict[str, Any]]:
        try:
            # Parse complete Debezium message
            msg = json.loads(raw_message)
            
            # Skip deletes
            if msg.get("op") == "d":
                return None
                
            # Extract from 'after' (new Debezium format)
            after = msg.get("after", {})
            
            event_id = after.get("id")
            user_id = after.get("user_id")
            content_id = after.get("content_id")
            duration_ms = after.get("duration_ms")
            event_ts = after.get("event_ts")
            event_type = after.get("event_type")
            device = after.get("device")

            if not (event_id and content_id):
                return None

            # Enrichment
            meta = self.content_lookup.get(str(content_id))
            content_type = meta["content_type"] if meta else None
            length_seconds = meta["length_seconds"] if meta else None

            # Calculations
            engagement_seconds = None
            engagement_pct = None
            if duration_ms and duration_ms > 0:
                engagement_seconds = round(float(duration_ms) / 1000.0, 3)
                if length_seconds and length_seconds > 0:
                    engagement_pct = round(engagement_seconds / float(length_seconds), 4)

            return {
                "id": event_id,
                "user_id": user_id,
                "content_id": content_id,
                "event_type": event_type,
                "event_ts": event_ts,
                "duration_ms": duration_ms,
                "engagement_seconds": engagement_seconds,
                "engagement_pct": engagement_pct,
                "content_type": content_type,
                "length_seconds": length_seconds,
                "device": device
            }
        except Exception as e:
            print(f"❌ Error processing: {e}")
            return None

    def send_to_redis(self, content_id: str, engagement_seconds: float):
        if self.redis_client:
            try:
                key = "engagement:10min"
                self.redis_client.zincrby(key, engagement_seconds, content_id)
                self.redis_client.expire(key, 600)
            except Exception as e:
                print(f"❌ Redis error: {e}")

    def send_to_external(self, event: Dict[str, Any]):
        try:
            requests.post(EXTERNAL_URL, json=event, timeout=3)
        except Exception as e:
            print(f"❌ External error: {e}")

    def run(self):
        print("🚀 Starting stream processor...")
        print(f"📡 Topic: {KAFKA_TOPIC}")
        time.sleep(5)

        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                group_id='python_stream_processor',
                value_deserializer=lambda m: m.decode('utf-8'),
                auto_offset_reset='earliest'
            )
            print("✅ Connected to Kafka!")
        except Exception as e:
            print(f"❌ Kafka failed: {e}")
            return

        try:
            for message in consumer:
                enriched_event = self.process_message(message.value)
                
                if enriched_event:
                    content_id = enriched_event["content_id"]
                    engagement_seconds = enriched_event["engagement_seconds"]
                    
                    # Send to destinations
                    if engagement_seconds and engagement_seconds > 0:
                        self.send_to_redis(str(content_id), engagement_seconds)
                    
                    self.send_to_external(enriched_event)
                    self.processed_count += 1
                    
                    print(f"✅ Event {self.processed_count}: {enriched_event['event_type']} | "
                          f"content={str(content_id)[:8]}... | "
                          f"engagement={engagement_seconds}s")

        except Exception as e:
            print(f"❌ Fatal error: {e}")
        finally:
            consumer.close()

if __name__ == "__main__":
    processor = StreamProcessor()
    processor.run()