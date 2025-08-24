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
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "engagement_events")  
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
        max_retries = 5  # 🔧 Plus de tentatives
        retry_count = 0
        while retry_count < max_retries:
            try:
                if self._conn is None or self._conn.closed:
                    self._conn = psycopg2.connect(
                        host=PG_HOST, 
                        dbname=PG_DB, 
                        user=PG_USER, 
                        password=PG_PASSWORD,
                        connect_timeout=10  # 🔧 Timeout de connexion
                    )
                    self._conn.autocommit = True
                    print("✅ PostgreSQL connected")
                return
            except Exception as e:
                retry_count += 1
                print(f"❌ PostgreSQL attempt {retry_count}/{max_retries}: {e}")
                time.sleep(2 ** retry_count)  # 🔧 Backoff exponentiel

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
            # 🔧 Réinitialiser la connexion en cas d'erreur
            self._conn = None
        return None

class StreamProcessor:
    def __init__(self):
        self.content_lookup = ContentLookup()
        self._init_redis()
        self.processed_count = 0
        print("🚀 StreamProcessor initialized")
        
    def _init_redis(self):
        max_retries = 5
        retry_count = 0
        while retry_count < max_retries:
            try:
                self.redis_client = redis.Redis(
                    host=REDIS_HOST, 
                    port=6379,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    retry_on_timeout=True
                )
                self.redis_client.ping()
                print("✅ Redis connected")
                return
            except Exception as e:
                retry_count += 1
                print(f"❌ Redis connection attempt {retry_count}/{max_retries}: {e}")
                if retry_count < max_retries:
                    time.sleep(2)
                else:
                    print("❌ Redis connection failed permanently - continuing without Redis")
                    self.redis_client = None

    def process_message(self, raw_message: str) -> Optional[Dict[str, Any]]:
        try:
            # 🔧 Debug: afficher le message brut
            print(f"📨 Raw message: {raw_message[:200]}...")
            
            # Parse complete Debezium message
            msg = json.loads(raw_message)
            
            # 🔧 Debug: afficher la structure
            print(f"📋 Message structure: {list(msg.keys())}")
            
            # Skip deletes and schema messages
            if msg.get("op") == "d" or "schema" in msg:
                print("⏭️ Skipping delete or schema message")
                return None
                
            # Extract from 'after' (new Debezium format) ou payload direct
            after = msg.get("after", msg.get("payload", {}))
            
            # 🔧 Si 'after' est vide, essayer la racine du message
            if not after and "id" in msg:
                after = msg
                
            print(f"📝 After data: {after}")
            
            event_id = after.get("id")
            user_id = after.get("user_id")
            content_id = after.get("content_id")
            duration_ms = after.get("duration_ms")
            event_ts = after.get("event_ts")
            event_type = after.get("event_type")
            device = after.get("device")

            if not (event_id and content_id):
                print(f"⚠️ Missing required fields: id={event_id}, content_id={content_id}")
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

            enriched_event = {
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
            
            print(f"✅ Enriched event: {enriched_event}")
            return enriched_event
            
        except json.JSONDecodeError as e:
            print(f"❌ JSON parsing error: {e}")
            return None
        except Exception as e:
            print(f"❌ Error processing message: {e}")
            print(f"📋 Raw message causing error: {raw_message}")
            return None

    def send_to_redis(self, content_id: str, engagement_seconds: float):
        if self.redis_client:
            try:
                key = "engagement:10min"
                # 🔧 Utiliser le content_id comme membre du sorted set
                self.redis_client.zincrby(key, engagement_seconds, content_id)
                self.redis_client.expire(key, 600)  # 10 minutes
                print(f"📊 Redis updated: {content_id} += {engagement_seconds}s")
            except Exception as e:
                print(f"❌ Redis error: {e}")
                # 🔧 Tenter de se reconnecter
                self._init_redis()

    def send_to_external(self, event: Dict[str, Any]):
        try:
            response = requests.post(EXTERNAL_URL, json=event, timeout=5)
            if response.status_code == 200:
                print(f"🌐 External system: Event sent successfully")
            else:
                print(f"⚠️ External system returned: {response.status_code}")
        except requests.exceptions.Timeout:
            print("⚠️ External system timeout")
        except Exception as e:
            print(f"❌ External system error: {e}")

    def run(self):
        print("🚀 Starting stream processor...")
        print(f"📡 Topic: {KAFKA_TOPIC}")
        
        # 🔧 Attendre que les services soient prêts
        print("⏳ Waiting for services to be ready...")
        time.sleep(10)

        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                group_id='python_stream_processor',
                value_deserializer=lambda m: m.decode('utf-8'),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                consumer_timeout_ms=10000  # 🔧 Timeout pour les tests
            )
            print("✅ Connected to Kafka!")
            
            # 🔧 Vérifier les topics disponibles
            topics = consumer.list_consumer_group_offsets()
            print(f"📋 Available topics: {list(topics.keys()) if topics else 'None yet'}")
            
        except Exception as e:
            print(f"❌ Kafka connection failed: {e}")
            return

        try:
            print("👂 Listening for messages...")
            message_count = 0
            
            for message in consumer:
                message_count += 1
                print(f"\n📬 Message #{message_count} received from topic {message.topic}")
                
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
                else:
                    print("⏭️ Event skipped or invalid")

        except KeyboardInterrupt:
            print("\n🛑 Shutting down gracefully...")
        except Exception as e:
            print(f"❌ Fatal error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            print("🔌 Closing Kafka consumer...")
            consumer.close()

if __name__ == "__main__":
    processor = StreamProcessor()
    processor.run()