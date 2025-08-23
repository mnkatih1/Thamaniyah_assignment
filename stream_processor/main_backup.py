#!/usr/bin/env python3
"""
Stream Processor Python - Alternative à Flink
Traite les événements Debezium depuis Kafka et les distribue vers Redis et External System
"""

import json
import os
import time
import threading
from typing import Dict, Any, Optional

import redis
import requests
import psycopg2
from kafka import KafkaConsumer

# Configuration depuis les variables d'environnement
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "pg.public.engagement_events")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
EXTERNAL_URL = os.environ.get("EXTERNAL_URL", "http://external-system:5001/webhook")

# PostgreSQL config
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_DB = os.environ.get("PG_DB", "streaming_db")
PG_USER = os.environ.get("PG_USER", "user")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "password")


class ContentLookup:
    """Cache simple pour les métadonnées de contenu"""
    
    def __init__(self):
        self._conn = None
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        print("📚 ContentLookup initialized")

    def _connect(self):
        """Établit la connexion PostgreSQL avec retry"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                if self._conn is None or self._conn.closed:
                    self._conn = psycopg2.connect(
                        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
                    )
                    self._conn.autocommit = True
                    print("✅ PostgreSQL connected")
                return
            except Exception as e:
                retry_count += 1
                print(f"❌ PostgreSQL connection attempt {retry_count}/{max_retries} failed: {e}")
                if retry_count < max_retries:
                    time.sleep(2)
                else:
                    raise

    def get(self, content_id: str) -> Optional[Dict[str, Any]]:
        """Récupère les métadonnées de contenu avec cache"""
        # Vérifier le cache
        with self._lock:
            if content_id in self._cache:
                return self._cache[content_id]
        
        # Cache miss - requête PostgreSQL
        try:
            self._connect()
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT content_type, length_seconds FROM content WHERE id = %s LIMIT 1",
                    (content_id,),
                )
                row = cur.fetchone()
                if not row:
                    print(f"⚠️  Content not found: {content_id}")
                    return None
                
                data = {"content_type": row[0], "length_seconds": row[1]}
                
                # Mettre en cache
                with self._lock:
                    self._cache[content_id] = data
                    # Limiter la taille du cache (LRU simple)
                    if len(self._cache) > 1000:
                        oldest_key = next(iter(self._cache))
                        del self._cache[oldest_key]
                
                return data
                
        except Exception as e:
            print(f"❌ Database lookup error for {content_id}: {e}")
            return None


class StreamProcessor:
    """Processeur principal du stream Kafka"""
    
    def __init__(self):
        self.content_lookup = ContentLookup()
        
        # Initialiser Redis avec retry
        self._init_redis()
        
        self.processed_count = 0
        self.error_count = 0
        
        print("🚀 StreamProcessor initialized")
        
    def _init_redis(self):
        """Initialise Redis avec retry"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=False)
                # Test de connexion
                self.redis_client.ping()
                print("✅ Redis connected")
                return
            except Exception as e:
                retry_count += 1
                print(f"❌ Redis connection attempt {retry_count}/{max_retries} failed: {e}")
                if retry_count < max_retries:
                    time.sleep(2)
                else:
                    print("⚠️  Continuing without Redis - metrics will be skipped")
                    self.redis_client = None
        
    def process_message(self, raw_message: str) -> Optional[Dict[str, Any]]:
        """Traite un message Debezium et retourne l'événement enrichi"""
        try:
            # Parser le message Debezium
            msg = json.loads(raw_message)
            
            # Vérifier que c'est un INSERT/UPDATE (pas DELETE)
            if msg.get("op") in ["d"]:  # d = delete
                return None
                
            after = msg.get("after") or {}
            
            # Extraire les champs essentiels
            event_id = after.get("id")
            user_id = after.get("user_id")
            content_id = after.get("content_id")
            duration_ms = after.get("duration_ms")
            event_ts = after.get("event_ts")
            event_type = after.get("event_type")
            device = after.get("device")
            raw_payload = after.get("raw_payload")

            if not (event_id and content_id and event_ts):
                print(f"⚠️  Incomplete event: id={event_id}, content_id={content_id}, event_ts={event_ts}")
                return None

            # Enrichissement avec métadonnées content
            meta = self.content_lookup.get(str(content_id))
            content_type = meta["content_type"] if meta else None
            length_seconds = meta["length_seconds"] if meta else None

            # Calculs d'engagement
            engagement_seconds = None
            engagement_pct = None
            if duration_ms is not None and duration_ms > 0:
                engagement_seconds = round(float(duration_ms) / 1000.0, 3)
                if length_seconds and length_seconds > 0:
                    engagement_pct = round(engagement_seconds / float(length_seconds), 4)

            # Construire l'événement enrichi
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
                "device": device,
                "raw_payload": raw_payload
            }

            return enriched_event

        except Exception as e:
            self.error_count += 1
            print(f"❌ Error processing message: {e}")
            if self.error_count <= 5:  # Log les premiers erreurs seulement
                print(f"Raw message: {raw_message[:200]}...")
            return None

    def send_to_redis(self, content_id: str, engagement_seconds: float):
        """Envoie les métriques vers Redis (top content 10min)"""
        if not self.redis_client:
            return
            
        try:
            key = "engagement:10min"
            # Incrémenter le score dans le sorted set
            self.redis_client.zincrby(key, engagement_seconds, content_id)
            # TTL de 10 minutes pour rolling window
            self.redis_client.expire(key, 600)
        except Exception as e:
            print(f"❌ Redis error: {e}")

    def send_to_external(self, event: Dict[str, Any]):
        """Envoie vers le système externe via webhook"""
        if not EXTERNAL_URL:
            return
            
        try:
            response = requests.post(EXTERNAL_URL, json=event, timeout=5)
            if response.status_code != 200:
                print(f"⚠️  External system returned {response.status_code}")
        except Exception as e:
            print(f"❌ External system error: {e}")

    def log_progress(self):
        """Log les statistiques de progression"""
        if self.processed_count % 50 == 0 and self.processed_count > 0:
            print(f"📊 Progress: {self.processed_count} events processed, {self.error_count} errors")
            
            # Afficher le top content Redis
            if self.redis_client:
                try:
                    top_content = self.redis_client.zrevrange("engagement:10min", 0, 4, withscores=True)
                    if top_content:
                        print("🏆 Top engaging content (last 10min):")
                        for content_id, score in top_content:
                            content_id_str = content_id.decode() if isinstance(content_id, bytes) else str(content_id)
                            print(f"   {content_id_str[:8]}... → {score:.1f}s")
                except:
                    pass

    def run(self):
        """Boucle principale du processeur"""
        print(f"🚀 Starting stream processor...")
        print(f"📡 Kafka: {BOOTSTRAP_SERVERS}")
        print(f"📋 Topic: {KAFKA_TOPIC}")
        print(f"⚡ Redis: {REDIS_HOST}")
        print(f"🌐 External: {EXTERNAL_URL}")

        # Attendre que Kafka soit prêt
        print("⏳ Waiting for Kafka to be ready...")
        time.sleep(10)

        # Configuration du consumer Kafka
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                group_id='python_stream_processor',
                value_deserializer=lambda m: m.decode('utf-8'),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                consumer_timeout_ms=1000  # Timeout pour permettre les logs périodiques
            )
            print("✅ Connected to Kafka. Waiting for messages...")
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            return

        try:
            while True:
                # Consommer les messages avec timeout
                message_batch = consumer.poll(timeout_ms=1000)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            # Traiter le message
                            enriched_event = self.process_message(message.value)
                            
                            if enriched_event:
                                # Fan-out vers les destinations
                                content_id = enriched_event["content_id"]
                                engagement_seconds = enriched_event["engagement_seconds"]
                                
                                # 1. Redis (métriques temps réel)
                                if engagement_seconds and engagement_seconds > 0:
                                    self.send_to_redis(str(content_id), engagement_seconds)
                                
                                # 2. Système externe
                                self.send_to_external(enriched_event)
                                
                                # 3. BigQuery (simulé - on pourrait ajouter un sink ici)
                                # TODO: Implémenter BigQuery sink si nécessaire
                                
                                self.processed_count += 1
                                
                                # Debug pour les premiers événements
                                if self.processed_count <= 10:
                                    print(f"✅ Event {self.processed_count}: {enriched_event['event_type']} | "
                                          f"content={str(content_id)[:8]}... | "
                                          f"engagement={engagement_seconds}s | "
                                          f"pct={enriched_event['engagement_pct']}")
                
                # Log de progression périodique
                self.log_progress()

        except KeyboardInterrupt:
            print(f"\n🛑 Shutting down. Processed {self.processed_count} events total.")
        except Exception as e:
            print(f"❌ Fatal error: {e}")
        finally:
            consumer.close()
            if self.redis_client:
                self.redis_client.close()


if __name__ == "__main__":
    processor = StreamProcessor()
    processor.run()