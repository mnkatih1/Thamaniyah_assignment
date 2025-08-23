#!/usr/bin/env python3
"""
Data Generator pour simuler des événements d'engagement
Génère des données réalistes pour tester le pipeline de streaming
"""

import os
import psycopg2
import time
import random
import uuid
from datetime import datetime, timedelta

# Configuration de la base de données
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "streaming_db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

# Paramètres de génération
INITIAL_CONTENT_COUNT = 20
EVENT_TYPES = ['play', 'pause', 'finish', 'click']
DEVICES = ['ios', 'web-safari', 'android', 'web-chrome', 'desktop']

def get_db_connection():
    """Établit une connexion à PostgreSQL avec retry"""
    conn = None
    while conn is None:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, 
                database=DB_NAME, 
                user=DB_USER, 
                password=DB_PASSWORD
            )
            print("✅ Database connection successful.")
        except psycopg2.OperationalError as e:
            print(f"❌ Could not connect to database: {e}. Retrying in 5 seconds...")
            time.sleep(5)
    return conn

def generate_initial_content(cursor):
    """Génère du contenu initial si la table est vide"""
    print("📝 Generating initial content...")
    
    content_data = [
        ('intro-kafka', 'Introduction à Kafka', 'video', 480),
        ('flink-basics', 'Les bases de Flink', 'podcast', 720),
        ('redis-patterns', 'Patterns Redis', 'newsletter', 300),
        ('cdc-explained', 'CDC expliqué', 'video', 600),
        ('streaming-architectures', 'Architectures de Streaming', 'podcast', 900),
        ('bigquery-tips', 'Tips BigQuery', 'newsletter', 240),
        ('kafka-streams-guide', 'Guide Kafka Streams', 'video', 1200),
        ('flink-windowing-advanced', 'Flink Windowing Avancé', 'podcast', 840),
        ('real-time-analytics', 'Analytics en Temps Réel', 'video', 660),
        ('data-pipeline-patterns', 'Patterns de Pipeline', 'newsletter', 420)
    ]
    
    for slug, title, content_type, length_seconds in content_data:
        cursor.execute("""
            INSERT INTO content (slug, title, content_type, length_seconds) 
            VALUES (%s, %s, %s, %s) ON CONFLICT (slug) DO NOTHING
        """, (slug, title, content_type, length_seconds))
    
    print(f"✅ {len(content_data)} content items generated.")

def get_content_ids_with_lengths(cursor):
    """Récupère tous les IDs de contenu avec leurs durées"""
    cursor.execute("SELECT id, length_seconds FROM content")
    return cursor.fetchall()

def generate_engagement_event(cursor, content_data):
    """Génère un événement d'engagement réaliste"""
    if not content_data:
        print("❌ No content available to generate events.")
        return

    # Sélectionner un contenu aléatoirement
    content_id, content_length = random.choice(content_data)
    
    # Générer des données réalistes
    user_id = str(uuid.uuid4())
    event_type = random.choice(EVENT_TYPES)
    device = random.choice(DEVICES)
    
    # Durée basée sur le type d'événement et la longueur du contenu
    if event_type in ['pause', 'finish'] and content_length:
        # Pour pause/finish, durée entre 10% et 95% du contenu
        min_duration = int(content_length * 0.1 * 1000)  # 10% en ms
        max_duration = int(content_length * 0.95 * 1000)  # 95% en ms
        duration_ms = random.randint(min_duration, max_duration)
    elif event_type == 'play':
        # Pour play, durée courte (début de lecture)
        duration_ms = random.randint(1000, 30000)  # 1-30 secondes
    else:
        # Pour click, pas de durée
        duration_ms = None
    
    # Timestamp récent (dernière heure)
    event_ts = datetime.now() - timedelta(seconds=random.randint(0, 3600))
    
    # Payload JSON avec informations supplémentaires
    raw_payload = {
        'session_id': str(uuid.uuid4()),
        'referrer': random.choice(['direct', 'search', 'social', 'email']),
        'quality': random.choice(['720p', '1080p', '480p']) if event_type == 'play' else None
    }
    
    # Insérer l'événement
    cursor.execute("""
        INSERT INTO engagement_events 
        (user_id, content_id, event_type, event_ts, duration_ms, device, raw_payload) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (user_id, content_id, event_type, event_ts, duration_ms, device, raw_payload))
    
    print(f"📊 Event: {event_type} | user={user_id[:8]} | content={str(content_id)[:8]} | duration={duration_ms}ms")

def main():
    """Fonction principale du générateur"""
    print("🚀 Starting data generator...")
    
    conn = get_db_connection()
    conn.autocommit = True  # Important pour CDC
    cursor = conn.cursor()

    # Vérifier et générer le contenu initial
    cursor.execute("SELECT COUNT(*) FROM content")
    if cursor.fetchone()[0] == 0:
        generate_initial_content(cursor)

    # Récupérer les données de contenu
    content_data = get_content_ids_with_lengths(cursor)
    if not content_data:
        print("❌ Error: No content found. Please check the content table.")
        return

    print(f"📚 Found {len(content_data)} content items. Starting event generation...")

    try:
        event_count = 0
        while True:
            generate_engagement_event(cursor, content_data)
            event_count += 1
            
            # Log de progression
            if event_count % 10 == 0:
                print(f"📈 Generated {event_count} events so far...")
            
            # Pause aléatoire pour simuler un flux réaliste
            time.sleep(random.uniform(0.5, 3.0))
            
    except KeyboardInterrupt:
        print(f"\n🛑 Data generation stopped. Total events generated: {event_count}")
    except Exception as e:
        print(f"❌ Error during generation: {e}")
    finally:
        cursor.close()
        conn.close()
        print("🔌 Database connection closed.")

if __name__ == "__main__":
    main()