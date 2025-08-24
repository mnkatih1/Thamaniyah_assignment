#!/usr/bin/env python3
"""
Data Generator to simulate engagement events
Generates realistic data to test the streaming pipeline
"""

import os
import psycopg2
import time
import random
import uuid
from datetime import datetime, timedelta

# Database configuration
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "streaming_db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

# Generation parameters
INITIAL_CONTENT_COUNT = 20
EVENT_TYPES = ['play', 'pause', 'finish', 'click']
DEVICES = ['ios', 'web-safari', 'android', 'web-chrome', 'desktop']

def get_db_connection():
    """Establishes a PostgreSQL connection with retry"""
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
    """Generates initial content if the table is empty"""
    print("📝 Generating initial content...")
    
    content_data = [
        ('intro-kafka', 'Introduction to Kafka', 'video', 480),
        ('flink-basics', 'Flink Basics', 'podcast', 720),
        ('redis-patterns', 'Redis Patterns', 'newsletter', 300),
        ('cdc-explained', 'CDC Explained', 'video', 600),
        ('streaming-architectures', 'Streaming Architectures', 'podcast', 900),
        ('bigquery-tips', 'BigQuery Tips', 'newsletter', 240),
        ('kafka-streams-guide', 'Kafka Streams Guide', 'video', 1200),
        ('flink-windowing-advanced', 'Advanced Flink Windowing', 'podcast', 840),
        ('real-time-analytics', 'Real-time Analytics', 'video', 660),
        ('data-pipeline-patterns', 'Data Pipeline Patterns', 'newsletter', 420)
    ]
    
    for slug, title, content_type, length_seconds in content_data:
        cursor.execute("""
            INSERT INTO content (slug, title, content_type, length_seconds) 
            VALUES (%s, %s, %s, %s) ON CONFLICT (slug) DO NOTHING
        """, (slug, title, content_type, length_seconds))
    
    print(f"✅ {len(content_data)} content items generated.")

def get_content_ids_with_lengths(cursor):
    """Retrieves all content IDs with their durations"""
    cursor.execute("SELECT id, length_seconds FROM content")
    return cursor.fetchall()

def generate_engagement_event(cursor, content_data):
    """Generates a realistic engagement event"""
    if not content_data:
        print("❌ No content available to generate events.")
        return

    # Select content randomly
    content_id, content_length = random.choice(content_data)
    
    # Generate realistic data
    user_id = str(uuid.uuid4())
    event_type = random.choice(EVENT_TYPES)
    device = random.choice(DEVICES)
    
    # Duration based on event type and content length
    if event_type in ['pause', 'finish'] and content_length:
        # For pause/finish, duration between 10% and 95% of content
        min_duration = int(content_length * 0.1 * 1000)  # 10% in ms
        max_duration = int(content_length * 0.95 * 1000)  # 95% in ms
        duration_ms = random.randint(min_duration, max_duration)
    elif event_type == 'play':
        # For play, short duration (beginning of playback)
        duration_ms = random.randint(1000, 30000)  # 1-30 seconds
    else:
        # For click, no duration
        duration_ms = None
    
    # Recent timestamp (last hour)
    event_ts = datetime.now() - timedelta(seconds=random.randint(0, 3600))
    
    # JSON payload with additional information
    raw_payload = {
        'session_id': str(uuid.uuid4()),
        'referrer': random.choice(['direct', 'search', 'social', 'email']),
        'quality': random.choice(['720p', '1080p', '480p']) if event_type == 'play' else None
    }
    
    # Insert the event
    cursor.execute("""
        INSERT INTO engagement_events 
        (user_id, content_id, event_type, event_ts, duration_ms, device, raw_payload) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (user_id, content_id, event_type, event_ts, duration_ms, device, raw_payload))
    
    print(f"📊 Event: {event_type} | user={user_id[:8]} | content={str(content_id)[:8]} | duration={duration_ms}ms")

def main():
    """Main generator function"""
    print("🚀 Starting data generator...")
    
    conn = get_db_connection()
    conn.autocommit = True  # Important for CDC
    cursor = conn.cursor()

    # Check and generate initial content
    cursor.execute("SELECT COUNT(*) FROM content")
    if cursor.fetchone()[0] == 0:
        generate_initial_content(cursor)

    # Retrieve content data
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
            
            # Progress log
            if event_count % 10 == 0:
                print(f"📈 Generated {event_count} events so far...")
            
            # Random pause to simulate realistic flow
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