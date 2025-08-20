import psycopg2
import time
import random
from datetime import datetime, timedelta

# --- Database Configuration ---
# Hostname for the PostgreSQL database. 'localhost' is used when running locally,
# but if running inside a Docker network, this would be the service name (e.g., 'postgres').
DB_HOST = "localhost"
# Name of the database to connect to.
DB_NAME = "streaming_db"
# Username for database access.
DB_USER = "user"
# Password for database access.
DB_PASSWORD = "password"

# --- Data Generation Settings ---
# Number of initial content entries to generate if the 'content' table is empty.
# This ensures there's always some content for engagement events to reference.
INITIAL_CONTENT_COUNT = 10

# Defines different content types and their typical duration ranges in seconds.
# This helps in generating realistic 'length_seconds' for content and 'duration_ms' for events.
CONTENT_TYPES = {
    "video": {"min_len": 60, "max_len": 1800},  # 1 minute to 30 minutes
    "article": {"min_len": 120, "max_len": 3600}, # 2 minutes to 60 minutes
    "audio": {"min_len": 300, "max_len": 7200}   # 5 minutes to 120 minutes
}

# --- Database Connection Function ---
def get_db_connection():
    """
    Establishes a connection to the PostgreSQL database.
    Includes a retry mechanism in case the database is not immediately available (e.g., during startup).
    """
    conn = None
    while conn is None:
        try:
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
            print("Database connection successful.")
        except psycopg2.OperationalError as e:
            print(f"Could not connect to database: {e}. Retrying in 5 seconds...")
            time.sleep(5) # Wait before retrying connection
    return conn

# --- Content Generation Function ---
def generate_initial_content(cursor):
    """
    Generates a set of initial content entries and inserts them into the 'content' table.
    This is called only if the 'content' table is found to be empty.
    """
    print("Generating initial content...")
    for i in range(INITIAL_CONTENT_COUNT):
        # Randomly select a content type (video, article, audio)
        content_type = random.choice(list(CONTENT_TYPES.keys()))
        # Determine a random length within the defined range for the selected content type
        min_len = CONTENT_TYPES[content_type]["min_len"]
        max_len = CONTENT_TYPES[content_type]["max_len"]
        length_seconds = random.randint(min_len, max_len)
        # Create a generic title for the content item
        title = f"{content_type.capitalize()} Title {i+1}"
        
        # Insert the new content item into the 'content' table.
        # ON CONFLICT DO NOTHING prevents errors if the script is run multiple times
        # and content with the same implicit ID (due to sequence) might conflict, though unlikely with BIGSERIAL.
        cursor.execute(
            "INSERT INTO content (title, content_type, length_seconds) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;",
            (title, content_type, length_seconds)
        )
    print(f"{INITIAL_CONTENT_COUNT} content items generated.")

# --- Content ID Retrieval Function ---
def get_content_ids(cursor):
    """
    Retrieves all existing content IDs from the 'content' table.
    These IDs are used to link engagement events to existing content.
    """
    cursor.execute("SELECT id FROM content;")
    return [row[0] for row in cursor.fetchall()]

# --- Engagement Event Generation Function ---
def generate_engagement_event(cursor, content_ids):
    """
    Generates a single random engagement event and inserts it into the 'engagement_events' table.
    Each event is linked to an existing content item and simulates user interaction.
    """
    if not content_ids:
        print("No content available to generate events. Please ensure 'content' table has data.")
        return

    user_id = random.randint(1, 1000) # Simulate user IDs from 1 to 1000
    content_id = random.choice(content_ids) # Pick a random existing content ID

    # Fetch the length of the selected content to generate a realistic engagement duration.
    cursor.execute("SELECT length_seconds FROM content WHERE id = %s;", (content_id,))
    result = cursor.fetchone()
    if result:
        content_length = result[0]
        # Simulate engagement duration as a percentage (10% to 100%) of the content's total length.
        # Converted to milliseconds as per 'duration_ms' column definition.
        duration_ms = random.randint(int(content_length * 0.1 * 1000), content_length * 1000)
    else:
        # Fallback duration if content length cannot be retrieved (e.g., content_id not found).
        duration_ms = random.randint(10000, 600000) # Default: 10 seconds to 10 minutes

    # Simulate event timestamp, slightly in the past to mimic real-time events.
    event_ts = datetime.now() - timedelta(seconds=random.randint(0, 60)) # Events within the last minute

    # Insert the generated engagement event into the 'engagement_events' table.
    cursor.execute(
        "INSERT INTO engagement_events (user_id, content_id, duration_ms, event_ts) VALUES (%s, %s, %s, %s);",
        (user_id, content_id, duration_ms, event_ts)
    )
    print(f"Event generated: user_id={user_id}, content_id={content_id}, duration_ms={duration_ms}")

# --- Main Execution Logic ---
def main():
    """
    Main function to run the data generator.
    Connects to the DB, ensures initial content exists, and then continuously generates engagement events.
    """
    conn = get_db_connection()
    # Set autocommit to True so that each INSERT statement is immediately committed.
    # This is important for CDC tools like Debezium to pick up changes in real-time.
    conn.autocommit = True 
    cursor = conn.cursor()

    # Check if 'content' table has any entries. If not, generate initial content.
    cursor.execute("SELECT COUNT(*) FROM content;")
    if cursor.fetchone()[0] == 0:
        generate_initial_content(cursor)

    # Get all content IDs. This list is refreshed only once at startup.
    content_ids = get_content_ids(cursor)
    if not content_ids:
        print("Error: No content IDs found. Please ensure the 'content' table has data.")
        return

    try:
        # Continuous loop for generating events.
        while True:
            generate_engagement_event(cursor, content_ids)
            # Pause for a random interval to simulate irregular event arrival.
            time.sleep(random.uniform(0.5, 2.0)) # Generate an event every 0.5 to 2 seconds
    except KeyboardInterrupt:
        # Allow graceful shutdown using Ctrl+C.
        print("Data generation stopped by user.")
    finally:
        # Ensure database resources are properly closed.
        cursor.close()
        conn.close()

# Entry point for the script.
if __name__ == "__main__":
    main()
