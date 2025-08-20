import psycopg2
import time
import random
from datetime import datetime, timedelta

# Configuration de la base de données PostgreSQL
DB_HOST = "localhost"
DB_NAME = "streaming_db"
DB_USER = "user"
DB_PASSWORD = "password"

# Nombre de contenus initiaux à générer si la table est vide
INITIAL_CONTENT_COUNT = 10

# Types de contenu et durées approximatives
CONTENT_TYPES = {
    "video": {"min_len": 60, "max_len": 1800},  # 1 min à 30 min
    "article": {"min_len": 120, "max_len": 3600}, # 2 min à 60 min
    "audio": {"min_len": 300, "max_len": 7200}   # 5 min à 120 min
}

# Fonction pour se connecter à la base de données
def get_db_connection():
    conn = None
    while conn is None:
        try:
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
            print("Connexion à la base de données réussie.")
        except psycopg2.OperationalError as e:
            print(f"Impossible de se connecter à la base de données: {e}. Réessai dans 5 secondes...")
            time.sleep(5)
    return conn

# Fonction pour générer des contenus initiaux
def generate_initial_content(cursor):
    print("Génération de contenus initiaux...")
    for i in range(INITIAL_CONTENT_COUNT):
        content_type = random.choice(list(CONTENT_TYPES.keys()))
        min_len = CONTENT_TYPES[content_type]["min_len"]
        max_len = CONTENT_TYPES[content_type]["max_len"]
        length_seconds = random.randint(min_len, max_len)
        title = f"{content_type.capitalize()} Title {i+1}"
        cursor.execute(
            "INSERT INTO content (title, content_type, length_seconds) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;",
            (title, content_type, length_seconds)
        )
    print(f"{INITIAL_CONTENT_COUNT} contenus générés.")

# Fonction pour obtenir des IDs de contenu existants
def get_content_ids(cursor):
    cursor.execute("SELECT id FROM content;")
    return [row[0] for row in cursor.fetchall()]

# Fonction pour générer un événement d'engagement
def generate_engagement_event(cursor, content_ids):
    if not content_ids:
        print("Aucun contenu disponible pour générer des événements.")
        return

    user_id = random.randint(1, 1000) # 1000 utilisateurs simulés
    content_id = random.choice(content_ids)

    # Récupérer la longueur du contenu pour simuler une durée réaliste
    cursor.execute("SELECT length_seconds FROM content WHERE id = %s;", (content_id,))
    result = cursor.fetchone()
    if result:
        content_length = result[0]
        # Simuler une durée de visionnage/lecture entre 10% et 100% de la longueur du contenu
        duration_ms = random.randint(int(content_length * 0.1 * 1000), content_length * 1000)
    else:
        duration_ms = random.randint(10000, 600000) # Durée par défaut si contenu non trouvé (10s à 10min)

    event_ts = datetime.now() - timedelta(seconds=random.randint(0, 60)) # Événements récents

    cursor.execute(
        "INSERT INTO engagement_events (user_id, content_id, duration_ms, event_ts) VALUES (%s, %s, %s, %s);",
        (user_id, content_id, duration_ms, event_ts)
    )
    print(f"Événement généré: user_id={user_id}, content_id={content_id}, duration_ms={duration_ms}")


def main():
    conn = get_db_connection()
    conn.autocommit = True # Pour que les insertions soient visibles immédiatement
    cursor = conn.cursor()

    # Vérifier si la table content est vide et générer des contenus initiaux
    cursor.execute("SELECT COUNT(*) FROM content;")
    if cursor.fetchone()[0] == 0:
        generate_initial_content(cursor)

    content_ids = get_content_ids(cursor)
    if not content_ids:
        print("Erreur: Aucune ID de contenu trouvée. Veuillez vérifier la table 'content'.")
        return

    try:
        while True:
            generate_engagement_event(cursor, content_ids)
            time.sleep(random.uniform(0.5, 2.0)) # Générer un événement toutes les 0.5 à 2 secondes
    except KeyboardInterrupt:
        print("Génération de données arrêtée.")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()


