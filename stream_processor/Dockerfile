# stream_processor/Dockerfile - Version simple et rapide
FROM python:3.11-slim

WORKDIR /app

# Installer les dépendances système nécessaires
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        && rm -rf /var/lib/apt/lists/*

# Installer les dépendances Python (beaucoup plus rapide)
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    redis==4.5.5 \
    requests==2.31.0 \
    psycopg2-binary==2.9.7

# Copier le code
COPY main.py /app/

# Variables d'environnement
ENV PYTHONUNBUFFERED=1

# Commande par défaut
CMD ["python", "main.py"]