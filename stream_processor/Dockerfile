# stream_processor/Dockerfile
FROM python:3.11-slim

WORKDIR /app

#Install the required system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        && rm -rf /var/lib/apt/lists/*

# Install the Python dependencies (much faster)
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    redis==4.5.5 \
    requests==2.31.0 \
    psycopg2-binary==2.9.7

# Copy the code
COPY main.py /app/

# Environment variables
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "main.py"]