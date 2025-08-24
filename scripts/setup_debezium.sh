#!/bin/bash
# scripts/setup_debezium.sh
# Script pour configurer automatiquement le connecteur Debezium PostgreSQL

set -e

echo "🚀 Setting up Debezium PostgreSQL connector..."

# Configuration
CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="engagement-events-connector"
CONFIG_FILE="connectors/engagement_debezium.json"

# Function to wait until a service is ready
wait_for_service() {
    local url=$1
    local service_name=$2
    echo "⏳ Waiting for $service_name to be ready..."
    
    until curl -s "$url" > /dev/null 2>&1; do
        echo "   Waiting for $service_name..."
        sleep 5
    done
    echo "✅ $service_name is ready!"
}

# Function to wait until a Debezium plugin is ready
wait_for_debezium_plugin() {
    echo "⏳ Waiting for Debezium PostgreSQL plugin..."
    
    until curl -s "$CONNECT_URL/connector-plugins" | grep -q "PostgresConnector"; do
        echo "   Waiting for Debezium plugin..."
        sleep 5
    done
    echo "✅ Debezium PostgreSQL plugin is available!"
}

# Check that the configuration file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ Error: Configuration file $CONFIG_FILE not found!"
    echo "Please create the file with the Debezium connector configuration."
    exit 1
fi

# Attendre que les services soient prêts
wait_for_service "$CONNECT_URL" "Kafka Connect"
wait_for_debezium_plugin

# Wait until the services are ready
if curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null 2>&1; then
    echo "⚠️  Connector $CONNECTOR_NAME already exists. Deleting it first..."
    curl -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME"
    sleep 2
fi

# Create the connector
echo "📡 Creating Debezium PostgreSQL connector..."
response=$(curl -s -X POST \
  "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d @"$CONFIG_FILE")

if echo "$response" | grep -q "error_code"; then
    echo "❌ Error creating connector:"
    echo "$response" | jq '.'
    exit 1
fi

echo "✅ Connector created successfully!"

# Wait a bit for the connector to initialize
echo "⏳ Waiting for connector to initialize..."
sleep 10

# Check the status of the connector
echo "📊 Checking connector status..."
status_response=$(curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status")

if command -v jq > /dev/null 2>&1; then
    echo "$status_response" | jq '.'
else
    echo "$status_response"
fi

# Check that the connector is in RUNNING state
if echo "$status_response" | grep -q '"state":"RUNNING"'; then
    echo "✅ Connector is running successfully!"
else
    echo "⚠️  Connector may not be running properly. Check the status above."
fi

# Display the Kafka topics that were created
echo "📋 Available Kafka topics:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:29092 2>/dev/null | grep -E "(pg\.|engagement)" || echo "   No engagement topics found yet. They will be created when data changes occur."

# Final instructions
echo ""
echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "1. Start the data generator: python data_generator.py"
echo "2. Check Flink job: http://localhost:8081"
echo "3. Monitor external system: http://localhost:5001"
echo "4. View Redis metrics: docker exec redis redis-cli ZREVRANGE engagement:10min 0 5 WITHSCORES"
echo ""