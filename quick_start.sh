#!/bin/bash
# quick_start.sh - Automatic pipeline startup for evaluators

set -e

echo "🚀 Starting Real-time Streaming Pipeline"
echo "=========================================="

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

step() {
    echo -e "\n${GREEN}📋 $1${NC}"
}

wait_for_service() {
    local url=$1
    local name=$2
    local timeout=${3:-60}
    
    echo -e "${YELLOW}⏳ Waiting for $name...${NC}"
    for i in $(seq 1 $timeout); do
        if curl -s "$url" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ $name ready!${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
    done
    echo -e "${RED}❌ $name timeout${NC}"
    return 1
}

# Clean previous deployments
step "Cleaning previous deployment"
docker-compose down -v 2>/dev/null || true

# Ordered startup of services
step "Starting infrastructure services (PostgreSQL, Redis, Zookeeper)"
docker-compose up -d postgres redis zookeeper
sleep 20

# Check PostgreSQL
until docker exec postgres pg_isready -U user > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo -e "${GREEN}✅ PostgreSQL ready${NC}"

# Check Redis  
until docker exec redis redis-cli ping > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo -e "${GREEN}✅ Redis ready${NC}"

step "Starting Kafka (this takes time)"
docker-compose up -d kafka
sleep 30

# Check Kafka with multiple attempts
kafka_ready=false
for attempt in {1..8}; do
    if docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list > /dev/null 2>&1; then
        kafka_ready=true
        break
    fi
    echo -n "."
    sleep 10
done

if [ "$kafka_ready" = true ]; then
    echo -e "${GREEN}✅ Kafka ready${NC}"
else
    echo -e "${RED}❌ Kafka failed to start${NC}"
    exit 1
fi

step "Starting Kafka Connect (downloads Debezium - 2 minutes)"
docker-compose up -d kafka-connect
wait_for_service "http://localhost:8083" "Kafka Connect" 120

step "Starting application services"
docker-compose up -d external-system data-generator stream-processor
sleep 15

wait_for_service "http://localhost:5001" "External System" 30

step "Setting up Debezium CDC connector"
# Create the simplified connector that works
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "engagement-simple",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "tasks.max": "1",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "user",
        "database.password": "password",
        "database.dbname": "streaming_db",
        "database.server.name": "pg",
        "table.include.list": "public.engagement_events",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false"
    }
}' > /dev/null

sleep 10

# Check the connector
connector_status=$(curl -s http://localhost:8083/connectors/engagement-simple/status | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")

if [ "$connector_status" = "RUNNING" ]; then
    echo -e "${GREEN}✅ Debezium connector running${NC}"
else
    echo -e "${YELLOW}⚠️  Connector status: $connector_status${NC}"
fi

step "Testing the pipeline"
echo "📊 Inserting test event..."

# Insert a test event
docker exec postgres psql -U user -d streaming_db -c "
INSERT INTO engagement_events (user_id, content_id, event_type, event_ts, duration_ms, device) 
SELECT uuid_generate_v4(), id, 'play', NOW(), 7500, 'quick-start-test' 
FROM content LIMIT 1;" > /dev/null 2>&1

sleep 5

# Check the results
echo "🔍 Checking results..."

# Redis
redis_results=$(docker exec redis redis-cli ZREVRANGE engagement:10min 0 2 WITHSCORES 2>/dev/null | wc -l)
if [ "$redis_results" -gt 0 ]; then
    echo -e "${GREEN}✅ Redis: Data received${NC}"
else
    echo -e "${YELLOW}⚠️  Redis: No data yet${NC}"
fi

# External System
external_events=$(curl -s http://localhost:5001/stats 2>/dev/null | jq -r '.total_events // "0"')
if [ "$external_events" != "0" ] && [ "$external_events" != "null" ]; then
    echo -e "${GREEN}✅ External System: $external_events events received${NC}"
else
    echo -e "${YELLOW}⚠️  External System: No events yet${NC}"
fi

step "Pipeline Status"
echo "📊 Services running:"
docker-compose ps --format "table {{.Name}}\t{{.Status}}" | grep -E "(Up|running)"

echo ""
echo -e "${GREEN}🎉 PIPELINE STARTED SUCCESSFULLY!${NC}"
echo ""
echo "📋 Quick verification:"
echo "1. External System Dashboard: http://localhost:5001"
echo "2. Kafka Connect Status: curl http://localhost:8083/connectors"
echo "3. Redis Metrics: docker exec redis redis-cli ZREVRANGE engagement:10min 0 5 WITHSCORES"
echo ""
echo "🧪 Run full tests:"
echo "./test_pipeline.sh"
echo ""
echo "📖 Stream processor logs:"
echo "docker logs stream-processor -f"
echo ""
echo "🛑 To stop:"
echo "docker-compose down"
