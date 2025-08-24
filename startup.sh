#!/bin/bash
# startup.sh - Ordered service startup

set -e

echo "🚀 Starting services in proper order..."

# Colors for logs
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

step() {
    echo -e "\n${GREEN}📋 $1${NC}"
}

wait_for_container() {
    local container=$1
    local timeout=${2:-60}
    
    echo -e "${YELLOW}⏳ Waiting for $container to be healthy...${NC}"
    for i in $(seq 1 $timeout); do
        if docker ps --format "table {{.Names}}\t{{.Status}}" | grep "$container" | grep -q "healthy\|Up"; then
            echo -e "${GREEN}✅ $container is ready!${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
    done
    echo -e "${RED}❌ $container failed to start within ${timeout}s${NC}"
    return 1
}

wait_for_service() {
    local url=$1
    local name=$2
    local timeout=${3:-60}
    
    echo -e "${YELLOW}⏳ Waiting for $name API...${NC}"
    for i in $(seq 1 $timeout); do
        if curl -s "$url" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ $name API is ready!${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
    done
    echo -e "${RED}❌ $name API failed to respond within ${timeout}s${NC}"
    return 1
}

# Complete cleanup
step "Cleaning up previous deployment"
docker-compose down -v 2>/dev/null || true
docker system prune -f > /dev/null 2>&1

# Step 1: PostgreSQL and Redis (base infrastructure)
step "1/6 Starting PostgreSQL and Redis"
docker-compose up -d postgres redis
sleep 10

# Check PostgreSQL
until docker exec postgres pg_isready -U user > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo -e "${GREEN}✅ PostgreSQL ready!${NC}"

# Check Redis
until docker exec redis redis-cli ping > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo -e "${GREEN}✅ Redis ready!${NC}"

# Step 2: Zookeeper
step "2/6 Starting Zookeeper"
docker-compose up -d zookeeper
sleep 15

# Wait for Zookeeper to be completely ready
echo -e "${YELLOW}⏳ Waiting for Zookeeper...${NC}"
until docker exec zookeeper bash -c 'echo ruok | nc localhost 2181' 2>/dev/null | grep -q imok; do
    echo -n "."
    sleep 3
done
echo -e "${GREEN}✅ Zookeeper ready!${NC}"

# Step 3: Kafka (waits for Zookeeper to be stable)
step "3/6 Starting Kafka"
docker-compose up -d kafka
sleep 30

# Check Kafka with multiple attempts
echo -e "${YELLOW}⏳ Waiting for Kafka (this takes time)...${NC}"
kafka_ready=false
for attempt in {1..10}; do
    if docker exec kafka kafka-topics --list --bootstrap-server localhost:29092 > /dev/null 2>&1; then
        kafka_ready=true
        break
    fi
    echo -n "."
    sleep 10
done

if [ "$kafka_ready" = true ]; then
    echo -e "${GREEN}✅ Kafka ready!${NC}"
else
    echo -e "${RED}❌ Kafka failed to start properly${NC}"
    echo "Kafka logs:"
    docker logs kafka --tail 20
    exit 1
fi

# Step 4: Kafka Connect (the longest)
step "4/6 Starting Kafka Connect (downloads Debezium plugin)"
docker-compose up -d kafka-connect
echo -e "${YELLOW}⏳ This will take 2-3 minutes for plugin download...${NC}"

wait_for_service "http://localhost:8083" "Kafka Connect" 180

# Step 5: Application services
step "5/6 Starting application services"
docker-compose up -d external-system data-generator
sleep 10

wait_for_service "http://localhost:5001" "External System" 30

# Step 6: Stream processor and setup
step "6/6 Starting stream processor and setup"
docker-compose up -d stream-processor setup-debezium
sleep 15

# Final verification
step "Final verification"
echo "📊 Services status:"
docker-compose ps

echo ""
echo -e "${GREEN}🎉 All services started successfully!${NC}"
echo ""
echo "Next steps:"
echo "1. Check connector: curl http://localhost:8083/connectors"
echo "2. Monitor external system: http://localhost:5001"
echo "3. Watch stream processor: docker logs stream-processor -f"
echo "4. Check Redis metrics: docker exec redis redis-cli MONITOR"
echo ""
echo "If everything looks good, the pipeline should be processing events!"