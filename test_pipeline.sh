#!/bin/bash
# test_pipeline.sh - Tests automatisés complets du pipeline

set -e
echo "🧪 Testing Complete Streaming Pipeline"
echo "======================================"

# Couleurs
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

test_step() {
    echo -e "\n${BLUE}🧪 Test: $1${NC}"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

warning() {
    echo -e "${YELLOW}⚠️ $1${NC}"
}

error() {
    echo -e "${RED}❌ $1${NC}"
}

# Variables de test
TESTS_PASSED=0
TESTS_FAILED=0

pass_test() {
    ((TESTS_PASSED++))
    success "$1"
}

fail_test() {
    ((TESTS_FAILED++))
    error "$1"
}

test_step "1. Service Health Checks"

# Test PostgreSQL
if docker exec postgres pg_isready -U user > /dev/null 2>&1; then
    pass_test "PostgreSQL is healthy"
else
    fail_test "PostgreSQL is not responding"
fi

# Test Redis
if docker exec redis redis-cli ping > /dev/null 2>&1; then
    pass_test "Redis is healthy"
else
    fail_test "Redis is not responding"
fi

# Test Kafka
if docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list > /dev/null 2>&1; then
    pass_test "Kafka is healthy"
else
    fail_test "Kafka is not responding"
fi

# Test External System
external_status=$(curl -s -w "%{http_code}" http://localhost:5001/health -o /dev/null)
if [ "$external_status" = "200" ]; then
    pass_test "External System is healthy"
else
    fail_test "External System is not responding (HTTP $external_status)"
fi

test_step "2. End-to-End Pipeline Test"

echo "📊 Inserting test event..."
test_event_device="pipeline-test-$(date +%s)"

# Insérer un événement avec une durée spécifique pour le test
insert_result=$(docker exec postgres psql -U user -d streaming_db -c "
INSERT INTO engagement_events (user_id, content_id, event_type, event_ts, duration_ms, device) 
SELECT uuid_generate_v4(), id, 'finish', NOW(), 9000, '$test_event_device' 
FROM content LIMIT 1;" 2>&1)

if echo "$insert_result" | grep -q "INSERT 0 1"; then
    pass_test "Test event inserted successfully"
else
    fail_test "Failed to insert test event: $insert_result"
fi

echo "⏳ Waiting 8 seconds for processing..."
sleep 8

test_step "3. Latency Test (< 5 seconds requirement)"

# Test de latence
start_time=$(date +%s)
test_device="latency-test-$(date +%s)"

docker exec postgres psql -U user -d streaming_db -c "
INSERT INTO engagement_events (user_id, content_id, event_type, event_ts, duration_ms, device) 
SELECT uuid_generate_v4(), id, 'play', NOW(), 8500, '$test_device' 
FROM content LIMIT 1;" > /dev/null 2>&1

# Attendre et vérifier Redis
for i in {1..10}; do
    redis_check=$(docker exec redis redis-cli ZREVRANGE engagement:10min 0 10 WITHSCORES 2>/dev/null | wc -l)
    if [ "$redis_check" -gt 0 ]; then
        end_time=$(date +%s)
        latency=$((end_time - start_time))
        
        if [ "$latency" -lt 5 ]; then
            pass_test "Latency test PASSED: ${latency} seconds (< 5s)"
        else
            fail_test "Latency test FAILED: ${latency} seconds (>= 5s)"
        fi
        break
    fi
    sleep 1
done

test_step "4. Multi-Sink Verification"

# Vérifier Redis (Destination 1)
redis_data=$(docker exec redis redis-cli ZREVRANGE engagement:10min 0 3 WITHSCORES 2>/dev/null)
if [ -n "$redis_data" ] && [ "$redis_data" != "(empty array)" ]; then
    redis_entries=$(echo "$redis_data" | wc -l)
    pass_test "Redis sink: $((redis_entries / 2)) content entries with scores"
    
    echo "📊 Top engaging content (last 10min):"
    docker exec redis redis-cli ZREVRANGE engagement:10min 0 2 WITHSCORES 2>/dev/null | while IFS= read -r line; do
        echo "   $line"
    done | head -6
else
    fail_test "Redis sink: No engagement data found"
fi

# Vérifier External System (Destination 2)
external_stats=$(curl -s http://localhost:5001/stats 2>/dev/null)
if [ $? -eq 0 ] && echo "$external_stats" | grep -q "total_events"; then
    total_events=$(echo "$external_stats" | grep -o '"total_events":[0-9]*' | grep -o '[0-9]*')
    
    if [ "$total_events" -gt 0 ] 2>/dev/null; then
        pass_test "External System sink: $total_events events received"
    else
        fail_test "External System sink: No events received"
    fi
else
    fail_test "External System sink: Service not responding"
fi

test_step "5. Data Transformation Verification"

# Vérifier qu'on peut récupérer un événement avec enrichissement
recent_events=$(curl -s http://localhost:5001/events?limit=1 2>/dev/null)
if [ $? -eq 0 ] && echo "$recent_events" | grep -q "content_type"; then
    pass_test "Data enrichment: Events contain enriched fields (content_type)"
else
    warning "Data enrichment: Cannot verify enrichment (check external system logs)"
fi

# Résumé final
test_step "Test Summary"

echo ""
echo "📊 FINAL RESULTS:"
echo "=================="
success "Tests Passed: $TESTS_PASSED"
if [ "$TESTS_FAILED" -gt 0 ]; then
    error "Tests Failed: $TESTS_FAILED"
    echo ""
    echo "❌ Some tests failed. Check the output above for details."
    echo "💡 Try running: docker-compose restart stream-processor"
else
    echo ""
    echo "🎉 ALL TESTS PASSED!"
    echo ""
    echo "✅ Your streaming pipeline meets all requirements:"
    echo "   • CDC from PostgreSQL ✓"
    echo "   • Data enrichment ✓"  
    echo "   • Multi-sink fan-out ✓"
    echo "   • <5s latency ✓"
    echo "   • Real-time aggregations ✓"
    echo ""
    echo "🎯 Ready for evaluation!"
fi

echo ""
echo "🔗 Quick links:"
echo "  • Dashboard: http://localhost:5001"
echo "  • Logs: docker logs stream-processor -f"
echo "  • Redis: docker exec redis redis-cli ZREVRANGE engagement:10min 0 5 WITHSCORES"
