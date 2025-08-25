# Real-time User Engagement Streaming Pipeline

A real-time streaming pipeline that captures user engagement events from PostgreSQL and distributes them to multiple destinations with data enrichment and transformations.

## 🎯 Project Overview

This system implements a Change Data Capture (CDC) pipeline that:
- **Streams** user engagement events from PostgreSQL in real-time using Debezium
- **Enriches** data by joining with content metadata 
- **Transforms** data with engagement calculations
- **Distributes** to multiple sinks: Redis and External System
- **Maintains** sub-5-second latency requirement
- **Provides** real-time aggregations for top engaging content

## 🏗️ Architecture

```
PostgreSQL (CDC) → Kafka → Stream Processor → [Redis, External System]
     ↑                ↑                              ↑
  WAL Logical     Debezium CDC              Multi-sink Fan-out
```

### Components

- **PostgreSQL**: Source database with WAL=logical for CDC
- **Debezium**: Change Data Capture connector via Kafka Connect
- **Kafka**: Message streaming bus
- **Stream Processor**: Core enrichment and transformation engine (Python)
- **Redis**: Real-time metrics and 10-minute rolling aggregations
- **External System**: Webhook receiver with monitoring dashboard
- **Data Generator**: Realistic event simulation for testing

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- 8GB RAM minimum
- Available ports: 5432, 6379, 9092, 8083, 5001

### One-Click Setup
```bash
# Clone and start the complete pipeline
git clone https://github.com/mnkatih1/Thamaniyah_assignment
cd Thamaniyah_assignment

# Start everything automatically (5 minutes)
chmod +x quick_start.sh
./quick_start.sh

# Run comprehensive tests (2 minutes)
chmod +x test_pipeline.sh
./test_pipeline.sh
```

### Manual Verification
```bash
# Insert a test event
docker exec postgres psql -U user -d streaming_db -c "
INSERT INTO engagement_events (user_id, content_id, event_type, duration_ms, device) 
SELECT uuid_generate_v4(), id, 'play', 8000, 'readme-test' 
FROM content LIMIT 1;"

# Verify results (< 5 seconds latency)
docker exec redis redis-cli ZREVRANGE engagement:10min 0 3 WITHSCORES
curl http://localhost:5001/stats
```

## 📊 Requirements Coverage

| Requirement | Status | Implementation | Notes |
|------------|--------|----------------|-------|
| ✅ **PostgreSQL Streaming** | Complete | Debezium CDC → Kafka | Working |
| ✅ **Data Enrichment** | Complete | Content lookup + engagement calculations | Working |
| ✅ **Multi-sink Fan-out** | **Partial** | Redis + External System | **BigQuery not implemented** |
| ✅ **<5s Latency** | **Tested ✓** | Direct stream processor | Measured: ~2-3s |
| ✅ **Time-based Aggregations** | Complete | Redis sorted sets with 10min TTL | Working |
| ⚠️ **Processing Guarantees** | **At-least-once** | Kafka consumer + HTTP calls | **Not exactly-once** |
| ❌ **Backfill Mechanism** | **Not implemented** | - | **Missing feature** |
| ✅ **Reproducible Environment** | Complete | Docker Compose + automated scripts | Working |

## 🔄 Data Transformations

### Enrichment Pipeline
For each `engagement_events` record:

1. **Join** with `content` table to fetch:
   - `content_type` (podcast, newsletter, video)
   - `length_seconds` (content duration)

2. **Calculate** derived fields:
   ```python
   engagement_seconds = duration_ms / 1000.0
   engagement_pct = engagement_seconds / length_seconds  # 4 decimal precision
   ```

3. **Handle NULL values**:
   - If `duration_ms` is NULL → `engagement_seconds` = NULL
   - If `length_seconds` is NULL → `engagement_pct` = NULL

### Real-time Aggregations
```python
# Redis Sorted Set: "engagement:10min"
# Key: content_id → Score: cumulative engagement_seconds
# TTL: 600 seconds (rolling 10-minute window)
ZREVRANGE engagement:10min 0 5 WITHSCORES  # Top engaging content
```

## 🎪 Monitoring & Dashboards

### Web Interfaces

#### External System Dashboard
- **Local environment**: http://localhost:5001
- **GitHub Codespaces**: `echo "Dashboard URL: https://$CODESPACE_NAME-5001.app.github.dev"`

#### Kafka Connect API
- **Local**: http://localhost:8083/connectors
- **Codespaces**: `echo "Dashboard URL: https://$CODESPACE_NAME-8083.app.github.dev/connectors"`

#### Stream Processor Logs
`docker logs stream-processor -f`

### Real-time Metrics
```bash
# Top engaging content (last 10 minutes)
docker exec redis redis-cli ZREVRANGE engagement:10min 0 5 WITHSCORES

# External system statistics
curl http://localhost:5001/stats | jq '.'

# Recent events
curl http://localhost:5001/events?limit=5 | jq '.'

# Live monitoring
watch "docker exec redis redis-cli ZREVRANGE engagement:10min 0 3 WITHSCORES"
```

## ⚠️ Current Limitations & Production Considerations

### Missing Features
- **BigQuery/Analytical Sink**: Not implemented (would require BigQuery client + authentication)
- **Exactly-once Processing**: Current implementation uses at-least-once semantics
- **Backfill Mechanism**: No historical data reprocessing capability
- **Schema Registry**: Not using Confluent Schema Registry for message evolution
- **Monitoring**: No Prometheus/Grafana metrics collection

### Processing Guarantees
The current implementation provides **at-least-once** delivery guarantees:
- Kafka consumer uses auto-commit
- No transactional writes to Redis/External System
- Possible duplicate processing on failures

### For Production Use, Consider Adding:
```python
# Exactly-once processing would require:
# 1. Idempotent operations (Redis: SET vs ZINCRBY)
# 2. Transactional writes or deduplication keys
# 3. Kafka transactional producer/consumer
```

## 🧪 Testing

### Automated Tests
```bash
# Run complete pipeline tests
./test_pipeline.sh

# Manual latency test
time docker exec postgres psql -U user -d streaming_db -c "INSERT INTO engagement_events..."
```

### Test Scenarios Covered
- ✅ Service health checks
- ✅ End-to-end data flow
- ✅ Latency requirements (<5s)
- ✅ Multi-sink verification
- ✅ Data enrichment validation

## 🛠️ Development

### Local Development
```bash
# View logs
docker logs stream-processor -f
docker logs kafka-connect -f

# Debug Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:29092
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic engagement_events --from-beginning

# Debug Redis
docker exec redis redis-cli
> ZREVRANGE engagement:10min 0 -1 WITHSCORES
```

### Troubleshooting
```bash
# Check connector status
curl http://localhost:8083/connectors/engagement-events-connector/status

# Restart failed components
docker-compose restart stream-processor
docker-compose restart kafka-connect
```

## 📈 Performance Characteristics

Based on testing:
- **Latency**: 2-3 seconds end-to-end
- **Throughput**: ~100-500 events/second (single partition)
- **Memory**: ~2GB total for all containers
- **Storage**: Minimal (Redis TTL + Kafka log retention)

## 🎯 Next Steps / Future Improvements

If more time was available, the following enhancements would be added:

1. **BigQuery Integration**: 
   ```python
   from google.cloud import bigquery
   # Batch writes to BigQuery tables
   ```

2. **Exactly-once Processing**:
   ```python
   # Kafka transactions + idempotent operations
   consumer.enable_auto_commit = False
   # Manual offset management with transactional writes
   ```

3. **Backfill Capability**:
   ```python
   # Historical data processing from PostgreSQL
   # Configurable time range replay
   ```

4. **Enhanced Monitoring**:
   - Prometheus metrics
   - Grafana dashboards
   - Alerting on processing delays

5. **Schema Evolution**:
   - Confluent Schema Registry
   - Backward/forward compatibility

This implementation successfully demonstrates the core streaming architecture with real-time CDC, data enrichment, and multi-sink distribution while being transparent about current limitations.
