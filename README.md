# DataGuardian: Real-Time Data Quality Monitoring

A production-grade backend system for real-time data quality assessment using PySpark, Kafka, Redis, and Spring Boot.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│ Data Sources                                        │
│ (Patient Data / Medical Records)                    │
└────────────────┬────────────────────────────────────┘
                 │
                 ▼
      ┌──────────────────────┐
      │ Data Generator       │
      │ (Node.js / Python)   │
      └──────────┬───────────┘
                 │
                 ▼
    ┌────────────────────────────┐
    │ Kafka Topic                │
    │ "patient_data"             │
    └────────────┬───────────────┘
                 │
     ┌───────────┴────────────┐
     │                        │
     ▼                        ▼
┌──────────────────┐   ┌──────────────────┐
│ Streaming Engine │   │ Batch Analytics  │
│ (PySpark)        │   │ (PySpark)        │
│ Real-time DQ     │   │ Historical       │
└────────┬─────────┘   └────────┬─────────┘
         │                      │
         │                      │
    ┌────┴──────────────────┐   │
    │                       │   │
    ▼                       ▼   ▼
┌─────────────────┐   ┌──────────────────┐
│ Kafka Topic     │   │ Redis Cache      │
│ "dq_issues"     │   │ Metrics          │
└─────────┬───────┘   └────────┬─────────┘
          │                    │
          └────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │ REST API             │
        │ (Spring Boot)        │
        │ Endpoints:           │
        │ - /metrics/{hospital}│
        │ - /status            │
        │ - /health            │
        └────────────┬─────────┘
                     │
                     ▼
        ┌──────────────────────┐
        │ Dashboard/Client     │
        │ (React - Optional)   │
        └──────────────────────┘
```

## Components

### 1. **Data Generator** (`pyspark/data_generator.py`)
- Simulates patient data from multiple hospitals
- Produces to Kafka `patient_data` topic
- Intentionally creates 10% bad data for testing

### 2. **Streaming Engine** (`pyspark/streaming_engine.py`)
- Real-time data quality checks using PySpark Streaming
- Completeness checks
- Validity checks (range validation)
- Statistical anomaly detection (Z-score)
- Outputs issues to Kafka and caches in Redis

### 3. **Batch Analytics** (`pyspark/batch_analytics.py`)
- Historical DQ analysis
- Daily metrics calculation
- Trend identification
- Pattern discovery
- Predictive scoring

### 4. **REST API** (`api/src/main/java/com/dataguardian/`)
- Spring Boot service
- Endpoints for metrics, status, health
- Redis integration for caching
- Kafka consumer (optional)

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.9+
- Java 11+
- Maven 3.8+

### 1. Clone & Setup

```bash
git clone https://github.com/AsthaChoudhury/data-guardian.git
cd data-guardian

# Create Python virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install Python dependencies
pip install -r pyspark/requirements.txt

# Copy environment file
cp .env.example .env
```

### 2. Start Infrastructure (Kafka + Redis)

```bash
# Start all services
docker-compose up -d zookeeper kafka redis

# Wait for Kafka to be ready (check health)
sleep 30

# Verify Kafka is running
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

### 3. Run Streaming Engine

```bash
# Terminal 1: Start streaming engine
python pyspark/streaming_engine.py
```

### 4. Generate Test Data

```bash
# Terminal 2: Generate patient data
python pyspark/data_generator.py
```

### 5. Start REST API

```bash
# Terminal 3: Build & run Spring Boot API
cd api
mvn clean install
mvn spring-boot:run
```

### 6. Test API

```bash
# Get metrics for Hospital_A
curl http://localhost:8080/api/v1/metrics/Hospital_A

# Get all metrics
curl http://localhost:8080/api/v1/metrics

# Health check
curl http://localhost:8080/api/v1/health

# Status
curl http://localhost:8080/api/v1/status
```

## Using Docker Compose (One Command)

```bash
# Start everything at once
docker-compose -f docker-compose.yml up -d

# View logs
docker-compose logs -f streaming-engine

# Stop everything
docker-compose down
```

## File Structure

```
data-guardian/
├── pyspark/
│   ├── streaming_engine.py        # Core DQ engine
│   ├── batch_analytics.py         # Historical analysis
│   ├── data_generator.py          # Test data producer
│   ├── config.py                  # Configuration
│   ├── requirements.txt           # Python dependencies
│   ├── Dockerfile.streaming       # Streaming container
│   └── Dockerfile.generator       # Generator container
│
├── api/
│   ├── pom.xml                    # Maven config
│   ├── Dockerfile                 # API container
│   ├── application.yml            # Spring config
│   └── src/main/java/com/dataguardian/
│       ├── controller/            # REST endpoints
│       ├── service/               # Business logic
│       ├── model/                 # Data models
│       ├── config/                # Spring config
│       └── repository/            # Data access
│
├── docker-compose.yml             # Container orchestration
├── requirements.txt               # All Python deps
├── .env                           # Environment variables
└── README.md                      # This file
```

## Data Quality Rules

All rules defined in `pyspark/config.py`:

```python
{
    "completeness": {
        "required_fields": ["patient_id", "age", "bp_systolic", "temperature"],
        "max_null_pct": 5
    },
    "validity": {
        "age": {"min": 0, "max": 150},
        "bp_systolic": {"min": 50, "max": 250},
        "temperature": {"min": 35.0, "max": 45.0}
    },
    "anomaly_detection": {
        "z_score_threshold": 3,
        "window_size": 1000
    }
}
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/metrics/{hospital}` | Get DQ metrics for hospital |
| GET | `/api/v1/metrics` | Get all hospital metrics |
| GET | `/api/v1/status` | Get current system status |
| GET | `/api/v1/health` | Health check |
| GET | `/api/v1/info` | API information |

## Performance

- **Data Volume**: 10K+ records/second
- **Latency**: <2 second detection
- **Storage**: Parquet (columnar compression)
- **Caching**: Redis (5 min TTL)

## Monitoring

### View Streaming Engine Logs
```bash
python pyspark/streaming_engine.py  # Watch console output
```

### View API Logs
```bash
docker logs -f api
```

### Check Kafka Topics
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

### Check Redis Cache
```bash
docker exec redis redis-cli KEYS "dg:*"
docker exec redis redis-cli GET "dg:daily_metrics:Hospital_A"
```

## Next Steps (Frontend)

To add React dashboard:
1. Create `dashboard/` directory
2. Build React app with `/api/v1/metrics` integration
3. Real-time updates via WebSocket
4. Deploy as separate container

## Troubleshooting

### Kafka not connecting
```bash
# Check Kafka is running
docker exec kafka kafka-broker-api-versions --bootstrap-server kafka:9092

# Check firewall
netstat -an | grep 9092
```

### Redis cache not working
```bash
# Check Redis connection
docker exec redis redis-cli ping

# Should return: PONG
```

### Spark memory issues
```bash
# Increase memory in docker-compose.yml
environment:
  SPARK_EXECUTOR_MEMORY: 2g
```

## Contributing

1. Fork repository
2. Create feature branch
3. Make changes
4. Submit pull request

## License

MIT
