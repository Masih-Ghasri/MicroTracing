# 🔍 MicroTracing

A comprehensive distributed microservices system demonstrating **observability**, **monitoring**, and **distributed tracing** using modern technologies.

## 🏗️ Architecture Overview

```mermaid
graph TB
    API[Producer API :8081] --> RMQ[RabbitMQ :5672]
    RMQ --> CONSUMER[Consumer Service :8082]
    
    API --> JAEGER[Jaeger :16686]
    CONSUMER --> JAEGER
    
    API --> ES[Elasticsearch :9200]
    CONSUMER --> ES
    ES --> KIBANA[Kibana :5601]
    
    API --> PROM[Prometheus :9090]
    CONSUMER --> PROM
    NODE[Node Exporter :9100] --> PROM
    
    LOGSTASH[Logstash :5044] --> ES
    REDIS[Redis :6379]
```

## 🚀 Features

### 📡 **Distributed Tracing**
- **OpenTelemetry** integration with Jaeger
- **Trace propagation** between services via RabbitMQ headers
- **Span correlation** across message queues
- **Performance monitoring** with custom metrics

### 📊 **Comprehensive Logging**
- **Structured logging** to Elasticsearch
- **Real-time log analysis** with Kibana
- **Log correlation** with trace IDs
- **Multiple log levels** (debug, info, error)

### 📈 **Monitoring & Metrics**
- **Prometheus** metrics collection
- **Custom metrics**: message counters, processing time, queue latency
- **System metrics** via Node Exporter
- **Health check endpoints**

### 🔄 **Message Processing**
- **Priority-based** message handling (high, normal, low)
- **Graceful error handling** with retry mechanism
- **Queue durability** and message TTL
- **Bulk message operations**

### 🛠️ **Developer Experience**
- **Hot reload** configuration
- **Load testing** endpoints
- **Comprehensive health checks**
- **Docker containerization**

## 🏃‍♂️ Quick Start

### Prerequisites
- Docker & Docker Compose
- Go 1.24+ (for development)

### 1️⃣ **Clone Repository**
```bash
git clone https://github.com/Masih-Ghasri/MicroTracing.git
cd MicroTracing
```

### 2️⃣ **Start All Services**
```bash
docker-compose up -d
cd /consumer
  go run main.go
cd /producer
  go run main.go
```

### 3️⃣ **Verify Deployment**
```bash
# Check all services are running
docker-compose ps

# Health checks
curl http://localhost:8085/health  # Producer
curl http://localhost:8086/health  # Consumer
```

## 🎯 Usage Examples

### 📤 **Send Single Message**
```bash
curl -X POST http://localhost:8085/send \
  -H "Content-Type: application/json" \
  -d '{
    "content": "Hello MicroTracing!",
    "priority": "high"
  }'
```

### 📦 **Bulk Message Send**
```bash
curl -X POST http://localhost:8085/send-bulk \
  -H "Content-Type: application/json" \
  -d '{
    "content": "Bulk test message",
    "count": 50,
    "priority": "normal"
  }'
```

### ⚡ **Load Testing**
```bash
curl -X POST http://localhost:8085/simulate-load \
  -H "Content-Type: application/json" \
  -d '{
    "duration_seconds": 60,
    "messages_per_second": 10
  }'
```

## 🌐 Service Endpoints

| Service | URL                          | Purpose |
|---------|------------------------------|---------|
| **Producer API** | http://localhost:8085        | Message sending endpoints |
| **Consumer Health** | http://localhost:8086/health | Consumer service status |
| **Jaeger UI** | http://localhost:16686       | Distributed tracing |
| **Kibana** | http://localhost:5601        | Log analysis |
| **RabbitMQ Management** | http://localhost:15672       | Queue management |
| **Prometheus** | http://localhost:9090        | Metrics & monitoring |
| **Elasticsearch** | http://localhost:9200        | Search engine |

**Default Credentials:**
- RabbitMQ: `admin` / `admin`

## 📊 Monitoring & Observability

### 🔍 **Jaeger Tracing**
1. Visit http://localhost:16686
2. Select `producer-service` or `consumer-service`
3. Search for traces to see end-to-end request flow
4. Analyze performance bottlenecks and latency

### 📈 **Kibana Logs**
1. Open http://localhost:5601
2. Create index pattern: `microservice-logs-*`
3. Explore logs with trace correlation
4. Create dashboards for monitoring

### ⚡ **Prometheus Metrics**
1. Access http://localhost:9090
2. Query custom metrics:
    - `messages_sent_total`
    - `messages_processed_total`
    - `message_processing_duration_seconds`
    - `message_queue_latency_seconds`

## 🛠️ Development

### **Local Development**
```bash
# Start dependencies only
docker-compose up -d rabbitmq elasticsearch kibana jaeger prometheus

# Run producer locally
cd producer
go mod tidy
go run main.go

# Run consumer locally (another terminal)
cd consumer
go mod tidy
go run main.go
```

### **Environment Variables**
```bash
# Producer
RABBITMQ_URL=amqp://admin:admin@localhost:5672/
JAEGER_ENDPOINT=http://localhost:14268/api/traces
ELASTICSEARCH_URL=http://localhost:9200
SERVICE_NAME=producer-service
PORT=8080

# Consumer
RABBITMQ_URL=amqp://admin:admin@localhost:5672/
JAEGER_ENDPOINT=http://localhost:14268/api/traces
ELASTICSEARCH_URL=http://localhost:9200
SERVICE_NAME=consumer-service
HTTP_PORT=8081
```

## 🏗️ Architecture Details

### **Message Flow**
1. **API Request** → Producer service receives HTTP request
2. **Trace Creation** → OpenTelemetry starts trace with unique ID
3. **Message Creation** → Producer creates message with trace context
4. **Queue Publishing** → Message sent to RabbitMQ with trace headers
5. **Consumer Processing** → Consumer picks up message and continues trace
6. **Logging** → Structured logs sent to Elasticsearch with trace correlation
7. **Metrics** → Performance metrics recorded in Prometheus

### **Priority Processing**
- **High Priority**: 100-300ms processing time
- **Normal Priority**: 200-500ms processing time
- **Low Priority**: 500-1500ms processing time

### **Error Handling**
- **Retry Logic**: Failed messages are requeued once
- **Dead Letter**: After retry failure, messages are rejected
- **Error Metrics**: All failures tracked in Prometheus
- **Error Logging**: Detailed error logs in Elasticsearch

## 📋 API Reference

### Producer Service (`localhost:8085`)

#### `POST /send`
Send single message
```json
{
  "content": "string (required)",
  "priority": "high|normal|low (optional)"
}
```

#### `POST /send-bulk`
Send multiple messages
```json
{
  "content": "string (required)",
  "count": "int (1-1000)",
  "priority": "high|normal|low (optional)"
}
```

#### `GET /health`
Service health status

#### `GET /metrics`
Service metrics (Prometheus format)

### Consumer Service (`localhost:8086`)

#### `GET /health`
Consumer health and connection status

#### `GET /stats`
Processing statistics
```json
{
  "processed_messages": 1234,
  "error_messages": 5,
  "uptime_seconds": 3600,
  "processing_rate_per_second": 2.5
}
```

## 🔧 Configuration

### **Docker Compose Services**
- **Elasticsearch**: Log storage and search
- **Kibana**: Log visualization
- **Logstash**: Log processing pipeline
- **RabbitMQ**: Message queue
- **Jaeger**: Distributed tracing
- **Producer**: Message sender service
- **Consumer**: Message processor service
- **Redis**: Caching (ready for use)
- **Prometheus**: Metrics collection
- **Node Exporter**: System metrics

### **Persistent Volumes**
- `rabbitmq_data`: Queue persistence
- `elasticsearch_data`: Log persistence
- `redis_data`: Cache persistence

## 🐛 Troubleshooting

### **Common Issues**

**Services won't start:**
```bash
# Check logs
docker-compose logs <service_name>

# Restart specific service
docker-compose restart <service_name>
```

**RabbitMQ connection refused:**
```bash
# Wait for RabbitMQ to be ready
docker-compose logs rabbitmq

# Check health
curl http://localhost:15672
```

**Elasticsearch not ready:**
```bash
# Check cluster health
curl http://localhost:9200/_cluster/health

# View logs
docker-compose logs elasticsearch
```

**No traces in Jaeger:**
- Verify `JAEGER_ENDPOINT` environment variable
- Check if services are sending traces: `docker-compose logs jaeger`

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## 📧 Contact

**Masih Ghasri** - [@Masih-Ghasri](https://github.com/Masih-Ghasri)

Project Link: [https://github.com/Masih-Ghasri/MicroTracing](https://github.com/Masih-Ghasri/MicroTracing)

---

⭐ **Star this repo if you found it helpful!**