# Kafka Pre-Batching Docker Environment

This Docker environment provides a complete setup for testing and comparing Kafka's native batching versus application-level pre-batching.
It includes a Kafka cluster, monitoring tools (Prometheus, Grafana), and OpenTelemetry for metrics collection.

## Quick Start

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop all services
docker-compose down
```

## Services Overview

### 1. Kafka Cluster
- **Service Name**: `kafka1`
- **Ports**: 
  - 19092 (internal)
  - 9092 (external)
  - 29092 (docker internal)
  - 10999 (JMX)
  - 7071 (JMX Prometheus exporter)
- **Configuration**:
  - Single broker for testing
  - 2GB heap size
  - JMX monitoring enabled
  - Auto topic creation enabled
  - Log retention: 1 hour
  - Log segment: 10 minutes

### 2. Zookeeper
- **Service Name**: `zoo1`
- **Port**: 2181
- **Purpose**: Manages Kafka cluster coordination
- **Configuration**:
  - Standalone mode
  - Server ID: 1

### 3. OpenTelemetry Collector
- **Service Name**: `kafka-otel-collector`
- **Ports**:
  - 9464 (Prometheus scrape)
  - 4317 (OTLP gRPC)
  - 4318 (OTLP HTTP)
- **Purpose**: Collects and exports metrics from Kafka and applications

### 4. Prometheus
- **Service Name**: `kafka-prometheus`
- **Port**: 9999
- **Purpose**: Metrics collection and storage
- **Configuration**:
  - Scrapes metrics from Kafka and OpenTelemetry collector
  - Stores metrics for visualization

### 5. Grafana
- **Service Name**: `kafka-grafana`
- **Port**: 3000
- **Purpose**: Metrics visualization
- **Default Credentials**:
  - Username: admin
  - Password: admin
- **Pre-configured Dashboard**:
  - Location: `grafana-dashes/kafka-dashboard.json`
  - Description: "Kafka resource usage and consumer lag overview"
  - Key Panels:
    - Pre-batching Metrics:
      - Bytes After Compression
      - Compression Ratio
      - Message Throughput
      - Batch Size Analysis
    - System Metrics:
      - CPU Usage
      - Memory Utilization
      - Network I/O
      - Disk Operations
    - Kafka Metrics:
      - Consumer Lag
      - Topic Size
      - Message Rate
      - Broker Health
  - Import steps:
    1. Login to Grafana (http://localhost:3000)
    2. Click "+" in the sidebar
    3. Select "Import"
    4. Click "Upload JSON file"
    5. Select `grafana-dashes/kafka-dashboard.json`
    6. Select Prometheus as the data source
    7. Click "Import"

### 6. Kafka UI
- **Service Name**: `kafka-ui`
- **Port**: 9000
- **Purpose**: Web interface for Kafka management
- **Features**:
  - Topic management
  - Message browsing
  - Consumer group monitoring
  - Dynamic configuration

## Monitoring

### Accessing Dashboards

1. **Grafana**
   - URL: http://localhost:3000
   - Login with admin/admin
   - Import dashboards for:
     - Kafka metrics
     - Producer performance
     - Consumer lag
     - Broker health

2. **Kafka UI**
   - URL: http://localhost:9000
   - Features:
     - Topic management
     - Message browsing
     - Consumer groups
     - Broker metrics

3. **Prometheus**
   - URL: http://localhost:9999
   - Query metrics directly
   - View targets status
   - Check alert rules

### Key Metrics

1. **Kafka Metrics**
   - Broker CPU and memory
   - Topic size and message rates
   - Consumer lag
   - Network throughput

2. **Producer Metrics**
   - Message rate
   - Batch size
   - Compression ratio
   - Send latency

3. **System Metrics**
   - Memory usage
   - Network I/O
   - Disk utilization

## Configuration

### Kafka Settings

The Kafka broker is configured with:
- 2GB heap size
- JMX monitoring
- External and internal listeners
- 1-hour log retention
- Auto topic creation

### Monitoring Configuration

1. **OpenTelemetry Collector**
   - Configuration: `otel-collector-config.yml`
   - Collects metrics from Kafka and applications
   - Exports to Prometheus

2. **Prometheus**
   - Configuration: `prometheus/prometheus.yml`
   - Scrapes metrics from Kafka and collector
   - 15s scrape interval

3. **Grafana**
   - Persistent storage for dashboards
   - Pre-configured data sources
   - Pre-built dashboard (`grafana-dashes/kafka-dashboard.json`) includes:
     - Real-time compression metrics
     - Message throughput visualization
     - System resource monitoring
     - Kafka broker health indicators
     - Consumer lag tracking
     - Topic-level statistics
   - Customizable panels and thresholds
   - Annotations for important events

## Troubleshooting

### Common Issues

1. **Services Not Starting**
```bash
# Check service logs
docker-compose logs [service-name]

# Verify network
docker network ls
docker network inspect kafka-pre-batch_kafka
```

2. **Metrics Not Showing**
- Check Prometheus targets: http://localhost:9999/targets
- Verify OpenTelemetry collector: http://localhost:9464/metrics
- Check Kafka JMX: http://localhost:7071/metrics

3. **Kafka UI Issues**
- Verify Kafka connectivity
- Check dynamic config: `kui/kui-config.yml`
- View logs: `docker-compose logs kafka-ui`

### Logs

Access logs for any service:
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f [service-name]
```

## Cleanup

```bash
# Stop and remove containers
docker-compose down

# Remove volumes (clears all data)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

## Contributing

Feel free to:
1. Submit PRs for new test scenarios
2. Add more monitoring dashboards
3. Improve documentation
4. Add support for other message brokers

## License

MIT License - See LICENSE file for details 