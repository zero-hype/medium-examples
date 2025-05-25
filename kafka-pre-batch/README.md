# Kafka Pre-Batching Example

This project demonstrates an alternative approach to Kafka message batching by implementing application-level pre-batching before sending messages to Kafka. This example compares two different batching strategies:

1. **Application-Level Pre-Batching** (Custom Implementation)
   - Messages are collected and batched at the application level
   - Batches are compressed using GZIP before sending to Kafka
   - Provides more control over batching logic and compression
   - Reduces network overhead by sending fewer, larger messages

2. **Native Kafka Batching** (Standard Approach)
   - Uses Kafka's built-in batching mechanism
   - Relies on Kafka's native compression
   - Simpler implementation but less control over batching

## Why Pre-Batching?

Kafka's native batching is powerful, but there are scenarios where application-level pre-batching can be beneficial:

1. **Network Efficiency**
   - Reduces the number of network calls to Kafka
   - Compresses multiple messages together for better compression ratios
   - Minimizes network overhead for high-throughput applications

2. **Custom Batching Logic**
   - Implement custom batching strategies based on your needs
   - Control batch sizes and timing more precisely
   - Add application-specific optimizations

3. **Compression Control**
   - Apply compression before sending to Kafka
   - Choose compression algorithms based on your data characteristics
   - Optimize for your specific use case

## How It Works

### 1. Message Collection
```java
KafkaPreBatcher batcher = new KafkaPreBatcher(manager, 1000, producer);
batcher.add("message").thenAccept(success -> {
    if (success) {
        // Message added to batch successfully
    }
});
```
- Messages are collected in a thread-safe queue
- Configurable batch size (default: 1000 messages)
- Asynchronous processing using CompletableFuture

### 2. Batch Processing
```java
// When batch size is reached:
List<String> batch = new ArrayList<>(batchSize);
queue.drainTo(batch, batchSize);
shipIt(batch);
```
- Messages are accumulated until batch size is reached
- Batches are processed asynchronously
- Thread-safe batch handling

### 3. Compression and Sending
```java
byte[] bytes = serialize(batch);  // Compresses the batch
kafkaProducer.publish(bytes);     // Sends to Kafka
```
- Batches are compressed using GZIP
- Compressed batches are sent as single Kafka messages
- Metrics are collected for monitoring

## Getting Started

1. **Prerequisites**
   - Java 24
   - Docker and Docker Compose
   - Maven

2. **Start the Infrastructure**
   ```bash
   cd docker/kafka
   docker-compose up -d
   ```

3. **Build the Project**
   ```bash
   mvn clean package
   ```

4. **Run the Example**
   ```bash
   # Terminal 1: Start the consumer
   java -cp target/kafka-pre-batch-1.0-SNAPSHOT.jar com.zero.hype.kafka.app.KafkaConsumerApp

   # Terminal 2: Run the pre-batching producer
   java -cp target/kafka-pre-batch-1.0-SNAPSHOT.jar com.zero.hype.kafka.app.ByteArrayKafkaProducerApp

   # Terminal 3: Run the native batching producer (for comparison)
   java -cp target/kafka-pre-batch-1.0-SNAPSHOT.jar com.zero.hype.kafka.app.NativeKafkaProducerApp
   ```

## Monitoring

The application includes a complete monitoring stack:
- OpenTelemetry for metrics collection
- Prometheus for metrics storage
- Grafana for visualization

Key metrics available:
- Message throughput
- Batch sizes
- Compression ratios
- Processing latency
- Error rates

Access the monitoring tools:
- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9999
- Kafka UI: http://localhost:9000

## Performance Considerations

### Pre-batching Advantages
- Reduced network calls to Kafka
- Better compression ratios
- More control over batching logic
- Custom optimization possibilities

### Pre-batching Trade-offs
- Additional application complexity
- Memory usage for message buffering
- Potential latency for small batches
- Need to handle batch failures

## When to Use Pre-batching

Consider pre-batching when:
1. Your application has high message throughput
2. Network efficiency is critical
3. You need custom batching logic
4. You want more control over compression
5. Your messages are small and benefit from batching

## Configuration

### Pre-batcher Configuration
```java
Map.of(
    KafkaConstants.CONFIG_TOPIC, "test-bytes",
    KafkaConstants.CONFIG_BOOTSTRAP_SERVERS, "localhost:9092",
    KafkaConstants.CONFIG_ACKS, "1",
    KafkaConstants.CONFIG_COMPRESSION_TYPE, "none",  // We handle compression
    KafkaConstants.CONFIG_LINGER_MS, "0"            // No additional batching
)
```

### Batch Size Considerations
- Default: 1000 messages
- Queue capacity: 100x batch size
- Adjust based on:
  - Message size
  - Memory constraints
  - Latency requirements
  - Throughput needs

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is licensed under the MIT License - see the LICENSE file for details.
