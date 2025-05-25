package com.zero.hype.kafka.producer;

import static com.zero.hype.kafka.util.KafkaConstants.TOPIC_LABEL;
import static com.zero.hype.kafka.util.KafkaConstants.TOPIC_TEST_BYTES;

import com.zero.hype.kafka.util.OtelMeterRegistryManager;
import io.micrometer.core.instrument.Counter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ByteArrayKafkaProducer is a specialized Kafka producer that handles sending pre-batched,
 * compressed messages to Kafka. This producer is designed to work in conjunction with
 * KafkaPreBatcher to send application-level batched messages.
 *
 * Key features:
 * - Handles byte array serialization for compressed batches
 * - Asynchronous message publishing using CompletableFuture
 * - Metrics collection for monitoring message throughput
 * - Thread-safe operation
 *
 * This producer is configured to work with the pre-batching approach, where messages
 * are batched and compressed at the application level before being sent to Kafka.
 *
 * Usage example:
 * <pre>
 * ByteArrayKafkaProducer producer = new ByteArrayKafkaProducer(manager, config);
 * producer.publish(compressedBatch).thenAccept(success -> {
 *     if (success) {
 *         // Batch published successfully
 *     }
 * });
 * </pre>
 */
public class ByteArrayKafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(ByteArrayKafkaProducer.class);
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final KafkaProducer<String, byte[]> producer;
    private final Counter counter;

    /**
     * Creates a new ByteArrayKafkaProducer instance.
     *
     * @param meterRegistryManager The OpenTelemetry meter registry manager for metrics collection
     * @param additionalConfig Additional Kafka producer configuration
     *                        (bootstrap servers, serializers, etc.)
     */
    public ByteArrayKafkaProducer(OtelMeterRegistryManager meterRegistryManager, Map<String, Object> additionalConfig) {
        producer = new KafkaProducer<>(additionalConfig);
        this.counter = meterRegistryManager.getCounter("kafka.producer.send", TOPIC_LABEL, TOPIC_TEST_BYTES);
    }

    /**
     * Publishes a compressed batch of messages to Kafka.
     * The batch should be pre-compressed using GZIP compression.
     *
     * @param bytes The compressed byte array containing the batched messages
     * @return A CompletableFuture that completes with true if the batch was published
     *         successfully, or completes exceptionally if an error occurs
     */
    public CompletableFuture<Boolean> publish(byte[] bytes) {
        if (!shutdown.get()) {
            ProducerRecord<String, byte[]> kafkaRecord = new ProducerRecord<>(TOPIC_TEST_BYTES, bytes);

            CompletableFuture<Boolean> future = new CompletableFuture<>();
            producer.send(kafkaRecord, (metadata, exception) -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    counter.increment();
                    future.complete(Boolean.TRUE);
                }
            });
            return future;
        } else {
            return CompletableFuture.completedFuture(Boolean.FALSE);
        }
    }

    /**
     * Closes the producer, releasing any resources held by it.
     * This method should be called when the producer is no longer needed.
     */
    public void close() {
        try {
            producer.flush();
            producer.close();
            logger.info("{} producer closed", getClass().getSimpleName());
        } catch (Exception e) {
            logger.error("Error closing Kafka producer {}", e.getMessage(), e);
        }
    }
}
