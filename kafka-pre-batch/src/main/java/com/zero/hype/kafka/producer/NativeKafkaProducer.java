package com.zero.hype.kafka.producer;

import com.zero.hype.kafka.util.OtelMeterRegistryManager;
import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * NativeKafkaProducer implements a standard Kafka producer that uses Kafka's native
 * batching mechanism. This producer is used as a comparison to the pre-batching approach
 * implemented in ByteArrayKafkaProducer.
 *
 * Key features:
 * - Uses Kafka's built-in batching and compression
 * - Asynchronous message publishing using CompletableFuture
 * - Metrics collection for monitoring message throughput
 * - Thread-safe operation
 *
 * This producer is configured to use Kafka's native batching settings:
 * - batch.size: 32KB
 * - compression.type: gzip
 * - linger.ms: 5000ms
 *
 * Usage example:
 * <pre>
 * NativeKafkaProducer producer = new NativeKafkaProducer(manager, config);
 * producer.publish("message").thenAccept(success -> {
 *     if (success) {
 *         // Message published successfully
 *     }
 * });
 * </pre>
 */
public class NativeKafkaProducer {

    private final KafkaProducer<String, String> producer;
    private final Counter counter;

    /**
     * Creates a new NativeKafkaProducer instance.
     *
     * @param meterRegistryManager The OpenTelemetry meter registry manager for metrics collection
     * @param additionalConfig Additional Kafka producer configuration
     *                        (bootstrap servers, serializers, etc.)
     */
    public NativeKafkaProducer(OtelMeterRegistryManager meterRegistryManager, Map<String, Object> additionalConfig) {
        producer = new KafkaProducer<>(additionalConfig);
        counter = meterRegistryManager.getCounter("kafka.producer.send", "topic", "test");
    }

    /**
     * Publishes a single message to Kafka using Kafka's native batching.
     * The message will be batched according to Kafka's batching configuration.
     *
     * @param message The message to publish
     * @return A CompletableFuture that completes with true if the message was published
     *         successfully, or completes exceptionally if an error occurs
     */
    public CompletableFuture<Boolean> publish(String message) {
        ProducerRecord<String, String> kafkaRecord =
                new ProducerRecord<>(
                        "test",
                        message
                );

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
    }
}
