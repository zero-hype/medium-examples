package com.zero.hype.kafka.producer;

import static com.zero.hype.kafka.util.KafkaConstants.TOPIC_LABEL;
import static com.zero.hype.kafka.util.KafkaConstants.TOPIC_TEST;

import com.zero.hype.kafka.util.KafkaConstants;
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
 * NativeKafkaProducer implements a standard Kafka producer that uses Kafka's native
 * batching mechanism. This producer is used as a comparison to the pre-batching approach
 * implemented in ByteArrayKafkaProducer.
 * <p>
 * Key features:
 * - Uses Kafka's built-in batching and compression
 * - Asynchronous message publishing using CompletableFuture
 * - Metrics collection for monitoring message throughput
 * - Thread-safe operation
 * <p>
 * This producer is configured to use Kafka's native batching settings:
 * - batch.size: 32KB
 * - compression.type: gzip
 * - linger.ms: 5000ms
 * <p>
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

    private static final Logger logger = LoggerFactory.getLogger(NativeKafkaProducer.class);
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final KafkaProducer<String, String> producer;
    private final Counter counter;
    private final Counter counterFail;

    /**
     * Creates a new NativeKafkaProducer instance.
     *
     * @param meterRegistryManager The OpenTelemetry meter registry manager for metrics collection
     * @param additionalConfig     Additional Kafka producer configuration
     *                             (bootstrap servers, serializers, etc.)
     */
    public NativeKafkaProducer(OtelMeterRegistryManager meterRegistryManager, Map<String, Object> additionalConfig) {
        producer = new KafkaProducer<>(additionalConfig);
        counter = meterRegistryManager.getCounter("kafka.producer.send", TOPIC_LABEL, TOPIC_TEST);
        counterFail = meterRegistryManager.getCounter("kafka.producer.send.fail", TOPIC_LABEL, TOPIC_TEST);

        String clientId = "producer-1";

        meterRegistryManager.registerJmxAttributeGauge(
                KafkaConstants.METRIC_COMPRESSION_RATE,
                "kafka.producer:type=producer-metrics,client-id=" + clientId,
                "compression-rate-avg",
                TOPIC_LABEL,
                TOPIC_TEST);

        meterRegistryManager.registerJmxAttributeGauge(
                KafkaConstants.METRIC_COMPRESSION_SIZE,
                "kafka.producer:type=producer-metrics,client-id=" + clientId,
                "batch-size-avg",
                TOPIC_LABEL,
                TOPIC_TEST);

        meterRegistryManager.registerJmxAttributeGauge(
                KafkaConstants.METRIC_MESSAGES_PER_BATCH,
                "kafka.producer:type=producer-metrics,client-id=" + clientId,
                "records-per-request-avg",
                TOPIC_LABEL,
                TOPIC_TEST);
    }

    /**
     * Publishes a single message to Kafka using Kafka's native batching.
     * The message will be batched according to Kafka's batching configuration.
     *
     * @param message The message to publish
     * @return A CompletableFuture that completes with true if the message was published
     * successfully, or completes exceptionally if an error occurs
     */
    public CompletableFuture<Boolean> publish(String message) {
        if (!shutdown.get()) {
            ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>("test", message);

            CompletableFuture<Boolean> future = new CompletableFuture<>();
            producer.send(kafkaRecord, (metadata, exception) -> {
                if (exception != null) {
                    counterFail.increment();
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
            shutdown.set(true);
            producer.flush();
            producer.close();
            logger.info("{} producer closed", getClass().getSimpleName());
        } catch (Exception e) {
            logger.error("Error closing Kafka producer {}", e.getMessage(), e);
        }
    }
}
