package com.zero.hype.kafka.producer;

import static com.zero.hype.kafka.util.KafkaConstants.*;

import com.zero.hype.kafka.util.GzipCompressor;
import com.zero.hype.kafka.util.KafkaConstants;
import com.zero.hype.kafka.util.OtelMeterRegistryManager;
import com.zero.hype.kafka.util.ZeroProperty;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaPreBatcher implements application-level batching for Kafka messages to optimize throughput
 * and reduce overhead. Instead of sending individual messages to Kafka, this class accumulates
 * messages into larger batches, compresses them, and sends them as a single Kafka message.
 * <p>
 * Key features:
 * - Thread-safe message accumulation using ArrayBlockingQueue
 * - GZIP compression of batched messages
 * - Asynchronous processing using CompletableFuture
 * - Configurable batch size
 * - Automatic batch shipping when size threshold is reached
 * - Comprehensive compression metrics:
 * - Compression ratio per batch
 * - Original and compressed sizes
 * - Compression time
 * - Batch success/failure rates
 * - Message counts per batch
 * <p>
 * Usage example:
 * <pre>
 * ByteArrayKafkaProducer producer = new ByteArrayKafkaProducer(manager, config);
 * KafkaPreBatcher batcher = new KafkaPreBatcher(1000, producer);
 * batcher.add("message").thenAccept(success -> {
 *     if (success) {
 *         // Message added to batch successfully
 *     }
 * });
 * </pre>
 * <p>
 * This class is not designed for production use as-is. It is a simplified example
 * to demonstrate the concept of pre-batching messages for Kafka.
 */
public class KafkaPreBatcher {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPreBatcher.class);

    private final GzipCompressor compressor = new GzipCompressor();
    private final ByteArrayKafkaProducer kafkaProducer;
    private final ArrayBlockingQueue<String> queue;
    private final AtomicBoolean drainInProgress = new AtomicBoolean(false);
    private final ExecutorService executor;
    private final DistributionSummary compressionSizeSummary;
    private final DistributionSummary messagesPerBatchSummary;
    private final Timer compressionTimer;
    private final Counter batchSuccessCounter;
    private final Counter batchFailureCounter;
    private final DistributionSummary compressionRatioSummary;
    private final DistributionSummary batchSizeSummary;

    private ZeroProperty<Integer> batchSizeRef;

    /**
     * Creates a new KafkaPreBatcher instance.
     *
     * @param meterRegistryManager The OpenTelemetry meter registry manager for metrics
     * @param batchSizeRef            The number of messages to accumulate before sending to Kafka
     * @param kafkaProducer        The producer instance used to send batched messages
     */
    public KafkaPreBatcher(
            OtelMeterRegistryManager meterRegistryManager,
            ZeroProperty<Integer> batchSizeRef,
            ByteArrayKafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
        this.batchSizeRef = batchSizeRef;
        // just set as a high value to allow runtime updates to batch size
        this.queue = new ArrayBlockingQueue<>(100000);
        this.executor = Executors.newFixedThreadPool(10);
        this.compressionSizeSummary =
                meterRegistryManager.getDistributionSummary(METRIC_COMPRESSION_SIZE, TOPIC_LABEL, TOPIC_TEST_BYTES);
        this.messagesPerBatchSummary =
                meterRegistryManager.getDistributionSummary(METRIC_MESSAGES_PER_BATCH, TOPIC_LABEL, TOPIC_TEST_BYTES);
        this.compressionTimer = meterRegistryManager.getTimer(METRIC_BATCH_TIME, TOPIC_LABEL, TOPIC_TEST_BYTES);
        this.batchSuccessCounter = meterRegistryManager.getCounter(METRIC_BATCH_SUCCESS, TOPIC_LABEL, TOPIC_TEST_BYTES);
        this.batchFailureCounter = meterRegistryManager.getCounter(METRIC_BATCH_FAILURE, TOPIC_LABEL, TOPIC_TEST_BYTES);
        this.compressionRatioSummary = meterRegistryManager.getDistributionSummary(
                KafkaConstants.METRIC_COMPRESSION_RATE, TOPIC_LABEL, TOPIC_TEST_BYTES);
        this.batchSizeSummary = meterRegistryManager.getDistributionSummary(
                KafkaConstants.METRIC_BATCH_SIZE, TOPIC_LABEL, TOPIC_TEST_BYTES);
    }

    /**
     * Adds a message to the batch queue. If the batch size threshold is reached,
     * triggers the batch to be sent to Kafka.
     *
     * @param message The message to add to the batch
     * @return A CompletableFuture that completes with true if the message was added
     * successfully, false otherwise
     */
    public CompletableFuture<Boolean> add(String message) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        executor.submit(() -> {
            try {
                // Try to add the message to the queue with a 1-second timeout
                boolean offered = queue.offer(message, 1, TimeUnit.SECONDS);
                future.complete(offered);
                int batchSize = this.batchSizeRef.getValue();

                // If message was added, and we've reached batch size, try to ship the batch
                if (offered && queue.size() >= batchSize && drainInProgress.compareAndSet(false, true)) {
                    executor.submit(() -> {
                        try {
                            List<String> batch = new ArrayList<>(batchSize);
                            queue.drainTo(batch, batchSize);
                            shipIt(batch);
                        } finally {
                            drainInProgress.set(false);
                        }
                    });
                }
            } catch (InterruptedException e) {
                future.completeExceptionally(e);
                Thread.currentThread().interrupt();
            }
        });

        return future;
    }

    /**
     * Ships a batch of messages to Kafka as a single compressed message.
     *
     * @param batch List of messages to be sent as a single batch
     */
    private void shipIt(List<String> batch) {
        try {
            // Record batch size metrics
            messagesPerBatchSummary.record(batch.size());
            batchSizeSummary.record(batch.stream().mapToInt(String::length).sum());

            // Time the compression operation
            byte[] bytes = compressionTimer.record(() -> {
                try {
                    return serialize(batch);
                } catch (IOException e) {
                    logger.error("Compression failed: " + e.getMessage(), e);
                    batchFailureCounter.increment();
                    throw new RuntimeException(e);
                }
            });

            kafkaProducer
                    .publish(bytes)
                    .orTimeout(5, TimeUnit.SECONDS)
                    .thenAccept(success -> {
                        if (success) {
                            batchSuccessCounter.increment();
                        } else {
                            batchFailureCounter.increment();
                        }
                    })
                    .exceptionally(ex -> {
                        logger.error("Failed to publish batch: " + ex.getMessage(), ex);
                        batchFailureCounter.increment();
                        return null;
                    });
        } catch (Exception e) {
            logger.error("Batch processing failed: " + e.getMessage(), e);
            batchFailureCounter.increment();
        }
    }

    /**
     * Serializes a list of messages into a single compressed byte array.
     * The serialization process:
     * 1. Converts each string message to UTF-8 bytes
     * 2. Combines all byte arrays into a single array
     * 3. Compresses the combined array using GZIP
     *
     * @param messagesToSerialize List of messages to serialize
     * @return Compressed byte array containing all messages
     * @throws IOException if serialization or compression fails
     */
    public byte[] serialize(List<String> messagesToSerialize) throws IOException {
        if (messagesToSerialize == null || messagesToSerialize.isEmpty()) {
            throw new IOException("Serialization error: empty or null message list");
        }

        try {
            // Precompute and store byte[] to avoid double encoding
            List<byte[]> encodedMessages = new ArrayList<>(messagesToSerialize.size());
            int totalSize = 0;

            for (String message : messagesToSerialize) {
                byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
                encodedMessages.add(bytes);
                totalSize += bytes.length;
            }

            byte[] combined = new byte[totalSize];
            int offset = 0;

            for (byte[] msgBytes : encodedMessages) {
                System.arraycopy(msgBytes, 0, combined, offset, msgBytes.length);
                offset += msgBytes.length;
            }

            byte[] compressedBytes = compressor.compress(combined);

            // Record original and compressed sizes
            compressionSizeSummary.record(compressedBytes.length);

            // Calculate and record compression metrics
            double compressionRatio = (double) compressedBytes.length / combined.length;
            compressionRatioSummary.record(compressionRatio);

            return compressedBytes;
        } catch (Exception e) {
            throw new IOException("GzipCompressionSerializer error: " + e.getMessage(), e);
        }
    }

    public void shutdown() {
        logger.info("Shutting down KafkaPreBatcher");
        try {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Shutdown hook interrupted", e);
        }
    }
}
