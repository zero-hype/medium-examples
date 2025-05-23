package com.zero.hype.kafka.producer;

import com.zero.hype.kafka.util.GzipCompressor;
import com.zero.hype.kafka.util.OtelMeterRegistryManager;
import io.micrometer.core.instrument.DistributionSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * KafkaPreBatcher implements application-level batching for Kafka messages to optimize throughput
 * and reduce overhead. Instead of sending individual messages to Kafka, this class accumulates
 * messages into larger batches, compresses them, and sends them as a single Kafka message.
 *
 * Key features:
 * - Thread-safe message accumulation using ArrayBlockingQueue
 * - GZIP compression of batched messages
 * - Asynchronous processing using CompletableFuture
 * - Configurable batch size
 * - Automatic batch shipping when size threshold is reached
 *
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
 *
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
    private final DistributionSummary compressionSummary;
    private final DistributionSummary compressionSizeSummary;
    private final DistributionSummary originalSizeSummary;

    private int batchSize;

    /**
     * Creates a new KafkaPreBatcher instance.
     *
     * @param meterRegistryManager The OpenTelemetry meter registry manager for metrics
     * @param batchSize The number of messages to accumulate before sending to Kafka
     * @param kafkaProducer The producer instance used to send batched messages
     */
    public KafkaPreBatcher(OtelMeterRegistryManager meterRegistryManager, int batchSize, ByteArrayKafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
        this.batchSize = batchSize;
        // Queue capacity is set to 100x batch size to handle burst loads
        this.queue = new ArrayBlockingQueue<>(batchSize * 100);
        this.executor = Executors.newFixedThreadPool(10);
        this.compressionSummary = meterRegistryManager.getDistributionSummary("kafka.prebatcher.compression", "type", "gzip");
        this.originalSizeSummary = meterRegistryManager.getDistributionSummary("kafka.prebatcher.original.size", "type", "gzip");
        this.compressionSizeSummary = meterRegistryManager.getDistributionSummary("kafka.prebatcher.original.size", "type", "gzip");
    }

    /**
     * Adds a message to the batch queue. If the batch size threshold is reached,
     * triggers the batch to be sent to Kafka.
     *
     * @param message The message to add to the batch
     * @return A CompletableFuture that completes with true if the message was added
     *         successfully, false otherwise
     */
    public CompletableFuture<Boolean> add(String message) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        executor.submit(() -> {
            try {
                // Try to add the message to the queue with a 1-second timeout
                boolean offered = queue.offer(message, 1, TimeUnit.SECONDS);
                future.complete(offered);

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
            byte[] bytes = serialize(batch);
            kafkaProducer.publish(bytes)
                .orTimeout(5, TimeUnit.SECONDS)
                .exceptionally(ex -> {
                    logger.error("Failed to publish batch: " + ex.getMessage(), ex);
                    return false;
                });
        } catch (IOException e) {
            logger.error(e.getMessage());
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

            byte[] compressedBytes =  compressor.compress(combined);

            originalSizeSummary.record(combined.length);
            compressionSizeSummary.record(compressedBytes.length);

            double compressedPercentage = ((double) compressedBytes.length / combined.length) * 100;
            compressionSummary.record(compressedPercentage);

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
