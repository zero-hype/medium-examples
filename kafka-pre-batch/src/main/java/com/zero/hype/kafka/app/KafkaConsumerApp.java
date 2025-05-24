package com.zero.hype.kafka.app;

import com.zero.hype.kafka.consumer.ByteArrayMessageStreamConsumer;
import com.zero.hype.kafka.consumer.StringMessageStreamConsumer;
import com.zero.hype.kafka.util.OtelMeterRegistryManager;

import java.util.Map;

/**
 * KafkaConsumerApp serves as the main entry point for the Kafka consumer demonstration,
 * showcasing both pre-batched and native message consumption patterns.
 * 
 * This application starts two different types of consumers:
 * 1. ByteArrayMessageStreamConsumer - Handles pre-batched messages from the 'test-bytes' topic
 *    that were compressed and batched at the application level
 * 2. StringMessageStreamConsumer - Processes individual messages from the 'test' topic
 *    that were sent using native Kafka batching
 *
 * The application uses OpenTelemetry for metrics collection, allowing comparison of 
 * performance between the two approaches.
 *
 * Usage:
 * <pre>
 * java -cp target/kafka-pre-batch-1.0-SNAPSHOT.jar com.zero.hype.kafka.app.KafkaConsumerApp
 * </pre>
 *
 * Note: Ensure the Kafka broker and required topics are available before starting this application.
 * Required topics:
 * - test-bytes: For pre-batched messages
 * - test: For regular string messages
 */
public class KafkaConsumerApp {
    /**
     * Main entry point for the Kafka consumer application.
     * Initializes and starts both types of consumers with their respective configurations.
     *
     * @param args Command line arguments (not used)
     */
    public static void main(String[] args) {
        // Initialize the metrics registry manager for monitoring
        OtelMeterRegistryManager manager = new OtelMeterRegistryManager();

        // Start the pre-batched message consumer
        new ByteArrayMessageStreamConsumer(manager, Map.of("topic", "test-bytes"));

        // Start the regular string message consumer
        new StringMessageStreamConsumer(manager, Map.of("topic", "test"));

        // shutdown hook to stop the manager gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            manager.stop();
        }));
    }
}
