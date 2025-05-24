package com.zero.hype.kafka.app;

import com.zero.hype.kafka.producer.NativeKafkaProducer;
import com.zero.hype.kafka.util.KafkaConstants;
import com.zero.hype.kafka.util.OtelMeterRegistryManager;

import java.util.Map;

/**
 * NativeKafkaProducerApp demonstrates a standard Kafka producer implementation
 * that uses Kafka's native batching mechanism. This application serves as a
 * comparison to the pre-batching approach implemented in ByteArrayKafkaProducerApp.
 *
 * Key features:
 * - Uses Kafka's built-in batching and compression
 * - Configurable batch size and linger time
 * - Metrics collection using OpenTelemetry
 * - Multi-threaded message generation
 *
 * Configuration:
 * - batch.size: 32KB
 * - compression.type: gzip
 * - linger.ms: 5000ms
 * - acks: 1
 *
 * The application uses MessageRunner to generate and send messages at a
 * configurable rate, allowing for performance comparison with the pre-batching
 * approach.
 *
 * Usage:
 * <pre>
 * java -cp target/kafka-pre-batch-1.0-SNAPSHOT.jar com.zero.hype.kafka.app.NativeKafkaProducerApp
 * </pre>
 *
 * Note: Ensure Kafka broker is running and the 'test' topic exists before
 * starting this application.
 */
public class NativeKafkaProducerApp {
    /**
     * Main entry point for the native Kafka producer application.
     * Initializes the producer with native batching configuration and
     * starts the message generation process.
     *
     * @param args Command line arguments (not used)
     */
    public static void main(String[] args) {
        OtelMeterRegistryManager manager = new OtelMeterRegistryManager();

        NativeKafkaProducer kafkaMessageStream = new NativeKafkaProducer(
                manager,
                Map.of(
                        KafkaConstants.CONFIG_BOOTSTRAP_SERVERS, "localhost:9092",
                        KafkaConstants.CONFIG_KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer",
                        KafkaConstants.CONFIG_VALUE_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer",
                        KafkaConstants.CONFIG_ACKS, "1",
                        KafkaConstants.CONFIG_COMPRESSION_TYPE, "gzip",
                        KafkaConstants.CONFIG_BATCH_SIZE, "32768"
                )
        );

        new MessageRunner(
                kafkaMessageStream::publish,
                1,  // Number of producer threads
                10,  // Messages per iteration
                100   // Sleep time between iterations (ms)
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            manager.stop();
        }));
    }
}
