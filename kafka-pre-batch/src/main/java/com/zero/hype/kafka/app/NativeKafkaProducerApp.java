package com.zero.hype.kafka.app;

import static com.zero.hype.kafka.util.KafkaConstants.TOPIC_TEST;

import com.zero.hype.kafka.producer.NativeKafkaProducer;
import com.zero.hype.kafka.util.KafkaConstants;
import com.zero.hype.kafka.util.OtelMeterRegistryManager;
import com.zero.hype.kafka.util.ZeroProperties;
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

        NativeKafkaProducer nativeKafkaProducer = new NativeKafkaProducer(
                manager,
                Map.of(
                        KafkaConstants.CONFIG_BOOTSTRAP_SERVERS, "localhost:9092",
                        KafkaConstants.CONFIG_KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer",
                        KafkaConstants.CONFIG_VALUE_SERIALIZER,
                                "org.apache.kafka.common.serialization.StringSerializer",
                        KafkaConstants.CONFIG_ACKS, "1",
                        KafkaConstants.CONFIG_LINGER_MS, "5000", // 5 seconds linger time
                        KafkaConstants.CONFIG_COMPRESSION_TYPE, "gzip",
                        KafkaConstants.CONFIG_BATCH_SIZE, "512000")); // 512KB batch size

        new MessageRunner(
                TOPIC_TEST,
                manager,
                nativeKafkaProducer::publish,
                // Number of producer threads
                ZeroProperties.getInteger(ZeroProperties.NATIVE_APP_THREAD_COUNT, 1),
                // Messages per iteration
                ZeroProperties.getInteger(ZeroProperties.NATIVE_APP_MESSAGE_PER_ITERATION, 10),
                // Sleep time between iterations (ms)
                ZeroProperties.getInteger(ZeroProperties.NATIVE_APP_SLEEP_TIME, 10));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            manager.stop();
            nativeKafkaProducer.close();
        }));
    }
}
