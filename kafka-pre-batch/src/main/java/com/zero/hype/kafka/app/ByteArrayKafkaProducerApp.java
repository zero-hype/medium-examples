package com.zero.hype.kafka.app;

import static com.zero.hype.kafka.util.KafkaConstants.TOPIC_TEST_BYTES;

import com.zero.hype.kafka.producer.ByteArrayKafkaProducer;
import com.zero.hype.kafka.producer.KafkaPreBatcher;
import com.zero.hype.kafka.util.KafkaConstants;
import com.zero.hype.kafka.util.OtelMeterRegistryManager;
import com.zero.hype.kafka.util.ZeroProperties;
import java.util.Map;

/**
 * ByteArrayKafkaProducerApp demonstrates a Kafka producer implementation that pre-batches messages
 * before sending them to Kafka.
 *
 * This application showcases an alternative approach to Kafka's native batching by:
 * 1. Collecting messages into application-level batches using KafkaPreBatcher
 * 2. Compressing these batches before sending them to Kafka
 * 3. Using byte array serialization for efficient message transfer
 *
 * The application uses a MessageRunner to continuously generate and send messages,
 * and leverages OpenTelemetry for metrics collection to monitor performance.
 *
 * Key components:
 * - ByteArrayKafkaProducer: Handles the actual publishing of batched messages to Kafka
 * - KafkaPreBatcher: Manages the application-level batching and compression
 * - MessageRunner: Generates and sends messages at a configured rate
 *
 * Usage:
 * <pre>
 * java -cp target/kafka-pre-batch-1.0-SNAPSHOT.jar com.zero.hype.kafka.app.ByteArrayKafkaProducerApp
 * </pre>
 *
 * Note: Ensure Kafka broker is running and the 'test-bytes' topic exists before starting this application.
 */
public class ByteArrayKafkaProducerApp {
    public static void main(String[] args) {

        OtelMeterRegistryManager manager = new OtelMeterRegistryManager();

        ByteArrayKafkaProducer batchKafkaProducer = new ByteArrayKafkaProducer(
                manager,
                Map.of(
                        KafkaConstants.CONFIG_TOPIC, "test-bytes",
                        KafkaConstants.CONFIG_BOOTSTRAP_SERVERS, "localhost:9092",
                        KafkaConstants.CONFIG_KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer",
                        KafkaConstants.CONFIG_VALUE_SERIALIZER,
                                "org.apache.kafka.common.serialization.ByteArraySerializer",
                        KafkaConstants.CONFIG_ACKS, "1",
                        KafkaConstants.CONFIG_COMPRESSION_TYPE, "none"));

        KafkaPreBatcher kafkaPreBatcher = new KafkaPreBatcher(
                manager, ZeroProperties.getInteger(ZeroProperties.BYTE_APP_BATCH_SIZE, 4000), batchKafkaProducer);

        new MessageRunner(
                TOPIC_TEST_BYTES,
                manager,
                (data) -> kafkaPreBatcher.add(data + "|"),
                // Number of producer threads
                ZeroProperties.getInteger(ZeroProperties.BYTE_APP_THREAD_COUNT, 1),
                // Messages per iteration
                ZeroProperties.getInteger(ZeroProperties.BYTE_APP_MESSAGE_PER_ITERATION, 10),
                // Sleep time between iterations (ms)
                ZeroProperties.getInteger(ZeroProperties.BYTE_APP_SLEEP_TIME, 10));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            manager.stop();
            kafkaPreBatcher.shutdown();
        }));
    }
}
