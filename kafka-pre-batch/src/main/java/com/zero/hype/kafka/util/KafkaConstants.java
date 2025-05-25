package com.zero.hype.kafka.util;

/**
 * Defines constants used throughout the Kafka pre-batching application.
 * This includes Kafka configuration keys, metric names, and topic names.
 */
public final class KafkaConstants {
    public static final String CONFIG_TOPIC = "topic";
    public static final String CONFIG_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String CONFIG_GROUP_ID = "group.id";
    public static final String CONFIG_KEY_SERIALIZER = "key.serializer";
    public static final String CONFIG_VALUE_SERIALIZER = "value.serializer";
    public static final String CONFIG_ACKS = "acks";
    public static final String CONFIG_COMPRESSION_TYPE = "compression.type";
    public static final String CONFIG_BATCH_SIZE = "batch.size";
    public static final String CONFIG_LINGER_MS = "linger.ms";

    public static final String METRIC_BATCH_SUCCESS = "kafka.producer.batch.success";
    public static final String METRIC_BATCH_FAILURE = "kafka.producer.batch.failure";
    public static final String METRIC_BATCH_TIME = "kafka.producer.batch.time";

    public static final String METRIC_COMPRESSION_RATE = "kafka.producer.compression.rate";
    public static final String METRIC_COMPRESSION_SIZE = "kafka.producer.compression.size";
    public static final String METRIC_BATCH_SIZE = "kafka.producer.batch.size";
    public static final String METRIC_MESSAGES_PER_BATCH = "kafka.producer.messages.per.batch";

    public static final String TOPIC_LABEL = "topic";
    public static final String TOPIC_TEST = "test";
    public static final String TOPIC_TEST_BYTES = "test-bytes";

    private KafkaConstants() {
        // Prevent instantiation
    }
}
