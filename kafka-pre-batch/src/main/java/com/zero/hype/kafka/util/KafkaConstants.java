package com.zero.hype.kafka.util;

public final class KafkaConstants {
    public static final String CONFIG_TOPIC = "topic";
    public static final String CONFIG_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String CONFIG_GROUP_ID = "group.id";
    public static final String CONFIG_KEY_SERIALIZER = "key.serializer";
    public static final String CONFIG_VALUE_SERIALIZER = "value.serializer";
    public static final String CONFIG_ACKS = "acks";
    public static final String CONFIG_COMPRESSION_GZIP_LEVEL = "compression.gzip.level";
    public static final String CONFIG_COMPRESSION_LZ4_LEVEL = "compression.lz4.level";
    public static final String CONFIG_COMPRESSION_TYPE = "compression.type";
    public static final String CONFIG_BATCH_SIZE = "batch.size";
    public static final String CONFIG_RETENTION_MS = "retention.ms";
    public static final String CONFIG_SEGMENT_MS = "segment.ms";
    public static final String CONFIG_LINGER_MS = "linger.ms";
    public static final String CONFIG_REPORTERS = "metric.reporters";

    private KafkaConstants() {
        // Prevent instantiation
    }
}
