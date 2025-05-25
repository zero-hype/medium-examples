package com.zero.hype.kafka.consumer;

import static com.zero.hype.kafka.util.KafkaConstants.TOPIC_LABEL;

import com.zero.hype.kafka.util.GzipCompressor;
import com.zero.hype.kafka.util.OtelMeterRegistryManager;
import com.zero.hype.kafka.util.RandomUtils;
import io.micrometer.core.instrument.Counter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes byte array messages from a Kafka topic, decompresses them if they are GZIP compressed,
 * and logs the messages. This consumer is designed to handle messages produced by
 * {@link com.zero.hype.kafka.producer.KafkaPreBatcher}.
 */
public class ByteArrayMessageStreamConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ByteArrayMessageStreamConsumer.class);
    private static final AtomicInteger counter = new AtomicInteger(0);

    private final String topic;

    /**
     * Constructs a new ByteArrayMessageStreamConsumer and starts a new thread to poll for messages.
     *
     * @param manager The OpenTelemetry meter registry manager for metrics.
     * @param additionalConfig A map of additional Kafka consumer configurations.
     *                         It must include the "topic" to consume from.
     */
    public ByteArrayMessageStreamConsumer(OtelMeterRegistryManager manager, Map<String, String> additionalConfig) {
        GzipCompressor gzipCompressor = new GzipCompressor();
        this.topic = additionalConfig.get("topic");
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        config.put("group.id", "test-byte-consumer");
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "true");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(config);

        consumer.subscribe(List.of(topic));
        logger.debug("Subscribed to topic {}", topic);

        Counter recordCounter = manager.getCounter("kafka.consumer.record", TOPIC_LABEL, topic);
        Counter eventCounter = manager.getCounter("kafka.consumer.event", TOPIC_LABEL, topic);

        Thread t = new Thread(() -> {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, byte[]> kafkaRecord : records) {
                    recordCounter.increment();
                    String strValue = "non-gzipped data";
                    byte[] value = kafkaRecord.value();
                    if (gzipCompressor.isCompressed(value)) {
                        try {
                            strValue = new String(gzipCompressor.decompress(value), StandardCharsets.UTF_8);
                            int total = strValue.split("\\|").length;
                            eventCounter.increment(total);
                        } catch (IOException e) {
                            logger.error(e.getMessage(), e);
                            continue;
                        }
                    }
                    if (logger.isDebugEnabled() && RandomUtils.chance(0.1)) {
                        logger.debug("Processing topic=" + topic + " "
                                + "key=" + kafkaRecord.key()
                                + ",partition=" + kafkaRecord.partition()
                                + ",offset=" + kafkaRecord.offset()
                                + ",value=" + strValue);
                    }
                }

                consumer.commitAsync();
            }
        });

        t.setName("ByteArrayMessageStreamConsumer-" + counter.incrementAndGet());
        t.start();
    }
}
