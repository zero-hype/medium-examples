package com.zero.hype.kafka.consumer;

import static com.zero.hype.kafka.util.KafkaConstants.TOPIC_LABEL;

import com.zero.hype.kafka.util.OtelMeterRegistryManager;
import com.zero.hype.kafka.util.RandomUtils;
import io.micrometer.core.instrument.Counter;
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
 * Consumes string messages from a Kafka topic and logs them. This consumer is typically used for
 * consuming messages produced by {@link com.zero.hype.kafka.producer.NativeKafkaProducer}.
 */
public class StringMessageStreamConsumer {
    private static final Logger logger = LoggerFactory.getLogger(StringMessageStreamConsumer.class);
    public static final AtomicInteger counter = new AtomicInteger(0);

    private final String topic;

    /**
     * Constructs a new StringMessageStreamConsumer and starts a new thread to poll for messages.
     *
     * @param manager The OpenTelemetry meter registry manager for metrics.
     * @param additionalConfig A map of additional Kafka consumer configurations.
     *                         It must include the "topic" to consume from.
     */
    public StringMessageStreamConsumer(OtelMeterRegistryManager manager, Map<String, String> additionalConfig) {
        this.topic = additionalConfig.get("topic");
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", "test-consumer");
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);

        consumer.subscribe(List.of(topic));

        Counter recordCounter = manager.getCounter("kafka.consumer.record", TOPIC_LABEL, topic);
        Counter eventCounter = manager.getCounter("kafka.consumer.event", TOPIC_LABEL, topic);

        logger.debug("Subscribed to topic {}", topic);
        Thread t = new Thread(() -> {
            // poll for new data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> kafkaRecord : records) {
                    recordCounter.increment();
                    eventCounter.increment();
                    if (RandomUtils.chance(0.1)) {
                        logger.debug("Processing topic=" + topic + " "
                                + "key=" + kafkaRecord.key()
                                + ",partition=" + kafkaRecord.partition()
                                + ",offset=" + kafkaRecord.offset()
                                + ",value=" + kafkaRecord.value());
                    }
                }
                consumer.commitAsync();
            }
        });

        t.setName("StringMessageStreamConsumer-" + counter.incrementAndGet());
        t.start();
    }
}
