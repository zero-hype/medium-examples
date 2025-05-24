package com.zero.hype.kafka.consumer;

import com.zero.hype.kafka.util.OtelMeterRegistryManager;
import com.zero.hype.kafka.util.RandomUtils;
import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class StringMessageStreamConsumer {
    private static final Logger logger = LoggerFactory.getLogger(StringMessageStreamConsumer.class);
    public static final AtomicInteger counter = new AtomicInteger(0);

    private final String topic;

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

        Counter recordCounter = manager.getCounter("kafka.consumer.record", "topic", topic);
        Counter eventCounter = manager.getCounter("kafka.consumer.event", "topic", topic);

        logger.debug("Subscribed to topic {}", topic);
        Thread t = new Thread(() -> {
            // poll for new data
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> kafkaRecord : records) {
                    recordCounter.increment();
                    eventCounter.increment();
                    if (RandomUtils.chance(0.1)) {
                        logger.debug("Processing topic=" + topic + " "
                                + "key=" + kafkaRecord.key()
                                + ",partition=" + kafkaRecord.partition()
                                + ",offset=" + kafkaRecord.offset()
                                + ",value=" + kafkaRecord.value()
                        );
                    }
                }
                consumer.commitAsync();
            }
        });

        t.setName("StringMessageStreamConsumer-" + counter.incrementAndGet());
        t.start();
    }
}
