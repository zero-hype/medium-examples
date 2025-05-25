package com.zero.hype.kafka.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages application properties loaded from a `zero.properties` file.
 * This class provides a way to define and access properties with default values
 * and supports dynamic reloading of properties at runtime.
 */
public class ZeroProperties {

    private static final Logger logger = LoggerFactory.getLogger(ZeroProperties.class);

    public static final Map<String, ZeroProperty<Integer>> integerPropertyMap = new ConcurrentHashMap<>();

    public static final String BYTE_APP_THREAD_COUNT = "byte.app.thread.count";
    public static final String BYTE_APP_MESSAGE_PER_ITERATION = "byte.app.message.per.iteration";
    public static final String BYTE_APP_SLEEP_TIME = "byte.app.sleep.time";
    public static final String BYTE_APP_BATCH_SIZE = "byte.app.batch.size";
    public static final String NATIVE_APP_THREAD_COUNT = "native.app.thread.count";
    public static final String NATIVE_APP_MESSAGE_PER_ITERATION = "native.app.message.per.iteration";
    public static final String NATIVE_APP_SLEEP_TIME = "native.app.sleep.time";
    public static final java.util.Properties properties;

    static {
        properties = new Properties();
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(
                        () -> {
                            loadProperties();
                            logger.debug("ZeroProperties loaded successfully.");
                        },
                        0,
                        10,
                        java.util.concurrent.TimeUnit.SECONDS);
    }

    private static void loadProperties() {
        try (InputStream stream = ZeroProperties.class.getResourceAsStream("/zero.properties")) {
            properties.load(stream);
            for (ZeroProperty<Integer> property : integerPropertyMap.values()) {
                String propertyName = property.getPropertyName();
                String value = properties.getProperty(propertyName);
                if (value != null) {
                    try {
                        property.getValueRef().set(Integer.parseInt(value));
                        logger.debug("Loaded property {}: {}", propertyName, value);
                    } catch (NumberFormatException e) {
                        logger.error("Invalid value for property {}: {}", propertyName, value);
                    }
                } else {
                    logger.warn("Property {} not found in zero.properties", propertyName);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves an integer property by its name. If the property is not found or has an invalid format,
     * the default value is used. The property is cached for subsequent lookups.
     *
     * @param propertyName The name of the property.
     * @param defaultValue The default value to use if the property is not found or invalid.
     * @return A {@link ZeroProperty} instance representing the integer property.
     */
    public static ZeroProperty<Integer> getInteger(String propertyName, int defaultValue) {
        return integerPropertyMap.computeIfAbsent(propertyName, key -> {
            AtomicReference<Integer> valueRef = new AtomicReference<>(defaultValue);
            ZeroProperty<Integer> property = new ZeroProperty<>(propertyName, valueRef);
            String value = properties.getProperty(key);
            if (value != null) {
                try {
                    valueRef.set(Integer.parseInt(value));
                } catch (NumberFormatException e) {
                    logger.error("Invalid value for property {}: {}", key, value);
                }
            }
            return property;
        });
    }
}
