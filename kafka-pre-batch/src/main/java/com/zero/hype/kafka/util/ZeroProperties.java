package com.zero.hype.kafka.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private static final String CLASSPATH_PROPERTIES_PATH = "/zero.properties";
    private static final String EXTERNAL_PROPERTIES_SYSTEM_PROPERTY = "zero.properties.path";

    public static final Map<String, ZeroProperty<Integer>> integerPropertyMap = new ConcurrentHashMap<>();

    public static final String BYTE_APP_THREAD_COUNT = "byte.app.thread.count";
    public static final String BYTE_APP_MESSAGE_PER_ITERATION = "byte.app.message.per.iteration";
    public static final String BYTE_APP_SLEEP_TIME = "byte.app.sleep.time";
    public static final String BYTE_APP_BATCH_SIZE = "byte.app.batch.size";
    public static final String NATIVE_APP_THREAD_COUNT = "native.app.thread.count";
    public static final String NATIVE_APP_MESSAGE_PER_ITERATION = "native.app.message.per.iteration";
    public static final String NATIVE_APP_SLEEP_TIME = "native.app.sleep.time";
    public static final java.util.Properties properties;

    /**
     * Static initializer block that creates the `properties` instance and schedules the `loadProperties`
     * method to be called periodically to reload properties from the source file.
     */
    static {
        properties = new Properties();
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(
                        () -> {
                            loadProperties();
                            logger.debug("ZeroProperties reloaded.");
                        },
                        0,
                        10,
                        java.util.concurrent.TimeUnit.SECONDS);
    }

    /**
     * Orchestrates the loading of properties. It first attempts to load from an external file path
     * (specified by the system property `zero.properties.path`). If that fails or is not specified,
     * it attempts to load from the classpath resource (`/zero.properties`).
     * If properties are successfully loaded from any source, the internal `properties` field is updated,
     * and then the values of all registered {@link ZeroProperty} instances are updated.
     */
    private static void loadProperties() {
        Properties tempProps = new Properties();
        boolean loaded = tryLoadFromExternalPath(System.getProperty(EXTERNAL_PROPERTIES_SYSTEM_PROPERTY), tempProps);

        if (!loaded) {
            loaded = tryLoadFromClasspath(tempProps);
        }

        if (loaded) {
            properties.putAll(tempProps);
            updateIntegerProperties(properties);
            logger.debug("ZeroProperties applied successfully.");
        } else {
            logger.error("Failed to load zero.properties from any source. Existing properties (if any) will be kept.");
        }
    }

    /**
     * Attempts to load properties into the `targetProperties` object from the given `InputStream`.
     *
     * @param stream The InputStream to read properties from. Must not be null.
     * @param targetProperties The Properties object to load the read properties into.
     * @param sourceInfoForLogging A descriptive string indicating the source of the stream (e.g., file path or classpath location) for logging purposes.
     * @return {@code true} if properties were successfully loaded from the stream, {@code false} otherwise (e.g., due to an IOException).
     */
    private static boolean attemptLoadFromStream(
            InputStream stream, Properties targetProperties, String sourceInfoForLogging) {
        try {
            targetProperties.load(stream);
            logger.debug("Successfully loaded ZeroProperties from {}", sourceInfoForLogging);
            return true;
        } catch (IOException e) {
            logger.warn("Error loading ZeroProperties from {}: {}", sourceInfoForLogging, e.getMessage());
            return false;
        }
    }

    /**
     * Attempts to load properties from an external file specified by `externalPath`.
     * If the path is valid, readable, and the file is successfully parsed, the properties are loaded
     * into `targetProperties`.
     *
     * @param externalPath The file system path to the external properties file. Can be null or empty, in which case loading is skipped.
     * @param targetProperties The Properties object to load into.
     * @return {@code true} if properties were successfully loaded from the external file, {@code false} otherwise.
     */
    private static boolean tryLoadFromExternalPath(String externalPath, Properties targetProperties) {
        if (externalPath == null || externalPath.trim().isEmpty()) {
            return false; // No path provided, not an error, just won't load from external.
        }
        Path path = Paths.get(externalPath);
        if (Files.exists(path) && Files.isReadable(path)) {
            try (InputStream stream = new FileInputStream(path.toFile())) {
                return attemptLoadFromStream(stream, targetProperties, "external file: " + externalPath);
            } catch (IOException e) {
                // This IOException is for FileInputStream creation or stream.close()
                logger.warn(
                        "Could not open or close external ZeroProperties file {}: {}", externalPath, e.getMessage());
                return false;
            }
        } else {
            logger.warn(
                    "External ZeroProperties file not found or not readable: {}. Will attempt classpath.",
                    externalPath);
            return false;
        }
    }

    /**
     * Attempts to load properties from the classpath resource defined by `CLASSPATH_PROPERTIES_PATH`
     * (i.e., `/zero.properties`). If the resource is found and successfully parsed, the properties are
     * loaded into `targetProperties`.
     *
     * @param targetProperties The Properties object to load into.
     * @return {@code true} if properties were successfully loaded from the classpath, {@code false} otherwise.
     */
    private static boolean tryLoadFromClasspath(Properties targetProperties) {
        try (InputStream stream = ZeroProperties.class.getResourceAsStream(CLASSPATH_PROPERTIES_PATH)) {
            if (stream == null) {
                logger.warn("Could not find zero.properties on classpath: {}", CLASSPATH_PROPERTIES_PATH);
                return false;
            }
            return attemptLoadFromStream(stream, targetProperties, "classpath resource: " + CLASSPATH_PROPERTIES_PATH);
        } catch (IOException e) {
            // This IOException is for stream.close() if getResourceAsStream somehow returned a problematic stream that
            // fails on close.
            // The load operation's IOException is handled in attemptLoadFromStream.
            logger.warn(
                    "IOException while closing classpath stream for {}: {}", CLASSPATH_PROPERTIES_PATH, e.getMessage());
            return false;
        }
    }

    /**
     * Updates all registered {@link ZeroProperty} instances in `integerPropertyMap` with values from the
     * provided `loadedProps`. For each `ZeroProperty`, its name is used to look up the corresponding
     * value in `loadedProps`. If found and valid, the `ZeroProperty`'s internal value is updated.
     * If a property name is not found in `loadedProps`, the `ZeroProperty` retains its current value.
     *
     * @param loadedProps The Properties object containing the most recently loaded key-value pairs.
     */
    private static void updateIntegerProperties(Properties loadedProps) {
        for (ZeroProperty<Integer> property : integerPropertyMap.values()) {
            String propertyName = property.getPropertyName();
            String value = loadedProps.getProperty(propertyName);
            if (value != null) {
                try {
                    property.getValueRef().set(Integer.parseInt(value.trim()));
                    logger.debug("Applied property {}: {}", propertyName, value);
                } catch (NumberFormatException e) {
                    logger.error("Invalid integer value for property {}: '{}'", propertyName, value);
                }
            } else {
                // If a property defined in code (and thus in integerPropertyMap) is NOT in the properties file,
                // it will retain its default value or the last successfully loaded value.
                // We could add a log here if specific behavior for missing properties is desired.
                logger.warn(
                        "Property '{}' not found in loaded properties. Corresponding ZeroProperty will retain its current value.",
                        propertyName);
            }
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
        // Ensure property is initialized and in the map, using its default if not yet loaded from file
        return integerPropertyMap.computeIfAbsent(propertyName, key -> {
            AtomicReference<Integer> valueRef = new AtomicReference<>(defaultValue);
            // Attempt to set from 'properties' if already loaded, otherwise default is used
            String initialValue = properties.getProperty(key);
            if (initialValue != null) {
                try {
                    valueRef.set(Integer.parseInt(initialValue.trim()));
                } catch (NumberFormatException e) {
                    logger.error(
                            "Invalid initial integer value for property {}: '{}'. Using default: {}",
                            key,
                            initialValue,
                            defaultValue);
                    // valueRef already holds defaultValue
                }
            }
            return new ZeroProperty<>(propertyName, valueRef);
        });
    }
}
