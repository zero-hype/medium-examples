package com.zero.hype.kafka.util;

import io.micrometer.core.instrument.*;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the OpenTelemetry (OTel) MeterRegistry and provides methods for creating and registering
 * metrics such as counters, timers, and distribution summaries. It also handles the setup of the
 * OTLP HTTP exporter and JMX attribute gauges.
 */
public class OtelMeterRegistryManager {

    private static final Logger logger = LoggerFactory.getLogger(OtelMeterRegistryManager.class);
    private static final String otelCollectorHost = "http://localhost:4318/v1/metrics";
    private static final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    private MeterRegistry registry;
    private MetricExporter metricExporter;
    private OpenTelemetry openTelemetry;

    private Map<Integer, Counter> counters = new ConcurrentHashMap<>();
    private Map<Integer, Timer> timers = new ConcurrentHashMap<>();
    private MBeanServerConnection mBeanServer;

    /**
     * Constructs a new OtelMeterRegistryManager and initializes the OpenTelemetry setup by calling {@link #start()}.
     */
    public OtelMeterRegistryManager() {
        start();
    }

    /**
     * Retrieves or creates a new Counter metric.
     *
     * @param key The name of the counter.
     * @param tags The tags to associate with the counter.
     * @return The Counter metric.
     */
    public Counter getCounter(String key, String... tags) {
        return counters.computeIfAbsent(
                hashMetric(key, tags), i -> Counter.builder(key).tags(tags).register(registry));
    }

    /**
     * Retrieves or creates a new DistributionSummary metric.
     *
     * @param key The name of the distribution summary.
     * @param tags The tags to associate with the distribution summary.
     * @return The DistributionSummary metric.
     */
    public DistributionSummary getDistributionSummary(String key, String... tags) {
        return DistributionSummary.builder(key).tags(tags).register(registry);
    }

    /**
     * Generates a hash code for a metric based on its key and tags.
     * Used internally for caching metrics.
     *
     * @param key The metric key.
     * @param tags The metric tags.
     * @return The hash code.
     */
    public int hashMetric(String key, String... tags) {
        return Objects.hash(key, tags);
    }

    /**
     * Returns the underlying MeterRegistry.
     *
     * @return The MeterRegistry instance.
     */
    public MeterRegistry getRegistry() {
        return registry;
    }

    /**
     * Returns the underlying OpenTelemetry instance.
     *
     * @return The OpenTelemetry instance.
     */
    public OpenTelemetry getOpenTelemetry() {
        return openTelemetry;
    }

    /**
     * Initializes the OpenTelemetry MeterProvider, OTLP exporter, and global OpenTelemetry instance.
     * Also attempts to connect to the JMX MBean server.
     */
    public synchronized void start() {

        try {
            mBeanServer = ManagementFactory.getPlatformMBeanServer();
            logger.info("Successfully connected to Platform MBeanServer.");
        } catch (Exception e) {
            logger.error("Failed to get JMX MBean Server connection. JMX metric collection will be disabled.", e);
        }

        Resource resource = Resource.getDefault().toBuilder()
                .put(AttributeKey.stringKey("service.name"), "kafka-pre-batch")
                .put(AttributeKey.stringKey("service.version"), "1.0-SNAPSHOT")
                .build();

        // Set up OTLP HTTP Metric Exporter
        metricExporter =
                OtlpHttpMetricExporter.builder().setEndpoint(otelCollectorHost).build();

        MetricReader metricReader = PeriodicMetricReader.builder(metricExporter)
                .setInterval(10, TimeUnit.SECONDS)
                .build();

        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(metricReader)
                .build();

        openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .setPropagators(ContextPropagators.create(TextMapPropagator.composite(
                        W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())))
                .buildAndRegisterGlobal();

        OtlpConfig otlpConfig = key -> {
            switch (key) {
                case "otel.exporter.otlp.endpoint":
                case "otlp.url":
                    return otelCollectorHost;
                case "otlp.step":
                    return "10s"; // sets export interval to 10 seconds
                case "otlp.batchSize":
                    return "512"; // sets batch size to 512
                default:
                    return null;
            }
        };

        registry = new OtlpMeterRegistry(otlpConfig, Clock.SYSTEM);
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(
                        () -> {
                            metricExporter.flush();
                        },
                        0,
                        10,
                        TimeUnit.SECONDS);
    }

    /**
     * Stops the metric exporter and closes the meter registry.
     */
    public void stop() {
        metricExporter.flush().join(10, TimeUnit.SECONDS);
        metricExporter.shutdown().join(10, TimeUnit.SECONDS);
        registry.close();
    }

    /**
     * Retrieves or creates a new Counter metric. Alias for {@link #getCounter(String, String...)}.
     *
     * @param name The name of the counter.
     * @param tags The tags to associate with the counter.
     * @return The Counter metric.
     */
    public Counter counter(String name, String... tags) {
        return counters.computeIfAbsent(
                hashMetric(name, tags), i -> Counter.builder(name).tags(tags).register(registry));
    }

    /**
     * Retrieves or creates a new Timer metric.
     *
     * @param name The name of the timer.
     * @param tags The tags to associate with the timer.
     * @return The Timer metric.
     */
    public Timer timer(String name, String... tags) {
        return timers.computeIfAbsent(
                hashMetric(name, tags), i -> Timer.builder(name).tags(tags).register(registry));
    }

    /**
     * Retrieves or creates a new Timer metric. Alias for {@link #timer(String, String...)} but doesn't use the internal cache.
     *
     * @param name The name of the timer.
     * @param tags The tags to associate with the timer.
     * @return The Timer metric.
     */
    public Timer getTimer(String name, String... tags) {
        return Timer.builder(name).tags(tags).register(registry);
    }

    /**
     * Registers a JMX attribute as a gauge metric. The gauge will periodically poll the JMX attribute
     * and record its value.
     *
     * @param metricName The name of the gauge metric.
     * @param objectNameString The JMX ObjectName string of the MBean.
     * @param attributeName The name of the attribute to monitor.
     * @param tags The tags to associate with the gauge.
     */
    public void registerJmxAttributeGauge(
            String metricName, String objectNameString, String attributeName, String... tags) {
        if (mBeanServer == null) {
            logger.warn(
                    "MBeanServer not initialized. Cannot register JMX gauge for: {}. MBean: {}, Attribute: {}",
                    metricName,
                    objectNameString,
                    attributeName);
            return;
        }
        try {
            final ObjectName objectNameVal = new ObjectName(objectNameString);

            if (!mBeanServer.isRegistered(objectNameVal)) {
                logger.warn(
                        "MBean {} is not currently registered. Gauge for metric {} (attribute {}) will report NaN until MBean appears.",
                        objectNameString,
                        metricName,
                        attributeName);
            }

            DistributionSummary distributionSummary = getDistributionSummary(metricName, tags);
            scheduledExecutor.scheduleAtFixedRate(
                    () -> {
                        try {
                            if (mBeanServer.isRegistered(objectNameVal)) {
                                Object value = mBeanServer.getAttribute(objectNameVal, attributeName);
                                if (value instanceof Number) {
                                    distributionSummary.record(((Number) value).doubleValue());
                                } else {
                                    logger.trace(
                                            "JMX attribute {} for {} (metric {}) is not a Number. Value: '{}', Type: {}",
                                            attributeName,
                                            objectNameString,
                                            metricName,
                                            value,
                                            (value != null ? value.getClass().getName() : "null"));
                                    distributionSummary.record(Double.NaN);
                                }
                            } else {
                                logger.trace(
                                        "MBean {} not registered at polling time for gauge {} (attribute {}).",
                                        objectNameString,
                                        metricName,
                                        attributeName);
                                distributionSummary.record(Double.NaN);
                            }
                        } catch (javax.management.InstanceNotFoundException infEx) {
                            logger.trace(
                                    "MBean {} not found during poll for gauge {} (attribute {}).",
                                    objectNameString,
                                    metricName,
                                    attributeName);
                            distributionSummary.record(Double.NaN);
                        } catch (javax.management.AttributeNotFoundException anfEx) {
                            logger.trace(
                                    "Attribute {} not found in MBean {} during poll for gauge {}.",
                                    attributeName,
                                    objectNameString,
                                    metricName);
                            distributionSummary.record(Double.NaN);
                        } catch (Exception e) {
                            logger.warn(
                                    "Failed to get JMX attribute {} for MBean {} (metric {}): {}",
                                    attributeName,
                                    objectNameString,
                                    metricName,
                                    e.getMessage(),
                                    e);
                            distributionSummary.record(Double.NaN);
                        }
                    },
                    0,
                    100,
                    TimeUnit.MILLISECONDS);

            logger.info(
                    "Successfully registered JMX gauge: metricName='{}', mbean='{}', attribute='{}', tags={}",
                    metricName,
                    objectNameString,
                    attributeName,
                    java.util.Arrays.toString(tags));

        } catch (javax.management.MalformedObjectNameException monEx) {
            logger.error(
                    "Malformed JMX ObjectName string '{}' for metric {}. Cannot register gauge.",
                    objectNameString,
                    metricName,
                    monEx);
        } catch (Exception e) {
            logger.error(
                    "Failed to register JMX gauge: metricName='{}', mbean='{}', attribute='{}'",
                    metricName,
                    objectNameString,
                    attributeName,
                    e);
        }
    }
}
