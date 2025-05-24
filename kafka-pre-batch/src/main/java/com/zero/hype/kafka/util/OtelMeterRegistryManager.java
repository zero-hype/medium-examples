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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class OtelMeterRegistryManager {

    private static final Logger logger = LoggerFactory.getLogger(OtelMeterRegistryManager.class);
    private static final String otelCollectorHost = "http://localhost:4318/v1/metrics";
    private MeterRegistry registry;
    private MetricExporter metricExporter;
    private OpenTelemetry openTelemetry;

    private Map<Integer, Counter> counters = new ConcurrentHashMap<>();
    private Map<Integer, Timer> timers = new ConcurrentHashMap<>();

    public OtelMeterRegistryManager() {
        start();
    }

    public Counter getCounter(String key, String... tags) {
        return counters.computeIfAbsent(hashMetric(key, tags), i ->
                Counter.builder(key).tags(tags).register(registry)
        );
    }

    public DistributionSummary getDistributionSummary(String key, String... tags) {
        return DistributionSummary
                .builder(key)
                .tags(tags)
                .register(registry);
    }

    public int hashMetric(String key, String... tags) {
        return Objects.hash(key, tags);
    }

    public MeterRegistry getRegistry() {
        return registry;
    }

    public OpenTelemetry getOpenTelemetry() {
        return openTelemetry;
    }

    public synchronized void start() {
        //System.setProperty("otel.javaagent.debug", "true");
        Resource resource = Resource.getDefault()
                .toBuilder()
                .put(AttributeKey.stringKey("service.name"), "kafka-pre-batch")
                .put(AttributeKey.stringKey("service.version"), "1.0-SNAPSHOT")
                .build();

        // Set up OTLP HTTP Metric Exporter
        metricExporter = OtlpHttpMetricExporter.builder()
                .setEndpoint(otelCollectorHost)
                .build();

        MetricReader metricReader = PeriodicMetricReader.builder(metricExporter)
                .setInterval(10, TimeUnit.SECONDS)
                .build();

        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(metricReader)
                .build();

        openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .setPropagators(ContextPropagators.create(
                        TextMapPropagator.composite(
                                W3CTraceContextPropagator.getInstance(),
                                W3CBaggagePropagator.getInstance()
                        )
                ))
                .buildAndRegisterGlobal();

        // Micrometer Registry (bridge to OpenTelemetry)
        OtlpConfig otlpConfig = key -> {
            if ("otel.exporter.otlp.endpoint".equals(key) || "otlp.url".equals(key)) {
                return otelCollectorHost;
            }
            return null;
        };

        registry = new OtlpMeterRegistry(otlpConfig, Clock.SYSTEM);
    }

    public void stop() {
        metricExporter.flush().join(10, TimeUnit.SECONDS);
        metricExporter.shutdown().join(10, TimeUnit.SECONDS);
        registry.close();
    }

    public Counter counter(String name, String... tags) {
        return counters.computeIfAbsent(hashMetric(name, tags), i ->
                Counter.builder(name).tags(tags).register(registry)
        );
    }

    public Timer timer(String name, String... tags) {
        return timers.computeIfAbsent(hashMetric(name, tags), i ->
                Timer.builder(name).tags(tags).register(registry)
        );
    }
}
