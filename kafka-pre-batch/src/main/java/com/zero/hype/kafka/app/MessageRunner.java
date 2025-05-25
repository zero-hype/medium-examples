package com.zero.hype.kafka.app;

import static com.zero.hype.kafka.util.KafkaConstants.TOPIC_LABEL;

import com.zero.hype.kafka.util.OtelMeterRegistryManager;
import com.zero.hype.kafka.util.ZeroProperty;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MessageRunner is a utility class that generates and sends messages at a configurable rate.
 * It's used to simulate message production in both pre-batching and native batching scenarios.
 *
 * Key features:
 * - Generates random messages of configurable size
 * - Supports multiple producer threads
 * - Configurable message rate and batch size
 * - Thread-safe message generation and sending
 *
 * The class pre-generates a pool of random messages to avoid the overhead of
 * generating new messages for each send operation.
 *
 * Usage example:
 * <pre>
 * MessageRunner runner = new MessageRunner(
 *     messageAdder,    // Message sending interface
 *     10,             // Number of producer threads
 *     100,            // Messages per iteration
 *     1000            // Sleep time between iterations (ms)
 * );
 * </pre>
 */
public class MessageRunner {

    private static final List<Thread> threads = Collections.synchronizedList(new ArrayList<>());
    private static final Logger logger = LoggerFactory.getLogger(MessageRunner.class);
    private static final int BYTE_SIZE = 100;
    private static final String CHARSET = " -_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private final MessageAdder messageAdder;
    private final ZeroProperty<Integer> messageCountPerIteration;
    private final ZeroProperty<Integer> threadSleep;
    private final List<String> generatedMessages;
    private final OtelMeterRegistryManager meterRegistryManager;
    private final String topic;

    /**
     * Creates a new MessageRunner instance.
     *
     * @param messageAdder The interface used to send messages
     * @param threadCount Number of producer threads to create
     * @param messageCountPerIteration Number of messages to send in each iteration
     * @param threadSleep Sleep time between iterations in milliseconds
     */
    public MessageRunner(
            String topic,
            OtelMeterRegistryManager meterRegistryManager,
            MessageAdder messageAdder,
            ZeroProperty<Integer> threadCount,
            ZeroProperty<Integer> messageCountPerIteration,
            ZeroProperty<Integer> threadSleep) {
        this.topic = topic;
        this.meterRegistryManager = meterRegistryManager;
        this.messageAdder = messageAdder;
        this.messageCountPerIteration = messageCountPerIteration;
        this.threadSleep = threadSleep;

        generatedMessages = new ArrayList<>();

        // Generate 2000 random messages
        for (int x = 0; x < 2000; x++) {
            generatedMessages.add(generateRandomAsciiString(BYTE_SIZE));
        }

        // Start the threads
        for (int x = 0; x < threadCount.getValue(); x++) {
            threads.add(startThread(x + 1));
        }
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(
                        () -> {
                            int threadsWantedCount = threadCount.getValue();
                            if (threadsWantedCount < 1) {
                                logger.warn("Thread count is set to zero or negative, no threads will be managed.");
                                return;
                            }

                            if (threadsWantedCount == threads.size()) {
                                logger.debug(
                                        "Current thread count: {}, as expected: {}",
                                        threads.size(),
                                        threadCount.getValue());
                            } else if (threadCount.getValue() > threads.size()) {
                                int threadsToStart = threadCount.getValue() - threads.size();
                                for (int x = 0; x < threadsToStart; x++) {
                                    Thread thread = startThread(threads.size() + 1);
                                    threads.add(thread);
                                    logger.debug("Started new thread: {}", thread.getName());
                                }
                            } else if (threadCount.getValue() < threads.size()) {
                                int threadsToStop = threads.size() - threadCount.getValue();
                                for (int x = 0; x < threadsToStop; x++) {
                                    Thread thread = threads.remove(threads.size() - 1);
                                    thread.interrupt();
                                    try {
                                        thread.join();
                                    } catch (InterruptedException e) {
                                        logger.error("Error stopping thread: {}", thread.getName(), e);
                                    }
                                    logger.debug("Stopped thread: {}", thread.getName());
                                }
                            }
                        },
                        0,
                        10,
                        TimeUnit.SECONDS);
    }

    /**
     * Starts a producer thread that continuously sends messages.
     * Each thread runs in an infinite loop, sending messages at the configured rate.
     *
     * @param threadId The ID of the thread to start
     */
    private Thread startThread(int threadId) {
        Thread thread = new Thread(() -> {
            Counter messageCounter = meterRegistryManager.getCounter(
                    "kafka.producer.message.runner.send", "thread", "MessageRunner-" + threadId, TOPIC_LABEL, topic);
            Counter messageFailure = meterRegistryManager.getCounter(
                    "kafka.producer.message.runner.send.fail",
                    "thread",
                    "MessageRunner-" + threadId,
                    TOPIC_LABEL,
                    topic);
            Timer sleepTimer = meterRegistryManager.getTimer(
                    "kafka.producer.message.runner.sleep.time",
                    "thread",
                    "MessageRunner-" + threadId,
                    TOPIC_LABEL,
                    topic);
            ThreadLocalRandom random = ThreadLocalRandom.current();
            boolean running = true;

            while (running) {
                for (int x = 0; x < messageCountPerIteration.getValue(); x++) {
                    messageCounter.increment();
                    messageAdder
                            .addMessage(generatedMessages.get(random.nextInt(generatedMessages.size())))
                            .orTimeout(5000, TimeUnit.MILLISECONDS)
                            .thenAccept(result -> {
                                if (!result) {
                                    logger.error("Error adding message");
                                }
                            })
                            .exceptionally(ex -> {
                                messageFailure.increment();
                                logger.error("Failed to publish message: " + ex.getMessage());
                                return null;
                            });
                }
                try {
                    long startTime = System.currentTimeMillis();
                    Thread.sleep(threadSleep.getValue());
                    sleepTimer.record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    running = false;
                    logger.info(
                            "Thread {} interrupted, stopping...",
                            Thread.currentThread().getName());
                }
            }
        });
        thread.setName("MessageRunner-" + threadId);
        thread.start();
        return thread;
    }

    /**
     * Generates a random ASCII string of the specified size.
     * The string contains a mix of letters, numbers, spaces, and special characters.
     *
     * @param byteSize The size of the string to generate
     * @return A random ASCII string
     */
    private String generateRandomAsciiString(int byteSize) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder sb = new StringBuilder(byteSize);
        for (int i = 0; i < byteSize; i++) {
            sb.append(CHARSET.charAt(random.nextInt(CHARSET.length())));
        }
        return sb.toString();
    }
}
