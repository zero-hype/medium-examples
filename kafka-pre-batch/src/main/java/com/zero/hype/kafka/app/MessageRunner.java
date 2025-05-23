package com.zero.hype.kafka.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

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

    private static final Logger logger = LoggerFactory.getLogger(MessageRunner.class);
    private static final int BYTE_SIZE = 1024;
    private static final String CHARSET = " -_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private final MessageAdder messageAdder;
    private final int messageCountPerIteration;
    private final int threadSleep;
    private final List<String> generatedMessages;

    /**
     * Creates a new MessageRunner instance.
     *
     * @param messageAdder The interface used to send messages
     * @param threadCount Number of producer threads to create
     * @param messageCountPerIteration Number of messages to send in each iteration
     * @param threadSleep Sleep time between iterations in milliseconds
     */
    public MessageRunner(MessageAdder messageAdder, int threadCount, int messageCountPerIteration, int threadSleep) {
        this.messageAdder = messageAdder;
        this.messageCountPerIteration = messageCountPerIteration;
        this.threadSleep = threadSleep;

        generatedMessages = new ArrayList<>();

        // Generate 2000 random messages
        for (int x = 0; x < 2000; x++) {
            generatedMessages.add(generateRandomAsciiString(BYTE_SIZE));
        }

        // Start the threads
        for (int x = 0; x < threadCount; x++) {
            startThread(x + 1);
        }
    }

    /**
     * Starts a producer thread that continuously sends messages.
     * Each thread runs in an infinite loop, sending messages at the configured rate.
     *
     * @param threadId The ID of the thread to start
     */
    private void startThread(int threadId) {
        Thread thread = new Thread(() -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            LongAdder counter = new LongAdder();
            while (true) {
                counter.increment();
                for (int x = 0; x < messageCountPerIteration; x++) {
                    messageAdder.addMessage(
                                    generatedMessages.get(random.nextInt(generatedMessages.size()))
                            ).orTimeout(5000, TimeUnit.MILLISECONDS)
                            .thenAccept(result -> {
                                if (!result) {
                                    logger.error("Error adding message");
                                }
                            })
                            .exceptionally(ex -> {
                                logger.error("Failed to publish message: " + ex.getMessage());
                                return null;
                            });
                }
                try {
                    Thread.sleep(threadSleep);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.setName("MessageRunner-" + threadId);
        thread.start();
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
