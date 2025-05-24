package com.zero.hype.kafka.app;

import java.util.concurrent.CompletableFuture;

/**
 * MessageAdder is a functional interface that defines the contract for adding messages
 * to a message processing pipeline. It's used by MessageRunner to send messages
 * to either the pre-batching or native batching producers.
 *
 * The interface provides an asynchronous API using CompletableFuture to handle
 * message sending operations, allowing for non-blocking message processing.
 *
 * Usage example:
 * <pre>
 * MessageAdder adder = (data) -> {
 *     return producer.publish(data)
 *         .thenApply(success -> {
 *             // Handle success/failure
 *             return success;
 *         });
 * };
 * </pre>
 */
public interface MessageAdder {
    /**
     * Adds a message to the processing pipeline.
     *
     * @param data The message to add
     * @return A CompletableFuture that completes with true if the message was added
     *         successfully, or completes exceptionally if an error occurs
     */
    CompletableFuture<Boolean> addMessage(String data);
}
