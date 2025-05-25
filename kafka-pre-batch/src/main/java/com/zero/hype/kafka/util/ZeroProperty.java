package com.zero.hype.kafka.util;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a dynamically updatable property of type {@code T}.
 * This class holds a reference to the property's value and its name.
 * The value is stored in an {@link AtomicReference} to allow for thread-safe updates.
 *
 * @param <T> The type of the property value.
 */
public class ZeroProperty<T> {
    private final AtomicReference<T> valueRef;
    private final String propertyName;

    /**
     * Constructs a new ZeroProperty.
     *
     * @param propertyName The name of the property.
     * @param valueRef An {@link AtomicReference} holding the initial value of the property.
     */
    public ZeroProperty(String propertyName, AtomicReference<T> valueRef) {
        this.propertyName = propertyName;
        this.valueRef = valueRef;
    }

    protected AtomicReference<T> getValueRef() {
        return valueRef;
    }

    /**
     * Gets the name of the property.
     *
     * @return The property name.
     */
    public String getPropertyName() {
        return propertyName;
    }

    /**
     * Gets the current value of the property.
     *
     * @return The current property value.
     */
    public T getValue() {
        return valueRef.get();
    }
}
