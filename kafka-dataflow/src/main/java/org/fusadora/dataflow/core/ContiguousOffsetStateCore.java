package org.fusadora.dataflow.core;

import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ValueState;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * org.fusadora.dataflow.core.ContiguousOffsetStateCore
 * This class encapsulates the core logic for managing contiguous offsets in a streaming data processing context.
 * It provides methods to load the expected offset, buffer out-of-order events, emit contiguous events, and check for buffered events.
 * The class is designed to be used with Apache Beam's state management APIs, allowing it to maintain state across different processing elements in a distributed environment.
 * The main functionalities include:
 * 1. getOrLoadExpectedOffset: Retrieves the expected offset from the state or loads it using a provided supplier if it is not already set.
 * 2. buffer: Buffers an event with its corresponding offset in the state.
 * 3. emitContiguous: Emits events in order starting from the expected offset and updates the expected offset accordingly.
 * 4. hasBufferedEvents: Checks if there are any buffered events in the state.
 * 5. getMinBufferedOffset: Retrieves the minimum offset among the buffered events, which can be useful for monitoring and debugging purposes.
 *
 * @author Parag Ghosh
 * @since 10/04/2026
 */
public final class ContiguousOffsetStateCore<T> implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Retrieves the expected offset from the state or loads it using a provided supplier if it is not already set.
     *
     * @param expectedOffsetState The ValueState to read/write the expected offset.
     * @param initialOffsetLoader A supplier that provides the initial offset if it is not already set in the state.
     * @return The expected offset, either read from the state or loaded using the supplier.
     */
    public long getOrLoadExpectedOffset(ValueState<Long> expectedOffsetState, LongSupplier initialOffsetLoader) {
        Long expectedOffset = expectedOffsetState.read();
        if (expectedOffset == null) {
            expectedOffset = initialOffsetLoader.getAsLong();
            expectedOffsetState.write(expectedOffset);
        }
        return expectedOffset;
    }

    /**
     * Buffers an event with its corresponding offset in the state.
     *
     * @param bufferedState The MapState to store the buffered events, where the key is the offset and the value is the event.
     * @param offset        The offset of the event to be buffered.
     * @param value         The event to be buffered.
     */
    public void buffer(MapState<Long, T> bufferedState, long offset, T value) {
        bufferedState.put(offset, value);
    }

    /**
     * Emits events in order starting from the expected offset and updates the expected offset accordingly.
     *
     * @param expectedOffset The current expected offset from which to start emitting events.
     * @param bufferedState  The MapState containing the buffered events, where the key is the offset and the value is the event.
     * @param emitter        A Consumer that accepts an event to be emitted.
     * @return The new expected offset after emitting contiguous events.
     */
    public long emitContiguous(long expectedOffset, MapState<Long, T> bufferedState, Consumer<T> emitter) {
        long current = expectedOffset;
        Map<Long, T> bufferedSnapshot = new HashMap<>();
        for (Map.Entry<Long, T> entry : bufferedState.entries().read()) {
            bufferedSnapshot.put(entry.getKey(), entry.getValue());
        }

        while (bufferedSnapshot.containsKey(current)) {
            T event = bufferedSnapshot.get(current);
            emitter.accept(event);
            bufferedState.remove(current);
            current++;
        }
        return current;
    }

    /**
     * Checks if there are any buffered events in the state.
     *
     * @param bufferedState The MapState containing the buffered events, where the key is the offset and the value is the event.
     * @return True if there are buffered events in the state, false otherwise.
     */
    public boolean hasBufferedEvents(MapState<Long, T> bufferedState) {
        Iterator<Map.Entry<Long, T>> iter = bufferedState.entries().read().iterator();
        return iter.hasNext();
    }

    /**
     * Retrieves the minimum offset among the buffered events, which can be useful for monitoring and debugging purposes.
     *
     * @param bufferedState The MapState containing the buffered events, where the key is the offset and the value is the event.
     * @return The minimum offset among the buffered events, or null if there are no buffered events.
     */
    public Long getMinBufferedOffset(MapState<Long, T> bufferedState) {
        Long min = null;
        for (Map.Entry<Long, T> entry : bufferedState.entries().read()) {
            if (min == null || entry.getKey() < min) {
                min = entry.getKey();
            }
        }
        return min;
    }
}


