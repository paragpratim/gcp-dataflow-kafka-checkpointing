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
 * Shared stateful contiguous-offset algorithm used by offset-aware transforms.
 */
public final class ContiguousOffsetStateCore<T> implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public long getOrLoadExpectedOffset(ValueState<Long> expectedOffsetState, LongSupplier initialOffsetLoader) {
        Long expectedOffset = expectedOffsetState.read();
        if (expectedOffset == null) {
            expectedOffset = initialOffsetLoader.getAsLong();
            expectedOffsetState.write(expectedOffset);
        }
        return expectedOffset;
    }

    public void buffer(MapState<Long, T> bufferedState, long offset, T value) {
        bufferedState.put(offset, value);
    }

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

    public boolean hasBufferedEvents(MapState<Long, T> bufferedState) {
        Iterator<Map.Entry<Long, T>> iter = bufferedState.entries().read().iterator();
        return iter.hasNext();
    }

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


