package org.fusadora.dataflow.testing.stubs;

import org.fusadora.dataflow.services.CheckpointService;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared in-memory checkpoint store for DI-driven tests.
 */
public class RecordingCheckpointService implements CheckpointService, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final Map<String, Long> NEXT_OFFSETS = new ConcurrentHashMap<>();
    private static final List<CheckpointUpdate> UPDATES = Collections.synchronizedList(new ArrayList<>());

    public static void reset() {
        NEXT_OFFSETS.clear();
        UPDATES.clear();
    }

    public static void seed(Map<String, Long> initialNextOffsets) {
        reset();
        NEXT_OFFSETS.putAll(initialNextOffsets);
    }

    public static long nextOffset(String topic, int partition) {
        return NEXT_OFFSETS.getOrDefault(docId(topic, partition), 0L);
    }

    public static List<CheckpointUpdate> updates() {
        synchronized (UPDATES) {
            return List.copyOf(UPDATES);
        }
    }

    private static String docId(String topic, int partition) {
        return topic + ":" + partition;
    }

    @Override
    public long getNextOffsetToRead(String topic, int partition) {
        return nextOffset(topic, partition);
    }

    @Override
    public Map<Integer, Long> getTopicPartitionOffsets(String topic) {
        Map<Integer, Long> offsets = new ConcurrentHashMap<>();
        NEXT_OFFSETS.forEach((key, value) -> {
            String[] parts = key.split(":", -1);
            if (parts.length == 2 && parts[0].equals(topic)) {
                offsets.put(Integer.parseInt(parts[1]), value);
            }
        });
        return offsets;
    }

    @Override
    public void updateOffsetCheckpoint(String topic, int partition, long lastAckedOffset, String jobId) {
        NEXT_OFFSETS.merge(docId(topic, partition), lastAckedOffset + 1, Math::max);
        UPDATES.add(new CheckpointUpdate(topic, partition, lastAckedOffset, jobId));
    }

    public record CheckpointUpdate(String topic, int partition, long lastAckedOffset, String jobId) {
    }
}

