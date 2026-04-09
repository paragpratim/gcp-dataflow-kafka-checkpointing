package org.fusadora.dataflow.services;

import java.io.Serializable;
import java.util.Map;

public interface CheckpointService extends Serializable {

    long getNextOffsetToRead(String topic, int partition);

    Map<Integer, Long> getTopicPartitionOffsets(String topic);

    void updateOffsetCheckpoint(String topic, int partition, long lastAckedOffset, String jobId);
}

