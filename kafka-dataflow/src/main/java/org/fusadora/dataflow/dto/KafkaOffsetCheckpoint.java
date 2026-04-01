package org.fusadora.dataflow.dto;

import java.io.Serial;

/**
 * Offset checkpoint document persisted in Firestore.
 */
public class KafkaOffsetCheckpoint extends BaseDto {

    @Serial
    private static final long serialVersionUID = 1L;

    private String topic;
    private int partition;
    private long nextOffsetToRead;
    private long lastAckedOffset;
    private long updatedAt;
    private String updatedByJobId;

    public KafkaOffsetCheckpoint() {
        super();
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getNextOffsetToRead() {
        return nextOffsetToRead;
    }

    public void setNextOffsetToRead(long nextOffsetToRead) {
        this.nextOffsetToRead = nextOffsetToRead;
    }

    public long getLastAckedOffset() {
        return lastAckedOffset;
    }

    public void setLastAckedOffset(long lastAckedOffset) {
        this.lastAckedOffset = lastAckedOffset;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getUpdatedByJobId() {
        return updatedByJobId;
    }

    public void setUpdatedByJobId(String updatedByJobId) {
        this.updatedByJobId = updatedByJobId;
    }
}

