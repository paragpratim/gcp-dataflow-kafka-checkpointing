package org.fusadora.dataflow.dto;

import com.google.cloud.firestore.DocumentSnapshot;

import java.io.Serial;
import java.util.HashMap;
import java.util.Map;

import static org.fusadora.dataflow.common.Constants.*;

/**
 * org.fusadora.dataflow.dto.KafkaOffsetCheckpoint
 * DTO representing the Kafka offset checkpoint for a specific topic and partition.
 * This class is used to track the next offset to read and the last acknowledged offset for a Kafka consumer,
 * along with metadata about when it was last updated and by which job.
 *
 * @author Parag Ghosh
 * @since 10/04/2026
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

    /**
     * Build a KafkaOffsetCheckpoint from a Firestore DocumentSnapshot.
     * Returns null if the snapshot does not exist.
     *
     * @param snapshot the Firestore document snapshot containing the checkpoint data
     */
    public static KafkaOffsetCheckpoint fromSnapshot(DocumentSnapshot snapshot) {
        if (snapshot == null || !snapshot.exists()) {
            return null;
        }
        KafkaOffsetCheckpoint cp = new KafkaOffsetCheckpoint();
        cp.setTopic(snapshot.getString(CHECKPOINT_DOCUMENT_TOPIC));
        Long partition = snapshot.getLong(CHECKPOINT_DOCUMENT_PARTITION);
        cp.setPartition(partition != null ? partition.intValue() : 0);
        Long nextOffset = snapshot.getLong(CHECKPOINT_DOCUMENT_NEXT_OFFSET_TO_READ);
        cp.setNextOffsetToRead(nextOffset != null ? nextOffset : 0L);
        Long lastAcked = snapshot.getLong(CHECKPOINT_DOCUMENT_LAST_ACKED_OFFSET);
        cp.setLastAckedOffset(lastAcked != null ? lastAcked : 0L);
        Long updatedAt = snapshot.getLong(CHECKPOINT_DOCUMENT_UPDATED_AT);
        cp.setUpdatedAt(updatedAt != null ? updatedAt : 0L);
        cp.setUpdatedByJobId(snapshot.getString(CHECKPOINT_DOCUMENT_UPDATED_BY));
        return cp;
    }

    /**
     * Serialize this checkpoint to a Firestore-compatible map for persistence.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(CHECKPOINT_DOCUMENT_TOPIC, topic);
        map.put(CHECKPOINT_DOCUMENT_PARTITION, partition);
        map.put(CHECKPOINT_DOCUMENT_NEXT_OFFSET_TO_READ, nextOffsetToRead);
        map.put(CHECKPOINT_DOCUMENT_LAST_ACKED_OFFSET, lastAckedOffset);
        map.put(CHECKPOINT_DOCUMENT_UPDATED_AT, updatedAt);
        map.put(CHECKPOINT_DOCUMENT_UPDATED_BY, updatedByJobId);
        return map;
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

