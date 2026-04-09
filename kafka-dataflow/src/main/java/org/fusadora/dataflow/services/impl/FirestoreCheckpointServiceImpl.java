package org.fusadora.dataflow.services.impl;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.*;
import org.apache.commons.lang3.StringUtils;
import org.fusadora.dataflow.dto.KafkaOffsetCheckpoint;
import org.fusadora.dataflow.exception.DataFlowException;
import org.fusadora.dataflow.services.CheckpointService;
import org.fusadora.dataflow.utilities.PropertyUtils;

import java.io.Serial;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.fusadora.dataflow.common.Constants.CHECKPOINT_DOCUMENT_TOPIC;

/**
 * org.fusadora.dataflow.services.impl.FirestoreCheckpointServiceImpl
 * Firestore-backed checkpoint service.
 * Reads and writes are funneled through {@link KafkaOffsetCheckpoint} to keep the DTO
 * and the stored document schema in sync.
 *
 * @author Parag Ghosh
 * @since 10/04/2026
 */
public class FirestoreCheckpointServiceImpl implements CheckpointService {

    @Serial
    private static final long serialVersionUID = 1L;

    private transient Firestore firestore;

    /**
     * Lazily initializes the Firestore client. The client is thread-safe and can be shared across threads.
     *
     * @return Firestore client instance
     */
    private Firestore getFirestore() {
        if (firestore == null) {
            FirestoreOptions.Builder optionsBuilder = FirestoreOptions.getDefaultInstance().toBuilder()
                    .setProjectId(PropertyUtils.getProperty(PropertyUtils.PROJECT_NAME));

            String databaseId = PropertyUtils.getProperty(PropertyUtils.CHECKPOINT_FIRESTORE_DATABASE_ID);
            if (StringUtils.isNotBlank(databaseId)) {
                optionsBuilder.setDatabaseId(databaseId);
            }

            FirestoreOptions options = optionsBuilder.build();
            firestore = options.getService();
        }
        return firestore;
    }

    /**
     * Helper method to get the Firestore collection reference for checkpoints. The collection name is read from properties.
     *
     * @return CollectionReference for checkpoints
     */
    private CollectionReference checkpoints() {
        return getFirestore().collection(PropertyUtils.getProperty(PropertyUtils.CHECKPOINT_COLLECTION));
    }

    /**
     * Helper method to construct a unique document ID for a given topic and partition. This ensures that each topic-partition pair has a single checkpoint document.
     *
     * @param topic     the Kafka topic name
     * @param partition the Kafka partition number
     * @return a unique document ID in the format "topic:partition"
     */
    private String getDocId(String topic, int partition) {
        return topic + ":" + partition;
    }

    /**
     * Retrieves the next offset to read for a given topic and partition. If no checkpoint exists, returns 0.
     *
     * @param topic     the Kafka topic name
     * @param partition the Kafka partition number
     * @return the next offset to read, or 0 if no checkpoint exists
     */
    @Override
    public long getNextOffsetToRead(String topic, int partition) {
        try {
            DocumentReference docRef = checkpoints().document(getDocId(topic, partition));
            DocumentSnapshot snapshot = docRef.get().get();
            KafkaOffsetCheckpoint checkpoint = KafkaOffsetCheckpoint.fromSnapshot(snapshot);
            return checkpoint == null ? 0L : checkpoint.getNextOffsetToRead();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new DataFlowException("Interrupted while reading checkpoint", ie);
        } catch (ExecutionException ee) {
            throw new DataFlowException("Failed to read checkpoint", ee);
        }
    }

    /**
     * Retrieves the next offsets to read for all partitions of a given topic. Returns a map of partition numbers to their respective next offsets.
     * If no checkpoints exist for the topic, returns an empty map.
     *
     * @param topic the Kafka topic name
     * @return a map of partition numbers to next offsets to read
     */
    @Override
    public Map<Integer, Long> getTopicPartitionOffsets(String topic) {
        Map<Integer, Long> offsetsByPartition = new HashMap<>();
        try {
            ApiFuture<QuerySnapshot> queryFuture = checkpoints().whereEqualTo(CHECKPOINT_DOCUMENT_TOPIC, topic).get();
            for (QueryDocumentSnapshot doc : queryFuture.get().getDocuments()) {
                KafkaOffsetCheckpoint checkpoint = KafkaOffsetCheckpoint.fromSnapshot(doc);
                if (checkpoint != null) {
                    offsetsByPartition.put(checkpoint.getPartition(), checkpoint.getNextOffsetToRead());
                }
            }
            return offsetsByPartition;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new DataFlowException("Interrupted while querying checkpoints", ie);
        } catch (ExecutionException ee) {
            throw new DataFlowException("Failed to query checkpoints", ee);
        }
    }

    /**
     * Updates the checkpoint for a given topic and partition. The update is performed in a transaction to ensure atomicity and to handle concurrent updates correctly.
     * The method checks the existing checkpoint and only updates it if the new next offset candidate (lastAckedOffset + 1) is greater than the current next offset to read.
     * This prevents regressing the checkpoint in case of out-of-order acknowledgments.
     *
     * @param topic           the Kafka topic name
     * @param partition       the Kafka partition number
     * @param lastAckedOffset the last offset that was acknowledged as processed. The next offset to read will be set to lastAckedOffset + 1.
     * @param jobId           the ID of the job that is updating the checkpoint, used for auditing purposes
     */
    @Override
    public void updateOffsetCheckpoint(String topic, int partition, long lastAckedOffset, String jobId) {
        try {
            DocumentReference docRef = checkpoints().document(getDocId(topic, partition));
            getFirestore().runTransaction(txn -> {
                DocumentSnapshot snapshot = txn.get(docRef).get();
                KafkaOffsetCheckpoint existing = KafkaOffsetCheckpoint.fromSnapshot(snapshot);
                long currentNextOffset = existing != null ? existing.getNextOffsetToRead() : 0L;
                long nextOffsetCandidate = lastAckedOffset + 1;
                if (nextOffsetCandidate <= currentNextOffset) {
                    return null;
                }

                KafkaOffsetCheckpoint updated = new KafkaOffsetCheckpoint();
                updated.setTopic(topic);
                updated.setPartition(partition);
                updated.setNextOffsetToRead(nextOffsetCandidate);
                updated.setLastAckedOffset(lastAckedOffset);
                updated.setUpdatedAt(System.currentTimeMillis());
                updated.setUpdatedByJobId(jobId);

                txn.set(docRef, updated.toMap(), SetOptions.merge());
                return null;
            }).get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new DataFlowException("Interrupted while updating checkpoint", ie);
        } catch (ExecutionException ee) {
            throw new DataFlowException("Failed to update checkpoint", ee);
        }
    }
}

