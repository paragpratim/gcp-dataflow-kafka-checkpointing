package org.fusadora.dataflow.services.impl;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.*;
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
 * Firestore-backed checkpoint service.
 * Reads and writes are funnelled through {@link KafkaOffsetCheckpoint} to keep the DTO
 * and the stored document schema in sync.
 */
public class FirestoreCheckpointServiceImpl implements CheckpointService {

    @Serial
    private static final long serialVersionUID = 1L;

    private transient Firestore firestore;

    private Firestore getFirestore() {
        if (firestore == null) {
            FirestoreOptions options = FirestoreOptions.getDefaultInstance().toBuilder()
                    .setProjectId(PropertyUtils.getProperty(PropertyUtils.PROJECT_NAME))
                    .build();
            firestore = options.getService();
        }
        return firestore;
    }

    private CollectionReference checkpoints() {
        return getFirestore().collection(PropertyUtils.getProperty(PropertyUtils.CHECKPOINT_COLLECTION));
    }

    private String getDocId(String topic, int partition) {
        return topic + ":" + partition;
    }

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

