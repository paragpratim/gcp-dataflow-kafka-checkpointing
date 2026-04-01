package org.fusadora.dataflow.services.impl;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.*;
import org.fusadora.dataflow.exception.DataFlowException;
import org.fusadora.dataflow.services.CheckpointService;
import org.fusadora.dataflow.utilities.PropertyUtils;

import java.io.Serial;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.fusadora.common.Constants.*;

/**
 * Firestore-backed checkpoint service.
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
            if (!snapshot.exists()) {
                return 0L;
            }
            Long nextOffset = snapshot.getLong(CHECKPOINT_DOCUMENT_NEXT_OFFSET_TO_READ);
            return nextOffset == null ? 0L : nextOffset;
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
                Long partition = doc.getLong(CHECKPOINT_DOCUMENT_PARTITION);
                Long nextOffset = doc.getLong(CHECKPOINT_DOCUMENT_NEXT_OFFSET_TO_READ);
                if (partition != null && nextOffset != null) {
                    offsetsByPartition.put(partition.intValue(), nextOffset);
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
                Long nextOffsetLong = snapshot.exists() ? snapshot.getLong(CHECKPOINT_DOCUMENT_NEXT_OFFSET_TO_READ) : null;
                long currentNextOffset = nextOffsetLong != null ? nextOffsetLong : 0L;
                long nextOffsetCandidate = lastAckedOffset + 1;
                if (nextOffsetCandidate <= currentNextOffset) {
                    return null;
                }

                Map<String, Object> checkpointData = new HashMap<>();
                checkpointData.put(CHECKPOINT_DOCUMENT_TOPIC, topic);
                checkpointData.put(CHECKPOINT_DOCUMENT_PARTITION, partition);
                checkpointData.put(CHECKPOINT_DOCUMENT_NEXT_OFFSET_TO_READ, nextOffsetCandidate);
                checkpointData.put(CHECKPOINT_DOCUMENT_LAST_ACKED_OFFSET, lastAckedOffset);
                checkpointData.put(CHECKPOINT_DOCUMENT_UPDATED_AT, System.currentTimeMillis());
                checkpointData.put(CHECKPOINT_DOCUMENT_UPDATED_BY, jobId);

                txn.set(docRef, checkpointData, SetOptions.merge());
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

