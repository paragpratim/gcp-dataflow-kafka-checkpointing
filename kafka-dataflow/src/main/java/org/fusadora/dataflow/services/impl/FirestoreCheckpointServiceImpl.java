package org.fusadora.dataflow.services.impl;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.*;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.StringUtils;
import org.fusadora.dataflow.dto.KafkaOffsetCheckpoint;
import org.fusadora.dataflow.exception.DataFlowException;
import org.fusadora.dataflow.services.CheckpointService;
import org.fusadora.dataflow.utilities.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(FirestoreCheckpointServiceImpl.class);

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
     * Updates the checkpoint for a given topic and partition using a non-blocking async write.
     * <p>
     * The monotonic forward-only guarantee is upheld by Beam state inside
     * {@code CommitContiguousHandledOffsetsDoFn} — the timer only fires with an offset that is
     * strictly greater than any previously committed value for the same key. It is therefore safe
     * to skip the transactional read-before-write and issue a plain {@code set(…, merge)} directly.
     * This removes one synchronous Firestore RPC per commit and eliminates the blocking wait on the
     * write result, significantly reducing latency on the Dataflow worker thread.
     * <p>
     * Write failures are logged as errors but do not throw; the checkpoint will be re-committed on
     * the next timer cycle, preserving at-least-once semantics.
     *
     * @param topic           the Kafka topic name
     * @param partition       the Kafka partition number
     * @param lastAckedOffset the last offset that was acknowledged as processed
     * @param jobId           the ID of the job that is updating the checkpoint
     */
    @Override
    public void updateOffsetCheckpoint(String topic, int partition, long lastAckedOffset, String jobId) {
        KafkaOffsetCheckpoint updated = new KafkaOffsetCheckpoint();
        updated.setTopic(topic);
        updated.setPartition(partition);
        updated.setNextOffsetToRead(lastAckedOffset + 1);
        updated.setLastAckedOffset(lastAckedOffset);
        updated.setUpdatedAt(System.currentTimeMillis());
        updated.setUpdatedByJobId(jobId);

        DocumentReference docRef = checkpoints().document(getDocId(topic, partition));
        ApiFuture<WriteResult> future = docRef.set(updated.toMap(), SetOptions.merge());
        ApiFutures.addCallback(future, new ApiFutureCallback<>() {
            @Override
            public void onSuccess(WriteResult result) {
                LOG.debug("Checkpoint async write succeeded for {}:{} nextOffset={}",
                        topic, partition, lastAckedOffset + 1);
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.error("Checkpoint async write FAILED for {}:{} lastAckedOffset={} — will retry on next timer cycle",
                        topic, partition, lastAckedOffset, t);
            }
        }, MoreExecutors.directExecutor());
    }
}

