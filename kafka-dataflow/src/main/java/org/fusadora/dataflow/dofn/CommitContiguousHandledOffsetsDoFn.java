package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.fusadora.dataflow.ptransform.ContiguousOffsetStateCore;
import org.fusadora.dataflow.services.CheckpointService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Advances checkpoints only across contiguous handled offsets per topic-partition.
 */
@SuppressWarnings("unused") // Instantiated from pipeline transform wiring
public class CommitContiguousHandledOffsetsDoFn extends DoFn<KV<String, Long>, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(CommitContiguousHandledOffsetsDoFn.class);
    private static final String KEY_SEPARATOR = ":";
    private static final int EXPECTED_KEY_PARTS = 2;
    private static final String STATE_EXPECTED = "expectedOffset";
    private static final String STATE_BUFFERED = "bufferedOffsets";

    private final CheckpointService checkpointService;
    private final String jobId;
    private final ContiguousOffsetStateCore<Long> offsetCore = new ContiguousOffsetStateCore<>();

    @DoFn.StateId(STATE_EXPECTED)
    @SuppressWarnings("unused") // Referenced by Beam runtime via @StateId
    private final StateSpec<ValueState<Long>> expectedOffsetSpec = StateSpecs.value(VarLongCoder.of());

    @DoFn.StateId(STATE_BUFFERED)
    @SuppressWarnings("unused") // Referenced by Beam runtime via @StateId
    private final StateSpec<MapState<Long, Long>> bufferedOffsetsSpec = StateSpecs.map(VarLongCoder.of(), VarLongCoder.of());

    @SuppressWarnings("unused") // Instantiated from pipeline transform wiring
    public CommitContiguousHandledOffsetsDoFn(CheckpointService checkpointService, String jobId) {
        this.checkpointService = Objects.requireNonNull(checkpointService, "checkpointService must not be null");
        this.jobId = Objects.requireNonNull(jobId, "jobId must not be null");
    }

    @SuppressWarnings("unused") // Invoked by Beam runtime via @ProcessElement
    @ProcessElement
    public void processElement(
            ProcessContext context,
            @DoFn.StateId(STATE_EXPECTED) ValueState<Long> expectedOffsetState,
            @DoFn.StateId(STATE_BUFFERED) MapState<Long, Long> bufferedOffsetsState) {

        KV<String, Long> keyedOffset = context.element();
        PartitionRef partitionRef = parsePartitionRef(keyedOffset.getKey());
        if (partitionRef == null) {
            return;
        }

        long expectedOffset = offsetCore.getOrLoadExpectedOffset(expectedOffsetState,
                () -> checkpointService.getNextOffsetToRead(partitionRef.topic, partitionRef.partition));

        long incomingOffset = keyedOffset.getValue();
        if (incomingOffset < expectedOffset) {
            return;
        }

        offsetCore.buffer(bufferedOffsetsState, incomingOffset, incomingOffset);
        long nextExpected = offsetCore.emitContiguous(expectedOffset, bufferedOffsetsState, ignored -> {
        });
        expectedOffsetState.write(nextExpected);

        long lastAckedOffset = nextExpected - 1;
        if (lastAckedOffset >= expectedOffset) {
            checkpointService.updateOffsetCheckpoint(partitionRef.topic, partitionRef.partition, lastAckedOffset, jobId);
            LOG.info("Checkpoint updated for {}:{} lastAckedOffset={}",
                    partitionRef.topic, partitionRef.partition, lastAckedOffset);
        }
    }

    private static PartitionRef parsePartitionRef(String key) {
        if (key == null || key.isBlank()) {
            return null;
        }

        String[] keyParts = key.split(KEY_SEPARATOR, -1);
        if (keyParts.length != EXPECTED_KEY_PARTS || keyParts[0].isBlank()) {
            return null;
        }

        try {
            return new PartitionRef(keyParts[0], Integer.parseInt(keyParts[1]));
        } catch (NumberFormatException nfe) {
            LOG.warn("Unable to parse partition from key={}", key);
            return null;
        }
    }

    private record PartitionRef(String topic, int partition) {
    }
}


