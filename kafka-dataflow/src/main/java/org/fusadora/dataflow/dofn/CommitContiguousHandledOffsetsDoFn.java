package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.fusadora.dataflow.core.ContiguousOffsetStateCore;
import org.fusadora.dataflow.services.CheckpointService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * org.fusadora.dataflow.dofn.CommitContiguousHandledOffsetsDoFn
 * This is a Beam DoFn that processes KV<String, Long> elements representing offsets for specific partitions of a topic.
 * The key is expected to be in the format "topic:partition" and the value is the offset that has been handled.
 * The DoFn maintains state to track the expected next offset for each partition and buffers out-of-order offsets until they can be committed contiguously.
 * When contiguous offsets are emitted, it updates the checkpoint service with the latest committed offset for the corresponding partition.
 * The DoFn uses two state variables: one for tracking the expected next offset and another for buffering out-of-order offsets.
 *
 * @author Parag Ghosh
 * @since 10/04/2026
 */
@SuppressWarnings("unused") // Instantiated from pipeline transform wiring
public class CommitContiguousHandledOffsetsDoFn extends DoFn<KV<String, Long>, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(CommitContiguousHandledOffsetsDoFn.class);
    private static final String KEY_SEPARATOR = ":";
    private static final int EXPECTED_KEY_PARTS = 2;
    private static final String STATE_EXPECTED = "expectedOffset";
    private static final String STATE_BUFFERED = "bufferedOffsets";
    private static final String STATE_PENDING_ACK = "pendingAckOffset";
    private static final String STATE_TIMER_ARMED = "commitTimerArmed";
    private static final String TIMER_COMMIT = "commitTimer";

    private final CheckpointService checkpointService;
    private final String jobId;
    private final long commitIntervalSeconds;
    private final ContiguousOffsetStateCore<Long> offsetCore = new ContiguousOffsetStateCore<>();

    @DoFn.StateId(STATE_EXPECTED)
    @SuppressWarnings("unused") // Referenced by Beam runtime via @StateId
    private final StateSpec<ValueState<Long>> expectedOffsetSpec = StateSpecs.value(VarLongCoder.of());

    @DoFn.StateId(STATE_BUFFERED)
    @SuppressWarnings("unused") // Referenced by Beam runtime via @StateId
    private final StateSpec<MapState<Long, Long>> bufferedOffsetsSpec = StateSpecs.map(VarLongCoder.of(), VarLongCoder.of());

    @DoFn.StateId(STATE_PENDING_ACK)
    @SuppressWarnings("unused") // Referenced by Beam runtime via @StateId
    private final StateSpec<ValueState<Long>> pendingAckOffsetSpec = StateSpecs.value(VarLongCoder.of());

    @DoFn.StateId(STATE_TIMER_ARMED)
    @SuppressWarnings("unused") // Referenced by Beam runtime via @StateId
    private final StateSpec<ValueState<Long>> timerArmedSpec = StateSpecs.value(VarLongCoder.of());

    @DoFn.TimerId(TIMER_COMMIT)
    @SuppressWarnings("unused") // Referenced by Beam runtime via @TimerId
    private final TimerSpec commitTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @SuppressWarnings("unused") // Instantiated from pipeline transform wiring
    public CommitContiguousHandledOffsetsDoFn(CheckpointService checkpointService, String jobId,
                                              long commitIntervalSeconds) {
        this.checkpointService = Objects.requireNonNull(checkpointService, "checkpointService must not be null");
        this.jobId = Objects.requireNonNull(jobId, "jobId must not be null");
        if (commitIntervalSeconds <= 0) {
            throw new IllegalArgumentException("commitIntervalSeconds must be > 0");
        }
        this.commitIntervalSeconds = commitIntervalSeconds;
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

    @SuppressWarnings("unused") // Invoked by Beam runtime via @ProcessElement
    @ProcessElement
    public void processElement(
            ProcessContext context,
            @DoFn.StateId(STATE_EXPECTED) ValueState<Long> expectedOffsetState,
            @DoFn.StateId(STATE_BUFFERED) MapState<Long, Long> bufferedOffsetsState,
            @DoFn.StateId(STATE_PENDING_ACK) ValueState<Long> pendingAckOffsetState,
            @DoFn.StateId(STATE_TIMER_ARMED) ValueState<Long> timerArmedState,
            @DoFn.TimerId(TIMER_COMMIT) Timer commitTimer) {

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
            Long pendingAckOffset = pendingAckOffsetState.read();
            if (pendingAckOffset == null || lastAckedOffset > pendingAckOffset) {
                pendingAckOffsetState.write(lastAckedOffset);
            }

            if (timerArmedState.read() == null) {
                commitTimer.offset(org.joda.time.Duration.standardSeconds(commitIntervalSeconds)).setRelative();
                timerArmedState.write(1L);
            }
        }
    }

    @SuppressWarnings("unused") // Invoked by Beam runtime via @OnTimer
    @DoFn.OnTimer(TIMER_COMMIT)
    public void onCommitTimer(
            OnTimerContext context,
            @DoFn.Key String key,
            @DoFn.StateId(STATE_PENDING_ACK) ValueState<Long> pendingAckOffsetState,
            @DoFn.StateId(STATE_TIMER_ARMED) ValueState<Long> timerArmedState) {
        PartitionRef partitionRef = parsePartitionRef(key);
        if (partitionRef == null) {
            timerArmedState.clear();
            return;
        }

        Long pendingAckOffset = pendingAckOffsetState.read();
        if (pendingAckOffset != null) {
            commitPendingOffset(partitionRef, pendingAckOffsetState);
        }

        timerArmedState.clear();
    }

    private void commitPendingOffset(PartitionRef partitionRef, ValueState<Long> pendingAckOffsetState) {
        Long pendingAckOffset = pendingAckOffsetState.read();
        if (pendingAckOffset == null) {
            return;
        }
        checkpointService.updateOffsetCheckpoint(partitionRef.topic, partitionRef.partition, pendingAckOffset, jobId);
        LOG.info("Checkpoint updated for {}:{} lastAckedOffset={}",
                partitionRef.topic, partitionRef.partition, pendingAckOffset);
        pendingAckOffsetState.clear();
    }


    private record PartitionRef(String topic, int partition) {
    }
}
