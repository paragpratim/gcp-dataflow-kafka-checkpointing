package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
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

import java.util.Map;
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
    private static final String STATE_BUFFER_METRIC_SAMPLE_COUNTER = "bufferMetricSampleCounter";
    private static final String TIMER_COMMIT = "commitTimer";
    private static final long BUFFER_METRIC_SAMPLE_EVERY = 1000L;

    private final CheckpointService checkpointService;
    private final String jobId;
    private final long commitIntervalSeconds;
    private final Map<Integer, Long> initialOffsetsByPartition;
    private final ContiguousOffsetStateCore<Long> offsetCore = new ContiguousOffsetStateCore<>();
    private final Counter handledOffsetsSeen = Metrics.counter(CommitContiguousHandledOffsetsDoFn.class,
            "handled_offsets_seen");
    private final Counter invalidPartitionKeysDropped = Metrics.counter(CommitContiguousHandledOffsetsDoFn.class,
            "invalid_partition_keys_dropped");
    private final Counter staleOffsetsIgnored = Metrics.counter(CommitContiguousHandledOffsetsDoFn.class,
            "stale_offsets_ignored");
    private final Counter noProgressBuffered = Metrics.counter(CommitContiguousHandledOffsetsDoFn.class,
            "no_progress_buffered");
    private final Counter pendingAckUpdated = Metrics.counter(CommitContiguousHandledOffsetsDoFn.class,
            "pending_ack_updated");
    private final Counter commitTimerArmed = Metrics.counter(CommitContiguousHandledOffsetsDoFn.class,
            "commit_timer_armed");
    private final Counter commitTimerFired = Metrics.counter(CommitContiguousHandledOffsetsDoFn.class,
            "commit_timer_fired");
    private final Counter commitTimerFiredWithoutPendingAck = Metrics.counter(CommitContiguousHandledOffsetsDoFn.class,
            "commit_timer_fired_without_pending_ack");
    private final Counter checkpointCommitAttempted = Metrics.counter(CommitContiguousHandledOffsetsDoFn.class,
            "checkpoint_commit_attempted");
    private final Counter offsetsContiguouslyAcked = Metrics.counter(CommitContiguousHandledOffsetsDoFn.class,
            "offsets_contiguously_acked");
    private final Distribution contiguousAdvanceSize = Metrics.distribution(CommitContiguousHandledOffsetsDoFn.class,
            "contiguous_advance_size");
    private final Distribution processElementLatencyMs = Metrics.distribution(CommitContiguousHandledOffsetsDoFn.class,
            "process_element_latency_ms");
    private final Distribution checkpointServiceCallLatencyMs = Metrics.distribution(CommitContiguousHandledOffsetsDoFn.class,
            "checkpoint_service_call_latency_ms");
    private final Distribution bufferedStateSize = Metrics.distribution(CommitContiguousHandledOffsetsDoFn.class,
            "buffered_state_size");

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

    @DoFn.StateId(STATE_BUFFER_METRIC_SAMPLE_COUNTER)
    @SuppressWarnings("unused") // Referenced by Beam runtime via @StateId
    private final StateSpec<ValueState<Long>> bufferMetricSampleCounterSpec = StateSpecs.value(VarLongCoder.of());

    @DoFn.TimerId(TIMER_COMMIT)
    @SuppressWarnings("unused") // Referenced by Beam runtime via @TimerId
    private final TimerSpec commitTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @SuppressWarnings("unused") // Instantiated from pipeline transform wiring
    public CommitContiguousHandledOffsetsDoFn(CheckpointService checkpointService, String jobId,
                                              long commitIntervalSeconds) {
        this(checkpointService, jobId, commitIntervalSeconds, Map.of());
    }

    @SuppressWarnings("unused") // Instantiated from pipeline transform wiring
    public CommitContiguousHandledOffsetsDoFn(CheckpointService checkpointService, String jobId,
                                              long commitIntervalSeconds,
                                              Map<Integer, Long> initialOffsetsByPartition) {
        this.checkpointService = Objects.requireNonNull(checkpointService, "checkpointService must not be null");
        this.jobId = Objects.requireNonNull(jobId, "jobId must not be null");
        if (commitIntervalSeconds <= 0) {
            throw new IllegalArgumentException("commitIntervalSeconds must be > 0");
        }
        this.commitIntervalSeconds = commitIntervalSeconds;
        this.initialOffsetsByPartition = Map.copyOf(Objects.requireNonNull(initialOffsetsByPartition,
                "initialOffsetsByPartition must not be null"));
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
            @DoFn.StateId(STATE_BUFFER_METRIC_SAMPLE_COUNTER) ValueState<Long> bufferMetricSampleCounterState,
            @DoFn.TimerId(TIMER_COMMIT) Timer commitTimer) {
        long startNanos = System.nanoTime();
        handledOffsetsSeen.inc();

        try {

            KV<String, Long> keyedOffset = context.element();
            PartitionRef partitionRef = parsePartitionRef(keyedOffset.getKey());
            if (partitionRef == null) {
                invalidPartitionKeysDropped.inc();
                return;
            }

            long expectedOffset = offsetCore.getOrLoadExpectedOffset(expectedOffsetState,
                    () -> {
                        Long preloadedOffset = initialOffsetsByPartition.get(partitionRef.partition);
                        return Objects.requireNonNullElseGet(preloadedOffset,
                                () -> checkpointService.getNextOffsetToRead(partitionRef.topic, partitionRef.partition));
                    });

            long incomingOffset = keyedOffset.getValue();
            if (incomingOffset < expectedOffset) {
                staleOffsetsIgnored.inc();
                return;
            }

            offsetCore.buffer(bufferedOffsetsState, incomingOffset, incomingOffset);
            long nextExpected = offsetCore.emitContiguous(expectedOffset, bufferedOffsetsState, ignored -> {
            });
            expectedOffsetState.write(nextExpected);
            long advancedCount = nextExpected - expectedOffset;
            contiguousAdvanceSize.update(advancedCount);
            Long sampleCounter = bufferMetricSampleCounterState.read();
            long nextSampleCounter = sampleCounter == null ? 1L : sampleCounter + 1L;
            bufferMetricSampleCounterState.write(nextSampleCounter);
            if (nextSampleCounter % BUFFER_METRIC_SAMPLE_EVERY == 0L) {
                bufferedStateSize.update(offsetCore.countBufferedEvents(bufferedOffsetsState));
            }

            long lastAckedOffset = nextExpected - 1;
            if (lastAckedOffset >= expectedOffset) {
                offsetsContiguouslyAcked.inc(advancedCount);
                Long pendingAckOffset = pendingAckOffsetState.read();
                if (pendingAckOffset == null || lastAckedOffset > pendingAckOffset) {
                    pendingAckOffsetState.write(lastAckedOffset);
                    pendingAckUpdated.inc();
                }

                if (timerArmedState.read() == null) {
                    commitTimer.offset(org.joda.time.Duration.standardSeconds(commitIntervalSeconds)).setRelative();
                    timerArmedState.write(1L);
                    commitTimerArmed.inc();
                }
            } else {
                noProgressBuffered.inc();
            }
        } finally {
            processElementLatencyMs.update((System.nanoTime() - startNanos) / 1_000_000L);
        }
    }

    @SuppressWarnings("unused") // Invoked by Beam runtime via @OnTimer
    @DoFn.OnTimer(TIMER_COMMIT)
    public void onCommitTimer(
            OnTimerContext context,
            @DoFn.Key String key,
            @DoFn.StateId(STATE_PENDING_ACK) ValueState<Long> pendingAckOffsetState,
            @DoFn.StateId(STATE_TIMER_ARMED) ValueState<Long> timerArmedState) {
        commitTimerFired.inc();
        PartitionRef partitionRef = parsePartitionRef(key);
        if (partitionRef == null) {
            invalidPartitionKeysDropped.inc();
            timerArmedState.clear();
            return;
        }

        Long pendingAckOffset = pendingAckOffsetState.read();
        if (pendingAckOffset != null) {
            commitPendingOffset(partitionRef, pendingAckOffsetState);
        } else {
            commitTimerFiredWithoutPendingAck.inc();
        }

        timerArmedState.clear();
    }

    private void commitPendingOffset(PartitionRef partitionRef, ValueState<Long> pendingAckOffsetState) {
        Long pendingAckOffset = pendingAckOffsetState.read();
        if (pendingAckOffset == null) {
            return;
        }
        checkpointCommitAttempted.inc();
        long startNanos = System.nanoTime();
        checkpointService.updateOffsetCheckpoint(partitionRef.topic, partitionRef.partition, pendingAckOffset, jobId);
        checkpointServiceCallLatencyMs.update((System.nanoTime() - startNanos) / 1_000_000L);
        LOG.info("Checkpoint updated for {}:{} lastAckedOffset={}",
                partitionRef.topic, partitionRef.partition, pendingAckOffset);
        pendingAckOffsetState.clear();
    }


    private record PartitionRef(String topic, int partition) {
    }
}
