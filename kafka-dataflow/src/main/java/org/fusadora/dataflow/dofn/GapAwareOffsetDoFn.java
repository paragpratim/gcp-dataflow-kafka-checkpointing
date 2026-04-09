package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.fusadora.dataflow.core.ContiguousOffsetStateCore;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.services.CheckpointService;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * org.fusadora.dataflow.dofn.GapAwareOffsetDoFn
 * This is a Beam DoFn that processes KafkaEventEnvelopes while tracking offsets and handling gaps in the offset sequence.
 * It uses state to keep track of the expected next offset, buffered events that have arrived out of order, and the start time of any detected gap.
 * If a gap is detected (i.e., an event arrives with an offset greater than the expected offset), it buffers the event and starts a timer.
 * If the gap is not resolved (i.e., the expected offset does not arrive) before the timer fires,
 * it logs an error and can optionally output the missing offset to a side output for further handling.
 * If the gap is resolved before the timer fires, it simply clears the gap state and continues processing.
 * The DoFn also includes metrics to track the number of gaps detected, resolved before timeout, skipped due to timeout,
 * and late events that arrive after a gap has been skipped.
 *
 * @author Parag Ghosh
 * @since 10/04/2026
 */
@SuppressWarnings("unused") // Instantiated from pipeline transform wiring
public class GapAwareOffsetDoFn extends DoFn<KV<String, KafkaEventEnvelope>, KafkaEventEnvelope> {

    private static final Logger LOG = LoggerFactory.getLogger(GapAwareOffsetDoFn.class);

    private static final String STATE_EXPECTED = "expectedOffset";
    private static final String STATE_BUFFERED = "bufferedEvents";
    private static final String STATE_GAP_START = "gapStartMs";
    private static final String TIMER_GAP_TIMEOUT = "gapTimeout";

    private final CheckpointService checkpointService;
    private final long gapWaitTimeoutMs;
    private final boolean auditEnabled;
    private final TupleTag<KV<String, Long>> gapTimeoutOffsetTag;
    private final ContiguousOffsetStateCore<KafkaEventEnvelope> offsetCore = new ContiguousOffsetStateCore<>();

    private final Counter gapDetected = Metrics.counter(GapAwareOffsetDoFn.class, "gap_detected");
    private final Counter gapResolvedBeforeTimeout = Metrics.counter(GapAwareOffsetDoFn.class,
            "gap_resolved_before_timeout");
    private final Counter gapTimeoutSkip = Metrics.counter(GapAwareOffsetDoFn.class, "gap_timeout_skip");
    private final Counter lateAfterSkip = Metrics.counter(GapAwareOffsetDoFn.class, "late_after_skip");

    @DoFn.StateId(STATE_EXPECTED)
    @SuppressWarnings("unused") // Referenced by Beam runtime via @StateId
    private final StateSpec<ValueState<Long>> expectedOffsetSpec = StateSpecs.value(VarLongCoder.of());

    @DoFn.StateId(STATE_BUFFERED)
    @SuppressWarnings("unused") // Referenced by Beam runtime via @StateId
    private final StateSpec<MapState<Long, KafkaEventEnvelope>> bufferedEventsSpec = StateSpecs.map(VarLongCoder.of(),
            SerializableCoder.of(KafkaEventEnvelope.class));

    @DoFn.StateId(STATE_GAP_START)
    @SuppressWarnings("unused") // Referenced by Beam runtime via @StateId
    private final StateSpec<ValueState<Long>> gapStartMsSpec = StateSpecs.value(VarLongCoder.of());

    @DoFn.TimerId(TIMER_GAP_TIMEOUT)
    @SuppressWarnings("unused") // Referenced by Beam runtime via @TimerId
    private final TimerSpec gapTimeoutTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    public GapAwareOffsetDoFn(CheckpointService checkpointService, long gapWaitTimeoutMs, boolean auditEnabled) {
        this(checkpointService, gapWaitTimeoutMs, auditEnabled, null);
    }

    public GapAwareOffsetDoFn(CheckpointService checkpointService, long gapWaitTimeoutMs, boolean auditEnabled,
                              TupleTag<KV<String, Long>> gapTimeoutOffsetTag) {
        this.checkpointService = Objects.requireNonNull(checkpointService, "checkpointService must not be null");
        if (gapWaitTimeoutMs <= 0) {
            throw new IllegalArgumentException("gapWaitTimeoutMs must be > 0");
        }
        this.gapWaitTimeoutMs = gapWaitTimeoutMs;
        this.auditEnabled = auditEnabled;
        this.gapTimeoutOffsetTag = gapTimeoutOffsetTag;
    }

    @SuppressWarnings("unused") // Invoked by Beam runtime via @ProcessElement
    @ProcessElement
    public void processElement(
            ProcessContext context,
            @DoFn.StateId(STATE_EXPECTED) ValueState<Long> expectedOffsetState,
            @DoFn.StateId(STATE_BUFFERED) MapState<Long, KafkaEventEnvelope> bufferedEventsState,
            @DoFn.StateId(STATE_GAP_START) ValueState<Long> gapStartMsState,
            @DoFn.TimerId(TIMER_GAP_TIMEOUT) Timer gapTimeoutTimer) {

        KafkaEventEnvelope envelope = Objects.requireNonNull(context.element().getValue(),
                "KafkaEventEnvelope must not be null");
        long expectedOffset = getOrLoadExpectedOffset(expectedOffsetState, envelope);

        if (envelope.getOffset() < expectedOffset) {
            lateAfterSkip.inc();
            return;
        }

        offsetCore.buffer(bufferedEventsState, envelope.getOffset(), envelope);
        long nextExpected = offsetCore.emitContiguous(expectedOffset, bufferedEventsState, context::output);
        expectedOffsetState.write(nextExpected);

        if (offsetCore.hasBufferedEvents(bufferedEventsState)) {
            if (gapStartMsState.read() == null) {
                gapStartMsState.write(Instant.now().getMillis());
                gapDetected.inc();
                gapTimeoutTimer.offset(Duration.millis(gapWaitTimeoutMs)).setRelative();
            }
            Long minBufferedOffset = offsetCore.getMinBufferedOffset(bufferedEventsState);
            if (minBufferedOffset != null) {
                LOG.warn("Offset gap detected for {} expected={} nextBuffered={}",
                        context.element().getKey(), nextExpected, minBufferedOffset);
            }
        } else if (gapStartMsState.read() != null) {
            gapResolvedBeforeTimeout.inc();
            gapStartMsState.clear();
        }
    }

    @SuppressWarnings("unused") // Invoked by Beam runtime via @OnTimer
    @DoFn.OnTimer(TIMER_GAP_TIMEOUT)
    public void onGapTimeout(
            OnTimerContext context,
            @DoFn.Key String partitionKey,
            @DoFn.StateId(STATE_EXPECTED) ValueState<Long> expectedOffsetState,
            @DoFn.StateId(STATE_BUFFERED) MapState<Long, KafkaEventEnvelope> bufferedEventsState,
            @DoFn.StateId(STATE_GAP_START) ValueState<Long> gapStartMsState,
            @DoFn.TimerId(TIMER_GAP_TIMEOUT) Timer gapTimeoutTimer) {

        if (gapStartMsState.read() == null || !offsetCore.hasBufferedEvents(bufferedEventsState)) {
            gapStartMsState.clear();
            return;
        }

        Long expectedOffsetFromState = expectedOffsetState.read();
        long expectedOffset = expectedOffsetFromState == null ? 0L : expectedOffsetFromState;
        Long minBufferedOffset = offsetCore.getMinBufferedOffset(bufferedEventsState);
        if (minBufferedOffset == null) {
            gapStartMsState.clear();
            return;
        }

        if (minBufferedOffset > expectedOffset) {
            long missingOffset = expectedOffset;
            gapTimeoutSkip.inc();
            LOG.error("Offset gap timeout reached after {} ms for {} missingOffset={} nextAvailableOffset={}",
                    gapWaitTimeoutMs, partitionKey, missingOffset, minBufferedOffset);
            if (auditEnabled) {
                LOG.error("OFFSET_GAP_AUDIT partitionKey={} missingOffset={} nextAvailableOffset={} timeoutMs={}",
                        partitionKey, missingOffset, minBufferedOffset, gapWaitTimeoutMs);
            }
            if (gapTimeoutOffsetTag != null) {
                context.output(gapTimeoutOffsetTag, KV.of(partitionKey, missingOffset));
            }
            expectedOffset = minBufferedOffset;
        }

        long nextExpected = offsetCore.emitContiguous(expectedOffset, bufferedEventsState, context::output);
        expectedOffsetState.write(nextExpected);

        if (offsetCore.hasBufferedEvents(bufferedEventsState)) {
            gapStartMsState.write(Instant.now().getMillis());
            gapTimeoutTimer.offset(Duration.millis(gapWaitTimeoutMs)).setRelative();
        } else {
            gapStartMsState.clear();
        }
    }

    private long getOrLoadExpectedOffset(ValueState<Long> expectedOffsetState, KafkaEventEnvelope envelope) {
        return offsetCore.getOrLoadExpectedOffset(expectedOffsetState,
                () -> checkpointService.getNextOffsetToRead(envelope.getTopic(), envelope.getPartition()));
    }
}
