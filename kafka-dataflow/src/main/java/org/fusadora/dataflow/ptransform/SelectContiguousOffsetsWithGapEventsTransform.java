package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.*;
import org.fusadora.dataflow.dofn.GapAwareOffsetDoFn;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.services.CheckpointService;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;

import java.util.Objects;

/**
 * org.fusadora.dataflow.ptransform.SelectContiguousOffsetsWithGapEventsTransform
 * This is a Beam PTransform that takes a PCollection of KafkaEventEnvelope and outputs a PCollectionTuple containing:
 * - A PCollection of KafkaEventEnvelope with contiguous offsets (main output)
 * - A PCollection of KV<String, Long> representing topic-partition and offset for which a gap timeout occurred (side output)
 * The transform uses a DoFn (GapAwareOffsetDoFn) to track offsets for each topic-partition and identify gaps.
 * If a gap is detected and not filled within the specified timeout, the offset is emitted to the side output.
 *
 * @author Parag Ghosh
 * @since 10/04/2026
 */
public class SelectContiguousOffsetsWithGapEventsTransform extends
        PTransform<@NotNull PCollection<KafkaEventEnvelope>, @NotNull PCollectionTuple> {

    public static final TupleTag<KafkaEventEnvelope> CONTIGUOUS_MAIN_TAG = new TupleTag<>() {
    };
    public static final TupleTag<KV<String, Long>> GAP_TIMEOUT_OFFSET_TAG = new TupleTag<>() {
    };

    private static final Duration DEFAULT_GAP_WAIT_TIMEOUT = Duration.standardMinutes(5);

    private final CheckpointService checkpointService;
    private final Duration gapWaitTimeout;
    private final boolean auditEnabled;

    public SelectContiguousOffsetsWithGapEventsTransform(CheckpointService checkpointService) {
        this(checkpointService, DEFAULT_GAP_WAIT_TIMEOUT, true);
    }

    public SelectContiguousOffsetsWithGapEventsTransform(CheckpointService checkpointService, Duration gapWaitTimeout,
                                                         boolean auditEnabled) {
        this.checkpointService = Objects.requireNonNull(checkpointService, "checkpointService must not be null");
        this.gapWaitTimeout = Objects.requireNonNull(gapWaitTimeout, "gapWaitTimeout must not be null");
        if (gapWaitTimeout.getMillis() <= 0) {
            throw new IllegalArgumentException("gapWaitTimeout must be > 0");
        }
        this.auditEnabled = auditEnabled;
    }

    @Override
    public @NotNull PCollectionTuple expand(PCollection<KafkaEventEnvelope> input) {
        return input
                .apply("Key by topic-partition", MapElements.via(new SimpleFunction<KafkaEventEnvelope, KV<String, KafkaEventEnvelope>>() {
                    @Override
                    public KV<String, KafkaEventEnvelope> apply(KafkaEventEnvelope envelope) {
                        return KV.of(envelope.getPartitionKey(), envelope);
                    }
                }))
                .apply("Filter contiguous offsets with gap events",
                        ParDo.of(new GapAwareOffsetDoFn(checkpointService, gapWaitTimeout.getMillis(), auditEnabled,
                                        GAP_TIMEOUT_OFFSET_TAG))
                                .withOutputTags(CONTIGUOUS_MAIN_TAG, TupleTagList.of(GAP_TIMEOUT_OFFSET_TAG)));
    }
}

