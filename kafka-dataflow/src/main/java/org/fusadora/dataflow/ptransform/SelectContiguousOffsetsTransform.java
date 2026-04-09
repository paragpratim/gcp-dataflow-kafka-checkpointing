package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.fusadora.dataflow.dofn.GapAwareOffsetDoFn;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.services.CheckpointService;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;

import java.util.Objects;

/**
 * org.fusadora.dataflow.ptransform.SelectContiguousOffsetsTransform
 * This is a Beam PTransform that takes a PCollection of KafkaEventEnvelope and filters out events to ensure that only contiguous offsets are processed.
 * It uses a CheckpointService to track the last processed offsets for each topic-partition and waits for a specified gap wait timeout
 * to allow for any missing offsets to arrive before proceeding with processing.
 *
 * @author Parag Ghosh
 * @since 10/04/2026
 */
public class SelectContiguousOffsetsTransform
        extends PTransform<@NotNull PCollection<KafkaEventEnvelope>, @NotNull PCollection<KafkaEventEnvelope>> {

    private static final Duration DEFAULT_GAP_WAIT_TIMEOUT = Duration.standardMinutes(5);

    private final CheckpointService checkpointService;
    private final Duration gapWaitTimeout;
    private final boolean auditEnabled;

    public SelectContiguousOffsetsTransform(CheckpointService checkpointService) {
        this(checkpointService, DEFAULT_GAP_WAIT_TIMEOUT, true);
    }

    public SelectContiguousOffsetsTransform(CheckpointService checkpointService, Duration gapWaitTimeout,
                                            boolean auditEnabled) {
        this.checkpointService = Objects.requireNonNull(checkpointService, "checkpointService must not be null");
        this.gapWaitTimeout = Objects.requireNonNull(gapWaitTimeout, "gapWaitTimeout must not be null");
        if (gapWaitTimeout.getMillis() <= 0) {
            throw new IllegalArgumentException("gapWaitTimeout must be > 0");
        }
        this.auditEnabled = auditEnabled;
    }

    @Override
    public @NotNull PCollection<KafkaEventEnvelope> expand(PCollection<KafkaEventEnvelope> input) {
        return input
                .apply("Key by topic-partition", MapElements.via(new SimpleFunction<KafkaEventEnvelope, KV<String, KafkaEventEnvelope>>() {
                    @Override
                    public KV<String, KafkaEventEnvelope> apply(KafkaEventEnvelope envelope) {
                        return KV.of(envelope.getPartitionKey(), envelope);
                    }
                }))
                .apply("Filter contiguous offsets", ParDo.of(new GapAwareOffsetDoFn(checkpointService,
                        gapWaitTimeout.getMillis(), auditEnabled)));
    }
}

