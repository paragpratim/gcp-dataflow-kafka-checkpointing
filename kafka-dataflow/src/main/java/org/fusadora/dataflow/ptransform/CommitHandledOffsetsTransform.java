package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.fusadora.dataflow.dofn.CommitContiguousHandledOffsetsDoFn;
import org.fusadora.dataflow.services.CheckpointService;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * org.fusadora.dataflow.ptransform.CommitHandledOffsetsTransform
 * This is a Beam PTransform that takes a PCollection of KV<String, Long> representing handled offsets and commits them using the provided CheckpointService.
 * The transform applies a ParDo with a DoFn that commits contiguous handled offsets for a specific jobId.
 *
 * @author Parag Ghosh
 * @since 10/04/2026
 */
public class CommitHandledOffsetsTransform extends PTransform<@NotNull PCollection<KV<String, Long>>, @NotNull PDone> {

    private final CheckpointService checkpointService;
    private final String jobId;
    private final long commitIntervalSeconds;

    public CommitHandledOffsetsTransform(CheckpointService checkpointService, String jobId, long commitIntervalSeconds) {
        this.checkpointService = Objects.requireNonNull(checkpointService, "checkpointService must not be null");
        this.jobId = Objects.requireNonNull(jobId, "jobId must not be null");
        if (commitIntervalSeconds <= 0) {
            throw new IllegalArgumentException("commitIntervalSeconds must be > 0");
        }
        this.commitIntervalSeconds = commitIntervalSeconds;
    }

    @Override
    public @NotNull PDone expand(PCollection<KV<String, Long>> input) {
        input.apply("Commit contiguous handled offsets",
                ParDo.of(new CommitContiguousHandledOffsetsDoFn(checkpointService, jobId, commitIntervalSeconds)));
        return PDone.in(input.getPipeline());
    }
}

