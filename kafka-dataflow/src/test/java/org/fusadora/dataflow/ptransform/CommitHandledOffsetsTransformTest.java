package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.fusadora.dataflow.testing.stubs.RecordingCheckpointService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CommitHandledOffsetsTransformTest {

    private final Pipeline pipeline = Pipeline.create();

    @BeforeEach
    void resetCheckpointStore() {
        RecordingCheckpointService.reset();
    }

    @Test
    void commitsContiguousHandledOffsetsAcrossOutOfOrderInputs() {
        RecordingCheckpointService.seed(Map.of("test_df:0", 0L));
        RecordingCheckpointService checkpointService = new RecordingCheckpointService();

        pipeline.apply(Create.of(
                        KV.of("test_df:0", 1L),
                        KV.of("test_df:0", 0L),
                        KV.of("test_df:0", 2L)))
                .apply(new CommitHandledOffsetsTransform(checkpointService, "job-transform"));

        pipeline.run().waitUntilFinish();

        assertEquals(3L, RecordingCheckpointService.nextOffset("test_df", 0));
    }
}
