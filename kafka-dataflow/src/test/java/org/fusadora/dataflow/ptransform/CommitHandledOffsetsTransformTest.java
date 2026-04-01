package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.fusadora.dataflow.testing.BeamTestSupport;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CommitHandledOffsetsTransformTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Before
    public void resetCheckpointStore() {
        BeamTestSupport.RecordingCheckpointService.reset();
    }

    @Test
    public void commitsContiguousHandledOffsetsAcrossOutOfOrderInputs() {
        BeamTestSupport.RecordingCheckpointService checkpointService =
                new BeamTestSupport.RecordingCheckpointService(Map.of("test_df:0", 0L));

        pipeline.apply(Create.of(
                        KV.of("test_df:0", 1L),
                        KV.of("test_df:0", 0L),
                        KV.of("test_df:0", 2L)))
                .apply(new CommitHandledOffsetsTransform(checkpointService, "job-transform"));

        pipeline.run().waitUntilFinish();

        assertEquals(3L, BeamTestSupport.RecordingCheckpointService.nextOffset("test_df", 0));
    }
}

