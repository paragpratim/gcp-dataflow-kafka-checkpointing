package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.KV;
import org.fusadora.dataflow.testing.stubs.RecordingCheckpointService;
import org.joda.time.Duration;
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

        pipeline.apply(TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
                        .addElements(
                                KV.of("test_df:0", 1L),
                                KV.of("test_df:0", 0L),
                                KV.of("test_df:0", 2L))
                        .advanceProcessingTime(Duration.standardSeconds(2))
                        .advanceWatermarkToInfinity())
                .apply(new CommitHandledOffsetsTransform(checkpointService, "job-transform", 1L));

        pipeline.run().waitUntilFinish();

        assertEquals(3L, RecordingCheckpointService.nextOffset("test_df", 0));
    }
}
