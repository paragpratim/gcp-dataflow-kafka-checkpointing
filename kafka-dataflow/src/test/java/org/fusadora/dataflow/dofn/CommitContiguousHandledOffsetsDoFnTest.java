package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.fusadora.dataflow.testing.stubs.RecordingCheckpointService;
import org.joda.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CommitContiguousHandledOffsetsDoFnTest {

    private final Pipeline pipeline = Pipeline.create();

    @BeforeEach
    void setUp() {
        RecordingCheckpointService.reset();
    }

    @Test
    void commitsBufferedOffsetsOnceContiguousFrontierArrives() {
        RecordingCheckpointService.seed(Map.of("test_df:0", 0L));
        RecordingCheckpointService checkpointService = new RecordingCheckpointService();

        TestStream<KV<String, Long>> stream = TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
                .addElements(KV.of("test_df:0", 1L))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(KV.of("test_df:0", 0L), KV.of("test_df:0", 2L))
                .advanceProcessingTime(Duration.standardSeconds(2))
                .advanceWatermarkToInfinity();

        pipeline.apply(stream)
                .apply(ParDo.of(new CommitContiguousHandledOffsetsDoFn(checkpointService, "job-1", 1L)));

        pipeline.run().waitUntilFinish();

        assertEquals(3L, RecordingCheckpointService.nextOffset("test_df", 0));
        assertEquals(2L,
                RecordingCheckpointService.updates().get(RecordingCheckpointService.updates().size() - 1).lastAckedOffset());
    }

    @Test
    void ignoresHandledOffsetsOlderThanCurrentCheckpoint() {
        RecordingCheckpointService.seed(Map.of("test_df:0", 2L));
        RecordingCheckpointService checkpointService = new RecordingCheckpointService();

        pipeline.apply(TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
                        .addElements(KV.of("test_df:0", 0L), KV.of("test_df:0", 1L), KV.of("test_df:0", 2L))
                        .advanceProcessingTime(Duration.standardSeconds(2))
                        .advanceWatermarkToInfinity())
                .apply(ParDo.of(new CommitContiguousHandledOffsetsDoFn(checkpointService, "job-2", 1L)));

        pipeline.run().waitUntilFinish();

        assertEquals(3L, RecordingCheckpointService.nextOffset("test_df", 0));
        assertEquals(1, RecordingCheckpointService.updates().size());
        assertEquals(2L, RecordingCheckpointService.updates().get(0).lastAckedOffset());
    }
}
