package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.fusadora.dataflow.testing.BeamTestSupport;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CommitContiguousHandledOffsetsDoFnTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Before
    public void setUp() {
        BeamTestSupport.RecordingCheckpointService.reset();
    }

    @Test
    public void commitsBufferedOffsetsOnceContiguousFrontierArrives() {
        BeamTestSupport.RecordingCheckpointService checkpointService =
                new BeamTestSupport.RecordingCheckpointService(Map.of("test_df:0", 0L));

        TestStream<KV<String, Long>> stream = TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
                .addElements(KV.of("test_df:0", 1L))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(KV.of("test_df:0", 0L), KV.of("test_df:0", 2L))
                .advanceWatermarkToInfinity();

        pipeline.apply(stream)
                .apply(ParDo.of(new CommitContiguousHandledOffsetsDoFn(checkpointService, "job-1")));

        pipeline.run().waitUntilFinish();

        assertEquals(3L, BeamTestSupport.RecordingCheckpointService.nextOffset("test_df", 0));
        assertEquals(2L, BeamTestSupport.RecordingCheckpointService.updates().get(1).lastAckedOffset());
    }

    @Test
    public void ignoresHandledOffsetsOlderThanCurrentCheckpoint() {
        BeamTestSupport.RecordingCheckpointService checkpointService =
                new BeamTestSupport.RecordingCheckpointService(Map.of("test_df:0", 2L));

        pipeline.apply(TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
                        .addElements(KV.of("test_df:0", 0L), KV.of("test_df:0", 1L), KV.of("test_df:0", 2L))
                        .advanceWatermarkToInfinity())
                .apply(ParDo.of(new CommitContiguousHandledOffsetsDoFn(checkpointService, "job-2")));

        pipeline.run().waitUntilFinish();

        assertEquals(3L, BeamTestSupport.RecordingCheckpointService.nextOffset("test_df", 0));
        assertEquals(1, BeamTestSupport.RecordingCheckpointService.updates().size());
        assertEquals(2L, BeamTestSupport.RecordingCheckpointService.updates().get(0).lastAckedOffset());
    }
}

