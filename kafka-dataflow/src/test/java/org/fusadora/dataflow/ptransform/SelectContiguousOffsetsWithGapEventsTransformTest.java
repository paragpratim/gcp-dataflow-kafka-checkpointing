package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.testing.BeamTestSupport;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class SelectContiguousOffsetsWithGapEventsTransformTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Before
    public void resetCheckpointStore() {
        BeamTestSupport.RecordingCheckpointService.reset();
    }

    @Test
    public void emitsGapTimeoutSideOutputWhenMissingOffsetDoesNotArrive() {
        BeamTestSupport.RecordingCheckpointService checkpointService =
                new BeamTestSupport.RecordingCheckpointService(Map.of("test_df:0", 0L));

        TestStream<KafkaEventEnvelope> stream = TestStream.create(SerializableCoder.of(KafkaEventEnvelope.class))
                .addElements(
                        BeamTestSupport.envelope("test_df", 0, 0L, "a"),
                        BeamTestSupport.envelope("test_df", 0, 2L, "c"))
                .advanceProcessingTime(Duration.standardMinutes(6))
                .advanceWatermarkToInfinity();

        PCollectionTuple outputs = pipeline.apply(stream)
                .apply(new SelectContiguousOffsetsWithGapEventsTransform(
                        checkpointService, Duration.standardMinutes(5), true));

        assertNotNull(outputs);

        PAssert.that(outputs.get(SelectContiguousOffsetsWithGapEventsTransform.CONTIGUOUS_MAIN_TAG))
                .containsInAnyOrder(
                        BeamTestSupport.envelope("test_df", 0, 0L, "a"),
                        BeamTestSupport.envelope("test_df", 0, 2L, "c"));

        PAssert.that(outputs.get(SelectContiguousOffsetsWithGapEventsTransform.GAP_TIMEOUT_OFFSET_TAG))
                .containsInAnyOrder(List.of(KV.of("test_df:0", 1L)));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void doesNotEmitGapTimeoutSideOutputWhenGapResolvesBeforeTimeout() {
        BeamTestSupport.RecordingCheckpointService checkpointService =
                new BeamTestSupport.RecordingCheckpointService(Map.of("test_df:0", 0L));

        TestStream<KafkaEventEnvelope> stream = TestStream.create(SerializableCoder.of(KafkaEventEnvelope.class))
                .addElements(
                        BeamTestSupport.envelope("test_df", 0, 0L, "a"),
                        BeamTestSupport.envelope("test_df", 0, 2L, "c"))
                .advanceProcessingTime(Duration.standardSeconds(30))
                .addElements(BeamTestSupport.envelope("test_df", 0, 1L, "b"))
                .advanceWatermarkToInfinity();

        PCollectionTuple outputs = pipeline.apply(stream)
                .apply(new SelectContiguousOffsetsWithGapEventsTransform(
                        checkpointService, Duration.standardMinutes(5), true));

        assertNotNull(outputs);

        PAssert.that(outputs.get(SelectContiguousOffsetsWithGapEventsTransform.CONTIGUOUS_MAIN_TAG))
                .containsInAnyOrder(
                        BeamTestSupport.envelope("test_df", 0, 0L, "a"),
                        BeamTestSupport.envelope("test_df", 0, 1L, "b"),
                        BeamTestSupport.envelope("test_df", 0, 2L, "c"));

        PAssert.that(outputs.get(SelectContiguousOffsetsWithGapEventsTransform.GAP_TIMEOUT_OFFSET_TAG)).empty();

        pipeline.run().waitUntilFinish();
    }
}
