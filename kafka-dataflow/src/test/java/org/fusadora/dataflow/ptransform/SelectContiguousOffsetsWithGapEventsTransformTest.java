package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.testing.KafkaTestData;
import org.fusadora.dataflow.testing.stubs.RecordingCheckpointService;
import org.joda.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class SelectContiguousOffsetsWithGapEventsTransformTest {

    private final Pipeline pipeline = Pipeline.create();

    @BeforeEach
    void resetCheckpointStore() {
        RecordingCheckpointService.reset();
    }

    @Test
    void emitsGapTimeoutSideOutputWhenMissingOffsetDoesNotArrive() {
        RecordingCheckpointService.seed(Map.of("test_df:0", 0L));
        RecordingCheckpointService checkpointService = new RecordingCheckpointService();

        TestStream<KafkaEventEnvelope> stream = TestStream.create(SerializableCoder.of(KafkaEventEnvelope.class))
                .addElements(
                        KafkaTestData.envelope("test_df", 0, 0L, "a"),
                        KafkaTestData.envelope("test_df", 0, 2L, "c"))
                .advanceProcessingTime(Duration.standardMinutes(6))
                .advanceWatermarkToInfinity();

        PCollectionTuple outputs = pipeline.apply(stream)
                .apply(new SelectContiguousOffsetsWithGapEventsTransform(
                        checkpointService, Duration.standardMinutes(5), true));

        assertNotNull(outputs);

        PAssert.that(outputs.get(SelectContiguousOffsetsWithGapEventsTransform.CONTIGUOUS_MAIN_TAG))
                .containsInAnyOrder(
                        KafkaTestData.envelope("test_df", 0, 0L, "a"),
                        KafkaTestData.envelope("test_df", 0, 2L, "c"));

        PAssert.that(outputs.get(SelectContiguousOffsetsWithGapEventsTransform.GAP_TIMEOUT_OFFSET_TAG))
                .containsInAnyOrder(List.of(KV.of("test_df:0", 1L)));

        pipeline.run().waitUntilFinish();
    }

    @Test
    void doesNotEmitGapTimeoutSideOutputWhenGapResolvesBeforeTimeout() {
        RecordingCheckpointService.seed(Map.of("test_df:0", 0L));
        RecordingCheckpointService checkpointService = new RecordingCheckpointService();

        TestStream<KafkaEventEnvelope> stream = TestStream.create(SerializableCoder.of(KafkaEventEnvelope.class))
                .addElements(
                        KafkaTestData.envelope("test_df", 0, 0L, "a"),
                        KafkaTestData.envelope("test_df", 0, 2L, "c"))
                .advanceProcessingTime(Duration.standardSeconds(30))
                .addElements(KafkaTestData.envelope("test_df", 0, 1L, "b"))
                .advanceWatermarkToInfinity();

        PCollectionTuple outputs = pipeline.apply(stream)
                .apply(new SelectContiguousOffsetsWithGapEventsTransform(
                        checkpointService, Duration.standardMinutes(5), true));

        assertNotNull(outputs);

        PAssert.that(outputs.get(SelectContiguousOffsetsWithGapEventsTransform.CONTIGUOUS_MAIN_TAG))
                .containsInAnyOrder(
                        KafkaTestData.envelope("test_df", 0, 0L, "a"),
                        KafkaTestData.envelope("test_df", 0, 1L, "b"),
                        KafkaTestData.envelope("test_df", 0, 2L, "c"));

        PAssert.that(outputs.get(SelectContiguousOffsetsWithGapEventsTransform.GAP_TIMEOUT_OFFSET_TAG)).empty();

        pipeline.run().waitUntilFinish();
    }
}
