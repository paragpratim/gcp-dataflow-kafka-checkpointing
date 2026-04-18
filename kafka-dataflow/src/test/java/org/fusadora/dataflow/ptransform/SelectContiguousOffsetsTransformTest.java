package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.services.CheckpointService;
import org.joda.time.Duration;
import org.junit.jupiter.api.Test;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class SelectContiguousOffsetsTransformTest {

    private final Pipeline pipeline = Pipeline.create();

    @Test
    void emitsOnlyContiguousOffsetsFromCheckpoint() {

        PCollection<KafkaEventEnvelope> input = pipeline.apply(Create.of(
                envelope(4, "a"),
                envelope(5, "b"),
                envelope(7, "c")
        ));

        PCollection<KafkaEventEnvelope> output = input.apply(
                new SelectContiguousOffsetsTransform(new FixedCheckpointService(5L)));

        assertNotNull(output);

        PAssert.that(output).containsInAnyOrder(envelope(5, "b"));

        pipeline.run().waitUntilFinish();
    }

    @Test
    void skipsGapAfterTimeoutAndContinuesForward() {

        TestStream<KafkaEventEnvelope> stream = TestStream.create(SerializableCoder.of(KafkaEventEnvelope.class))
                .addElements(envelope(0, "a"), envelope(2, "c"))
                .advanceProcessingTime(Duration.standardMinutes(6))
                .advanceWatermarkToInfinity();

        PCollection<KafkaEventEnvelope> output = pipeline
                .apply(stream)
                .apply(new SelectContiguousOffsetsTransform(new FixedCheckpointService(0L)));

        assertNotNull(output);

        PAssert.that(output).containsInAnyOrder(
                envelope(0, "a"),
                envelope(2, "c")
        );

        pipeline.run().waitUntilFinish();
    }

    @Test
    void resolvesGapBeforeTimeout() {
        TestStream<KafkaEventEnvelope> stream = TestStream.create(SerializableCoder.of(KafkaEventEnvelope.class))
                .addElements(envelope(0, "a"), envelope(2, "c"))
                .advanceProcessingTime(Duration.standardSeconds(30))
                .addElements(envelope(1, "b"))
                .advanceWatermarkToInfinity();

        PCollection<KafkaEventEnvelope> output = pipeline
                .apply(stream)
                .apply(new SelectContiguousOffsetsTransform(
                        new FixedCheckpointService(0L), Duration.standardMinutes(5), true));

        assertNotNull(output);

        PAssert.that(output).containsInAnyOrder(
                envelope(0, "a"),
                envelope(1, "b"),
                envelope(2, "c")
        );

        pipeline.run().waitUntilFinish();
    }

    private static KafkaEventEnvelope envelope(long offset, String payload) {
        return new KafkaEventEnvelope("test", 0, offset, payload);
    }

    private record FixedCheckpointService(long nextOffset) implements CheckpointService, Serializable {

            @Serial
            private static final long serialVersionUID = 1L;

        @Override
            public long getNextOffsetToRead(String topic, int partition) {
                return nextOffset;
            }

            @Override
            public Map<Integer, Long> getTopicPartitionOffsets(String topic) {
                return Map.of();
            }

            @Override
            public void updateOffsetCheckpoint(String topic, int partition, long lastAckedOffset, String jobId) {
                // no-op for unit tests
            }
        }
}

