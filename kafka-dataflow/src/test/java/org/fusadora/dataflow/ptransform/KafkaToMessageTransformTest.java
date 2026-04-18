package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.testing.stubs.TestInputService;
import org.fusadora.dataflow.testing.KafkaTestData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class KafkaToMessageTransformTest {

    @BeforeEach
    void setUp() {
        TestInputService.reset();
    }

    @Test
    void convertsKafkaRecordsIntoKafkaEventEnvelopes() {
        Pipeline pipeline = Pipeline.create();
        TestInputService.setSourceTransform(Create.of(
                        KafkaTestData.kafkaRecord("test_df", 0, 10L, "a"),
                        KafkaTestData.kafkaRecord("test_df", 1, 11L, "b"))
                .withCoder(KafkaTestData.kafkaRecordCoder()));

        PCollection<KafkaEventEnvelope> envelopes = new KafkaToMessageTransform(new TestInputService(), "test_df", "localhost")
                .expand(pipeline.begin());

        assertNotNull(envelopes);

        PAssert.that(envelopes).containsInAnyOrder(
                KafkaTestData.envelope("test_df", 0, 10L, "a"),
                KafkaTestData.envelope("test_df", 1, 11L, "b"));

        pipeline.run().waitUntilFinish();
        assertEquals(0, TestInputService.bootstrapTopics().size());
    }
}
