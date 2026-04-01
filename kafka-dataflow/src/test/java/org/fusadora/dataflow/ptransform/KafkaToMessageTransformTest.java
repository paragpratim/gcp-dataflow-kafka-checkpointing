package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.testing.BeamTestSupport;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KafkaToMessageTransformTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Test
    public void convertKafkaRecordsIntoKafkaEventEnvelopes() {
        BeamTestSupport.FakeInputService inputService = new BeamTestSupport.FakeInputService(
                Create.of(
                        BeamTestSupport.kafkaRecord("test_df", 0, 10L, "a"),
                        BeamTestSupport.kafkaRecord("test_df", 1, 11L, "b"))
                        .withCoder(BeamTestSupport.kafkaRecordCoder()));

        PCollection<KafkaEventEnvelope> envelopes = new KafkaToMessageTransform(inputService, "test_df")
                .expand(pipeline.begin());

        assertNotNull(envelopes);

        PAssert.that(envelopes).containsInAnyOrder(
                BeamTestSupport.envelope("test_df", 0, 10L, "a"),
                BeamTestSupport.envelope("test_df", 1, 11L, "b"));

        pipeline.run().waitUntilFinish();
        assertEquals(0, inputService.getBootstrapTopics().size());
    }
}

