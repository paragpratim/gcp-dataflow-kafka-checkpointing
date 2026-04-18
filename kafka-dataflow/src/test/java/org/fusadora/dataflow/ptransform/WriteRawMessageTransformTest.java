package org.fusadora.dataflow.ptransform;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.fusadora.dataflow.testing.KafkaTestData;
import org.fusadora.dataflow.testing.stubs.TestOutputService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.fusadora.dataflow.common.BigquerySchemaConstants.SCHEMA_RAW_MESSAGE;
import static org.fusadora.dataflow.common.KafkaMetadataConstants.META_KAFKA_OFFSET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class WriteRawMessageTransformTest {

    @BeforeEach
    void setUp() {
        TestOutputService.reset();
    }

    @Test
    void writesOnlyNonErrorPayloadRowsWithoutKafkaMetadata() {
        Pipeline pipeline = Pipeline.create();
        TestOutputService.setFailingOffsets(Set.of());
        TestOutputService outputService = new TestOutputService();
        assertNotNull(outputService);

        pipeline.apply(Create.of(
                        KafkaTestData.envelope("test_df", 0, 1L, "payload-1"),
                        KafkaTestData.envelope("test_df", 0, 2L, "{\"errorMessage\":\"skip\"}")))
                .apply(new WriteRawMessageTransform(outputService, KafkaTestData.topicConfig("test_df", "dataset")));

        PAssert.that(TestOutputService.capturedRows()).satisfies(rows -> {
            List<TableRow> list = new ArrayList<>();
            rows.forEach(list::add);
            assertEquals(1, list.size(), "Expected one BQ row (error-payload row filtered out)");
            TableRow row = list.get(0);
            assertEquals("payload-1", row.get(SCHEMA_RAW_MESSAGE));
            // Kafka metadata must NOT be present in BQ rows (offset tracking is done pre-write via envelopes).
            assertNull(row.get(META_KAFKA_OFFSET), "BQ rows must not carry Kafka offset metadata");
            return null;
        });

        pipeline.run().waitUntilFinish();
    }
}
