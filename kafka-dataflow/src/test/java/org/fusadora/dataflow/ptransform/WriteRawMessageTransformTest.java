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

import static org.fusadora.dataflow.common.BigquerySchemaConstants.SCHEMA_METADATA_RECORD;
import static org.fusadora.dataflow.common.BigquerySchemaConstants.SCHEMA_RAW_MESSAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class WriteRawMessageTransformTest {

    @BeforeEach
    void setUp() {
        TestOutputService.reset();
    }

    @Test
    void writesOnlyNonErrorPayloadRowsWithMetadataRecord() {
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
            // __metadata RECORD must be present in BQ rows for post-write offset extraction.
            assertNotNull(row.get(SCHEMA_METADATA_RECORD), "BQ rows must carry the __metadata record");
            return null;
        });

        pipeline.run().waitUntilFinish();
    }
}
