package org.fusadora.dataflow.ptransform;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.fusadora.dataflow.testing.BeamTestSupport;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class WriteRawMessageTransformTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Test
    public void writesOnlyNonErrorPayloadRowsAndPreservesKafkaMetadata() {
        BeamTestSupport.FakeOutputService outputService = new BeamTestSupport.FakeOutputService(Set.of());
        assertNotNull(outputService);

        pipeline.apply(Create.of(
                        BeamTestSupport.envelope("test_df", 0, 1L, "payload-1"),
                        BeamTestSupport.envelope("test_df", 0, 2L, "{\"errorMessage\":\"skip\"}")))
                .apply(new WriteRawMessageTransform(outputService, BeamTestSupport.topicConfig("test_df", "dataset")));

        PAssert.that(outputService.getCapturedRows()).satisfies(rows -> {
            List<TableRow> list = new ArrayList<>();
            rows.forEach(list::add);
            assertEquals("Expected one BQ row", 1, list.size());
            TableRow row = list.get(0);
            assertEquals("payload-1", row.get("message"));
            assertEquals(1L, BeamTestSupport.parseOffset(row));
            return null;
        });

        pipeline.run().waitUntilFinish();
    }
}

