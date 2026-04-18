package org.fusadora.dataflow.dofn;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.testing.BigQueryTestUtils;
import org.fusadora.dataflow.testing.KafkaTestData;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.fusadora.dataflow.common.BigquerySchemaConstants.SCHEMA_METADATA_RECORD;
import static org.fusadora.dataflow.common.KafkaMetadataConstants.*;
import static org.junit.Assert.assertNotNull;

public class MappingDoFnsTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Test
    public void kafkaRecordToEnvelopeConvertsKafkaMetadataAndPayload() {
        PCollection<KafkaEventEnvelope> output = pipeline
                .apply(Create.of(KafkaTestData.kafkaRecord("test_df", 2, 17L, "payload"))
                        .withCoder(KafkaTestData.kafkaRecordCoder()))
                .apply(ParDo.of(new KafkaRecordToEnvelopeDoFn("test_df")));

        assertNotNull(output);

        PAssert.that(output).containsInAnyOrder(List.of(
                KafkaTestData.envelope("test_df", 2, 17L, "payload")));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void filterValidPayloadDropsEnvelopesContainingInvalidKeyword() {
        PCollection<KafkaEventEnvelope> output = pipeline
                .apply(Create.of(
                        KafkaTestData.envelope("test_df", 0, 1L, "ok-payload"),
                        KafkaTestData.envelope("test_df", 0, 2L, "{\"errorMessage\":\"bad\"}")))
                .apply(ParDo.of(new FilterValidPayloadDoFn("errorMessage")));

        assertNotNull(output);
        PAssert.that(output).containsInAnyOrder(List.of(
                KafkaTestData.envelope("test_df", 0, 1L, "ok-payload")));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void filterValidPayloadPassesThroughEnvelopesWithNoInvalidKeyword() {
        PCollection<KafkaEventEnvelope> output = pipeline
                .apply(Create.of(
                        KafkaTestData.envelope("test_df", 0, 3L, "hello"),
                        KafkaTestData.envelope("test_df", 0, 4L, "world")))
                .apply(ParDo.of(new FilterValidPayloadDoFn("errorMessage")));

        assertNotNull(output);
        PAssert.that(output).containsInAnyOrder(List.of(
                KafkaTestData.envelope("test_df", 0, 3L, "hello"),
                KafkaTestData.envelope("test_df", 0, 4L, "world")));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void kafkaEnvelopeToTableRowMapsAllFieldsWithoutFiltering() {
        PCollection<TableRow> rows = pipeline
                .apply(Create.of(
                        KafkaTestData.envelope("test_df", 0, 5L, "ok-payload"),
                        KafkaTestData.envelope("test_df", 0, 6L, "{\"errorMessage\":\"bad\"}")))
                .apply(ParDo.of(new KafkaEnvelopeToTableRowDoFn(KafkaTestData.topicConfig("test_df", "dataset"))));

        assertNotNull(rows);

        // Pure mapper: both rows are emitted regardless of payload content, each with __metadata record.
        PAssert.that(rows).satisfies(outputRows -> {
            List<TableRow> list = new ArrayList<>();
            outputRows.forEach(list::add);
            if (list.size() != 2) {
                throw new AssertionError("Expected 2 rows from pure mapper but found " + list.size());
            }
            for (TableRow row : list) {
                Object metadataObj = row.get(SCHEMA_METADATA_RECORD);
                if (!(metadataObj instanceof Map<?, ?> metadataMap)) {
                    throw new AssertionError("Expected __metadata RECORD in row but was: " + metadataObj);
                }
                if (metadataMap.get(META_KAFKA_TOPIC) == null
                        || metadataMap.get(META_KAFKA_PARTITION) == null
                        || metadataMap.get(META_KAFKA_OFFSET) == null) {
                    throw new AssertionError("__metadata record is missing topic/partition/offset: " + metadataMap);
                }
            }
            return null;
        });

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void extractHandledWriteOffsetsParsesValidRowsAndDropInvalidHandledRowsFiltersSentinel() {
        TableRow validMetadata = new TableRow()
                .set("topic", "test_df")
                .set("partition", 1)
                .set("offset", 9L);
        TableRow valid = new TableRow().set("__metadata", validMetadata);

        TableRow invalidMetadata = new TableRow().set("topic", "test_df"); // missing partition and offset
        TableRow invalid = new TableRow().set("__metadata", invalidMetadata);

        PCollection<KV<String, Long>> offsets = pipeline
                .apply(Create.of(valid, invalid))
                .apply(MapElements.via(new ExtractHandledWriteOffsetsFn()))
                .apply(ParDo.of(new DropInvalidHandledRowsFn()));

        assertNotNull(offsets);
        PAssert.that(offsets).containsInAnyOrder(List.of(KV.of("test_df:1", 9L)));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void extractFailedRowsEmitsOriginalRow() {
        TableRow failedMetadata = new TableRow()
                .set("topic", "test_df")
                .set("partition", 0)
                .set("offset", 11L);
        TableRow failedRow = new TableRow().set("__metadata", failedMetadata);

        PCollection<TableRow> rows = pipeline
                .apply(Create.of(new BigQueryStorageApiInsertError(failedRow, "boom")))
                .apply(ParDo.of(new ExtractFailedRowsDoFn("test_df")));

        assertNotNull(rows);

        PAssert.that(rows).satisfies(outputRows -> {
            List<TableRow> list = new ArrayList<>();
            outputRows.forEach(list::add);
            if (list.size() != 1 || BigQueryTestUtils.parseOffset(list.get(0)) != 11L) {
                throw new AssertionError("Unexpected failed rows output: " + list);
            }
            return null;
        });

        pipeline.run().waitUntilFinish();
    }
}
