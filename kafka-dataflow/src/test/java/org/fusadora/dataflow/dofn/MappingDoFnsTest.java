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
import org.fusadora.dataflow.testing.BeamTestSupport;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;

public class MappingDoFnsTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Test
    public void kafkaRecordToEnvelopeConvertsKafkaMetadataAndPayload() {
        PCollection<KafkaEventEnvelope> output = pipeline
                .apply(Create.of(BeamTestSupport.kafkaRecord("test_df", 2, 17L, "payload"))
                        .withCoder(BeamTestSupport.kafkaRecordCoder()))
                .apply(ParDo.of(new KafkaRecordToEnvelopeDoFn("test_df")));

        assertNotNull(output);

        PAssert.that(output).containsInAnyOrder(List.of(
                BeamTestSupport.envelope("test_df", 2, 17L, "payload")));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void kafkaEnvelopeToTableRowAddsMetadataAndFiltersErrorPayloads() {
        PCollection<TableRow> rows = pipeline
                .apply(Create.of(
                        BeamTestSupport.envelope("test_df", 0, 1L, "ok-payload"),
                        BeamTestSupport.envelope("test_df", 0, 2L, "{\"errorMessage\":\"bad\"}")))
                .apply(ParDo.of(new KafkaEnvelopeToTableRowDoFn(BeamTestSupport.topicConfig("test_df", "dataset"))));

        assertNotNull(rows);

        PAssert.that(rows).satisfies(outputRows -> {
            List<TableRow> list = new ArrayList<>();
            outputRows.forEach(list::add);
            if (list.size() != 1) {
                throw new AssertionError("Expected 1 row but found " + list.size());
            }
            TableRow row = list.get(0);
            if (!"ok-payload".equals(row.get("message"))) {
                throw new AssertionError("Unexpected payload row: " + row);
            }
            if (BeamTestSupport.parseOffset(row) != 1L) {
                throw new AssertionError("Expected offset metadata 1 but found " + row.get("_meta_kafka_offset"));
            }
            return null;
        });

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void extractHandledWriteOffsetsParsesValidRowsAndDropInvalidHandledRowsFiltersSentinel() {
        TableRow valid = new TableRow()
                .set("_meta_kafka_topic", "test_df")
                .set("_meta_kafka_partition", 1)
                .set("_meta_kafka_offset", 9L);
        TableRow invalid = new TableRow().set("_meta_kafka_topic", "test_df");

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
        TableRow failedRow = new TableRow()
                .set("_meta_kafka_topic", "test_df")
                .set("_meta_kafka_partition", 0)
                .set("_meta_kafka_offset", 11L);

        PCollection<TableRow> rows = pipeline
                .apply(Create.of(new BigQueryStorageApiInsertError(failedRow, "boom")))
                .apply(ParDo.of(new ExtractFailedRowsDoFn("test_df")));

        assertNotNull(rows);

        PAssert.that(rows).satisfies(outputRows -> {
            List<TableRow> list = new ArrayList<>();
            outputRows.forEach(list::add);
            if (list.size() != 1 || BeamTestSupport.parseOffset(list.get(0)) != 11L) {
                throw new AssertionError("Unexpected failed rows output: " + list);
            }
            return null;
        });

        pipeline.run().waitUntilFinish();
    }
}

