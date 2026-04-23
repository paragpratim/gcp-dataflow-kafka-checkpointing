package org.fusadora.dataflow.dofn;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.fusadora.dataflow.common.KafkaMetadataConstants;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.testing.BigQueryTestUtils;
import org.fusadora.dataflow.testing.KafkaTestData;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.io.InputStream;
import java.io.OutputStream;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class MappingDoFnsTest {

    private final Pipeline pipeline = Pipeline.create();

    @Test
    void kafkaRecordToEnvelopeConvertsKafkaMetadataAndPayload() {
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
    void filterValidPayloadDropsEnvelopesContainingInvalidKeyword() {
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
    void filterValidPayloadPassesThroughEnvelopesWithNoInvalidKeyword() {
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
    void filterValidPayloadEmitsDroppedOffsetsToSideOutputWhenConfigured() {
        TupleTag<KafkaEventEnvelope> validTag = new TupleTag<>();
        TupleTag<KV<String, Long>> droppedOffsetTag = new TupleTag<>();

        PCollectionTuple output = pipeline
                .apply(Create.of(
                        KafkaTestData.envelope("test_df", 0, 10L, "ok-payload"),
                        KafkaTestData.envelope("test_df", 0, 11L, "{\"errorMessage\":\"bad\"}")))
                .apply(ParDo.of(new FilterValidPayloadDoFn("errorMessage", droppedOffsetTag))
                        .withOutputTags(validTag, TupleTagList.of(droppedOffsetTag)));

        PAssert.that(output.get(validTag)).containsInAnyOrder(List.of(
                KafkaTestData.envelope("test_df", 0, 10L, "ok-payload")));
        PCollection<KV<String, Long>> droppedOffsets = output.get(droppedOffsetTag)
                .setCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));
        PAssert.that(droppedOffsets).containsInAnyOrder(List.of(KV.of("test_df:0", 11L)));

        pipeline.run().waitUntilFinish();
    }

    @Test
    void kafkaEnvelopeToTableRowMapsAllFieldsWithoutFiltering() {
        PCollection<TableRow> rows = pipeline
                .apply(Create.of(
                        KafkaTestData.envelope("test_df", 0, 5L, "ok-payload"),
                        KafkaTestData.envelope("test_df", 0, 6L, "{\"errorMessage\":\"bad\"}")))
                .apply(ParDo.of(new KafkaEnvelopeToTableRowDoFn(KafkaTestData.topicConfig("test_df", "dataset"))));

        assertNotNull(rows);

        // Pure mapper: both rows are emitted regardless of payload content
        PAssert.that(rows).satisfies(outputRows -> {
            List<TableRow> list = new ArrayList<>();
            outputRows.forEach(list::add);
            if (list.size() != 2) {
                throw new AssertionError("Expected 2 rows from pure mapper but found " + list.size());
            }
            return null;
        });

        pipeline.run().waitUntilFinish();
    }

    @Test
    void extractHandledWriteOffsetsParsesValidRowsAndDropInvalidHandledRowsFiltersSentinel() {
        TableRow valid = new TableRow()
                .set(KafkaMetadataConstants.METADATA_RECORD_FIELD, new TableRow()
                        .set(KafkaMetadataConstants.META_KAFKA_TOPIC, "test_df")
                        .set(KafkaMetadataConstants.META_KAFKA_PARTITION, 1)
                        .set(KafkaMetadataConstants.META_KAFKA_OFFSET, 9L));
        TableRow invalid = new TableRow().set(KafkaMetadataConstants.METADATA_RECORD_FIELD,
                new TableRow().set(KafkaMetadataConstants.META_KAFKA_TOPIC, "test_df"));

        PCollection<KV<String, Long>> offsets = pipeline
                .apply(Create.of(valid, invalid))
                .apply(MapElements.via(new ExtractHandledWriteOffsetsFn()))
                .apply(ParDo.of(new DropInvalidHandledRowsFn()));

        assertNotNull(offsets);
        PAssert.that(offsets).containsInAnyOrder(List.of(KV.of("test_df:1", 9L)));

        pipeline.run().waitUntilFinish();
    }

    @Test
    void extractFailedRowsEmitsOriginalRow() {
        TableRow failedRow = new TableRow().set(KafkaMetadataConstants.METADATA_RECORD_FIELD, new TableRow()
                .set(KafkaMetadataConstants.META_KAFKA_TOPIC, "test_df")
                .set(KafkaMetadataConstants.META_KAFKA_PARTITION, 0)
                .set(KafkaMetadataConstants.META_KAFKA_OFFSET, 11L));

        PCollection<TableRow> rows = pipeline
                .apply(Create.of(new BigQueryStorageApiInsertError(failedRow, "boom"))
                        .withCoder(new BigQueryStorageApiInsertErrorTestCoder()))
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

    private static final class BigQueryStorageApiInsertErrorTestCoder extends AtomicCoder<BigQueryStorageApiInsertError> {

        @Override
        public void encode(BigQueryStorageApiInsertError value, @NonNull OutputStream outStream) throws java.io.IOException {
            assert value != null;
            TableRowJsonCoder.of().encode(value.getRow(), outStream);
            assert value.getErrorMessage() != null;
            StringUtf8Coder.of().encode(value.getErrorMessage(), outStream);
        }

        @Override
        public BigQueryStorageApiInsertError decode(@NonNull InputStream inStream) throws java.io.IOException {
            TableRow row = TableRowJsonCoder.of().decode(inStream);
            String message = StringUtf8Coder.of().decode(inStream);
            return new BigQueryStorageApiInsertError(row, message);
        }
    }
}
