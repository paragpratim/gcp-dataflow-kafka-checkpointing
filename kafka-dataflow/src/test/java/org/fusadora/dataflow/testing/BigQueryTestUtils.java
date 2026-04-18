package org.fusadora.dataflow.testing;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.fusadora.dataflow.common.KafkaMetadataConstants;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Stateless BigQuery-related fixtures used by tests.
 */
public final class BigQueryTestUtils {

    private BigQueryTestUtils() {
    }

    public static WriteResult newWriteResult(Pipeline pipeline,
                                             PCollection<TableRow> successfulStorageApiInserts,
                                             PCollection<BigQueryStorageApiInsertError> failedStorageApiInserts,
                                             String nameSuffix) {
        try {
            Method inMethod = WriteResult.class.getDeclaredMethod(
                    "in",
                    Pipeline.class,
                    TupleTag.class,
                    PCollection.class,
                    PCollection.class,
                    TupleTag.class,
                    PCollection.class,
                    TupleTag.class,
                    PCollection.class,
                    TupleTag.class,
                    PCollection.class);
            inMethod.setAccessible(true);

            PCollection<TableRow> emptyRows = pipeline.apply("EmptyRows-" + nameSuffix,
                    Create.empty(TableRowJsonCoder.of()));
            PCollection<TableDestination> emptyDestinations = pipeline.apply("EmptyDestinations-" + nameSuffix,
                    Create.empty(SerializableCoder.of(TableDestination.class)));

            return (WriteResult) inMethod.invoke(
                    null,
                    pipeline,
                    new TupleTag<TableRow>() {
                    },
                    emptyRows,
                    emptyRows,
                    new TupleTag<TableDestination>() {
                    },
                    emptyDestinations,
                    new TupleTag<BigQueryStorageApiInsertError>() {
                    },
                    failedStorageApiInserts,
                    new TupleTag<TableRow>() {
                    },
                    successfulStorageApiInserts);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException reflectionException) {
            throw new IllegalStateException("Failed to construct WriteResult for tests", reflectionException);
        }
    }

    public static long parseOffset(TableRow row) {
        Object metadataRecord = row.get(KafkaMetadataConstants.METADATA_RECORD_FIELD);
        if (metadataRecord instanceof TableRow metadataTableRow) {
            return Long.parseLong(metadataTableRow.get(KafkaMetadataConstants.META_KAFKA_OFFSET).toString());
        }
        if (metadataRecord instanceof Map<?, ?> metadataMap) {
            return Long.parseLong(metadataMap.get(KafkaMetadataConstants.META_KAFKA_OFFSET).toString());
        }
        return Long.parseLong(row.get(KafkaMetadataConstants.META_KAFKA_OFFSET).toString());
    }

    public static Collection<Long> offsetsFromRows(Iterable<TableRow> rows) {
        List<Long> offsets = new ArrayList<>();
        for (TableRow row : rows) {
            offsets.add(parseOffset(row));
        }
        return offsets;
    }
}
