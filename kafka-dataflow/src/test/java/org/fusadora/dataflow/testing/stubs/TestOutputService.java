package org.fusadora.dataflow.testing.stubs;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.fusadora.dataflow.services.OutputService;
import org.fusadora.dataflow.testing.BigQueryTestUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.util.Set;

/**
 * Shared configurable BigQuery output stub for DI-driven tests.
 */
public class TestOutputService implements OutputService, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static Set<Long> failingOffsets = Set.of();
    private static PCollection<TableRow> capturedRows;

    public static void reset() {
        failingOffsets = Set.of();
        capturedRows = null;
    }

    public static void setFailingOffsets(Set<Long> offsets) {
        failingOffsets = offsets;
    }

    public static PCollection<TableRow> capturedRows() {
        return capturedRows;
    }

    @Override
    public WriteResult writeToBqFileLoad(PCollection<TableRow> input, String transformName, String bqTableName,
                                         TableSchema bqTableSchema, String partitionType) {
        capturedRows = input;

        PCollection<TableRow> successRows = input.apply(transformName + "-success-filter",
                Filter.by(row -> row != null && !failingOffsets.contains(BigQueryTestUtils.parseOffset(row))));

        PCollection<TableRow> failedRowValues = input.apply(transformName + "-failure-filter",
                Filter.by(row -> row != null && failingOffsets.contains(BigQueryTestUtils.parseOffset(row))));

        PCollection<BigQueryStorageApiInsertError> failedRows = failedRowValues.apply(
                transformName + "-to-errors", ParDo.of(new DoFn<TableRow, BigQueryStorageApiInsertError>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                        context.output(new BigQueryStorageApiInsertError(context.element(), "forced test failure"));
                    }
                })).setCoder(new BigQueryStorageApiInsertErrorTestCoder());

        return BigQueryTestUtils.newWriteResult(input.getPipeline(), successRows, failedRows, transformName);
    }

    private static final class BigQueryStorageApiInsertErrorTestCoder extends AtomicCoder<BigQueryStorageApiInsertError> {

        @Override
        public void encode(BigQueryStorageApiInsertError value, OutputStream outStream) throws java.io.IOException {
            TableRowJsonCoder.of().encode(value.getRow(), outStream);
            assert value.getErrorMessage() != null;
            StringUtf8Coder.of().encode(value.getErrorMessage(), outStream);
        }

        @Override
        public BigQueryStorageApiInsertError decode(InputStream inStream) throws java.io.IOException {
            TableRow row = TableRowJsonCoder.of().decode(inStream);
            String message = StringUtf8Coder.of().decode(inStream);
            return new BigQueryStorageApiInsertError(row, message);
        }
    }
}
