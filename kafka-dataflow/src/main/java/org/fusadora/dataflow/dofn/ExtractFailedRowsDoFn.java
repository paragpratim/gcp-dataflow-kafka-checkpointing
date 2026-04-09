package org.fusadora.dataflow.dofn;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs BigQuery storage API insert failures and emits the failed row for handled-offset accounting.
 */
@SuppressWarnings("unused") // Instantiated from pipeline transform wiring
public class ExtractFailedRowsDoFn extends DoFn<BigQueryStorageApiInsertError, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(ExtractFailedRowsDoFn.class);

    private final String topicName;

    public ExtractFailedRowsDoFn(String topicName) {
        this.topicName = topicName;
    }

    @SuppressWarnings("unused") // Invoked by Beam runtime via @ProcessElement
    @ProcessElement
    public void processElement(ProcessContext context) {
        BigQueryStorageApiInsertError error = context.element();
        TableRow failedRow = error.getRow();
        LOG.error("BQ write failed for topic {} row={} error={}", topicName, failedRow, error.getErrorMessage());
        context.output(failedRow);
    }
}

