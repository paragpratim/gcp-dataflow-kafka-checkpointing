package org.fusadora.dataflow.services.impl;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.values.PCollection;
import org.fusadora.dataflow.services.OutputService;
import org.joda.time.Duration;

/**
 * org.fusadora.dataflow.services.impl.OutputServiceImpl
 * Output Service Implementations.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class OutputServiceImpl implements OutputService {
    @Override
    public void writeToBqFileLoad(PCollection<TableRow> input, String transformName, String bqTableName, TableSchema bqTableSchema, String partitionType, BigQueryIO.Write.WriteDisposition writeDisposition) {
        input.apply(transformName,
                BigQueryIO.writeTableRows().to(bqTableName).withSchema(bqTableSchema)
                        .withWriteDisposition(writeDisposition)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                        .withTriggeringFrequency(Duration.standardMinutes(1))
                        .withTimePartitioning(new TimePartitioning().setType(partitionType)));
    }
}
