package org.fusadora.dataflow.ptransform;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.fusadora.dataflow.dto.TopicConfig;
import org.fusadora.dataflow.services.OutputService;
import org.fusadora.dataflow.utilities.BQSchema;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Objects;

import static org.fusadora.common.BigquerySchemaConstants.*;

/**
 * org.fusadora.dataflow.ptransform.WriteRawMessageTransform
 * Write Raw Kafka message to BigQuery table
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class WriteRawMessageTransform extends PTransform<@NotNull PCollection<String>, @NotNull PDone> {
    public static final String BQ_TABLE_RAW_MESSAGE = "KAFKA_RAW_MESSAGE";
    public static final String BQ_SCHEMA_RAW_MESSAGE = "schema/raw_message_schema.txt";
    private static final Logger LOG = LoggerFactory.getLogger(WriteRawMessageTransform.class);
    private final OutputService outputService;
    private final TopicConfig topicConfig;

    public WriteRawMessageTransform(OutputService outputService, TopicConfig topicConfig) {
        this.outputService = outputService;
        this.topicConfig = topicConfig;
    }

    @Override
    public @NotNull PDone expand(PCollection<String> input) {
        BQSchema rawMessageSchema = BQSchema.fromFile(BQ_SCHEMA_RAW_MESSAGE);

        PCollection<TableRow> rawMessageRow = input.apply("Get Raw Message TableRow", ParDo.of(new DoFn<String, TableRow>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                if (!Objects.requireNonNull(processContext.element()).contains("errorMessage")) {
                    TableRow tr = new TableRow();
                    tr.put(SCHEMA_RAW_MESSAGE, processContext.element());
                    tr.put(SCHEMA_KAFKA_TOPIC, topicConfig.getTopicName());
                    tr.put(SCHEMA_VERSION, new Date().getTime());
                    processContext.output(tr);
                }
            }
        }));

        outputService.writeToBqFileLoad(rawMessageRow, "Write Raw Message To Bq", topicConfig.getDatasetName()
                        .concat(".").concat(BQ_TABLE_RAW_MESSAGE),
                rawMessageSchema.getTableSchema(), "DAY", BigQueryIO.Write.WriteDisposition.WRITE_APPEND);

        return PDone.in(input.getPipeline());
    }
}
