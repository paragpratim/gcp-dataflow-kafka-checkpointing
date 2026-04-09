package org.fusadora.dataflow.ptransform;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.fusadora.dataflow.common.KafkaMetadataConstants;
import org.fusadora.dataflow.dofn.FilterValidPayloadDoFn;
import org.fusadora.dataflow.dofn.KafkaEnvelopeToTableRowDoFn;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.dto.TopicConfig;
import org.fusadora.dataflow.services.OutputService;
import org.fusadora.dataflow.utilities.BQSchema;
import org.jetbrains.annotations.NotNull;

/**
 * org.fusadora.dataflow.ptransform.WriteRawMessageTransform
 * Write Raw Kafka message to BigQuery table
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class WriteRawMessageTransform extends PTransform<@NotNull PCollection<KafkaEventEnvelope>, @NotNull WriteResult> {
    public static final String BQ_TABLE_RAW_MESSAGE = "KAFKA_RAW_MESSAGE";
    public static final String BQ_SCHEMA_RAW_MESSAGE = "schema/raw_message_schema.txt";
    public static final String META_KAFKA_TOPIC = KafkaMetadataConstants.META_KAFKA_TOPIC;
    public static final String META_KAFKA_PARTITION = KafkaMetadataConstants.META_KAFKA_PARTITION;
    public static final String META_KAFKA_OFFSET = KafkaMetadataConstants.META_KAFKA_OFFSET;
    /** Payloads containing this keyword are treated as invalid and dropped before BQ write. */
    public static final String INVALID_PAYLOAD_KEYWORD = "errorMessage";

    private final OutputService outputService;
    private final TopicConfig topicConfig;

    public WriteRawMessageTransform(OutputService outputService, TopicConfig topicConfig) {
        this.outputService = outputService;
        this.topicConfig = topicConfig;
    }

    @Override
    public @NotNull WriteResult expand(PCollection<KafkaEventEnvelope> input) {
        BQSchema rawMessageSchema = BQSchema.fromFile(BQ_SCHEMA_RAW_MESSAGE);

        PCollection<TableRow> rawMessageRow = input
                .apply("Filter Invalid Payloads [" + topicConfig.getTopicName() + "]",
                        ParDo.of(new FilterValidPayloadDoFn(INVALID_PAYLOAD_KEYWORD)))
                .apply("Get Raw Message TableRow",
                        ParDo.of(new KafkaEnvelopeToTableRowDoFn(topicConfig)));

        return outputService.writeToBqFileLoad(rawMessageRow, "Write Raw Message To Bq", topicConfig.getDatasetName()
                        .concat(".").concat(BQ_TABLE_RAW_MESSAGE),
                rawMessageSchema.getTableSchema(), "DAY");
    }
}
