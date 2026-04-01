package org.fusadora.dataflow.dofn;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.dto.TopicConfig;
import org.fusadora.dataflow.ptransform.WriteRawMessageTransform;

import java.util.Date;
import java.util.Objects;

import static org.fusadora.common.BigquerySchemaConstants.SCHEMA_KAFKA_TOPIC;
import static org.fusadora.common.BigquerySchemaConstants.SCHEMA_RAW_MESSAGE;
import static org.fusadora.common.BigquerySchemaConstants.SCHEMA_VERSION;

/**
 * Converts Kafka envelopes into BigQuery TableRow with checkpoint metadata.
 */
@SuppressWarnings("unused") // Instantiated from pipeline transform wiring
public class KafkaEnvelopeToTableRowDoFn extends DoFn<KafkaEventEnvelope, TableRow> {

    private final TopicConfig topicConfig;

    public KafkaEnvelopeToTableRowDoFn(TopicConfig topicConfig) {
        this.topicConfig = topicConfig;
    }

    @SuppressWarnings("unused") // Invoked by Beam runtime via @ProcessElement
    @ProcessElement
    public void processElement(ProcessContext processContext) {
        KafkaEventEnvelope envelope = Objects.requireNonNull(processContext.element());
        if (!envelope.getPayload().contains("errorMessage")) {
            TableRow tr = new TableRow();
            tr.put(SCHEMA_RAW_MESSAGE, envelope.getPayload());
            tr.put(SCHEMA_KAFKA_TOPIC, topicConfig.getTopicName());
            tr.put(SCHEMA_VERSION, new Date().getTime());
            // Metadata is carried only for post-write checkpointing and ignored by BQ schema.
            tr.put(WriteRawMessageTransform.META_KAFKA_TOPIC, envelope.getTopic());
            tr.put(WriteRawMessageTransform.META_KAFKA_PARTITION, envelope.getPartition());
            tr.put(WriteRawMessageTransform.META_KAFKA_OFFSET, envelope.getOffset());
            processContext.output(tr);
        }
    }
}

