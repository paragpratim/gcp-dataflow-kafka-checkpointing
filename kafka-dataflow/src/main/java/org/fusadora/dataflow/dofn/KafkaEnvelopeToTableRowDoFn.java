package org.fusadora.dataflow.dofn;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.dto.TopicConfig;

import java.util.Objects;

import static org.fusadora.dataflow.common.BigquerySchemaConstants.*;

/**
 * org.fusadora.dataflow.dofn.KafkaEnvelopeToTableRowDoFn
 * This is a Beam DoFn that transforms a KafkaEventEnvelope into a TableRow suitable for BigQuery insertion.
 * It maps the raw message payload and Kafka topic name to the BigQuery schema fields.
 * The version field is set to the current timestamp to facilitate versioning in BigQuery.
 * This DoFn is designed to be used in a Beam pipeline that reads from Kafka and writes to BigQuery.
 *
 * @author Parag Ghosh
 * @since 10/04/2026
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
        TableRow tr = new TableRow();
        tr.put(SCHEMA_RAW_MESSAGE, envelope.getPayload());
        tr.put(SCHEMA_KAFKA_TOPIC, topicConfig.getTopicName());
        tr.put(SCHEMA_VERSION, System.currentTimeMillis());
        processContext.output(tr);
    }
}

