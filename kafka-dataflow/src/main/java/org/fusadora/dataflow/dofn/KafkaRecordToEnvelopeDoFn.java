package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;

import java.util.Objects;

/**
 * org.fusadora.dataflow.dofn.KafkaRecordToEnvelopeDoFn
 * This is a Beam DoFn that transforms a KafkaRecord<String, String> into a KafkaEventEnvelope.
 * It extracts the topic, partition, offset, and value from the KafkaRecord and constructs a KafkaEventEnvelope with this information.
 * This DoFn is used in the data processing pipeline to convert raw Kafka records into a more structured format for downstream processing.
 *
 * @author Parag Ghosh
 * @since 10/04/2026
 */
@SuppressWarnings("unused") // Instantiated from pipeline transform wiring
public class KafkaRecordToEnvelopeDoFn extends DoFn<KafkaRecord<String, String>, KafkaEventEnvelope> {

    private final String topic;

    public KafkaRecordToEnvelopeDoFn(String topic) {
        this.topic = topic;
    }

    @SuppressWarnings("unused") // Invoked by Beam runtime via @ProcessElement
    @ProcessElement
    public void processElement(ProcessContext processContext) {
        KafkaRecord<String, String> kafkaRecord = Objects.requireNonNull(processContext.element());
        processContext.output(new KafkaEventEnvelope(
                topic,
                kafkaRecord.getPartition(),
                kafkaRecord.getOffset(),
                kafkaRecord.getKV().getValue()
        ));
    }
}

