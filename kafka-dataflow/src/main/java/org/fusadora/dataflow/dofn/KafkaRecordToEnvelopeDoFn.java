package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;

import java.util.Objects;

/**
 * Converts KafkaRecord into KafkaEventEnvelope.
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

