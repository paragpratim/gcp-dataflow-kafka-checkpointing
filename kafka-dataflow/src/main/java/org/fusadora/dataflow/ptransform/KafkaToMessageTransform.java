package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.fusadora.dataflow.dofn.KafkaRecordToEnvelopeDoFn;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.services.InputService;
import org.jetbrains.annotations.NotNull;

/**
 * org.fusadora.dataflow.ptransform.KafkaToMessageTransform
 * This is a Beam PTransform that reads from Kafka and converts records to {@link KafkaEventEnvelope}.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class KafkaToMessageTransform extends PTransform<@NotNull PBegin, @NotNull PCollection<KafkaEventEnvelope>> {

    private final InputService inputService;
    private final String topic;
    private final String brokerHost;

    public KafkaToMessageTransform(InputService inputService, String topic, String brokerHost) {
        this.inputService = inputService;
        this.topic = topic;
        this.brokerHost = brokerHost;
    }

    @Override
    public @NotNull PCollection<KafkaEventEnvelope> expand(PBegin input) {
        PCollection<KafkaRecord<String, String>> kafkaRecordPCollection = inputService.readFromKafka(
                input.getPipeline(), brokerHost, topic, "Get from Kafka [" + topic + "]");
        return kafkaRecordPCollection.apply("Convert to Envelope", ParDo.of(new KafkaRecordToEnvelopeDoFn(topic)));
    }
}
