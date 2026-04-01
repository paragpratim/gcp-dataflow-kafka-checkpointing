package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.fusadora.dataflow.dofn.KafkaRecordToEnvelopeDoFn;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.services.InputService;
import org.fusadora.dataflow.utilities.PropertyUtils;
import org.jetbrains.annotations.NotNull;

/**
 * org.fusadora.dataflow.ptransform.KafkaToMessageTransform
 * Convert Kafka message to String message
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class KafkaToMessageTransform extends PTransform<@NotNull PBegin, @NotNull PCollection<KafkaEventEnvelope>> {

    private final InputService inputService;
    private final String topic;

    public KafkaToMessageTransform(InputService inputService, String topic) {
        this.inputService = inputService;
        this.topic = topic;
    }

    @Override
    public @NotNull PCollection<KafkaEventEnvelope> expand(PBegin input) {
        PCollection<KafkaRecord<String, String>> kafkaRecordPCollection = inputService.readFromKafka(input.getPipeline(),
                PropertyUtils.getProperty(PropertyUtils.KAFKA_BROKER_HOST),
                topic, "Get from Kafka [" + topic + "]");

        return kafkaRecordPCollection.apply("Convert to Envelope", ParDo.of(new KafkaRecordToEnvelopeDoFn(topic)));
    }
}
