package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.fusadora.dataflow.services.InputService;
import org.fusadora.dataflow.utilities.PropertyUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * org.fusadora.dataflow.ptransform.KafkaToMessageTransform
 * Convert Kafka message to String message
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class KafkaToMessageTransform extends PTransform<@NotNull PBegin, @NotNull PCollection<String>> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaToMessageTransform.class);
    private final InputService inputService;
    private final String topic;

    public KafkaToMessageTransform(InputService inputService, String topic) {
        this.inputService = inputService;
        this.topic = topic;
    }

    @Override
    public @NotNull PCollection<String> expand(PBegin input) {
        PCollection<KafkaRecord<String, String>> kafkaRecordPCollection = inputService.readFromKafka(input.getPipeline(),
                PropertyUtils.getProperty(PropertyUtils.KAFKA_NODE_HOST_1),
                topic, "Get from Kafka [" + topic + "]");

        return kafkaRecordPCollection.apply("Convert to String", ParDo.of(new DoFn<KafkaRecord<String, String>, String>() {

            @DoFn.ProcessElement
            public void processElement(ProcessContext processContext) {
                LOG.info("Element received in KafkaToMessageTransform DoFn processElement{}", Objects.requireNonNull(processContext.element()).getKV().getValue());
                processContext.output(Objects.requireNonNull(processContext.element()).getKV().getValue());
            }
        }));
    }
}
